"""PostgresEventStore com driver falso: os dois caminhos do ADR-0022, ordem dos comandos e
classificação. O banco real prova o resto em ``tests/stores/test_postgres_only.py``."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg import errors as pg_errors
from tests.fakes.frames import SUBSCRIPTION_ID, frame
from tests.fakes.memory import EXCHANGES
from tests.fakes.postgres_driver import FakeConnection, FakePool, Result, unique_violation

from ohip_streaming.adapters.postgres import event_store as sql
from ohip_streaming.adapters.postgres.database import FENCE_SQL, DedicatedSession
from ohip_streaming.adapters.postgres.event_store import PostgresEventStore, encodes_cleanly
from ohip_streaming.application.errors import (
    BatchFailedError,
    LeaseLostError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    BatchToPersist,
    ConsumeDlqRecord,
    NewEventRecord,
    ProcessingStatus,
)
from ohip_streaming.domain.events import Event, parse_next_frame
from ohip_streaming.domain.masking import MaskingPolicy
from ohip_streaming.domain.messages import ExchangeKind, build_queue_message
from ohip_streaming.domain.offset import Offset

CHAIN = "CHAIN1"
RECEIVED = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
Behavior = Callable[[dict[str, Any]], Any]


class Db:
    """Roteiro por comando: ``raw`` decide cada INSERT do bruto; o resto afeta 1 linha."""

    def __init__(self) -> None:
        self.raw: Behavior = lambda params: [(params["unique_event_id"],)]
        self.other: dict[str, Any] = {}
        self.connection = FakeConnection(self.script)

    def script(self, statement: str, params: Any) -> Any:
        if statement == sql.INSERT_RAW_SQL:
            return self.raw(params)
        return self.other.get(statement, Result(rowcount=1))

    def statements(self) -> list[str]:
        return [entry[1] for entry in self.connection.log if entry[0] == "execute"]

    def flow(self) -> list[str]:
        """Comandos e marcos de transação, com o SQL abreviado."""
        names = {sql.INSERT_RAW_SQL: "raw", sql.INSERT_OUTBOX_SQL: "outbox",
                 sql.INSERT_DLQ_CONSUME_SQL: "dlq", sql.UPDATE_OFFSET_SQL: "offset",
                 FENCE_SQL: "fence", sql.RESOLVE_RETRIED_SQL: "resolve",
                 sql.RETRY_FAILED_SQL: "retry_failed"}  # fmt: skip
        return [
            names.get(entry[1], entry[1]) if entry[0] == "execute" else entry[0]
            for entry in self.connection.log
        ]


@pytest.fixture
def db() -> Db:
    return Db()


@pytest.fixture
def store(db: Db) -> Iterator[PostgresEventStore]:
    executor = ThreadPoolExecutor(1)
    session = DedicatedSession(FakePool([db.connection]), executor=executor)
    yield PostgresEventStore(session, exchange_names=EXCHANGES, code_version="abc123")
    executor.shutdown()


def event(offset: int, **kwargs: Any) -> Event:
    parsed = parse_next_frame(
        frame(str(offset), **kwargs),
        chain_code=CHAIN,
        subscription_id=SUBSCRIPTION_ID,
        received_at=RECEIVED,
        event_tz=ZoneInfo("UTC"),
    )
    assert isinstance(parsed, Event)
    return parsed


def record(offset: int, *, ignored: bool = False, ev: Event | None = None) -> NewEventRecord:
    ev = ev or event(offset)
    if ignored:
        return NewEventRecord(offset, ev, ProcessingStatus.IGNORED, None)
    message, _ = build_queue_message(
        ev, raw_event_id=offset, masking=MaskingPolicy(), module_codes={}
    )
    return NewEventRecord(offset, ev, ProcessingStatus.RECEIVED, message)


def batch(*records: NewEventRecord, **kwargs: Any) -> BatchToPersist:
    defaults: dict[str, Any] = {
        "chain_code": CHAIN,
        "lease_epoch": 7,
        "events": records,
        "rejected": (),
        "offset": Offset(str(records[-1].raw_event_id)) if records else None,
        "last_unique_event_id": records[-1].event.unique_event_id if records else None,
    }
    defaults.update(kwargs)
    return BatchToPersist(**defaults)


def by_uid(**outcomes: Any) -> Behavior:
    """``uid-N`` → resultado do INSERT: erro, ``[]`` (duplicado uq_evt) ou o padrão."""

    def behave(params: dict[str, Any]) -> Any:
        return outcomes.get(
            params["unique_event_id"].replace("-", "_"), [(params["unique_event_id"],)]
        )

    return behave


# ------------------------------------------------------------------ caminho rápido


async def test_fast_path_order_and_values(store: PostgresEventStore, db: Db) -> None:
    result = await store.persist_batch(batch(record(1), record(2, ignored=True), record(3)))

    assert result.inserted == ("uid-1", "uid-2", "uid-3")
    assert db.flow() == [
        "BEGIN", "SAVEPOINT", "raw", "raw", "raw", "RELEASE",
        "outbox", "outbox",  # IGNORED não gera outbox
        "offset", "fence", "COMMIT",  # barreira sempre o último comando
    ]  # fmt: skip
    raw = [entry[2] for entry in db.connection.log if entry[1] == sql.INSERT_RAW_SQL]
    assert raw[0]["id"] == 1
    assert raw[0]["received_at"] == RECEIVED
    assert raw[1]["processing_status"] == "IGNORED"
    fence = db.connection.log[-2]
    assert fence[2] == {"lease_name": "consumer:CHAIN1", "epoch": 7}


async def test_fast_path_without_return_is_a_uq_evt_duplicate(
    store: PostgresEventStore, db: Db
) -> None:
    db.raw = by_uid(uid_2=[])
    result = await store.persist_batch(batch(record(1), record(2), record(3)))
    assert (result.inserted, result.duplicates) == (("uid-1", "uid-3"), ("uid-2",))
    assert db.flow().count("outbox") == 2


async def test_fast_path_unavailable_undoes_the_batch(store: PostgresEventStore, db: Db) -> None:
    db.raw = lambda params: pg_errors.DiskFull("disco")
    with pytest.raises(StoreUnavailableError, match="53100"):
        await store.persist_batch(batch(record(1)))
    assert "ROLLBACK" in db.flow()
    assert "COMMIT" not in db.flow()
    assert db.connection.closed  # indisponível: a conexão de escrita é descartada


async def test_fast_path_other_errors_fail_the_batch(store: PostgresEventStore, db: Db) -> None:
    db.raw = lambda params: pg_errors.UndefinedColumn("bug")
    with pytest.raises(BatchFailedError, match="42703"):
        await store.persist_batch(batch(record(1)))


# ------------------------------------------------------------------ caminho lento


async def test_data_error_falls_back_to_one_by_one(store: PostgresEventStore, db: Db) -> None:
    failing = {"uid-2"}

    def raw(params: dict[str, Any]) -> Any:
        if params["unique_event_id"] in failing:
            return pg_errors.StringDataRightTruncation("value too long")
        if params["unique_event_id"] == "uid-3":
            return unique_violation("ohip_event_raw_uq_off")
        if params["unique_event_id"] == "uid-4":
            return []  # uq_evt no caminho lento
        return [(params["unique_event_id"],)]

    db.raw = raw
    result = await store.persist_batch(batch(record(1), record(2), record(3), record(4)))

    assert result.inserted == ("uid-1",)
    assert result.duplicates == ("uid-3", "uid-4")
    (failure,) = result.row_failures
    assert (failure.unique_event_id, failure.error_class) == ("uid-2", "22001")
    flow = db.flow()
    assert flow[:5] == ["BEGIN", "SAVEPOINT", "raw", "raw", "ROLLBACK TO"]  # rápido desfeito
    assert flow[5:9] == ["SAVEPOINT", "raw", "RELEASE", "SAVEPOINT"]  # linha a linha
    assert flow[-5:] == ["outbox", "dlq", "offset", "fence", "COMMIT"]
    dlq = next(entry[2] for entry in db.connection.log if entry[1] == sql.INSERT_DLQ_CONSUME_SQL)
    assert (dlq["unique_event_id"], dlq["error_class"]) == ("uid-2", "22001")


async def test_unavailable_in_the_slow_path_undoes_the_whole_batch(
    store: PostgresEventStore, db: Db
) -> None:
    """Teste 3 do ADR-0022: classe 53 numa linha do caminho lento → nada na DLQ, nada gravado."""
    calls = {"n": 0}

    def raw(params: dict[str, Any]) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            return pg_errors.StringDataRightTruncation("x")  # força o caminho lento
        if params["unique_event_id"] == "uid-2":
            return pg_errors.DiskFull("disco")
        return [(params["unique_event_id"],)]

    db.raw = raw
    with pytest.raises(StoreUnavailableError, match="53100"):
        await store.persist_batch(batch(record(1), record(2), record(3)))
    flow = db.flow()
    assert not {"dlq", "offset", "COMMIT"} & set(flow)
    assert flow[-1] == "ROLLBACK"


async def test_systemic_error_in_the_slow_path_fails_the_batch(
    store: PostgresEventStore, db: Db
) -> None:
    """Só erro de dado vai para a DLQ; um erro sistêmico na linha (ex.: 42xxx) falha o lote,
    que segue para a bisseção e o disjuntor (ADR-0009, ADR-0011) em vez de lotar a DLQ."""
    calls = {"n": 0}

    def raw(params: dict[str, Any]) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            return pg_errors.StringDataRightTruncation("x")  # força o caminho lento
        return pg_errors.InsufficientPrivilege("permissão")  # 42501

    db.raw = raw
    with pytest.raises(BatchFailedError, match="42501"):
        await store.persist_batch(batch(record(1), record(2)))
    assert "dlq" not in db.flow()


async def test_repeated_primary_key_is_a_row_failure(store: PostgresEventStore, db: Db) -> None:
    db.raw = by_uid(uid_2=unique_violation("ohip_event_raw_pk"))
    result = await store.persist_batch(batch(record(1), record(2)))
    assert result.duplicates == ()
    assert [(f.unique_event_id, f.error_class) for f in result.row_failures] == [("uid-2", "23505")]


async def test_unencodable_rows_skip_the_fast_path(store: PostgresEventStore, db: Db) -> None:
    surrogate = replace(event(2), primary_key="pk\ud800")
    db.raw = lambda params: (
        UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogates")
        if params["unique_event_id"] == "uid-2"
        else [(params["unique_event_id"],)]
    )
    result = await store.persist_batch(batch(record(1), record(2, ev=surrogate)))
    assert result.inserted == ("uid-1",)
    assert [f.error_class for f in result.row_failures] == ["UnicodeEncodeError"]
    assert db.flow()[:3] == ["BEGIN", "SAVEPOINT", "raw"]  # direto linha a linha
    assert db.flow().count("ROLLBACK TO") == 1


def test_encodes_cleanly() -> None:
    assert encodes_cleanly({"a": "ok", "b": 1, "c": None})
    assert not encodes_cleanly({"a": "x\x00"})
    assert not encodes_cleanly({"a": "x\ud800"})


# ------------------------------------------------------------------ resto do lote


async def test_rejected_and_dlq_text_are_cleaned(store: PostgresEventStore, db: Db) -> None:
    rejected = ConsumeDlqRecord(
        unique_event_id="bad\x00", offset=Offset("9"), raw_message="lixo\x00\ud800", reason="?"
    )
    await store.persist_batch(batch(rejected=(rejected,), offset=None))
    (row,) = [entry[2] for entry in db.connection.log if entry[1] == sql.INSERT_DLQ_CONSUME_SQL]
    assert row["raw_message"] == "lixo��"
    assert row["unique_event_id"] == "bad�"
    assert row["error_class"] == sql.REJECTED_ERROR_CLASS
    assert "offset" not in db.flow()  # sem offset no lote


async def test_unknown_chain_and_lost_lease(store: PostgresEventStore, db: Db) -> None:
    db.other[sql.UPDATE_OFFSET_SQL] = Result(rowcount=0)
    with pytest.raises(UnknownChainError):
        await store.persist_batch(batch(record(1)))
    db.other[sql.UPDATE_OFFSET_SQL] = Result(rowcount=1)
    db.other[FENCE_SQL] = Result(rowcount=0)
    with pytest.raises(LeaseLostError):
        await store.persist_batch(batch(record(1)))


async def test_retry_settlement(store: PostgresEventStore, db: Db) -> None:
    await store.persist_batch(batch(record(1), retry_of_dlq_id=5))
    assert "resolve" in db.flow()
    assert "dlq" not in db.flow()

    db.connection.log.clear()
    db.raw = lambda params: pg_errors.StringDataRightTruncation("x")
    db.other[sql.RETRY_FAILED_SQL] = Result(rowcount=0)  # item já resolvido: só loga
    await store.persist_batch(batch(record(1), retry_of_dlq_id=5))
    assert "retry_failed" in db.flow()
    assert "dlq" not in db.flow()


async def test_reserve_ids_and_pending_retries(store: PostgresEventStore, db: Db) -> None:
    db.other[sql.RESERVE_IDS_SQL] = [(12,), (11,)]
    assert await store.reserve_event_ids(2) == [11, 12]
    assert await store.reserve_event_ids(0) == []
    db.other[sql.PENDING_RETRIES_SQL] = [(3, None, RECEIVED)]
    (item,) = await store.pending_consume_retries(CHAIN, 10)
    assert (item.dlq_id, item.raw_message, item.created_at) == (3, "", RECEIVED)


async def test_mark_consume_retry_failed(store: PostgresEventStore, db: Db) -> None:
    db.other[sql.RETRY_FAILED_RETURNING_SQL] = [("CHAIN1",)]
    await store.mark_consume_retry_failed(3, "recusado", 7)
    assert db.flow()[-2:] == ["fence", "COMMIT"]

    db.connection.log.clear()
    db.other[sql.RETRY_FAILED_RETURNING_SQL] = []
    await store.mark_consume_retry_failed(3, "recusado", 7)
    assert "fence" not in db.flow()


def test_composition_requires_every_exchange() -> None:
    with pytest.raises(ValueError, match="reprocess"):
        PostgresEventStore(
            object(),  # type: ignore[arg-type]
            exchange_names={ExchangeKind.EVENTS: "ohip.events"},
            code_version="x",
        )


def test_psycopg_is_the_driver() -> None:
    assert psycopg.__version__ >= "3.2"
