"""OracleEventStore com driver falso: ordem dos comandos, erros por linha, barreira e rollback."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import oracledb
import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, frame
from tests.fakes.memory import EXCHANGES
from tests.fakes.oracle_driver import (
    FakeCursor,
    FakeOracle,
    db_error,
    ora,
    unique_violation,
)

from ohip_streaming.adapters.oracle import event_store as sql
from ohip_streaming.adapters.oracle.database import FENCE_SQL, DedicatedSession
from ohip_streaming.adapters.oracle.event_store import OracleEventStore
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


@pytest.fixture
def oracle() -> FakeOracle:
    return FakeOracle()


@pytest.fixture
def store(oracle: FakeOracle) -> Iterator[OracleEventStore]:
    executor = ThreadPoolExecutor(1)
    session = DedicatedSession(oracle, call_timeout_ms=5_000, executor=executor)
    yield OracleEventStore(session, exchange_names=EXCHANGES, code_version="abc123")
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


def record(offset: int, *, ignored: bool = False) -> NewEventRecord:
    ev = event(offset)
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


def batch_errors(*errors: Any) -> Any:
    def handler(cursor: FakeCursor, _rows: Any) -> None:
        cursor.batch_errors = list(errors)

    return handler


async def test_happy_path_order_and_values(store: OracleEventStore, oracle: FakeOracle) -> None:
    result = await store.persist_batch(batch(record(1), record(2, ignored=True), record(3)))

    assert result.inserted == ("uid-1", "uid-2", "uid-3")
    assert oracle.statements() == [
        sql.INSERT_RAW_SQL,
        sql.INSERT_OUTBOX_SQL,
        sql.UPDATE_OFFSET_SQL,
        FENCE_SQL,  # sempre o último comando
    ]
    assert oracle.kinds()[-1] == "commit"
    raw_rows = oracle.last(sql.INSERT_RAW_SQL).parameters
    assert raw_rows[0]["received_at"] == RECEIVED.replace(tzinfo=None)  # UTC sem fuso
    assert raw_rows[1]["processing_status"] == "IGNORED"
    assert set(oracle.last(sql.INSERT_RAW_SQL).input_sizes) == {
        "payload",
        "event_ts",
        "received_at",
    }
    outbox = oracle.last(sql.INSERT_OUTBOX_SQL).parameters
    assert [r["unique_event_id"] for r in outbox] == ["uid-1", "uid-3"]  # IGNORED sem outbox
    assert outbox[0]["exchange_name"] == "ohip.events"
    assert oracle.last(sql.UPDATE_OFFSET_SQL).parameters == {
        "last_offset": "3",
        "last_unique_event_id": "uid-3",
        "chain_code": CHAIN,
    }
    assert oracle.last(FENCE_SQL).parameters == {"lease_name": "consumer:CHAIN1", "epoch": 7}


async def test_row_errors_are_duplicates_or_dlq(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    oracle.on(
        sql.INSERT_RAW_SQL,
        batch_errors(
            unique_violation("OHIP_EVENT_RAW_UQ_EVT", offset=1),
            unique_violation("OHIP_EVENT_RAW_UQ_OFF", offset=2),
            ora(12899, 'value too large for column "MODULE_NAME"\nhttps://help', offset=3),
        ),
    )
    result = await store.persist_batch(batch(*(record(n) for n in range(1, 6))))

    assert result.inserted == ("uid-1", "uid-5")
    assert result.duplicates == ("uid-2", "uid-3")
    assert [f.unique_event_id for f in result.row_failures] == ["uid-4"]
    outbox = oracle.last(sql.INSERT_OUTBOX_SQL).parameters
    assert [r["unique_event_id"] for r in outbox] == ["uid-1", "uid-5"]  # ordem de chegada
    (dlq,) = oracle.last(sql.INSERT_DLQ_CONSUME_SQL).parameters
    assert dlq["unique_event_id"] == "uid-4"
    assert dlq["error_class"] == "ORA-12899"
    assert dlq["error_message"] == 'ORA-12899: value too large for column "MODULE_NAME"'
    assert dlq["offset_value"] == "4"
    assert '"uniqueEventId":"uid-4"' in dlq["raw_message"]
    assert dlq["code_version"] == "abc123"
    assert oracle.last(sql.UPDATE_OFFSET_SQL).parameters["last_offset"] == "5"


async def test_pk_violation_is_a_row_failure(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.on(sql.INSERT_RAW_SQL, batch_errors(unique_violation("OHIP_EVENT_RAW_PK", offset=0)))
    result = await store.persist_batch(batch(record(1)))
    assert result.duplicates == ()
    assert [f.error_class for f in result.row_failures] == ["ORA-00001"]


async def test_rejected_messages_go_to_dlq(store: OracleEventStore, oracle: FakeOracle) -> None:
    rejected = ConsumeDlqRecord("{lixo", "JSON inválido " + "é" * 3000, Offset("9"), None)
    await store.persist_batch(batch(rejected=(rejected,), offset=Offset("9")))

    assert oracle.statements() == [sql.INSERT_DLQ_CONSUME_SQL, sql.UPDATE_OFFSET_SQL, FENCE_SQL]
    (row,) = oracle.last(sql.INSERT_DLQ_CONSUME_SQL).parameters
    assert row["error_class"] == "REJECTED"
    assert len(row["error_message"].encode()) <= 4000  # VARCHAR2(4000) conta bytes
    assert row["offset_value"] == "9"


@pytest.mark.parametrize("code", [1653, 1654, 1688, 1691, 30036, 1536])
async def test_space_error_on_a_row_is_systemic(
    store: OracleEventStore, oracle: FakeOracle, code: int
) -> None:
    oracle.on(sql.INSERT_RAW_SQL, batch_errors(ora(code, "unable to extend", offset=0)))
    with pytest.raises(StoreUnavailableError, match=f"ORA-{code:05d}"):
        await store.persist_batch(batch(record(1), record(2)))
    assert "commit" not in oracle.kinds()
    assert oracle.kinds()[-2:] == ["rollback", "drop"]  # conexão descartada


async def test_whole_batch_failure_is_batch_failed(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    oracle.fail(sql.INSERT_OUTBOX_SQL, db_error(ora(600, "internal error")))
    with pytest.raises(BatchFailedError, match="ORA-00600"):
        await store.persist_batch(batch(record(1)))
    assert oracle.kinds()[-1] == "rollback"
    assert "drop" not in oracle.kinds()  # o banco respondeu: a conexão continua


async def test_lease_lost_rolls_back(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.rowcount(FENCE_SQL, 0)
    with pytest.raises(LeaseLostError):
        await store.persist_batch(batch(record(1)))
    assert oracle.kinds()[-1] == "rollback"
    assert "commit" not in oracle.kinds()


async def test_unprovisioned_chain(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.rowcount(sql.UPDATE_OFFSET_SQL, 0)
    with pytest.raises(UnknownChainError):
        await store.persist_batch(batch(record(1)))
    assert FENCE_SQL not in oracle.statements()


async def test_commit_lost_connection_is_unavailable_and_reconnects(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    oracle.fail_commit = [db_error(ora(3113, "end-of-file on communication channel"))]
    with pytest.raises(StoreUnavailableError):
        await store.persist_batch(batch(record(1)))
    await store.persist_batch(batch(record(2)))
    assert [c.parameters for c in oracle.calls if c.kind == "acquire"] == [1, 2]


async def test_rollback_failure_is_swallowed(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.rowcount(FENCE_SQL, 0)
    oracle.fail_rollback = [db_error(ora(3113))]
    with pytest.raises(LeaseLostError):
        await store.persist_batch(batch(record(1)))


async def test_retry_of_dlq_item_resolves_or_records_failure(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    await store.persist_batch(batch(record(1), offset=None, retry_of_dlq_id=55))
    assert oracle.last(sql.RESOLVE_RETRIED_SQL).parameters == {"id": 55, "resolved_by": "consumer"}
    assert sql.INSERT_DLQ_CONSUME_SQL not in oracle.statements()
    assert sql.UPDATE_OFFSET_SQL not in oracle.statements()  # retry não mexe no offset

    oracle.calls.clear()
    oracle.on(sql.INSERT_RAW_SQL, batch_errors(ora(12899, "too large", offset=0)))
    await store.persist_batch(batch(record(2), offset=None, retry_of_dlq_id=56))
    params = oracle.last(sql.RETRY_FAILED_SQL).parameters
    assert params["id"] == 56
    assert params["error_class"] == "ORA-12899"
    assert sql.INSERT_DLQ_CONSUME_SQL not in oracle.statements()  # sem item novo
    assert oracle.statements()[-1] == FENCE_SQL


async def test_retry_of_existing_event_resolves(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    oracle.on(sql.INSERT_RAW_SQL, batch_errors(unique_violation("OHIP_EVENT_RAW_UQ_EVT")))
    oracle.rowcount(sql.RESOLVE_RETRIED_SQL, 0)  # já resolvido por outro caminho: só loga
    result = await store.persist_batch(batch(record(1), offset=None, retry_of_dlq_id=9))
    assert result.duplicates == ("uid-1",)
    assert oracle.kinds()[-1] == "commit"


def test_missing_exchange_name_fails_at_composition(oracle: FakeOracle) -> None:
    session = DedicatedSession(oracle, call_timeout_ms=1)
    with pytest.raises(ValueError, match="reprocess"):
        OracleEventStore(
            session, exchange_names={ExchangeKind.EVENTS: "ohip.events"}, code_version="x"
        )


async def test_reserve_event_ids(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.rows(sql.RESERVE_IDS_SQL, [(12,), (10,), (11,)])
    assert await store.reserve_event_ids(3) == [10, 11, 12]
    assert oracle.last(sql.RESERVE_IDS_SQL).parameters == {"count": 3}
    assert await store.reserve_event_ids(0) == []


async def test_reserve_ids_unavailable(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.fail(sql.RESERVE_IDS_SQL, db_error(ora(12541, "no listener")))
    with pytest.raises(StoreUnavailableError):
        await store.reserve_event_ids(1)


async def test_pending_consume_retries(store: OracleEventStore, oracle: FakeOracle) -> None:
    oracle.rows(
        sql.PENDING_RETRIES_SQL,
        [(5, '{"x":1}', RECEIVED.replace(tzinfo=None)), (6, None, RECEIVED.replace(tzinfo=None))],
    )
    items = await store.pending_consume_retries(CHAIN, 20)
    assert [(i.dlq_id, i.raw_message) for i in items] == [(5, '{"x":1}'), (6, "")]
    assert items[0].created_at.tzinfo is UTC


async def test_mark_consume_retry_failed_fences_with_the_item_chain(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    def returning(cursor: FakeCursor, params: Any) -> None:
        cursor.rowcount = 1
        params["chain_code"].values = ["CHAIN9"]

    oracle.on(sql.RETRY_FAILED_RETURNING_SQL, returning)
    await store.mark_consume_retry_failed(5, "ainda inválido", 3)
    assert oracle.last(FENCE_SQL).parameters == {"lease_name": "consumer:CHAIN9", "epoch": 3}
    assert oracle.kinds()[-1] == "commit"


async def test_mark_consume_retry_failed_on_resolved_item(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    oracle.rowcount(sql.RETRY_FAILED_RETURNING_SQL, 0)
    await store.mark_consume_retry_failed(5, "x", 3)
    assert FENCE_SQL not in oracle.statements()


async def test_interface_error_without_detail_is_unavailable(
    store: OracleEventStore, oracle: FakeOracle
) -> None:
    oracle.fail(sql.INSERT_RAW_SQL, oracledb.InterfaceError("conexão fechada"))
    with pytest.raises(StoreUnavailableError, match="InterfaceError"):
        await store.persist_batch(batch(record(1)))
