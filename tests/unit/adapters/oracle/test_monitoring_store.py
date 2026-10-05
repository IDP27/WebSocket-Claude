"""Consultas da API no Oracle (``OracleMonitoringStore``) com driver falso."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import oracledb
import pytest
from tests.fakes.frames import new_event
from tests.fakes.oracle_driver import FakeOracle, db_error, ora

from ohip_streaming.adapters.oracle import monitoring_store as sql
from ohip_streaming.adapters.oracle.database import InlineSession, PooledSession
from ohip_streaming.adapters.oracle.monitoring_store import OracleMonitoringStore
from ohip_streaming.application.errors import StoreOperationError, StoreUnavailableError
from ohip_streaming.application.ports import (
    DlqQuery,
    DlqStage,
    EventFilter,
    OutboxQuery,
    PageRequest,
    ProcessingStatus,
    ReplayQuery,
    ReplayStatus,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.offset import Offset

AWARE = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
NAIVE = AWARE.replace(tzinfo=None)


@pytest.fixture
def oracle() -> FakeOracle:
    return FakeOracle()


@pytest.fixture
def store(oracle: FakeOracle) -> OracleMonitoringStore:
    return OracleMonitoringStore(InlineSession(PooledSession(oracle, call_timeout_ms=5_000)))


def event_row(raw_id: int, offset: str = "100") -> tuple[Any, ...]:
    return (raw_id, f"uid-{raw_id}", "C1", "H1", offset, "RESERVATION", "UPDATE RESERVATION",
            "123", NAIVE, NAIVE, NAIVE, "RECEIVED")  # fmt: skip


def outbox_row(row_id: int) -> tuple[Any, ...]:
    return (row_id, 7, "C1", "uid-7", "ohip.events", "ohip.rsv.X", "PENDING", 0, NAIVE, None,
            NAIVE, None)  # fmt: skip


def dlq_row(item_id: int) -> tuple[Any, ...]:
    return (item_id, "PUBLISH", "C1", 7, 3, "uid-7", "100", "NACK", "recusada", "v1", 2, NAIVE,
            None, None, NAIVE, "ana", "RETRIED")  # fmt: skip


def replay_row(request_id: int, status: str = "PENDING") -> tuple[Any, ...]:
    return (request_id, "C1", "90", "lacuna", "ana", status, None, NAIVE, None, None, None)


# ------------------------------------------------------------------ status


async def test_status_joins_counts_and_lag(
    store: OracleMonitoringStore, oracle: FakeOracle
) -> None:
    oracle.rows(sql.DB_NOW_SQL, [(NAIVE,)])
    oracle.rows(
        sql.STATUS_SQL,
        [
            (
                "C1",
                "SUBSCRIBED",
                "vm1:1:a",
                "sub-1",
                "97863",
                NAIVE,
                14,
                0,
                None,
                1000,
                "token",
                NAIVE,
                NAIVE,
            ),
            ("C2", "STOPPED", None, None, None, None, None, None, None, None, None, None, None),
        ],
    )
    oracle.rows(
        sql.OUTBOX_COUNTS_SQL,
        [("C1", "PENDING", 4, NAIVE - timedelta(seconds=30)), ("C1", "FAILED", 1, NAIVE)],
    )
    oracle.rows(sql.DLQ_OPEN_SQL, [("C1", "ENRICH", 3)])
    oracle.rows(sql.RECENT_EVENTS_SQL, [("C1", 185, 1.4)])

    snapshot = await store.status()

    assert snapshot.generated_at == AWARE
    c1, c2 = snapshot.chains
    assert (c1.state, c1.last_offset, c1.reconnects_total) == (
        ConsumerState.SUBSCRIBED,
        Offset("97863"),
        14,
    )
    assert (c1.events_last_5m, c1.lag_seconds_p95_5m) == (185, 1.4)
    assert (c1.outbox.pending, c1.outbox.failed) == (4, 1)
    assert c1.outbox.oldest_pending_at == AWARE - timedelta(seconds=30)
    assert c1.dlq_open == {
        DlqStage.CONSUME: 0,
        DlqStage.PUBLISH: 0,
        DlqStage.NORMALIZE: 0,
        DlqStage.ENRICH: 3,
    }
    assert (c1.last_close_code, c1.last_disconnect_at) == (1000, AWARE)
    assert (c2.last_offset, c2.reconnects_total, c2.events_last_5m) == (None, 0, 0)
    assert c2.lag_seconds_p95_5m is None
    # uma conexão do pool para as cinco leituras
    assert oracle.kinds().count("acquire") == 1


async def test_status_without_chains(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    oracle.rows(sql.DB_NOW_SQL, [(NAIVE,)])
    snapshot = await store.status()
    assert snapshot.chains == ()


def test_status_sql_uses_only_11g_features() -> None:
    for statement in (sql.STATUS_SQL, sql.RECENT_EVENTS_SQL, sql.OUTBOX_COUNTS_SQL):
        assert "FETCH FIRST" not in statement
        assert "JSON" not in statement.upper()


# ------------------------------------------------------------------ eventos


async def test_events_filters_are_bound(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    query = EventFilter(
        chain_code="C1",
        hotel_id="H1",
        event_name="UPDATE RESERVATION",
        module_name="reservation",
        primary_key="123",
        received_from=AWARE,
        received_to=AWARE + timedelta(hours=1),
        processing_status=ProcessingStatus.RECEIVED,
    )
    page = PageRequest(limit=2, cursor=50)
    conditions, _, _ = sql.event_conditions(query, page)
    statement = sql.page_sql(sql.EVENT_COLUMNS, "ohip_event_raw", conditions)
    oracle.rows(statement, [event_row(49), event_row(48), event_row(47)])

    result = await store.events(query, page)

    assert [e.raw_event_id for e in result.items] == [49, 48]
    assert result.next_cursor == 48  # leu limit + 1: há próxima página
    call = oracle.last(statement)
    assert call.parameters == {
        "chain_code": "C1",
        "hotel_id": "H1",
        "event_name": "UPDATE RESERVATION",
        "primary_key": "123",
        "module_name": "RESERVATION",
        "received_from": NAIVE,
        "received_to": NAIVE + timedelta(hours=1),
        "processing_status": "RECEIVED",
        "cursor": 50,
        "fetch": 3,
    }
    assert call.input_sizes == {
        "received_from": oracledb.DB_TYPE_TIMESTAMP,
        "received_to": oracledb.DB_TYPE_TIMESTAMP,
    }
    assert "ROWNUM <= :fetch" in statement
    assert "ORDER BY id DESC" in statement


async def test_events_last_page(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    statement = sql.page_sql(sql.EVENT_COLUMNS, "ohip_event_raw", [])
    oracle.rows(statement, [event_row(2)])
    result = await store.events(EventFilter(), PageRequest(limit=50))
    assert result.next_cursor is None
    assert result.items[0].event_ts == AWARE
    assert oracle.last(statement).parameters == {"fetch": 51}


async def test_event_detail(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    payload = json.dumps(new_event("100", "uid-7"))
    oracle.rows(
        sql.EVENT_DETAIL_SQL,
        [(7, "C1", "sub-1", NAIVE, payload, "NORMALIZED", NAIVE, NAIVE, None)],
    )
    oracle.rows(sql.EVENT_OUTBOX_SQL, [outbox_row(3)])
    oracle.rows(sql.EVENT_DLQ_SQL, [dlq_row(9)])

    record = await store.event("uid-7")

    assert record is not None
    assert record.stored.raw_event_id == 7
    assert record.stored.status is ProcessingStatus.NORMALIZED
    assert record.stored.event.unique_event_id == "uid-7"
    assert record.persisted_at == AWARE
    assert [o.id for o in record.outbox] == [3]
    assert [d.id for d in record.dlq] == [9]
    assert record.dlq[0].resolution == "RETRIED"
    assert oracle.last(sql.EVENT_OUTBOX_SQL).parameters == {"raw_id": 7}


async def test_event_not_found(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    assert await store.event("nao-existe") is None
    assert oracle.count(sql.EVENT_OUTBOX_SQL) == 0


# ------------------------------------------------------------------ outbox, DLQ e replay


async def test_outbox_page(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    query = OutboxQuery(status="FAILED", chain_code="C1")
    page = PageRequest(limit=1)
    conditions, _ = sql.outbox_conditions(query, page)
    statement = sql.page_sql(sql.OUTBOX_COLUMNS, "ohip_outbox", conditions)
    oracle.rows(statement, [outbox_row(5), outbox_row(4)])

    result = await store.outbox(query, page)

    assert ([r.id for r in result.items], result.next_cursor) == ([5], 5)
    assert oracle.last(statement).parameters == {"status": "FAILED", "chain_code": "C1", "fetch": 2}


async def test_oldest_pending(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    oracle.rows(sql.oldest_pending_sql("C1"), [(NAIVE, NAIVE + timedelta(seconds=42))])
    assert await store.oldest_pending_age_s("C1") == 42.0  # relógio do banco nos dois lados
    assert oracle.last(sql.oldest_pending_sql("C1")).parameters == {"chain_code": "C1"}
    oracle.rows(sql.oldest_pending_sql(None), [(None, NAIVE)])
    assert await store.oldest_pending_age_s(None) is None


@pytest.mark.parametrize(
    ("resolved", "fragment"), [(True, "resolved_at IS NOT NULL"), (False, "resolved_at IS NULL")]
)
async def test_dlq_page(
    store: OracleMonitoringStore, oracle: FakeOracle, resolved: bool, fragment: str
) -> None:
    query = DlqQuery(stage=DlqStage.PUBLISH, chain_code="C1", resolved=resolved)
    conditions, _ = sql.dlq_conditions(query, PageRequest())
    statement = sql.page_sql(sql.DLQ_COLUMNS, "ohip_dlq", conditions)
    assert fragment in statement
    oracle.rows(statement, [dlq_row(9)])

    result = await store.dlq(query, PageRequest())

    (item,) = result.items
    assert (item.stage, item.outbox_id, item.attempts, item.resolved_by) == (
        DlqStage.PUBLISH,
        3,
        2,
        "ana",
    )
    assert oracle.last(statement).parameters == {
        "stage": "PUBLISH",
        "chain_code": "C1",
        "fetch": 51,
    }
    assert "raw_message" not in sql.DLQ_COLUMNS
    assert "stack_trace" not in sql.DLQ_COLUMNS


async def test_replays(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    query = ReplayQuery(chain_code="C1", status=ReplayStatus.PENDING)
    conditions, _ = sql.replay_conditions(query, PageRequest())
    statement = sql.page_sql(sql.REPLAY_COLUMNS, "ohip_replay_request", conditions)
    oracle.rows(statement, [replay_row(2)])
    oracle.rows(sql.REPLAY_BY_ID_SQL, [replay_row(2, "CANCELLED")])

    (entry,) = (await store.replays(query, PageRequest())).items
    assert (entry.from_offset, entry.status) == (Offset("90"), ReplayStatus.PENDING)
    assert oracle.last(statement).parameters == {
        "chain_code": "C1",
        "status": "PENDING",
        "fetch": 51,
    }

    single = await store.replay(2)
    assert single is not None
    assert single.status is ReplayStatus.CANCELLED
    oracle.rows(sql.REPLAY_BY_ID_SQL, [])
    assert await store.replay(3) is None


# ------------------------------------------------------------------ erros e sessão


async def test_ping_and_unavailable(store: OracleMonitoringStore, oracle: FakeOracle) -> None:
    await store.ping()
    assert oracle.statements() == [sql.PING_SQL]
    oracle.fail(sql.PING_SQL, db_error(ora(3113, "end-of-file")))
    with pytest.raises(StoreUnavailableError):
        await store.ping()
    assert oracle.kinds()[-1] == "drop"  # conexão perdida não volta ao pool


async def test_query_error_is_store_operation_error(
    store: OracleMonitoringStore, oracle: FakeOracle
) -> None:
    oracle.fail(sql.REPLAY_BY_ID_SQL, db_error(ora(942, "table or view does not exist")))
    with pytest.raises(StoreOperationError):
        await store.replay(1)
    assert oracle.kinds()[-1] == "release"


async def test_inline_session_runs_in_the_calling_thread(oracle: FakeOracle) -> None:
    import threading

    session = InlineSession(PooledSession(oracle, call_timeout_ms=5_000))
    caller = threading.get_ident()
    assert await session.run(lambda _connection: threading.get_ident()) == caller
