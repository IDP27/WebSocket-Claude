"""Outbox, replay, operações da API e status no Oracle, com driver falso."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import oracledb
import pytest
from tests.fakes.frames import new_event
from tests.fakes.oracle_driver import FakeCursor, FakeOracle, db_error, ora, unique_violation

from ohip_streaming.adapters.oracle import operations_store as ops_sql
from ohip_streaming.adapters.oracle import outbox_store as outbox_sql
from ohip_streaming.adapters.oracle import replay_store as replay_sql
from ohip_streaming.adapters.oracle import status_store as status_sql
from ohip_streaming.adapters.oracle.database import FENCE_SQL, PooledSession
from ohip_streaming.adapters.oracle.operations_store import OracleOperationsStore
from ohip_streaming.adapters.oracle.outbox_store import OracleOutboxStore
from ohip_streaming.adapters.oracle.replay_store import OracleReplayStore
from ohip_streaming.adapters.oracle.status_store import OracleConsumerStatusStore
from ohip_streaming.application.errors import (
    LeaseLostError,
    ReplayAlreadyPendingError,
    StoreOperationError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    ConnectionHealth,
    DlqStage,
    ProcessingStatus,
    ReplayRequest,
    ReplayStatus,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage
from ohip_streaming.domain.offset import Offset

AWARE = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
NAIVE = AWARE.replace(tzinfo=None)  # como o Oracle devolve TIMESTAMP


@pytest.fixture
def oracle() -> FakeOracle:
    return FakeOracle()


@pytest.fixture
def session(oracle: FakeOracle) -> PooledSession:
    return PooledSession(oracle, call_timeout_ms=5_000)


# ------------------------------------------------------------------ outbox


async def test_fetch_head_maps_rows(session: PooledSession, oracle: FakeOracle) -> None:
    oracle.rows(
        outbox_sql.FETCH_HEAD_SQL,
        [(10, "C1", "ohip.events", "ohip.rsv.X", "uid-1", '{"a":1}', 1, 2, NAIVE)],
    )
    (row,) = await OracleOutboxStore(session, code_version="v").fetch_head("C1", 50)
    assert (row.id, row.message_id, row.body, row.attempts) == (10, "uid-1", '{"a":1}', 2)
    assert row.next_attempt_at == AWARE
    assert oracle.last(outbox_sql.FETCH_HEAD_SQL).parameters == {"chain_code": "C1", "limit": 50}


async def test_chains_with_pending(session: PooledSession, oracle: FakeOracle) -> None:
    oracle.rows(outbox_sql.CHAINS_WITH_PENDING_SQL, [("C1",), ("C2",)])
    assert await OracleOutboxStore(session, code_version="v").chains_with_pending() == ["C1", "C2"]


async def test_mark_sent_and_failure_are_fenced(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleOutboxStore(session, code_version="v")
    await store.mark_sent(10, 4)
    assert oracle.statements() == [outbox_sql.MARK_SENT_SQL, FENCE_SQL]
    assert oracle.last(FENCE_SQL).parameters == {"lease_name": "publisher", "epoch": 4}

    await store.record_failure(10, 2, AWARE, "nack " + "x" * 5000, 4)
    call = oracle.last(outbox_sql.RECORD_FAILURE_SQL)
    params = call.parameters
    assert params["next_attempt_at"] == NAIVE
    assert call.input_sizes == {"next_attempt_at": oracledb.DB_TYPE_TIMESTAMP}  # fração
    assert len(params["last_error"].encode()) <= 4000


async def test_mark_failed_writes_dlq_only_for_pending_rows(
    session: PooledSession, oracle: FakeOracle
) -> None:
    store = OracleOutboxStore(session, code_version="v9")
    await store.mark_failed(10, 10, "esgotado", 4)
    assert oracle.statements() == [
        outbox_sql.MARK_FAILED_SQL,
        outbox_sql.INSERT_DLQ_PUBLISH_SQL,
        FENCE_SQL,
    ]
    assert oracle.last(outbox_sql.INSERT_DLQ_PUBLISH_SQL).parameters["code_version"] == "v9"

    oracle.calls.clear()
    oracle.rowcount(outbox_sql.MARK_FAILED_SQL, 0)  # já não estava na fila
    await store.mark_failed(10, 10, "esgotado", 4)
    assert oracle.statements() == [outbox_sql.MARK_FAILED_SQL, FENCE_SQL]


async def test_publisher_lease_lost(session: PooledSession, oracle: FakeOracle) -> None:
    oracle.rowcount(FENCE_SQL, 0)
    with pytest.raises(LeaseLostError):
        await OracleOutboxStore(session, code_version="v").mark_sent(1, 1)
    assert oracle.kinds()[-2:] == ["rollback", "release"]


# ------------------------------------------------------------------ replay


def _request(status: str = "PENDING") -> tuple[Any, ...]:
    return (3, "C1", "90", "lacuna", "ana", status, NAIVE)


async def test_last_offset(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleReplayStore(session)
    oracle.rows(replay_sql.LAST_OFFSET_SQL, [("100",)])
    assert await store.last_offset("C1") == Offset("100")
    oracle.rows(replay_sql.LAST_OFFSET_SQL, [(None,)])
    assert await store.last_offset("C1") is None
    oracle.rows(replay_sql.LAST_OFFSET_SQL, [])
    with pytest.raises(UnknownChainError):
        await store.last_offset("C9")


async def test_offset_received_at(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleReplayStore(session)
    oracle.rows(replay_sql.OFFSET_RECEIVED_AT_SQL, [(NAIVE,)])
    assert await store.offset_received_at("C1", Offset("5")) == AWARE
    oracle.rows(replay_sql.OFFSET_RECEIVED_AT_SQL, [(None,)])
    assert await store.offset_received_at("C1", Offset("5")) is None


async def test_create_request(session: PooledSession, oracle: FakeOracle) -> None:
    def returning(cursor: FakeCursor, params: Any) -> None:
        cursor.rowcount = 1
        params["id"].values = [3]
        params["created_at"].values = [NAIVE]

    oracle.on(replay_sql.INSERT_REQUEST_SQL, returning)
    request = await OracleReplayStore(session).create_request("C1", Offset("90"), "lacuna", "ana")
    assert request == ReplayRequest(
        3, "C1", Offset("90"), "lacuna", "ana", ReplayStatus.PENDING, AWARE
    )


async def test_create_request_already_pending(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleReplayStore(session)
    oracle.fail(
        replay_sql.INSERT_REQUEST_SQL,
        db_error(unique_violation("OHIP_REPLAY_UX_PENDING"), oracledb.IntegrityError),
    )
    with pytest.raises(ReplayAlreadyPendingError):
        await store.create_request("C1", Offset("90"), "x", "ana")

    oracle.fail(
        replay_sql.INSERT_REQUEST_SQL,
        db_error(ora(2291, "parent key not found"), oracledb.IntegrityError),
    )
    with pytest.raises(StoreOperationError, match="ORA-02291"):
        await store.create_request("C1", Offset("90"), "x", "ana")


async def test_pending_request(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleReplayStore(session)
    oracle.rows(replay_sql.PENDING_REQUEST_SQL, [_request()])
    pending = await store.pending_request("C1")
    assert pending is not None
    assert pending.from_offset == Offset("90")
    oracle.rows(replay_sql.PENDING_REQUEST_SQL, [])
    assert await store.pending_request("C1") is None


async def test_apply_request(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleReplayStore(session)
    request = ReplayRequest(3, "C1", Offset("90"), "x", "ana", ReplayStatus.PENDING, AWARE)
    assert await store.apply_request(request, 8)
    assert oracle.statements() == [
        replay_sql.APPLY_REQUEST_SQL,
        replay_sql.MOVE_OFFSET_SQL,
        FENCE_SQL,
    ]
    assert oracle.last(FENCE_SQL).parameters == {"lease_name": "consumer:C1", "epoch": 8}

    oracle.calls.clear()
    oracle.rowcount(replay_sql.APPLY_REQUEST_SQL, 0)  # cancelado nesse meio-tempo
    assert not await store.apply_request(request, 8)
    assert oracle.statements() == [replay_sql.APPLY_REQUEST_SQL]

    oracle.rowcount(replay_sql.APPLY_REQUEST_SQL, 1)
    oracle.rowcount(replay_sql.MOVE_OFFSET_SQL, 0)
    with pytest.raises(UnknownChainError):
        await store.apply_request(request, 8)


async def test_cancel_request(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleReplayStore(session)
    assert await store.cancel_request(3, "ana")
    assert oracle.last(replay_sql.CANCEL_REQUEST_SQL).parameters == {"id": 3, "cancelled_by": "ana"}
    oracle.rowcount(replay_sql.CANCEL_REQUEST_SQL, 0)
    assert not await store.cancel_request(3, "ana")


# ------------------------------------------------------------------ operações da API


MESSAGE = QueueMessage(ExchangeKind.REPROCESS, "ohip.rsv.X", "uid-1", '{"a":1}')


def _returning_id(value: int) -> Any:
    def handler(cursor: FakeCursor, params: Any) -> None:
        cursor.rowcount = 1
        params["new_id"].values = [value]

    return handler


async def test_find_and_get_event_rebuild_from_payload(
    session: PooledSession, oracle: FakeOracle
) -> None:
    payload = json.dumps(new_event("97", "uid-97"))
    row = (5, "C1", "sub-1", NAIVE, payload, "RECEIVED", NAIVE)
    oracle.rows(ops_sql.EVENT_BY_UID_SQL, [row])
    store = OracleOperationsStore(session)

    stored = await store.find_event("uid-97")

    assert stored is not None
    assert (stored.raw_event_id, stored.status) == (5, ProcessingStatus.RECEIVED)
    assert stored.event.offset == Offset("97")
    assert stored.event.subscription_id == "sub-1"
    assert stored.event.event_ts == AWARE
    assert stored.event.received_at == AWARE
    oracle.rows(ops_sql.EVENT_BY_ID_SQL, [])
    assert await store.get_event(5) is None


async def test_unreadable_stored_event_raises(session: PooledSession, oracle: FakeOracle) -> None:
    oracle.rows(ops_sql.EVENT_BY_ID_SQL, [(5, "C1", None, None, "{lixo", "RECEIVED", NAIVE)])
    with pytest.raises(ValueError, match="ilegível"):
        await OracleOperationsStore(session).get_event(5)


async def test_enqueue_returns_new_id(session: PooledSession, oracle: FakeOracle) -> None:
    oracle.on(ops_sql.ENQUEUE_SQL, _returning_id(77))
    new_id = await OracleOperationsStore(session).enqueue(
        chain_code="C1", raw_event_id=5, message=MESSAGE, exchange_name="ohip.reprocess"
    )
    assert new_id == 77
    call = oracle.last(ops_sql.ENQUEUE_SQL)
    assert call.parameters["exchange_name"] == "ohip.reprocess"
    assert "message" in call.input_sizes


async def test_get_dlq_item(session: PooledSession, oracle: FakeOracle) -> None:
    oracle.rows(ops_sql.DLQ_ITEM_SQL, [(9, "PUBLISH", "C1", 5, 10, "uid-1", None)])
    item = await OracleOperationsStore(session).get_dlq_item(9)
    assert item is not None
    assert (item.stage, item.outbox_id, item.resolved) == (DlqStage.PUBLISH, 10, False)
    oracle.rows(ops_sql.DLQ_ITEM_SQL, [(9, "CONSUME", "C1", None, None, None, "RETRIED")])
    item = await OracleOperationsStore(session).get_dlq_item(9)
    assert item is not None
    assert (item.event_raw_id, item.resolved) == (None, True)
    oracle.rows(ops_sql.DLQ_ITEM_SQL, [])
    assert await OracleOperationsStore(session).get_dlq_item(9) is None


async def test_request_consume_retry(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleOperationsStore(session)
    assert await store.request_consume_retry(9, "ana")
    oracle.rowcount(ops_sql.REQUEST_CONSUME_RETRY_SQL, 0)
    assert not await store.request_consume_retry(9, "ana")


async def test_retry_publish_resolves_first_then_copies(
    session: PooledSession, oracle: FakeOracle
) -> None:
    oracle.rows(ops_sql.NEXT_OUTBOX_ID_SQL, [(501,)])
    new_id = await OracleOperationsStore(session).retry_publish(9, 10, "ana")
    assert new_id == 501
    assert oracle.statements() == [
        ops_sql.RESOLVE_SQL,  # trava o item antes de inserir (atomicidade)
        ops_sql.NEXT_OUTBOX_ID_SQL,
        ops_sql.COPY_OUTBOX_SQL,
    ]
    assert oracle.last(ops_sql.COPY_OUTBOX_SQL).parameters == {"new_id": 501, "outbox_id": 10}


async def test_retry_publish_on_resolved_item_inserts_nothing(
    session: PooledSession, oracle: FakeOracle
) -> None:
    oracle.rowcount(ops_sql.RESOLVE_SQL, 0)
    assert await OracleOperationsStore(session).retry_publish(9, 10, "ana") is None
    assert oracle.statements() == [ops_sql.RESOLVE_SQL]


async def test_retry_publish_of_missing_outbox_row_rolls_back(
    session: PooledSession, oracle: FakeOracle
) -> None:
    oracle.rows(ops_sql.NEXT_OUTBOX_ID_SQL, [(501,)])
    oracle.rowcount(ops_sql.COPY_OUTBOX_SQL, 0)
    with pytest.raises(StoreOperationError):
        await OracleOperationsStore(session).retry_publish(9, 10, "ana")
    assert "rollback" in oracle.kinds()
    assert "commit" not in oracle.kinds()


async def test_enqueue_and_resolve(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleOperationsStore(session)
    oracle.on(ops_sql.ENQUEUE_SQL, _returning_id(88))
    kwargs: dict[str, Any] = {
        "chain_code": "C1",
        "raw_event_id": 5,
        "message": MESSAGE,
        "exchange_name": "ohip.reprocess",
        "requested_by": "ana",
    }
    assert await store.enqueue_and_resolve(9, **kwargs) == 88
    assert oracle.statements() == [ops_sql.RESOLVE_SQL, ops_sql.ENQUEUE_SQL]

    oracle.calls.clear()
    oracle.rowcount(ops_sql.RESOLVE_SQL, 0)
    assert await store.enqueue_and_resolve(9, **kwargs) is None
    assert oracle.statements() == [ops_sql.RESOLVE_SQL]


# ------------------------------------------------------------------ status


async def test_status_updates(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleConsumerStatusStore(session)
    await store.record_state("C1", ConsumerState.CONNECTING, "vm1:42")
    assert oracle.last(status_sql.RECORD_STATE_SQL).parameters == {
        "state": "CONNECTING",
        "instance_id": "vm1:42",
        "chain_code": "C1",
    }
    await store.record_disconnect(
        "C1",
        ConsumerState.WAITING,
        4409,
        "x" * 900,
        consecutive_failures=3,
        reconnect=True,
        next_attempt_in_s=120.4567,
    )
    params = oracle.last(status_sql.RECORD_DISCONNECT_SQL).parameters
    assert params["close_code"] == 4409
    assert len(params["close_reason"].encode()) <= 500
    assert (params["consecutive_failures"], params["reconnect"], params["wait_s"]) == (
        3,
        1,
        120.457,
    )
    assert "NUMTODSINTERVAL(:wait_s, 'SECOND')" in status_sql.RECORD_DISCONNECT_SQL

    # Parada (sem próxima tentativa): limpa next_attempt_at e não soma reconexão.
    await store.record_disconnect("C1", ConsumerState.STOPPED, None, None)
    params = oracle.last(status_sql.RECORD_FINAL_DISCONNECT_SQL).parameters
    assert (params["close_reason"], params["reconnect"], params["consecutive_failures"]) == (
        None,
        0,
        0,
    )
    assert "wait_s" not in params
    assert "next_attempt_at = NULL" in status_sql.RECORD_FINAL_DISCONNECT_SQL

    oracle.rowcount(status_sql.RECORD_STATE_SQL, 0)
    with pytest.raises(UnknownChainError):
        await store.record_state("C9", ConsumerState.STOPPED, "vm1:42")


async def test_record_subscribed(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleConsumerStatusStore(session)
    await store.record_subscribed("C1", "vm1:42", "sub-1", AWARE)
    assert oracle.last(status_sql.RECORD_SUBSCRIBED_SQL).parameters == {
        "instance_id": "vm1:42",
        "subscription_id": "sub-1",
        "token_expires_at": NAIVE,
        "chain_code": "C1",
    }
    assert "connected_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)" in status_sql.RECORD_SUBSCRIBED_SQL


async def test_record_health_writes_only_known_values(
    session: PooledSession, oracle: FakeOracle
) -> None:
    store = OracleConsumerStatusStore(session)
    health = ConnectionHealth(last_message_at=AWARE, rtt_ms=42)
    statement, params = status_sql.health_sql("C1", health)
    assert statement is not None
    assert "SET last_message_at = :last_message_at, last_rtt_ms = :last_rtt_ms," in statement
    assert "last_ping_at" not in statement  # None não apaga o dado anterior
    assert params == {"last_message_at": NAIVE, "last_rtt_ms": 42, "chain_code": "C1"}
    await store.record_health("C1", health)
    assert oracle.last(statement).parameters == params

    assert status_sql.health_sql("C1", ConnectionHealth()) == (None, {})
    before = len(oracle.statements())
    await store.record_health("C1", ConnectionHealth())  # nada novo: sem ida ao banco
    assert len(oracle.statements()) == before


async def test_disconnect_snapshot(session: PooledSession, oracle: FakeOracle) -> None:
    store = OracleConsumerStatusStore(session)
    oracle.rows(status_sql.SNAPSHOT_SQL, [(NAIVE, NAIVE, "WAITING")])
    snapshot = await store.disconnect_snapshot("C1")
    assert (snapshot.db_now, snapshot.last_disconnect_at) == (AWARE, AWARE)
    assert snapshot.last_state is ConsumerState.WAITING
    oracle.rows(status_sql.SNAPSHOT_SQL, [(NAIVE, None, None)])
    snapshot = await store.disconnect_snapshot("C1")
    assert (snapshot.last_disconnect_at, snapshot.last_state) == (None, None)
    oracle.rows(status_sql.SNAPSHOT_SQL, [])
    with pytest.raises(UnknownChainError):
        await store.disconnect_snapshot("C9")
