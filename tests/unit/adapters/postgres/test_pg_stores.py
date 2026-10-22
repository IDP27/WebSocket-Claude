"""Stores PostgreSQL (fora o do lote) com driver falso: SQL, mapeamento de erros e barreira.

O banco real prova o mesmo em ``tests/stores/`` (``make test-postgres``); aqui fica o que o
``make check`` precisa cobrir sem infraestrutura (achado da revisão da Fase 11).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import pytest
from psycopg import errors as pg_errors
from tests.fakes.postgres_driver import FakePool, Result, unique_violation

from ohip_streaming.adapters.postgres import enrichment_store as enrichment_sql
from ohip_streaming.adapters.postgres import lease_store as lease_sql
from ohip_streaming.adapters.postgres import operations_store as operations_sql
from ohip_streaming.adapters.postgres import outbox_store as outbox_sql
from ohip_streaming.adapters.postgres import purge_store as purge_sql
from ohip_streaming.adapters.postgres import replay_store as replay_sql
from ohip_streaming.adapters.postgres import status_store as status_sql
from ohip_streaming.adapters.postgres.database import FENCE_SQL, PooledSession
from ohip_streaming.adapters.postgres.enrichment_store import PostgresEnrichmentStore, merge_sql
from ohip_streaming.adapters.postgres.lease_store import PostgresLeaseStore
from ohip_streaming.adapters.postgres.operations_store import PostgresOperationsStore
from ohip_streaming.adapters.postgres.outbox_store import PostgresOutboxStore
from ohip_streaming.adapters.postgres.purge_store import PostgresPurgeStore
from ohip_streaming.adapters.postgres.replay_store import PostgresReplayStore
from ohip_streaming.adapters.postgres.status_store import PostgresConsumerStatusStore, health_sql
from ohip_streaming.application.errors import (
    LeaseLostError,
    LeaseNotProvisionedError,
    NotFoundError,
    ReplayAlreadyPendingError,
    StoreOperationError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    ConnectionHealth,
    DlqStage,
    DomainWrite,
    EventFilter,
    PageRequest,
    ProcessingStatus,
    PurgeTarget,
    ReplayQuery,
    ReplayRequest,
    ReplayStatus,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage
from ohip_streaming.domain.offset import Offset

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


@pytest.fixture
def pool() -> FakePool:
    return FakePool()


@pytest.fixture
def session(pool: FakePool) -> PooledSession:
    return PooledSession(pool)


def executed(pool: FakePool) -> list[str]:
    return pool.statements()


def params_of(pool: FakePool, statement: str) -> Any:
    return next(
        entry[2]
        for connection in pool.given
        for entry in connection.log
        if entry[0] == "execute" and entry[1] == statement
    )


# ------------------------------------------------------------------ enricher (merge_sql)


def write(**overrides: Any) -> DomainWrite:
    values: dict[str, Any] = {
        "table": "OHIP_RSV_RESERVATION",
        "key": {"chain_code": "C1", "hotel_key": "H1", "primary_key": "123"},
        "values": {"status": "RESERVED", "arrival": "2026-10-11"},
        "source_event_raw_id": 7,
        "source_offset_num": 100,
    }
    values.update(overrides)
    return DomainWrite(**values)


def test_merge_is_an_on_conflict_conditional_on_the_offset() -> None:
    statement, params = merge_sql(write())
    assert "INSERT INTO ohip_rsv_reservation AS t (chain_code, hotel_key, primary_key" in statement
    assert "VALUES (%(k1)s, %(k2)s, %(k3)s, %(v1)s, %(v2)s," in statement
    assert "ON CONFLICT (chain_code, hotel_key, primary_key) DO UPDATE SET" in statement
    assert "status = EXCLUDED.status, arrival = EXCLUDED.arrival" in statement
    assert "source_offset_num = EXCLUDED.source_offset_num" in statement
    assert "WHERE t.source_offset_num <= EXCLUDED.source_offset_num" in statement
    assert params == {
        "k1": "C1",
        "k2": "H1",
        "k3": "123",
        "v1": "RESERVED",
        "v2": "2026-10-11",
        "source_event_raw_id": 7,
        "source_offset_num": 100,
    }
    assert set(re.findall(r"%\((\w+)\)s", statement)) == set(params)
    assert "RESERVED" not in statement  # valores só por bind


def test_long_identifiers_up_to_the_postgres_limit() -> None:
    statement, _ = merge_sql(write(key={"k" * 63: "C1"}, values={"v" * 63: "x"}))
    assert f"ON CONFLICT ({'k' * 63})" in statement


def test_null_key_is_refused() -> None:
    # NULL nunca conflita: cada evento inseriria uma linha nova.
    with pytest.raises(StoreOperationError, match=r"chave nula.*\['hotel_key'\]"):
        merge_sql(write(key={"chain_code": "C1", "hotel_key": None, "primary_key": "1"}))


@pytest.mark.parametrize(
    "overrides",
    [
        {"table": "t; DROP TABLE x"},
        {"table": "a" * 64},
        {"key": {"chain code": "C1"}},
        {"values": {'status"': "x"}},
        {"key": {}},
        {"key": {"status": "1"}},  # chave repetida nos valores
        {"values": {"source_offset_num": 1}},
    ],
)
def test_invalid_writes_are_refused(overrides: dict[str, Any]) -> None:
    with pytest.raises(StoreOperationError):
        merge_sql(write(**overrides))


async def test_apply_counts_skipped_writes_and_updates_status(
    pool: FakePool, session: PooledSession
) -> None:
    store = PostgresEnrichmentStore(session, code_version="v9")
    older_write = write(table="OHIP_CRM_PROFILE")
    older, _ = merge_sql(older_write)
    pool.answers[older] = Result(rowcount=0)  # existia com offset maior: a condição barrou
    assert await store.apply(7, [write(), older_write], ProcessingStatus.NORMALIZED) == 1
    assert executed(pool)[-1] == enrichment_sql.UPDATE_STATUS_SQL
    assert params_of(pool, enrichment_sql.UPDATE_STATUS_SQL) == {
        "status": "NORMALIZED",
        "raw_event_id": 7,
    }

    pool.answers[enrichment_sql.UPDATE_STATUS_SQL] = Result(rowcount=0)
    with pytest.raises(NotFoundError):
        await store.apply(8, [], ProcessingStatus.UNMAPPED)


async def test_enricher_dlq(pool: FakePool, session: PooledSession) -> None:
    store = PostgresEnrichmentStore(session, code_version="v9")
    with pytest.raises(ValueError, match="NORMALIZE/ENRICH"):
        await store.add_dlq(7, DlqStage.CONSUME, "x", "y")
    await store.add_dlq(7, DlqStage.ENRICH, "Erro\x00", "falhou")
    row = params_of(pool, enrichment_sql.INSERT_DLQ_SQL)
    assert (row["stage"], row["error_class"], row["code_version"]) == ("ENRICH", "Erro�", "v9")
    pool.answers[enrichment_sql.INSERT_DLQ_SQL] = Result(rowcount=0)
    with pytest.raises(NotFoundError):
        await store.add_dlq(9, DlqStage.NORMALIZE, "x", "y")
    pool.answers[operations_sql.EVENT_BY_ID_SQL] = []
    assert await store.get_event(1) is None


# ------------------------------------------------------------------ lease


async def test_lease(pool: FakePool, session: PooledSession) -> None:
    store = PostgresLeaseStore(session)
    pool.answers[lease_sql.ACQUIRE_SQL] = [(5,)]
    assert await store.acquire("publisher", "vm1", 30) == 5
    assert params_of(pool, lease_sql.ACQUIRE_SQL) == {
        "lease_name": "publisher",
        "owner": "vm1",
        "ttl_s": 30,
    }

    pool.answers[lease_sql.ACQUIRE_SQL] = []
    pool.answers[lease_sql.EXISTS_SQL] = [(1,)]
    assert await store.acquire("publisher", "vm2", 30) is None  # outro dono com prazo
    pool.answers[lease_sql.EXISTS_SQL] = [(0,)]
    with pytest.raises(LeaseNotProvisionedError):
        await store.acquire("consumer:X", "vm2", 30)

    assert await store.renew("publisher", "vm1", 5, 30) is True
    pool.answers[lease_sql.RENEW_SQL] = Result(rowcount=0)
    assert await store.renew("publisher", "vm1", 5, 30) is False
    await store.release("publisher", "vm1", 5)
    assert lease_sql.RELEASE_SQL in executed(pool)

    pool.answers[lease_sql.RENEW_SQL] = pg_errors.UndefinedTable("ohip_lease")
    with pytest.raises(StoreOperationError, match="42P01"):
        await store.renew("publisher", "vm1", 5, 30)


# ------------------------------------------------------------------ outbox


async def test_outbox_reads_and_marks_with_the_fence(
    pool: FakePool, session: PooledSession
) -> None:
    store = PostgresOutboxStore(session, code_version="v9")
    pool.answers[outbox_sql.CHAINS_WITH_PENDING_SQL] = [("C1",), ("C2",)]
    assert await store.chains_with_pending() == ["C1", "C2"]
    pool.answers[outbox_sql.FETCH_HEAD_SQL] = [
        (1, "C1", "ohip.events", "ohip.rsv.X", "uid-1", "{}", 1, 0, NOW)
    ]
    (row,) = await store.fetch_head("C1", 10)
    assert (row.id, row.message_id, row.next_attempt_at) == (1, "uid-1", NOW)

    await store.mark_sent(1, 4)
    assert executed(pool)[-2:] == [outbox_sql.MARK_SENT_SQL, FENCE_SQL]
    assert params_of(pool, FENCE_SQL) == {"lease_name": "publisher", "epoch": 4}

    await store.record_failure(1, 2, NOW, "nack\x00", 4)
    assert params_of(pool, outbox_sql.RECORD_FAILURE_SQL)["last_error"] == "nack�"

    await store.mark_failed(1, 10, "esgotado", 4)
    assert executed(pool)[-3:] == [
        outbox_sql.MARK_FAILED_SQL,
        outbox_sql.INSERT_DLQ_PUBLISH_SQL,
        FENCE_SQL,
    ]

    pool.answers[outbox_sql.MARK_FAILED_SQL] = Result(rowcount=0)  # já saiu da fila
    await store.mark_failed(2, 10, "esgotado", 4)
    assert executed(pool)[-2:] == [outbox_sql.MARK_FAILED_SQL, FENCE_SQL]  # sem DLQ

    pool.answers[FENCE_SQL] = Result(rowcount=0)
    with pytest.raises(LeaseLostError):
        await store.mark_sent(1, 3)


# ------------------------------------------------------------------ status


async def test_status_writes(pool: FakePool, session: PooledSession) -> None:
    store = PostgresConsumerStatusStore(session)
    await store.record_state("C1", ConsumerState.SUBSCRIBED, "vm1")
    await store.record_subscribed("C1", "vm1", "sub-1", NOW)
    assert params_of(pool, status_sql.RECORD_SUBSCRIBED_SQL)["token_expires_at"] == NOW

    await store.record_disconnect(
        "C1", ConsumerState.WAITING, 4409, "x" * 600, next_attempt_in_s=-1
    )
    params = params_of(pool, status_sql.RECORD_DISCONNECT_SQL)
    assert params["wait_s"] == 0.0
    assert len(params["close_reason"].encode()) <= 500
    await store.record_disconnect("C1", ConsumerState.STOPPED, 4403, None, reconnect=True)
    assert params_of(pool, status_sql.RECORD_FINAL_DISCONNECT_SQL)["close_reason"] is None

    pool.answers[status_sql.RECORD_STATE_SQL] = Result(rowcount=0)
    with pytest.raises(UnknownChainError):
        await store.record_state("NOPE", ConsumerState.WAITING, "vm1")


async def test_status_health_and_snapshot(pool: FakePool, session: PooledSession) -> None:
    assert health_sql("C1", ConnectionHealth()) == (None, {})
    statement, params = health_sql("C1", ConnectionHealth(last_pong_at=NOW, rtt_ms=12))
    assert statement is not None
    assert "last_pong_at = %(last_pong_at)s, last_rtt_ms = %(last_rtt_ms)s" in statement
    assert params == {"last_pong_at": NOW, "last_rtt_ms": 12, "chain_code": "C1"}

    store = PostgresConsumerStatusStore(session)
    await store.record_health("C1", ConnectionHealth())  # nada novo: nenhum comando
    assert executed(pool) == []
    await store.record_health("C1", ConnectionHealth(rtt_ms=3))
    assert len(executed(pool)) == 1

    pool.answers[status_sql.SNAPSHOT_SQL] = [(NOW, NOW, "WAITING")]
    snapshot = await store.disconnect_snapshot("C1")
    assert (snapshot.db_now, snapshot.last_state) == (NOW, ConsumerState.WAITING)
    pool.answers[status_sql.SNAPSHOT_SQL] = []
    with pytest.raises(UnknownChainError):
        await store.disconnect_snapshot("NOPE")


# ------------------------------------------------------------------ replay


def request(**overrides: Any) -> ReplayRequest:
    values: dict[str, Any] = {
        "id": 1,
        "chain_code": "C1",
        "from_offset": Offset("10"),
        "reason": "teste",
        "requested_by": "ana",
        "status": ReplayStatus.PENDING,
        "created_at": NOW,
    }
    values.update(overrides)
    return ReplayRequest(**values)


async def test_replay_requests(pool: FakePool, session: PooledSession) -> None:
    store = PostgresReplayStore(session)
    pool.answers[replay_sql.INSERT_REQUEST_SQL] = [(3, NOW)]
    created = await store.create_request("C1", Offset("10"), "teste", "ana")
    assert (created.id, created.created_at, created.status) == (3, NOW, ReplayStatus.PENDING)

    pool.answers[replay_sql.INSERT_REQUEST_SQL] = unique_violation("ohip_replay_ux_pending")
    with pytest.raises(ReplayAlreadyPendingError):
        await store.create_request("C1", Offset("10"), "teste", "ana")
    pool.answers[replay_sql.INSERT_REQUEST_SQL] = pg_errors.NotNullViolation("reason")
    with pytest.raises(StoreOperationError, match="23502"):
        await store.create_request("C1", Offset("10"), "", "ana")

    pool.answers[replay_sql.PENDING_REQUEST_SQL] = [(1, "C1", "10", "teste", "ana", "PENDING", NOW)]
    assert await store.pending_request("C1") == request()
    pool.answers[replay_sql.LAST_OFFSET_SQL] = [("42",)]
    assert await store.last_offset("C1") == Offset("42")
    pool.answers[replay_sql.LAST_OFFSET_SQL] = [(None,)]
    assert await store.last_offset("C1") is None
    pool.answers[replay_sql.LAST_OFFSET_SQL] = []
    with pytest.raises(UnknownChainError):
        await store.last_offset("NOPE")
    pool.answers[replay_sql.OFFSET_RECEIVED_AT_SQL] = [(NOW,)]
    assert await store.offset_received_at("C1", Offset("1")) == NOW


async def test_replay_apply_and_cancel(pool: FakePool, session: PooledSession) -> None:
    store = PostgresReplayStore(session)
    assert await store.apply_request(request(), 6) is True
    assert executed(pool)[-3:] == [
        replay_sql.APPLY_REQUEST_SQL,
        replay_sql.MOVE_OFFSET_SQL,
        FENCE_SQL,
    ]
    assert params_of(pool, FENCE_SQL) == {"lease_name": "consumer:C1", "epoch": 6}

    pool.answers[replay_sql.MOVE_OFFSET_SQL] = Result(rowcount=0)
    with pytest.raises(UnknownChainError):
        await store.apply_request(request(), 6)
    pool.answers[replay_sql.APPLY_REQUEST_SQL] = Result(rowcount=0)  # cancelado antes
    assert await store.apply_request(request(), 6) is False

    assert await store.cancel_request(1, "ana") is True
    pool.answers[replay_sql.CANCEL_REQUEST_SQL] = Result(rowcount=0)
    assert await store.cancel_request(1, "ana") is False


# ------------------------------------------------------------------ operações da API


def queue_message() -> QueueMessage:
    return QueueMessage(ExchangeKind.REPROCESS, "ohip.rsv.X", "uid-1", "{}")


async def test_operations(pool: FakePool, session: PooledSession) -> None:
    store = PostgresOperationsStore(session)
    pool.answers[operations_sql.ENQUEUE_SQL] = [(77,)]
    assert (
        await store.enqueue(
            chain_code="C1", raw_event_id=1, message=queue_message(), exchange_name="ohip.reprocess"
        )
        == 77
    )
    pool.answers[operations_sql.DLQ_ITEM_SQL] = [(5, "PUBLISH", "C1", 1, 9, "uid-1", None)]
    item = await store.get_dlq_item(5)
    assert item is not None
    assert (item.stage, item.outbox_id, item.resolved) == (DlqStage.PUBLISH, 9, False)
    pool.answers[operations_sql.DLQ_ITEM_SQL] = []
    assert await store.get_dlq_item(6) is None
    assert await store.request_consume_retry(5, "ana") is True

    pool.answers[operations_sql.COPY_OUTBOX_SQL] = [(88,)]
    assert await store.retry_publish(5, 9, "ana") == 88
    assert executed(pool)[-2:] == [operations_sql.RESOLVE_SQL, operations_sql.COPY_OUTBOX_SQL]
    pool.answers[operations_sql.COPY_OUTBOX_SQL] = []
    with pytest.raises(StoreOperationError, match="não existe"):
        await store.retry_publish(5, 9, "ana")

    pool.answers[operations_sql.RESOLVE_SQL] = Result(rowcount=0)  # já resolvido
    assert await store.retry_publish(5, 9, "ana") is None
    assert (
        await store.enqueue_and_resolve(
            5,
            chain_code="C1",
            raw_event_id=1,
            message=queue_message(),
            exchange_name="ohip.reprocess",
            requested_by="ana",
        )
        is None
    )
    pool.answers[operations_sql.EVENT_BY_UID_SQL] = []
    assert await store.find_event("uid-x") is None


# ------------------------------------------------------------------ expurgo


async def test_purge(pool: FakePool, session: PooledSession) -> None:
    store = PostgresPurgeStore(session)
    statement = purge_sql.delete_sql(PurgeTarget.RAW)
    assert statement.startswith("DELETE FROM ohip_event_raw WHERE id IN (SELECT r.id FROM")
    assert statement.endswith("LIMIT %(n)s)")
    pool.answers[statement] = Result(rowcount=17)
    assert await store.purge_batch(PurgeTarget.RAW, 90, 1000) == 17
    assert params_of(pool, statement) == {"days": 90, "n": 1000}
    pool.answers[purge_sql.count_sql(PurgeTarget.DLQ)] = [(4,)]
    assert await store.count_expired(PurgeTarget.DLQ, 90) == 4
    pool.answers[purge_sql.count_sql(PurgeTarget.DLQ)] = []
    assert await store.count_expired(PurgeTarget.DLQ, 90) == 0


async def test_unavailable_database_is_translated(pool: FakePool, session: PooledSession) -> None:
    pool.answers[purge_sql.count_sql(PurgeTarget.RAW)] = pg_errors.AdminShutdown("desligando")
    with pytest.raises(StoreUnavailableError, match="57P01"):
        await PostgresPurgeStore(session).count_expired(PurgeTarget.RAW, 90)


# ------------------------------------------------------------------ monitoramento (API)


def test_event_filter_builds_fixed_fragments_and_binds() -> None:
    from ohip_streaming.adapters.postgres.monitoring_store import event_conditions, page_sql

    conditions, params = event_conditions(
        EventFilter(
            chain_code="C1",
            module_name="reservation",
            received_from=NOW,
            received_to=NOW,
            processing_status=ProcessingStatus.UNMAPPED,
        ),
        PageRequest(limit=10, cursor=99),
    )
    assert conditions == [
        "chain_code = %(chain_code)s",
        "UPPER(module_name) = %(module_name)s",
        "received_at >= %(received_from)s",
        "received_at < %(received_to)s",
        "processing_status = %(processing_status)s",
        "id < %(cursor)s",
    ]
    assert params["module_name"] == "RESERVATION"
    assert params["fetch"] == 11  # limit + 1: sabe se há próxima página
    statement = page_sql("id", "ohip_event_raw", conditions)
    assert statement.rstrip().endswith("ORDER BY id DESC\n LIMIT %(fetch)s")


async def test_monitoring_status_and_pages(pool: FakePool, session: PooledSession) -> None:
    from ohip_streaming.adapters.postgres import monitoring_store as monitoring_sql
    from ohip_streaming.adapters.postgres.monitoring_store import PostgresMonitoringStore

    store = PostgresMonitoringStore(session)
    pool.answers[monitoring_sql.DB_NOW_SQL] = [(NOW,)]
    pool.answers[monitoring_sql.STATUS_SQL] = [
        ("C1", "SUBSCRIBED", "vm1", "sub", "42", NOW, 3, 0, None, None, None, NOW, NOW)
    ]
    pool.answers[monitoring_sql.OUTBOX_COUNTS_SQL] = [
        ("C1", "PENDING", 2, NOW),
        ("C1", "FAILED", 1, NOW),
    ]
    pool.answers[monitoring_sql.DLQ_OPEN_SQL] = [("C1", "CONSUME", 4)]
    pool.answers[monitoring_sql.RECENT_EVENTS_SQL] = [("C1", 10, 0.5)]
    snapshot = await store.status()
    (chain,) = snapshot.chains
    assert (chain.last_offset, chain.events_last_5m, chain.lag_seconds_p95_5m) == (
        Offset("42"),
        10,
        0.5,
    )
    assert (chain.outbox.pending, chain.outbox.failed) == (2, 1)
    assert chain.dlq_open[DlqStage.CONSUME] == 4
    await store.ping()
    assert monitoring_sql.PING_SQL in executed(pool)

    rows = [(i, "C1", "10", "r", "ana", "PENDING", None, NOW, None, None, None) for i in (5, 4, 3)]
    query = monitoring_sql.page_sql(
        monitoring_sql.REPLAY_COLUMNS, "ohip_replay_request", ["id < %(cursor)s"]
    )
    pool.answers[query] = rows
    page = await store.replays(ReplayQuery(), PageRequest(limit=2, cursor=6))
    assert [e.id for e in page.items] == [5, 4]
    assert page.next_cursor == 4
    pool.answers[monitoring_sql.oldest_pending_sql("C1")] = [(NOW, NOW)]
    assert await store.oldest_pending_age_s("C1") == 0.0
    pool.answers[monitoring_sql.REPLAY_BY_ID_SQL] = []
    assert await store.replay(1) is None
