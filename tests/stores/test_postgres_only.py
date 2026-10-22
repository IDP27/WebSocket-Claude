"""Comportamentos que só um PostgreSQL de verdade prova (ADR-0021, ADR-0022 §6).

Rodam contra o contêiner descartável do ``make test-db`` (``make test-postgres``). Sem DDL:
os erros de recurso vêm de ``RAISE ... USING ERRCODE``, que produz o mesmo SQLSTATE que o
servidor manda quando o disco enche.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import psycopg
import pytest
from tests.stores.backends import PostgresBackend, postgres_test_settings
from tests.stores.test_store_scenarios import CHAIN, batch, event, persist, records, uid

from ohip_streaming.adapters.postgres import database as pg_db
from ohip_streaming.adapters.postgres.database import query, transaction
from ohip_streaming.adapters.postgres.enrichment_store import PostgresEnrichmentStore
from ohip_streaming.adapters.postgres.monitoring_store import (
    OUTBOX_COUNTS_SQL,
    RECENT_EVENTS_SQL,
    oldest_pending_sql,
)
from ohip_streaming.adapters.postgres.outbox_store import CHAINS_WITH_PENDING_SQL
from ohip_streaming.adapters.postgres.purge_store import delete_sql
from ohip_streaming.application.errors import (
    BatchFailedError,
    LeaseLostError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import (
    DomainWrite,
    NewEventRecord,
    ProcessingStatus,
    PurgeTarget,
)
from ohip_streaming.config import Environment
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage

_settings = postgres_test_settings()
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(_settings is None, reason="PostgreSQL de teste não configurado"),
]

RAISE_SQL = "DO $$ BEGIN RAISE EXCEPTION 'teste' USING ERRCODE = '{code}'; END $$"


@pytest.fixture
def pg() -> Iterator[PostgresBackend]:
    assert _settings is not None
    backend = PostgresBackend.open(_settings)
    try:
        yield backend
    finally:
        backend.close()


# ------------------------------------------------------------------ classificação


@pytest.mark.parametrize(
    "code", ["53100", "53200", "53300", "57P01", "57014", "40001", "40P01", "25006", "55P03"]
)
def test_unavailable_states_from_the_server(pg: PostgresBackend, code: str) -> None:
    with pg.pool.connection() as connection, pytest.raises(StoreUnavailableError, match=code):
        transaction(
            connection,
            lambda cursor: cursor.execute(RAISE_SQL.format(code=code)),
            failure=BatchFailedError,
        )


def test_statement_timeout_is_unavailable(pg: PostgresBackend) -> None:
    def slow(cursor: Any) -> None:
        cursor.execute("SET LOCAL statement_timeout = 50")
        cursor.execute("SELECT pg_sleep(1)")

    with pg.pool.connection() as connection, pytest.raises(StoreUnavailableError, match="57014"):
        transaction(connection, slow, failure=BatchFailedError)


def test_other_errors_are_failures(pg: PostgresBackend) -> None:
    with pg.pool.connection() as connection, pytest.raises(BatchFailedError, match="22012"):
        transaction(
            connection,
            lambda cursor: cursor.execute("SELECT 1/0"),
            failure=BatchFailedError,
        )


def test_reads_and_writes_leave_no_open_transaction(pg: PostgresBackend) -> None:
    """ADR-0021 §3: ``autocommit``; nada fica "idle in transaction" depois da chamada."""
    with pg.pool.connection() as connection:
        query(connection, "SELECT 1", {}, failure=StoreOperationError)
        assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        transaction(connection, lambda cursor: cursor.execute("SELECT 1"), failure=BatchFailedError)
        assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        with pytest.raises(StoreOperationError):
            transaction(
                connection, lambda cursor: cursor.execute("SELECT 1/0"), failure=StoreOperationError
            )
        assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE


# ------------------------------------------------------------------ lote (ADR-0022 §6)


async def test_mixed_batch_takes_the_slow_path_and_keeps_every_guarantee(
    pg: PostgresBackend,
) -> None:
    """Teste 2 do ADR-0022: gravado, duplicado ``uq_evt``, duplicado ``uq_off``, texto longo,
    NUL no payload e surrogate isolado, no mesmo lote."""
    pg.acquire(f"consumer:{CHAIN}")
    first = event(1)
    await persist(pg, first)

    good = event(2)
    same_uid = first
    same_offset = event(1, unique_id=uid(901))  # outro uid, mesmo (chain, offset, pk)
    too_long = replace(event(3), module_name="M" * 101)
    with_nul = replace(event(4), payload_json=event(4).payload_json + "\x00")
    with_surrogate = replace(event(5), primary_key="pk\ud800")
    last = event(6)
    result = await persist(
        pg, good, same_uid, same_offset, too_long, with_nul, with_surrogate, last
    )

    assert result.inserted == (good.unique_event_id, last.unique_event_id)
    assert set(result.duplicates) == {same_uid.unique_event_id, same_offset.unique_event_id}
    failed = {f.unique_event_id: f.error_class for f in result.row_failures}
    assert failed == {
        too_long.unique_event_id: "22001",
        with_nul.unique_event_id: "DataError",
        with_surrogate.unique_event_id: "UnicodeEncodeError",
    }
    items = pg.dlq_items(CHAIN)
    assert sorted(i.unique_event_id or "" for i in items) == sorted(failed)
    for item in items:  # a DLQ grava sem NUL nem surrogate (ADR-0021 §6)
        assert item.raw_message is not None
        assert "\x00" not in item.raw_message
        item.raw_message.encode("utf-8")
    outbox = [o.unique_event_id for o in pg.outbox_rows(CHAIN)]
    assert outbox == [first.unique_event_id, good.unique_event_id, last.unique_event_id]
    assert str(pg.offset(CHAIN)) == "6"


async def test_repeated_primary_key_is_a_row_failure_not_a_duplicate(pg: PostgresBackend) -> None:
    """``ON CONFLICT`` só no ``uq_evt``: conflito na PK nunca é engolido (ADR-0022 §2)."""
    pg.acquire(f"consumer:{CHAIN}")
    first, second = event(1), event(2)
    (rec1,) = await records(pg, first)
    (rec2,) = await records(pg, second)
    clash = NewEventRecord(rec1.raw_event_id, second, rec2.status, rec2.outbox)
    result = await pg.events.persist_batch(batch(pg, (rec1, clash)))

    assert result.inserted == (first.unique_event_id,)
    assert result.duplicates == ()
    (failure,) = result.row_failures
    assert (failure.unique_event_id, failure.error_class) == (second.unique_event_id, "23505")
    assert [i.unique_event_id for i in pg.dlq_items(CHAIN)] == [second.unique_event_id]


async def test_outbox_ids_follow_write_order_across_sessions(pg: PostgresBackend) -> None:
    """``CACHE 1`` em ``ohip_outbox_seq``: a API grava entre dois lotes do consumer e os ids
    seguem a ordem de gravação (ADR-0021 §4, ADR-0022 §4)."""
    pg.acquire(f"consumer:{CHAIN}")
    await persist(pg, event(1), event(2))
    stored = await pg.operations.find_event(uid(1))
    assert stored is not None
    message = QueueMessage(
        kind=ExchangeKind.REPROCESS,
        routing_key="ohip.rsv.X",
        message_id=f"reprocess-{uid(1)}",
        body="{}",
    )
    await pg.operations.enqueue(
        chain_code=CHAIN,
        raw_event_id=stored.raw_event_id,
        message=message,
        exchange_name="ohip.reprocess",
    )
    await persist(pg, event(3))

    rows = pg.outbox_rows(CHAIN)
    assert [r.unique_event_id for r in rows] == [
        uid(1),
        uid(2),
        message.message_id,
        uid(3),
    ]
    assert [r.id for r in rows] == sorted(r.id for r in rows)


# ------------------------------------------------------------------ concorrência real


async def test_concurrent_retry_publish_inserts_once(pg: PostgresBackend) -> None:
    """Outra sessão resolve o item enquanto o retry espera o lock: o retry não insere nada."""
    pg.acquire(f"consumer:{CHAIN}")
    await persist(pg, event(1))
    epoch = pg.acquire("publisher")
    (row,) = await pg.outbox.fetch_head(CHAIN, 1)
    await pg.outbox.mark_failed(row.id, 10, "esgotado", epoch)
    (item,) = pg.dlq_items(CHAIN)

    with pg.pool.connection() as rival:
        with rival.transaction():
            cursor = rival.execute(
                "UPDATE ohip_dlq SET resolution = 'RETRIED', resolved_by = 'rival'"
                " WHERE id = %(id)s AND resolution IS NULL",
                {"id": item.id},
            )
            assert cursor.rowcount == 1  # lock na linha, sem commit ainda

            retry = asyncio.create_task(pg.operations.retry_publish(item.id, row.id, "ana"))
            await asyncio.sleep(1.0)
            assert not retry.done()  # esperando o lock do rival
        assert await asyncio.wait_for(retry, timeout=30) is None

    assert [o.status for o in pg.outbox_rows(CHAIN)] == ["FAILED"]  # nenhuma cópia


async def test_zombie_waiting_on_the_fence_loses_after_the_new_owner_commits(
    pg: PostgresBackend,
) -> None:
    """ADR-0008: o novo dono incrementa o epoch (sem commit); a barreira do zumbi espera o
    lock; depois do commit do rival, o UPDATE do zumbi não acha a linha e tudo é desfeito."""
    stale = pg.acquire(f"consumer:{CHAIN}")
    recs = await records(pg, event(1), event(2))

    with pg.pool.connection() as rival:
        with rival.transaction():
            rival.execute(
                "UPDATE ohip_lease SET epoch = epoch + 1 WHERE lease_name = %(n)s",
                {"n": f"consumer:{CHAIN}"},
            )
            zombie = asyncio.create_task(
                pg.events.persist_batch(batch(pg, recs, lease_epoch=stale))
            )
            await asyncio.sleep(1.0)
            assert not zombie.done()  # parado na barreira, esperando o lock do lease
        with pytest.raises(LeaseLostError):
            await asyncio.wait_for(zombie, timeout=30)

    assert pg.raw_count(CHAIN) == 0
    assert pg.outbox_rows(CHAIN) == []
    assert pg.offset(CHAIN) is None


# ------------------------------------------------------------------ índices (11.2, B1, B2)

SEED_ROWS = 20_000
SEED_PENDING_EVERY = 1_000  # 1 linha PENDING a cada 1 000; o resto é SENT (retenção de 90 dias)

_SEED_RAW_SQL = """
INSERT INTO ohip_event_raw (id, chain_code, offset_value, unique_event_id, module_name,
                            event_name, primary_key, payload, received_at)
SELECT nextval('ohip_event_raw_seq'), CASE WHEN g %% 2 = 0 THEN 'ZZT1' ELSE 'ZZT2' END,
       (1000000 + g)::text, 'zzt-seed-' || g, 'RESERVATION', 'UPDATE RESERVATION', 'pk' || g,
       '{}', statement_timestamp() - make_interval(days => g %% 120)
  FROM generate_series(1, %(n)s) AS g"""
_SEED_OUTBOX_SQL = """
INSERT INTO ohip_outbox (id, event_raw_id, chain_code, unique_event_id, routing_key, message,
                         schema_version, status, created_at, sent_at)
SELECT nextval('ohip_outbox_seq'), r.id, r.chain_code, r.unique_event_id, 'ohip.rsv.X', '{}', 1,
       CASE WHEN r.id %% %(every)s = 0 THEN 'PENDING' ELSE 'SENT' END, r.received_at,
       CASE WHEN r.id %% %(every)s = 0 THEN NULL ELSE r.received_at END
  FROM ohip_event_raw r
 WHERE r.unique_event_id LIKE 'zzt-seed-%%'"""


@pytest.fixture
def seeded(pg: PostgresBackend) -> PostgresBackend:
    """Volume com a forma da produção (quase tudo SENT) e estatísticas atualizadas. Sem isso o
    planejador decide com a tabela vazia e o plano não diz nada (B1). Só DML nas chains ZZT;
    ``ANALYZE`` não é DDL (num banco onde o papel não é dono, vira só um aviso)."""
    with pg.pool.connection() as connection:
        with connection.transaction():
            connection.execute(_SEED_RAW_SQL, {"n": SEED_ROWS})
            connection.execute(_SEED_OUTBOX_SQL, {"every": SEED_PENDING_EVERY})
        connection.execute("ANALYZE ohip_event_raw, ohip_outbox, ohip_dlq")
    return pg


def _plan(pg: PostgresBackend, statement: str, params: dict[str, Any], *, seqscan: bool) -> str:
    with pg.pool.connection() as connection, connection.transaction():
        if not seqscan:  # prova que o índice **pode** ser usado
            connection.execute("SET LOCAL enable_seqscan = off")
        rows = connection.execute(f"EXPLAIN {statement}", params).fetchall()
    return "\n".join(row[0] for row in rows)


def test_status_window_uses_the_received_at_index(seeded: PostgresBackend) -> None:
    plan = _plan(seeded, RECENT_EVENTS_SQL, {}, seqscan=False)
    assert "ohip_event_raw_ix_recv" in plan, plan


@pytest.mark.parametrize(
    ("target", "index"),
    [
        (PurgeTarget.RAW, "ohip_event_raw_ix_recv"),
        (PurgeTarget.OUTBOX, "ohip_outbox_ix_sent"),
        (PurgeTarget.DLQ, "ohip_dlq_ix_open"),
    ],
)
def test_purge_uses_the_date_indexes(
    seeded: PostgresBackend, target: PurgeTarget, index: str
) -> None:
    plan = _plan(seeded, delete_sql(target), {"days": 90, "n": 1000}, seqscan=False)
    assert index in plan, plan


@pytest.mark.parametrize(
    ("name", "statement", "params"),
    [
        ("chains_with_pending", CHAINS_WITH_PENDING_SQL, {}),
        ("status_outbox_counts", OUTBOX_COUNTS_SQL, {}),
        ("oldest_pending_global", oldest_pending_sql(None), {}),
        ("oldest_pending_chain", oldest_pending_sql("ZZT1"), {"chain_code": "ZZT1"}),
    ],
    ids=["chains_with_pending", "status_outbox_counts", "oldest_global", "oldest_chain"],
)
def test_pending_outbox_reads_never_scan_the_whole_outbox(
    seeded: PostgresBackend, name: str, statement: str, params: dict[str, Any]
) -> None:
    """B2: o publisher (a cada rodada), o ``/status`` e o ``/metrics`` leem o que está pendente.
    Com o outbox retido (quase tudo SENT), nenhuma dessas leituras pode varrer a tabela: o
    custo tem de depender das chains e das linhas pendentes, não do histórico. Plano com a
    varredura sequencial **ligada** (o que o banco escolheria de verdade)."""
    plan = _plan(seeded, statement, params, seqscan=True)
    assert "Seq Scan on ohip_outbox" not in plan, f"{name}:\n{plan}"


# ------------------------------------------------------------------ enricher (ADR-0022 §5)


@pytest.mark.skipif(
    os.environ.get("TEST_POSTGRES_LOCAL_CONTAINER") != "sim",
    reason="tabela TEMP só no contêiner criado pelo make test-db",
)
async def test_enricher_on_conflict_is_conditional_on_the_offset(pg: PostgresBackend) -> None:
    """Executa o ``INSERT ... ON CONFLICT DO UPDATE ... WHERE`` do enricher contra o banco
    real, antes da Q-1, numa tabela TEMP (some com a sessão; só no contêiner do test-db)."""
    assert _settings is not None
    pg.acquire(f"consumer:{CHAIN}")
    await persist(pg, event(1))
    stored = await pg.operations.find_event(uid(1))
    assert stored is not None
    single = pg_db.open_pool(
        _settings.model_copy(update={"pool_min": 1, "pool_max": 1}),
        environment=Environment.DESENVOLVIMENTO,
        application_name="ohip-test-enricher",
    )
    try:
        with single.connection() as connection:
            connection.execute(
                "CREATE TEMP TABLE zzt_domain (chain_code varchar(20), hotel_key varchar(50),"
                " primary_key varchar(100), status varchar(20), source_event_raw_id bigint,"
                " source_offset_num numeric(20), PRIMARY KEY (chain_code, hotel_key, primary_key))"
            )
        store = PostgresEnrichmentStore(pg_db.PooledSession(single), code_version="test")

        def write(status: str, offset: int) -> DomainWrite:
            return DomainWrite(
                table="zzt_domain",
                key={"chain_code": CHAIN, "hotel_key": "#CHAIN", "primary_key": "R1"},
                values={"status": status},
                source_event_raw_id=stored.raw_event_id,
                source_offset_num=offset,
            )

        raw = stored.raw_event_id
        assert await store.apply(raw, [write("RESERVED", 100)], ProcessingStatus.NORMALIZED) == 0
        assert await store.apply(raw, [write("ANTIGO", 50)], ProcessingStatus.NORMALIZED) == 1
        assert await store.apply(raw, [write("CHECKED IN", 150)], ProcessingStatus.NORMALIZED) == 0
        assert await store.apply(raw, [write("CHECKED IN", 150)], ProcessingStatus.NORMALIZED) == 0
        with single.connection() as connection:
            rows = connection.execute("SELECT status, source_offset_num FROM zzt_domain").fetchall()
        assert rows == [("CHECKED IN", 150)]  # o antigo não sobrescreveu; reaplicar é idempotente
    finally:
        single.close()
    assert (
        await pg.operations.get_event(stored.raw_event_id)
    ).status is ProcessingStatus.NORMALIZED  # type: ignore[union-attr]
