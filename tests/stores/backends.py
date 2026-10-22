"""Backends da suíte de cenários dos stores: o fake em memória e o PostgreSQL de teste.

Os **mesmos cenários** rodam nos dois (PLAN, Fases 3 e 11): o fake é o critério de aceitação
do adapter, e o PostgreSQL prova que o adapter cumpre esse contrato de verdade.

PostgreSQL de teste — travas de segurança (CLAUDE.md: DDL nunca em banco existente):
- só roda com ``TEST_POSTGRES_DSN`` **e** ``TEST_POSTGRES_DISPOSABLE=sim``. O ``make test-db``
  cria um contêiner descartável, aplica ``sql/postgres/`` nele e grava as variáveis em
  ``.test-db.env`` (ADR-0021 §9);
- os testes **não rodam DDL**; só DML nas linhas das chains ``ZZT...``, apagadas e semeadas
  de novo antes de cada teste;
- antes de qualquer DML, recusa o banco se houver chain que não seja ``ZZT...`` em
  OHIP_OFFSET/OHIP_OUTBOX ou lease ``publisher`` ativo com outro dono (proteção contra apontar
  por engano para homologação);
- uma execução da suíte por vez em cada banco: a primeira abertura do processo segura um
  advisory lock de sessão até o processo sair, e outra execução (outro terminal, um subagente)
  espera aqui em vez de apagar as chains ZZT no meio dos testes desta.
"""

from __future__ import annotations

import atexit
import os
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

import psycopg
from pydantic import SecretStr
from tests.fakes.memory import (
    EXCHANGES,
    InMemoryDatabase,
    MemoryLeaseStore,
    MemoryMonitoringStore,
)

from ohip_streaming.adapters.postgres import database as pg
from ohip_streaming.adapters.postgres.enrichment_store import PostgresEnrichmentStore
from ohip_streaming.adapters.postgres.event_store import PostgresEventStore
from ohip_streaming.adapters.postgres.lease_store import PostgresLeaseStore
from ohip_streaming.adapters.postgres.monitoring_store import PostgresMonitoringStore
from ohip_streaming.adapters.postgres.operations_store import PostgresOperationsStore
from ohip_streaming.adapters.postgres.outbox_store import PostgresOutboxStore
from ohip_streaming.adapters.postgres.purge_store import PostgresPurgeStore
from ohip_streaming.adapters.postgres.replay_store import PostgresReplayStore
from ohip_streaming.adapters.postgres.status_store import PostgresConsumerStatusStore
from ohip_streaming.application.ports import (
    ConsumerStatusStore,
    EnrichmentStore,
    EventStore,
    LeaseStore,
    MonitoringStore,
    OperationsStore,
    OutboxStore,
    PurgeStore,
    ReplayStore,
)
from ohip_streaming.config import Environment, PostgresSettings
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.leases import consumer_lease
from ohip_streaming.domain.offset import Offset

TEST_CHAINS = ("ZZT1", "ZZT2")
# Advisory lock de sessão que serializa as execuções da suíte num mesmo banco de teste.
TEST_SESSION_LOCK = 0x0F1E_7E57
SESSION_LOCK_WAIT_MS = 15 * 60 * 1000  # uma execução completa leva menos de 1 min


@dataclass(frozen=True)
class DlqView:
    id: int
    stage: str
    unique_event_id: str | None
    offset: str | None
    error: str
    resolution: str | None
    attempts: int
    retry_requested: bool
    raw_message: str | None


@dataclass(frozen=True)
class OutboxView:
    id: int
    unique_event_id: str
    status: str
    exchange_name: str
    attempts: int


class Backend:
    name: str
    broken_code = "22001"  # código do erro que ``broken`` provoca (valor longo demais)
    events: EventStore
    outbox: OutboxStore
    replay: ReplayStore
    operations: OperationsStore
    status: ConsumerStatusStore
    leases: LeaseStore
    monitoring: MonitoringStore
    enrichment: EnrichmentStore
    purge: PurgeStore

    def acquire(self, lease: str) -> int:
        """Novo dono do lease: devolve o epoch novo (o anterior vira zumbi)."""
        raise NotImplementedError

    def age(self, chain: str, days: int) -> None:
        """Envelhece em ``days`` os horários de bruto, outbox e DLQ da chain (expurgo)."""
        raise NotImplementedError

    def epoch(self, lease: str) -> int:
        raise NotImplementedError

    def broken(self, event: Event) -> Event:
        """Evento cujo INSERT falha por linha (não é duplicado nem falta de recurso)."""
        raise NotImplementedError

    def offset(self, chain: str) -> Offset | None:
        raise NotImplementedError

    def outbox_rows(self, chain: str) -> list[OutboxView]:
        raise NotImplementedError

    def dlq_items(self, chain: str) -> list[DlqView]:
        raise NotImplementedError

    def raw_count(self, chain: str) -> int:
        raise NotImplementedError

    def query(self, statement: str, **parameters: Any) -> list[Any]:
        raise NotImplementedError

    def close(self) -> None:
        pass


# ======================================================================= memória


class MemoryBackend(Backend):
    name = "memory"

    def __init__(self) -> None:
        self.db = InMemoryDatabase()
        for chain in TEST_CHAINS:
            self.db.provision_chain(chain)
        self.db.leases.setdefault("publisher", 0)
        self.events = self.outbox = self.replay = self.operations = self.status = self.db
        self.leases = MemoryLeaseStore(self.db)
        self.monitoring = MemoryMonitoringStore(self.db)
        self.enrichment = self.purge = self.db

    def acquire(self, lease: str) -> int:
        return self.db.acquire(lease)

    def age(self, chain: str, days: int) -> None:
        delta = timedelta(days=days)
        for row in self.db.raw.values():
            if row.event.chain_code == chain:
                row.event = replace(row.event, received_at=row.event.received_at - delta)
        for o in self.db.outbox.values():
            if o.chain_code == chain:
                o.created_at -= delta
                o.sent_at = o.sent_at - delta if o.sent_at else None
        for d in self.db.dlq.values():
            if d.chain_code == chain:
                d.created_at -= delta
                d.resolved_at = d.resolved_at - delta if d.resolved_at else None

    def epoch(self, lease: str) -> int:
        return self.db.leases[lease]

    def broken(self, event: Event) -> Event:
        self.db.row_errors[event.unique_event_id] = ("22001", "value too long")
        return event

    def offset(self, chain: str) -> Offset | None:
        return self.db.offsets[chain].last_offset

    def outbox_rows(self, chain: str) -> list[OutboxView]:
        return [
            OutboxView(o.id, o.unique_event_id, o.status, o.exchange_name, o.attempts)
            for o in sorted(self.db.outbox.values(), key=lambda o: o.id)
            if o.chain_code == chain
        ]

    def dlq_items(self, chain: str) -> list[DlqView]:
        return [
            DlqView(
                d.id,
                d.stage.value,
                d.unique_event_id,
                d.offset,
                d.error,
                d.resolution,
                d.attempts,
                d.retry_requested_at is not None,
                d.raw_message,
            )
            for d in sorted(self.db.dlq.values(), key=lambda d: d.id)
            if d.chain_code == chain
        ]

    def raw_count(self, chain: str) -> int:
        return sum(1 for r in self.db.raw.values() if r.event.chain_code == chain)


# ======================================================================= PostgreSQL


_PG_AGE_SQL = (
    """UPDATE ohip_event_raw SET received_at = received_at - make_interval(days => %(days)s)
 WHERE chain_code = %(chain)s""",
    """UPDATE ohip_outbox SET created_at = created_at - make_interval(days => %(days)s),
       sent_at = sent_at - make_interval(days => %(days)s)
 WHERE chain_code = %(chain)s""",
    """UPDATE ohip_dlq SET created_at = created_at - make_interval(days => %(days)s),
       resolved_at = resolved_at - make_interval(days => %(days)s)
 WHERE chain_code = %(chain)s""",
)
_PG_OUTBOX_SQL = """
SELECT id, unique_event_id, status, exchange_name, attempts
  FROM ohip_outbox WHERE chain_code = %(chain)s ORDER BY id"""
_PG_DLQ_SQL = """
SELECT id, stage, unique_event_id, offset_value, error_class || ': ' || error_message,
       resolution, attempts, CASE WHEN retry_requested_at IS NULL THEN 0 ELSE 1 END, raw_message
  FROM ohip_dlq WHERE chain_code = %(chain)s ORDER BY id"""
_PG_CLEANUP = (
    "DELETE FROM ohip_dlq WHERE chain_code = %(chain)s",
    "DELETE FROM ohip_outbox WHERE chain_code = %(chain)s",
    "DELETE FROM ohip_event_raw WHERE chain_code = %(chain)s",
    "DELETE FROM ohip_replay_request WHERE chain_code = %(chain)s",
    "DELETE FROM ohip_offset WHERE chain_code = %(chain)s",
    "DELETE FROM ohip_consumer_status WHERE chain_code = %(chain)s",
    f"DELETE FROM ohip_lease WHERE lease_name = '{consumer_lease('')}' || %(chain)s",  # noqa: S608
)
_PG_SEED = (
    "INSERT INTO ohip_lease (lease_name, epoch, expires_at)"  # noqa: S608 - prefixo do domínio
    f" VALUES ('{consumer_lease('')}' || %(chain)s, 0, statement_timestamp())",
    "INSERT INTO ohip_offset (chain_code, last_offset) VALUES (%(chain)s, NULL)",
    "INSERT INTO ohip_consumer_status (chain_code, state) VALUES (%(chain)s, 'STOPPED')",
)
_PG_FOREIGN_DATA_SQL = """
SELECT chain_code FROM ohip_offset WHERE chain_code NOT LIKE 'ZZT%%'
UNION
(SELECT chain_code FROM ohip_outbox WHERE chain_code NOT LIKE 'ZZT%%' LIMIT 5)"""
_PG_LIVE_PUBLISHER_SQL = """
SELECT 1 FROM ohip_lease
 WHERE lease_name = 'publisher'
   AND expires_at > statement_timestamp()
   AND COALESCE(owner, '-') <> 'zzt-test'"""
_PG_ACQUIRE_SQL = """
UPDATE ohip_lease
   SET epoch = epoch + 1, owner = 'zzt-test', expires_at = statement_timestamp()
 WHERE lease_name = %(lease)s"""


_session_lock: list[psycopg.Connection[Any]] = []  # a conexão que segura TEST_SESSION_LOCK


def hold_session_lock(settings: PostgresSettings) -> None:
    """Segura a trava até este processo sair (na primeira abertura do processo)."""
    if not _session_lock:
        connection = take_session_lock(settings, wait_ms=SESSION_LOCK_WAIT_MS)
        _session_lock.append(connection)
        atexit.register(connection.close)


def take_session_lock(settings: PostgresSettings, *, wait_ms: int) -> psycopg.Connection[Any]:
    """Espera outra execução da suíte terminar e devolve a conexão que segura a trava.

    A trava é da conexão, não do pool: o servidor a libera sozinho se o processo morrer."""
    connection = psycopg.connect(
        **pg.connection_kwargs(
            settings,
            environment=Environment.DESENVOLVIMENTO,
            application_name="ohip-test-lock",
            statement_timeout_ms=wait_ms,
        )
    )
    try:
        connection.execute("SELECT pg_advisory_lock(%(key)s)", {"key": TEST_SESSION_LOCK})
    except psycopg.errors.QueryCanceled as exc:
        connection.close()
        raise RuntimeError(
            f"outra execução dos testes segura o banco de teste há mais de {wait_ms} ms: recusado"
        ) from exc
    return connection


def postgres_test_settings() -> PostgresSettings | None:
    dsn = os.environ.get("TEST_POSTGRES_DSN")
    if not dsn or os.environ.get("TEST_POSTGRES_DISPOSABLE") != "sim":
        return None
    from psycopg.conninfo import conninfo_to_dict

    parts = conninfo_to_dict(dsn)
    return PostgresSettings(
        host=str(parts["host"]),
        port=int(parts.get("port") or 5432),
        db=str(parts["dbname"]),
        user=str(parts["user"]),
        password=SecretStr(str(parts.get("password") or "")),
        sslmode=parts.get("sslmode") or "disable",
        statement_timeout_ms=30_000,
    )


class PostgresBackend(Backend):
    name = "postgres"

    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self._writer = pg.DedicatedSession(pool)
        pooled = pg.PooledSession(pool)
        self.events = PostgresEventStore(
            self._writer, exchange_names=EXCHANGES, code_version="test"
        )
        self.outbox = PostgresOutboxStore(pooled, code_version="test")
        self.replay = PostgresReplayStore(pooled)
        self.operations = PostgresOperationsStore(pooled)
        self.status = PostgresConsumerStatusStore(pooled)
        self.leases = PostgresLeaseStore(pooled)
        self.monitoring = PostgresMonitoringStore(pooled)
        self.enrichment = PostgresEnrichmentStore(pooled, code_version="test")
        self.purge = PostgresPurgeStore(pooled)
        self._reset()

    @classmethod
    def open(cls, settings: PostgresSettings) -> PostgresBackend:
        hold_session_lock(settings)
        return cls(
            pg.open_pool(
                settings, environment=Environment.DESENVOLVIMENTO, application_name="ohip-test"
            )
        )

    def _execute(self, statements: tuple[str, ...] | list[str], **parameters: Any) -> int:
        with self.pool.connection() as connection, connection.transaction():
            cursor = connection.cursor()
            rowcount = 0
            for statement in statements:
                cursor.execute(statement, parameters)
                rowcount = cursor.rowcount
            return int(rowcount)

    def _refuse_if_not_disposable(self) -> None:
        """Recusa um banco com dados reais ou com publisher ativo (ex.: homologação)."""
        foreign = self.query(_PG_FOREIGN_DATA_SQL)
        if foreign:
            raise RuntimeError(
                "banco de teste tem chains que não são de teste "
                f"({[row[0] for row in foreign][:5]}): recusado"
            )
        if self.query(_PG_LIVE_PUBLISHER_SQL):
            raise RuntimeError("lease 'publisher' ativo com dono que não é de teste: recusado")

    def _reset(self) -> None:
        self._refuse_if_not_disposable()
        for chain in TEST_CHAINS:
            self._execute(_PG_CLEANUP, chain=chain)
            self._execute(_PG_SEED, chain=chain)

    def query(self, statement: str, **parameters: Any) -> list[Any]:
        with self.pool.connection() as connection:
            cursor = connection.execute(statement, parameters or None)
            return [tuple(row) for row in cursor.fetchall()]

    def acquire(self, lease: str) -> int:
        self._execute([_PG_ACQUIRE_SQL], lease=lease)
        return self.epoch(lease)

    def age(self, chain: str, days: int) -> None:
        self._execute(_PG_AGE_SQL, chain=chain, days=days)  # DML no banco descartável

    def epoch(self, lease: str) -> int:
        ((value,),) = self.query(
            "SELECT epoch FROM ohip_lease WHERE lease_name = %(lease)s", lease=lease
        )
        return int(value)

    def broken(self, event: Event) -> Event:
        return replace(event, module_name="M" * 101)  # varchar(100): 22001 nesta linha

    def offset(self, chain: str) -> Offset | None:
        ((value,),) = self.query(
            "SELECT last_offset FROM ohip_offset WHERE chain_code = %(chain)s", chain=chain
        )
        return Offset(value) if value is not None else None

    def outbox_rows(self, chain: str) -> list[OutboxView]:
        return [
            OutboxView(int(r[0]), r[1], r[2], r[3], int(r[4]))
            for r in self.query(_PG_OUTBOX_SQL, chain=chain)
        ]

    def dlq_items(self, chain: str) -> list[DlqView]:
        return [
            DlqView(int(r[0]), r[1], r[2], r[3], r[4], r[5], int(r[6]), bool(r[7]), r[8])
            for r in self.query(_PG_DLQ_SQL, chain=chain)
        ]

    def raw_count(self, chain: str) -> int:
        ((value,),) = self.query(
            "SELECT COUNT(*) FROM ohip_event_raw WHERE chain_code = %(chain)s", chain=chain
        )
        return int(value)

    def close(self) -> None:
        self._writer.close()
        self.pool.close()
