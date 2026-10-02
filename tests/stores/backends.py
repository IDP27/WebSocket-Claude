"""Backends da suíte de cenários dos stores: o fake em memória e o Oracle de teste.

Os **mesmos cenários** rodam nos dois (PLAN, Fase 3): o fake é o critério de aceitação do
adapter, e o Oracle prova que o adapter cumpre esse contrato de verdade.

Oracle de teste — travas de segurança (CLAUDE.md: nunca DDL em banco real):
- só roda com ``TEST_ORACLE_DSN``/``TEST_ORACLE_USER``/``TEST_ORACLE_PASSWORD`` **e**
  ``TEST_ORACLE_DISPOSABLE_SCHEMA=sim`` (o schema é descartável, criado pelo DBA com os
  scripts de ``sql/``); ``TEST_ORACLE_CLIENT_LIB_DIR`` opcional;
- os testes **não rodam DDL**; só DML nas linhas das chains ``ZZT...``, apagadas e semeadas
  de novo antes de cada teste;
- antes de qualquer DML, recusa o schema se houver chain que não seja ``ZZT...`` em
  OHIP_OFFSET/OHIP_OUTBOX ou lease ``publisher`` ativo com outro dono (proteção contra apontar
  por engano para homologação).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from pydantic import SecretStr
from tests.fakes.memory import EXCHANGES, InMemoryDatabase, MemoryLeaseStore

from ohip_streaming.adapters.oracle.database import (
    DedicatedSession,
    Pool,
    PooledSession,
    open_pool,
)
from ohip_streaming.adapters.oracle.event_store import OracleEventStore
from ohip_streaming.adapters.oracle.lease_store import OracleLeaseStore
from ohip_streaming.adapters.oracle.operations_store import OracleOperationsStore
from ohip_streaming.adapters.oracle.outbox_store import OracleOutboxStore
from ohip_streaming.adapters.oracle.replay_store import OracleReplayStore
from ohip_streaming.adapters.oracle.status_store import OracleConsumerStatusStore
from ohip_streaming.application.ports import (
    ConsumerStatusStore,
    EventStore,
    LeaseStore,
    OperationsStore,
    OutboxStore,
    ReplayStore,
)
from ohip_streaming.config import OracleSettings
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.offset import Offset

TEST_CHAINS = ("ZZT1", "ZZT2")
_ENV = ("TEST_ORACLE_DSN", "TEST_ORACLE_USER", "TEST_ORACLE_PASSWORD")


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
    events: EventStore
    outbox: OutboxStore
    replay: ReplayStore
    operations: OperationsStore
    status: ConsumerStatusStore
    leases: LeaseStore

    def acquire(self, lease: str) -> int:
        """Novo dono do lease: devolve o epoch novo (o anterior vira zumbi)."""
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

    def acquire(self, lease: str) -> int:
        return self.db.acquire(lease)

    def epoch(self, lease: str) -> int:
        return self.db.leases[lease]

    def broken(self, event: Event) -> Event:
        self.db.row_errors[event.unique_event_id] = ("ORA-12899", "value too large")
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


# ======================================================================= Oracle


_OUTBOX_SQL = """
SELECT id, unique_event_id, status, exchange_name, attempts
  FROM ohip_outbox WHERE chain_code = :chain ORDER BY id"""

_DLQ_SQL = """
SELECT id, stage, unique_event_id, offset_value, error_class || ': ' || error_message,
       resolution, attempts, CASE WHEN retry_requested_at IS NULL THEN 0 ELSE 1 END, raw_message
  FROM ohip_dlq WHERE chain_code = :chain ORDER BY id"""

_CLEANUP = (
    "DELETE FROM ohip_dlq WHERE chain_code = :chain",
    "DELETE FROM ohip_outbox WHERE chain_code = :chain",
    "DELETE FROM ohip_event_raw WHERE chain_code = :chain",
    "DELETE FROM ohip_replay_request WHERE chain_code = :chain",
    "DELETE FROM ohip_offset WHERE chain_code = :chain",
    "DELETE FROM ohip_consumer_status WHERE chain_code = :chain",
    "DELETE FROM ohip_lease WHERE lease_name = 'consumer:' || :chain",
)
_SEED = (
    "INSERT INTO ohip_lease (lease_name, epoch, expires_at)"
    " VALUES ('consumer:' || :chain, 0, SYS_EXTRACT_UTC(SYSTIMESTAMP))",
    "INSERT INTO ohip_offset (chain_code, last_offset) VALUES (:chain, NULL)",
    "INSERT INTO ohip_consumer_status (chain_code, state) VALUES (:chain, 'STOPPED')",
)


# Dados fora das chains de teste = schema não descartável.
_FOREIGN_DATA_SQL = """
SELECT chain_code FROM ohip_offset WHERE chain_code NOT LIKE 'ZZT%'
UNION
SELECT chain_code FROM ohip_outbox WHERE chain_code NOT LIKE 'ZZT%' AND ROWNUM <= 5"""
_LIVE_PUBLISHER_SQL = """
SELECT 1 FROM ohip_lease
 WHERE lease_name = 'publisher'
   AND expires_at > SYS_EXTRACT_UTC(SYSTIMESTAMP)
   AND NVL(owner, '-') <> 'zzt-test'"""
# O teste vira dono, com prazo já vencido: nunca parece um publisher real em execução.
_ACQUIRE_SQL = """
UPDATE ohip_lease
   SET epoch = epoch + 1, owner = 'zzt-test', expires_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE lease_name = :lease"""


def oracle_test_settings() -> OracleSettings | None:
    if not all(os.environ.get(name) for name in _ENV):
        return None
    if os.environ.get("TEST_ORACLE_DISPOSABLE_SCHEMA") != "sim":
        return None
    lib_dir = os.environ.get("TEST_ORACLE_CLIENT_LIB_DIR")
    return OracleSettings(
        dsn=os.environ["TEST_ORACLE_DSN"],
        user=os.environ["TEST_ORACLE_USER"],
        password=SecretStr(os.environ["TEST_ORACLE_PASSWORD"]),
        client_lib_dir=Path(lib_dir) if lib_dir else None,
    )


class OracleBackend(Backend):
    name = "oracle"

    def __init__(self, pool: Pool) -> None:
        self.pool = pool
        self._writer = DedicatedSession(pool, call_timeout_ms=30_000)
        pooled = PooledSession(pool, call_timeout_ms=30_000)
        self.events = OracleEventStore(self._writer, exchange_names=EXCHANGES, code_version="test")
        self.outbox = OracleOutboxStore(pooled, code_version="test")
        self.replay = OracleReplayStore(pooled)
        self.operations = OracleOperationsStore(pooled)
        self.status = OracleConsumerStatusStore(pooled)
        self.leases = OracleLeaseStore(pooled)
        self._reset()

    @classmethod
    def open(cls, settings: OracleSettings) -> OracleBackend:
        return cls(open_pool(settings))

    def _execute(self, statements: tuple[str, ...] | list[str], **parameters: Any) -> int:
        connection = self.pool.acquire()
        try:
            cursor = connection.cursor()
            rowcount = 0
            for statement in statements:
                cursor.execute(statement, parameters)
                rowcount = cursor.rowcount
            connection.commit()
            return int(rowcount)
        finally:
            self.pool.release(connection)

    def _refuse_if_not_disposable(self) -> None:
        """Recusa um schema com dados reais ou com publisher ativo (ex.: homologação)."""
        foreign = self.query(_FOREIGN_DATA_SQL)
        if foreign:
            raise RuntimeError(
                "schema de teste tem chains que não são de teste "
                f"({[row[0] for row in foreign][:5]}): recusado"
            )
        if self.query(_LIVE_PUBLISHER_SQL):
            raise RuntimeError("lease 'publisher' ativo com dono que não é de teste: recusado")

    def _reset(self) -> None:
        self._refuse_if_not_disposable()
        for chain in TEST_CHAINS:
            self._execute(_CLEANUP, chain=chain)
            self._execute(_SEED, chain=chain)
        # 'publisher' é semeado pelo provisionamento; os testes só incrementam o epoch.

    def query(self, statement: str, **parameters: Any) -> list[Any]:
        connection = self.pool.acquire()
        try:
            cursor = connection.cursor()
            cursor.execute(statement, parameters)
            return [tuple(_read(v) for v in row) for row in cursor.fetchall()]
        finally:
            self.pool.release(connection)

    def acquire(self, lease: str) -> int:
        self._execute([_ACQUIRE_SQL], lease=lease)
        return self.epoch(lease)

    def epoch(self, lease: str) -> int:
        ((value,),) = self.query(
            "SELECT epoch FROM ohip_lease WHERE lease_name = :lease", lease=lease
        )
        return int(value)

    def broken(self, event: Event) -> Event:
        return replace(event, module_name="M" * 101)  # VARCHAR2(100): ORA-12899 nesta linha

    def offset(self, chain: str) -> Offset | None:
        ((value,),) = self.query(
            "SELECT last_offset FROM ohip_offset WHERE chain_code = :chain", chain=chain
        )
        return Offset(value) if value is not None else None

    def outbox_rows(self, chain: str) -> list[OutboxView]:
        return [
            OutboxView(int(r[0]), r[1], r[2], r[3], int(r[4]))
            for r in self.query(_OUTBOX_SQL, chain=chain)
        ]

    def dlq_items(self, chain: str) -> list[DlqView]:
        return [
            DlqView(int(r[0]), r[1], r[2], r[3], r[4], r[5], int(r[6]), bool(r[7]), r[8])
            for r in self.query(_DLQ_SQL, chain=chain)
        ]

    def raw_count(self, chain: str) -> int:
        ((value,),) = self.query(
            "SELECT COUNT(*) FROM ohip_event_raw WHERE chain_code = :chain", chain=chain
        )
        return int(value)

    def close(self) -> None:
        self._writer.close()
        self.pool.close(force=True)


def _read(value: Any) -> Any:
    return value.read() if hasattr(value, "read") else value
