"""API do fake em memória (refatoração 5 do /entender, caracterização).

Escrito antes de dividir ``tests/fakes/memory.py`` por port: todo nome importado pelos
testes continua em ``tests.fakes.memory`` e o ``InMemoryDatabase`` continua implementando os
mesmos ports, com o mesmo estado (campos) e os mesmos métodos.
"""

from __future__ import annotations

import dataclasses
import inspect

from tests.fakes import memory

from ohip_streaming.application import ports

PUBLIC = {
    "DlqRecord", "EXCHANGES", "FakeClock", "FakeFetcher", "FakeMetrics", "FakePublisher",
    "FakeQueueDedup", "FakeSeenCache", "FakeTokenCache", "FakeTokenIssuer", "InMemoryDatabase",
    "MemoryLeaseStore", "MemoryMonitoringStore", "OffsetState", "OutboxRecord", "RawRow",
    "StatusRow", "T0",
}  # fmt: skip

FIELDS = [
    "clock", "leases", "offsets", "raw", "outbox", "dlq", "replays", "cancelled_by", "statuses",
    "domain_tables", "fail_persist", "fail_persist_when", "row_errors", "fail_apply",
    "fail_status", "fail_add_dlq", "persisted_batches", "_sequences",
]  # fmt: skip

# Ports que o InMemoryDatabase implementa (os testes de cenário usam o mesmo objeto para todos).
PORTS = (
    ports.EventStore,
    ports.OutboxStore,
    ports.ReplayStore,
    ports.ConsumerStatusStore,
    ports.OperationsStore,
    ports.EnrichmentStore,
    ports.PurgeStore,
)


def test_every_public_name_is_still_importable() -> None:
    assert sorted(name for name in PUBLIC if not hasattr(memory, name)) == []


def test_database_state_is_unchanged() -> None:
    assert [f.name for f in dataclasses.fields(memory.InMemoryDatabase)] == FIELDS


def test_database_implements_every_port_method() -> None:
    db = memory.InMemoryDatabase()
    for port in PORTS:
        methods = {n for n, _ in inspect.getmembers(port) if not n.startswith("_")}
        missing = sorted(n for n in methods if not callable(getattr(db, n, None)))
        assert missing == [], port.__name__
        for name in methods:  # mesma assinatura do port (nomes dos parâmetros)
            expected = list(inspect.signature(getattr(port, name)).parameters)[1:]
            assert list(inspect.signature(getattr(db, name)).parameters) == expected, name
