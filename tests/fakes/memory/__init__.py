"""Implementações em memória dos ports, com o mesmo contrato do adapter Oracle (Fase 3).

Simulam o que importa para os casos de uso: as duas constraints UNIQUE do evento bruto, a
barreira de epoch (ADR-0008), a atomicidade da transação do lote e a injeção de falhas.

Pacote por port (refatoração 5 do /entender); os nomes continuam importáveis daqui.
"""

from tests.fakes.memory.basics import (
    EXCHANGES,
    T0,
    FakeClock,
    FakeFetcher,
    FakeMetrics,
    FakePublisher,
    FakeQueueDedup,
    FakeSeenCache,
)
from tests.fakes.memory.database import InMemoryDatabase
from tests.fakes.memory.lease import MemoryLeaseStore
from tests.fakes.memory.monitoring import MemoryMonitoringStore
from tests.fakes.memory.rows import DlqRecord, OffsetState, OutboxRecord, RawRow, StatusRow
from tests.fakes.memory.token import FakeTokenCache, FakeTokenIssuer

__all__ = [
    "EXCHANGES",
    "T0",
    "DlqRecord",
    "FakeClock",
    "FakeFetcher",
    "FakeMetrics",
    "FakePublisher",
    "FakeQueueDedup",
    "FakeSeenCache",
    "FakeTokenCache",
    "FakeTokenIssuer",
    "InMemoryDatabase",
    "MemoryLeaseStore",
    "MemoryMonitoringStore",
    "OffsetState",
    "OutboxRecord",
    "RawRow",
    "StatusRow",
]
