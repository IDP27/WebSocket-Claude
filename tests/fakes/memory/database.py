"""O "banco" em memória: um objeto que implementa todos os ports do Oracle."""

from __future__ import annotations

from dataclasses import dataclass

from tests.fakes.memory.enrichment import EnrichmentFake
from tests.fakes.memory.event_store import EventStoreFake
from tests.fakes.memory.operations import OperationsFake
from tests.fakes.memory.outbox import OutboxFake
from tests.fakes.memory.purge import PurgeFake
from tests.fakes.memory.replay import ReplayFake
from tests.fakes.memory.status import StatusFake


@dataclass
class InMemoryDatabase(
    EventStoreFake,
    OutboxFake,
    ReplayFake,
    StatusFake,
    PurgeFake,
    OperationsFake,
    EnrichmentFake,
):
    """Implementa os 7 ports do Oracle: EventStore, OutboxStore, ReplayStore,
    ConsumerStatusStore, PurgeStore, OperationsStore e EnrichmentStore.

    Cada port fica numa classe própria (refatoração 5 do /entender); todas compartilham o
    estado de ``MemoryState``."""
