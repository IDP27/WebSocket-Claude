"""Estado comum do "banco" em memória e apoio usado por mais de um port."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ohip_streaming.application.errors import (
    LeaseLostError,
)
from ohip_streaming.application.ports import (
    BatchToPersist,
    ReplayRequest,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.messages import QueueMessage
from tests.fakes.memory.basics import EXCHANGES, FakeClock
from tests.fakes.memory.rows import DlqRecord, OffsetState, OutboxRecord, RawRow, StatusRow


@dataclass
class MemoryState:
    """Estado comum do "banco" em memória e apoio usado por mais de um port."""

    clock: FakeClock = field(default_factory=FakeClock)
    leases: dict[str, int] = field(default_factory=dict)
    offsets: dict[str, OffsetState] = field(default_factory=dict)
    raw: dict[int, RawRow] = field(default_factory=dict)
    outbox: dict[int, OutboxRecord] = field(default_factory=dict)
    dlq: dict[int, DlqRecord] = field(default_factory=dict)
    replays: dict[int, ReplayRequest] = field(default_factory=dict)
    cancelled_by: dict[int, str] = field(default_factory=dict)
    statuses: dict[str, StatusRow] = field(default_factory=dict)  # OHIP_CONSUMER_STATUS
    domain_tables: dict[tuple[str, tuple[Any, ...]], tuple[dict[str, Any], int]] = field(
        default_factory=dict
    )
    # Injeção de falhas
    fail_persist: list[Exception] = field(default_factory=list)  # consumidas a cada chamada
    fail_persist_when: Callable[[BatchToPersist], Exception | None] | None = None
    row_errors: dict[str, tuple[str, str]] = field(default_factory=dict)  # uid → (classe, msg)
    fail_apply: list[Exception] = field(default_factory=list)
    fail_status: list[Exception] = field(default_factory=list)  # OHIP_CONSUMER_STATUS
    fail_add_dlq: list[Exception] = field(default_factory=list)
    persisted_batches: list[BatchToPersist] = field(default_factory=list)
    # Uma sequence por tabela, como na DDL (sql/001_sequences.sql).
    _sequences: Counter[str] = field(default_factory=Counter)

    def provision_chain(self, chain_code: str) -> None:
        self.offsets.setdefault(chain_code, OffsetState())
        self.leases.setdefault(f"consumer:{chain_code}", 0)
        self.statuses.setdefault(chain_code, StatusRow(ConsumerState.STOPPED))

    def acquire(self, lease_name: str) -> int:
        self.leases[lease_name] = self.leases.get(lease_name, 0) + 1
        return self.leases[lease_name]

    def _next_id(self, table: str) -> int:
        self._sequences[table] += 1
        return self._sequences[table]

    def _fence(self, lease_name: str, epoch: int) -> None:
        if self.leases.get(lease_name) != epoch:
            raise LeaseLostError(f"epoch {epoch} não é mais o dono de {lease_name}")

    def _raw_by_uid(self, unique_event_id: str) -> RawRow | None:
        return next(
            (r for r in self.raw.values() if r.event.unique_event_id == unique_event_id), None
        )

    def _open_dlq(self, item_id: int | None) -> DlqRecord | None:
        item = self.dlq.get(item_id) if item_id is not None else None
        return item if item is not None and item.resolution is None else None

    def _resolve(self, item: DlqRecord, resolved_by: str) -> None:
        item.resolution, item.resolved_by = "RETRIED", resolved_by
        item.resolved_at = self.clock.now()
        item.retry_requested_at = item.retry_requested_by = None

    def _outbox_record(
        self, chain_code: str, raw_event_id: int, message: QueueMessage
    ) -> OutboxRecord:
        return OutboxRecord(
            id=0,
            chain_code=chain_code,
            raw_event_id=raw_event_id,
            unique_event_id=message.message_id,
            exchange_name=EXCHANGES[message.kind],
            routing_key=message.routing_key,
            body=message.body,
            schema_version=message.schema_version,
            next_attempt_at=self.clock.now(),
            created_at=self.clock.now(),
        )
