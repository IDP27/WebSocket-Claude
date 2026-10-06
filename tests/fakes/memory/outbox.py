"""``OutboxStore``: cabeça da fila por chain, confirmações e DLQ PUBLISH."""

from __future__ import annotations

from datetime import datetime

from ohip_streaming.application.ports import (
    DlqStage,
    OutboxRow,
)
from tests.fakes.memory.rows import DlqRecord
from tests.fakes.memory.state import MemoryState


class OutboxFake(MemoryState):
    """``OutboxStore``: cabeça da fila por chain, confirmações e DLQ PUBLISH."""

    async def chains_with_pending(self) -> list[str]:
        return sorted({o.chain_code for o in self.outbox.values() if o.status == "PENDING"})

    async def fetch_head(self, chain_code: str, limit: int) -> list[OutboxRow]:
        rows = sorted(
            (
                o
                for o in self.outbox.values()
                if o.chain_code == chain_code and o.status == "PENDING"
            ),
            key=lambda o: o.id,
        )[:limit]
        return [
            OutboxRow(
                id=o.id,
                chain_code=o.chain_code,
                exchange_name=o.exchange_name,
                routing_key=o.routing_key,
                message_id=o.unique_event_id,
                body=o.body,
                schema_version=o.schema_version,
                attempts=o.attempts,
                next_attempt_at=o.next_attempt_at,
            )
            for o in rows
        ]

    async def mark_sent(self, row_id: int, lease_epoch: int) -> None:
        self._fence("publisher", lease_epoch)
        self.outbox[row_id].status = "SENT"
        self.outbox[row_id].sent_at = self.clock.now()

    async def record_failure(
        self, row_id: int, attempts: int, next_attempt_at: datetime, error: str, lease_epoch: int
    ) -> None:
        self._fence("publisher", lease_epoch)
        row = self.outbox[row_id]
        row.attempts, row.next_attempt_at, row.last_error = attempts, next_attempt_at, error

    async def mark_failed(self, row_id: int, attempts: int, error: str, lease_epoch: int) -> None:
        self._fence("publisher", lease_epoch)
        row = self.outbox[row_id]
        row.status, row.attempts, row.last_error = "FAILED", attempts, error
        item_id = self._next_id("dlq")
        self.dlq[item_id] = DlqRecord(
            id=item_id,
            stage=DlqStage.PUBLISH,
            chain_code=row.chain_code,
            error=error,
            event_raw_id=row.raw_event_id,
            outbox_id=row.id,
            unique_event_id=row.unique_event_id,
            created_at=self.clock.now(),
        )
