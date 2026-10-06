"""``PurgeStore``: expurgo por retenção (ADR-0020 §2)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from ohip_streaming.application.ports import (
    PurgeTarget,
)
from tests.fakes.memory.rows import OutboxRecord
from tests.fakes.memory.state import MemoryState


class PurgeFake(MemoryState):
    """``PurgeStore``: expurgo por retenção (ADR-0020 §2)."""

    def _expired(self, target: PurgeTarget, retention_days: int) -> list[int]:
        cutoff = self.clock.now() - timedelta(days=retention_days)
        if target is PurgeTarget.DLQ:
            ids = [d.id for d in self.dlq.values() if d.resolved_at and d.resolved_at < cutoff]
        elif target in (PurgeTarget.OUTBOX, PurgeTarget.OUTBOX_FAILED):
            referenced: set[int | None] = {d.outbox_id for d in self.dlq.values()}

            def expired(o: OutboxRecord) -> bool:
                if target is PurgeTarget.OUTBOX:
                    return o.status == "SENT" and o.sent_at is not None and o.sent_at < cutoff
                return o.status == "FAILED" and o.created_at < cutoff

            ids = [o.id for o in self.outbox.values() if expired(o) and o.id not in referenced]
        else:
            referenced = {o.raw_event_id for o in self.outbox.values()}
            referenced |= {d.event_raw_id for d in self.dlq.values()}
            ids = [
                r.id
                for r in self.raw.values()
                if r.event.received_at < cutoff and r.id not in referenced
            ]
        return sorted(ids)

    async def purge_batch(self, target: PurgeTarget, retention_days: int, batch_size: int) -> int:
        tables: dict[PurgeTarget, dict[int, Any]] = {
            PurgeTarget.DLQ: self.dlq,
            PurgeTarget.OUTBOX: self.outbox,
            PurgeTarget.OUTBOX_FAILED: self.outbox,
            PurgeTarget.RAW: self.raw,
        }
        table = tables[target]
        ids = self._expired(target, retention_days)[:batch_size]
        for row_id in ids:
            del table[row_id]
        return len(ids)

    async def count_expired(self, target: PurgeTarget, retention_days: int) -> int:
        return len(self._expired(target, retention_days))
