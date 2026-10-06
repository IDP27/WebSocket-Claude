"""``EnrichmentStore``: MERGE condicional e DLQ NORMALIZE/ENRICH (ADR-0019)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ohip_streaming.application.errors import (
    NotFoundError,
)
from ohip_streaming.application.ports import (
    DlqStage,
    DomainWrite,
    ProcessingStatus,
)
from tests.fakes.memory.rows import DlqRecord
from tests.fakes.memory.state import MemoryState


class EnrichmentFake(MemoryState):
    """``EnrichmentStore``: MERGE condicional e DLQ NORMALIZE/ENRICH (ADR-0019).

    O ``get_event`` do port vem de ``OperationsFake`` (a mesma assinatura está nos dois
    ports); o ``EnrichmentStore`` completo é o ``InMemoryDatabase``."""

    async def apply(
        self, raw_event_id: int, writes: Sequence[DomainWrite], status: ProcessingStatus
    ) -> int:
        if self.fail_apply:
            raise self.fail_apply.pop(0)
        # Transação: calcula tudo antes de aplicar.
        staged: dict[tuple[str, tuple[Any, ...]], tuple[dict[str, Any], int]] = {}
        skipped = 0
        for write in writes:
            key = (write.table, tuple(sorted(write.key.items())))
            existing = staged.get(key) or self.domain_tables.get(key)
            if existing is not None and existing[1] > write.source_offset_num:
                skipped += 1
                continue
            staged[key] = (dict(write.values), write.source_offset_num)
        self.domain_tables.update(staged)
        self.raw[raw_event_id].status = status
        return skipped

    async def add_dlq(
        self, raw_event_id: int, stage: DlqStage, error_class: str, error: str
    ) -> None:
        if self.fail_add_dlq:
            raise self.fail_add_dlq.pop(0)
        row = self.raw.get(raw_event_id)
        if row is None:
            raise NotFoundError(f"evento bruto {raw_event_id} não encontrado")
        item_id = self._next_id("dlq")
        self.dlq[item_id] = DlqRecord(
            id=item_id,
            stage=stage,
            chain_code=row.event.chain_code,
            error=f"{error_class}: {error}",
            event_raw_id=raw_event_id,
            unique_event_id=row.event.unique_event_id,
            offset=row.event.offset.value,
            created_at=self.clock.now(),
        )
        row.status = ProcessingStatus.FAILED  # mesma transação (ADR-0019)
