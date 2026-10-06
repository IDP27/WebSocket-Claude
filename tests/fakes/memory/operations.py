"""``OperationsStore``: reprocessamento e retry de DLQ pela API."""

from __future__ import annotations

from dataclasses import replace

from ohip_streaming.application.ports import (
    DlqItem,
    StoredEvent,
)
from ohip_streaming.domain.messages import QueueMessage
from tests.fakes.memory.state import MemoryState


class OperationsFake(MemoryState):
    """``OperationsStore``: reprocessamento e retry de DLQ pela API. ``get_event`` também
    atende o ``EnrichmentStore`` (mesma assinatura nos dois ports)."""

    async def find_event(self, unique_event_id: str) -> StoredEvent | None:
        row = self._raw_by_uid(unique_event_id)
        return StoredEvent(row.id, row.event, row.status) if row else None

    async def get_event(self, raw_event_id: int) -> StoredEvent | None:
        row = self.raw.get(raw_event_id)
        return StoredEvent(row.id, row.event, row.status) if row else None

    async def enqueue(
        self, *, chain_code: str, raw_event_id: int, message: QueueMessage, exchange_name: str
    ) -> int:
        record = self._outbox_record(chain_code, raw_event_id, message)
        record.exchange_name = exchange_name
        record.id = self._next_id("outbox")
        self.outbox[record.id] = record
        return record.id

    async def get_dlq_item(self, item_id: int) -> DlqItem | None:
        item = self.dlq.get(item_id)
        if item is None:
            return None
        return DlqItem(
            id=item.id,
            stage=item.stage,
            chain_code=item.chain_code,
            event_raw_id=item.event_raw_id,
            outbox_id=item.outbox_id,
            unique_event_id=item.unique_event_id,
            resolved=item.resolution is not None,
        )

    async def request_consume_retry(self, item_id: int, requested_by: str) -> bool:
        item = self._open_dlq(item_id)
        if item is None or item.retry_requested_at is not None:
            return False
        item.retry_requested_at, item.retry_requested_by = self.clock.now(), requested_by
        return True

    async def retry_publish(self, item_id: int, outbox_id: int, requested_by: str) -> int | None:
        item = self._open_dlq(item_id)
        if item is None:  # UPDATE ... WHERE resolution IS NULL afetou 0 linhas
            return None
        self._resolve(item, requested_by)
        original = self.outbox[outbox_id]
        copy = replace(
            original,
            id=self._next_id("outbox"),
            status="PENDING",
            attempts=0,
            last_error=None,
            next_attempt_at=self.clock.now(),
        )
        self.outbox[copy.id] = copy
        return copy.id

    async def enqueue_and_resolve(
        self,
        item_id: int,
        *,
        chain_code: str,
        raw_event_id: int,
        message: QueueMessage,
        exchange_name: str,
        requested_by: str,
    ) -> int | None:
        item = self._open_dlq(item_id)
        if item is None:  # UPDATE ... WHERE resolution IS NULL afetou 0 linhas
            return None
        self._resolve(item, requested_by)
        return await self.enqueue(
            chain_code=chain_code,
            raw_event_id=raw_event_id,
            message=message,
            exchange_name=exchange_name,
        )
