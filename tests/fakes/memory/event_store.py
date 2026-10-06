"""``EventStore``: lote do consumer (UNIQUE, barreira de epoch e DLQ CONSUME)."""

from __future__ import annotations

from ohip_streaming.application.ports import (
    BatchResult,
    BatchToPersist,
    ConsumeRetryItem,
    DlqStage,
    RowFailure,
)
from tests.fakes.memory.rows import DlqRecord, OffsetState, OutboxRecord, RawRow
from tests.fakes.memory.state import MemoryState


class EventStoreFake(MemoryState):
    """``EventStore``: lote do consumer (UNIQUE, barreira de epoch e DLQ CONSUME)."""

    async def reserve_event_ids(self, count: int) -> list[int]:
        return [self._next_id("raw") for _ in range(count)]

    async def persist_batch(self, batch: BatchToPersist) -> BatchResult:
        if self.fail_persist:
            raise self.fail_persist.pop(0)
        if self.fail_persist_when is not None and (error := self.fail_persist_when(batch)):
            raise error
        self._fence(f"consumer:{batch.chain_code}", batch.lease_epoch)
        # Retry de DLQ: o UPDATE do item é condicionado a resolution IS NULL.
        retry_item = self._open_dlq(batch.retry_of_dlq_id)

        # Transação: prepara tudo e só aplica no fim.
        new_raw: dict[int, RawRow] = {}
        new_outbox: list[OutboxRecord] = []
        new_dlq: list[DlqRecord] = []
        inserted: list[str] = []
        duplicates: list[str] = []
        failures: list[RowFailure] = []
        keys = {
            (r.event.chain_code, r.event.offset.value, r.event.primary_key)
            for r in self.raw.values()
        }
        uids = {r.event.unique_event_id for r in self.raw.values()}

        for record in batch.events:
            event = record.event
            key = (event.chain_code, event.offset.value, event.primary_key)
            if event.unique_event_id in uids or key in keys:
                duplicates.append(event.unique_event_id)
                continue
            if event.unique_event_id in self.row_errors:
                error_class, message = self.row_errors[event.unique_event_id]
                failures.append(RowFailure(event.unique_event_id, error_class, message))
                if retry_item is not None:  # o item do retry continua aberto
                    continue
                new_dlq.append(
                    DlqRecord(
                        id=0,
                        stage=DlqStage.CONSUME,
                        chain_code=event.chain_code,
                        error=f"{error_class}: {message}",
                        unique_event_id=event.unique_event_id,
                        offset=event.offset.value,
                        raw_message=event.payload_json,
                        created_at=self.clock.now(),
                    )
                )
                continue
            uids.add(event.unique_event_id)
            keys.add(key)
            new_raw[record.raw_event_id] = RawRow(record.raw_event_id, event, record.status)
            inserted.append(event.unique_event_id)
            if record.outbox is not None:
                new_outbox.append(
                    self._outbox_record(event.chain_code, record.raw_event_id, record.outbox)
                )

        for rejected in batch.rejected:
            new_dlq.append(
                DlqRecord(
                    id=0,
                    stage=DlqStage.CONSUME,
                    chain_code=batch.chain_code,
                    error=rejected.reason,
                    unique_event_id=rejected.unique_event_id,
                    offset=rejected.offset.value if rejected.offset else None,
                    raw_message=rejected.raw_message,
                    created_at=self.clock.now(),
                )
            )

        # commit
        self.raw.update(new_raw)
        for row in new_outbox:
            row.id = self._next_id("outbox")
            self.outbox[row.id] = row
        for item in new_dlq:
            item.id = self._next_id("dlq")
            self.dlq[item.id] = item
        if retry_item is not None:
            retry_item.retry_requested_at = retry_item.retry_requested_by = None
            if failures:
                retry_item.error = f"{failures[0].error_class}: {failures[0].error_message}"
                retry_item.attempts += 1
            else:
                retry_item.resolution = "RETRIED"
                retry_item.resolved_by = "consumer"
                retry_item.resolved_at = self.clock.now()
        if batch.offset is not None:
            self.offsets[batch.chain_code] = OffsetState(batch.offset, batch.last_unique_event_id)
        self.persisted_batches.append(batch)
        return BatchResult(tuple(inserted), tuple(duplicates), tuple(failures))

    async def pending_consume_retries(self, chain_code: str, limit: int) -> list[ConsumeRetryItem]:
        items = sorted(
            (
                d
                for d in self.dlq.values()
                if d.stage is DlqStage.CONSUME
                and d.chain_code == chain_code
                and d.resolution is None
                and d.retry_requested_at is not None
            ),
            key=lambda d: d.id,
        )[:limit]
        return [ConsumeRetryItem(d.id, d.raw_message or "", d.created_at) for d in items]

    async def mark_consume_retry_failed(self, dlq_id: int, reason: str, lease_epoch: int) -> None:
        item = self.dlq[dlq_id]
        self._fence(f"consumer:{item.chain_code}", lease_epoch)
        item.error = reason
        item.attempts += 1
        item.retry_requested_at = item.retry_requested_by = None
