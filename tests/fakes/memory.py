"""Implementações em memória dos ports, com o mesmo contrato do adapter Oracle (Fase 3).

Simulam o que importa para os casos de uso: as duas constraints UNIQUE do evento bruto, a
barreira de epoch (ADR-0008), a atomicidade da transação do lote e a injeção de falhas.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from ohip_streaming.application.errors import (
    LeaseLostError,
    ReplayAlreadyPendingError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    BatchResult,
    BatchToPersist,
    ConsumeRetryItem,
    DisconnectSnapshot,
    DlqItem,
    DlqStage,
    DomainWrite,
    OutboxRow,
    OutgoingMessage,
    ProcessingStatus,
    PublishOutcome,
    ReplayRequest,
    ReplayStatus,
    RowFailure,
    StoredEvent,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage
from ohip_streaming.domain.offset import Offset

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
EXCHANGES = {ExchangeKind.EVENTS: "ohip.events", ExchangeKind.REPROCESS: "ohip.reprocess"}


# =============================================================== infraestrutura simples


class FakeClock:
    def __init__(self, start: datetime = T0) -> None:
        self.current = start
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.current

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)
        await asyncio.sleep(0)

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class FakeMetrics:
    def __init__(self) -> None:
        self.counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        self.counters[(name, tuple(sorted(labels.items())))] += value

    def total(self, name: str) -> int:
        return sum(v for (n, _), v in self.counters.items() if n == name)


class FakeSeenCache:
    def __init__(self) -> None:
        self.seen: set[str] = set()
        self.fail_filter = False
        self.fail_mark = False
        self.mark_calls: list[list[str]] = []

    async def filter_seen(self, unique_event_ids: Sequence[str]) -> set[str]:
        if self.fail_filter:
            raise ConnectionError("redis fora")
        return self.seen.intersection(unique_event_ids)

    async def mark_seen(self, unique_event_ids: Sequence[str]) -> None:
        if self.fail_mark:
            raise ConnectionError("redis fora")
        self.mark_calls.append(list(unique_event_ids))
        self.seen.update(unique_event_ids)


class FakeQueueDedup:
    def __init__(self) -> None:
        self.processed: set[str] = set()

    async def is_processed(self, message_id: str) -> bool:
        return message_id in self.processed

    async def mark_processed(self, message_id: str) -> None:
        self.processed.add(message_id)


class FakeFetcher:
    def __init__(self, resource: Mapping[str, Any] | None = None) -> None:
        self.resource = resource
        self.calls: list[tuple[str, str | None, str]] = []

    async def fetch(
        self, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        self.calls.append((module_name, hotel_id, primary_key))
        return self.resource


class FakePublisher:
    """Responde com um roteiro: cada item é um ``PublishOutcome`` ou uma exceção a levantar."""

    def __init__(self, script: Sequence[PublishOutcome | Exception] = ()) -> None:
        self.script = list(script)
        self.published: list[OutgoingMessage] = []
        self.attempts: list[OutgoingMessage] = []

    async def publish(self, message: OutgoingMessage) -> PublishOutcome:
        self.attempts.append(message)
        outcome = self.script.pop(0) if self.script else PublishOutcome.ACKED
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is PublishOutcome.ACKED:
            self.published.append(message)
        return outcome


# =============================================================== "banco" em memória


@dataclass
class RawRow:
    id: int
    event: Event
    status: ProcessingStatus


@dataclass
class OutboxRecord:
    id: int
    chain_code: str
    raw_event_id: int
    unique_event_id: str
    exchange_name: str
    routing_key: str
    body: str
    schema_version: int
    status: str = "PENDING"
    attempts: int = 0
    next_attempt_at: datetime = T0
    last_error: str | None = None


@dataclass
class DlqRecord:
    id: int
    stage: DlqStage
    chain_code: str | None
    error: str
    event_raw_id: int | None = None
    outbox_id: int | None = None
    unique_event_id: str | None = None
    offset: str | None = None
    raw_message: str | None = None
    created_at: datetime = T0
    attempts: int = 0
    resolution: str | None = None
    resolved_by: str | None = None
    retry_requested_at: datetime | None = None
    retry_requested_by: str | None = None


@dataclass
class OffsetState:
    last_offset: Offset | None = None
    last_unique_event_id: str | None = None


@dataclass
class InMemoryDatabase:
    """Implementa EventStore, OutboxStore, ReplayStore, OperationsStore e EnrichmentStore."""

    clock: FakeClock = field(default_factory=FakeClock)
    leases: dict[str, int] = field(default_factory=dict)
    offsets: dict[str, OffsetState] = field(default_factory=dict)
    raw: dict[int, RawRow] = field(default_factory=dict)
    outbox: dict[int, OutboxRecord] = field(default_factory=dict)
    dlq: dict[int, DlqRecord] = field(default_factory=dict)
    replays: dict[int, ReplayRequest] = field(default_factory=dict)
    cancelled_by: dict[int, str] = field(default_factory=dict)
    # chain → (estado, instance_id, last_disconnect_at, último código de fechamento)
    statuses: dict[str, tuple[ConsumerState, str | None, datetime | None, int | None]] = field(
        default_factory=dict
    )
    domain_tables: dict[tuple[str, tuple[Any, ...]], tuple[dict[str, Any], int]] = field(
        default_factory=dict
    )
    # Injeção de falhas
    fail_persist: list[Exception] = field(default_factory=list)  # consumidas a cada chamada
    fail_persist_when: Callable[[BatchToPersist], Exception | None] | None = None
    row_errors: dict[str, tuple[str, str]] = field(default_factory=dict)  # uid → (classe, msg)
    fail_apply: list[Exception] = field(default_factory=list)
    persisted_batches: list[BatchToPersist] = field(default_factory=list)
    # Uma sequence por tabela, como na DDL (sql/001_sequences.sql).
    _sequences: Counter[str] = field(default_factory=Counter)

    # ----------------------------------------------------------- provisionamento

    def provision_chain(self, chain_code: str) -> None:
        self.offsets.setdefault(chain_code, OffsetState())
        self.leases.setdefault(f"consumer:{chain_code}", 0)
        self.statuses.setdefault(chain_code, (ConsumerState.STOPPED, None, None, None))

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

    # ----------------------------------------------------------- EventStore

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

    def _open_dlq(self, item_id: int | None) -> DlqRecord | None:
        item = self.dlq.get(item_id) if item_id is not None else None
        return item if item is not None and item.resolution is None else None

    def _resolve(self, item: DlqRecord, resolved_by: str) -> None:
        item.resolution, item.resolved_by = "RETRIED", resolved_by
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
        )

    # ----------------------------------------------------------- OutboxStore

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

    # ----------------------------------------------------------- ReplayStore

    async def last_offset(self, chain_code: str) -> Offset | None:
        if chain_code not in self.offsets:
            raise UnknownChainError(chain_code)
        return self.offsets[chain_code].last_offset

    async def offset_received_at(self, chain_code: str, offset: Offset) -> datetime | None:
        for row in self.raw.values():
            if row.event.chain_code == chain_code and row.event.offset == offset:
                return row.event.received_at
        return None

    async def create_request(
        self, chain_code: str, from_offset: Offset, reason: str, requested_by: str
    ) -> ReplayRequest:
        if await self.pending_request(chain_code) is not None:
            raise ReplayAlreadyPendingError(chain_code)
        request = ReplayRequest(
            id=self._next_id("replay"),
            chain_code=chain_code,
            from_offset=from_offset,
            reason=reason,
            requested_by=requested_by,
            status=ReplayStatus.PENDING,
            created_at=self.clock.now(),
        )
        self.replays[request.id] = request
        return request

    async def pending_request(self, chain_code: str) -> ReplayRequest | None:
        return next(
            (
                r
                for r in self.replays.values()
                if r.chain_code == chain_code and r.status is ReplayStatus.PENDING
            ),
            None,
        )

    async def apply_request(self, request: ReplayRequest, lease_epoch: int) -> bool:
        self._fence(f"consumer:{request.chain_code}", lease_epoch)
        current = self.replays.get(request.id)
        if current is None or current.status is not ReplayStatus.PENDING:
            return False
        self.replays[request.id] = replace(current, status=ReplayStatus.APPLIED)
        self.offsets[request.chain_code] = OffsetState(request.from_offset, None)
        return True

    async def cancel_request(self, request_id: int, cancelled_by: str) -> bool:
        current = self.replays.get(request_id)
        if current is None or current.status is not ReplayStatus.PENDING:
            return False
        self.replays[request_id] = replace(current, status=ReplayStatus.CANCELLED)
        self.cancelled_by[request_id] = cancelled_by
        return True

    # ----------------------------------------------------------- ConsumerStatusStore

    def _status(
        self, chain_code: str
    ) -> tuple[ConsumerState, str | None, datetime | None, int | None]:
        if chain_code not in self.statuses:
            raise UnknownChainError(chain_code)
        return self.statuses[chain_code]

    async def record_state(self, chain_code: str, state: ConsumerState, instance_id: str) -> None:
        _, _, disconnected_at, code = self._status(chain_code)
        self.statuses[chain_code] = (state, instance_id, disconnected_at, code)

    async def record_disconnect(
        self,
        chain_code: str,
        state: ConsumerState,
        close_code: int | None,
        close_reason: str | None,
    ) -> None:
        _, instance_id, _, _ = self._status(chain_code)
        self.statuses[chain_code] = (state, instance_id, self.clock.now(), close_code)

    async def disconnect_snapshot(self, chain_code: str) -> DisconnectSnapshot:
        state, _, disconnected_at, _ = self._status(chain_code)
        return DisconnectSnapshot(self.clock.now(), disconnected_at, state)

    # ----------------------------------------------------------- OperationsStore / Enrichment

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

    async def add_dlq(self, raw_event_id: int, stage: DlqStage, error: str) -> None:
        item_id = self._next_id("dlq")
        self.dlq[item_id] = DlqRecord(
            id=item_id,
            stage=stage,
            chain_code=None,
            error=error,
            event_raw_id=raw_event_id,
            created_at=self.clock.now(),
        )
