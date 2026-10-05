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
    LeaseNotProvisionedError,
    NotFoundError,
    ReplayAlreadyPendingError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    AccessToken,
    BatchResult,
    BatchToPersist,
    ChainStatus,
    Clock,
    ConsumeRetryItem,
    DisconnectSnapshot,
    DlqEntry,
    DlqItem,
    DlqQuery,
    DlqStage,
    DomainWrite,
    EventFilter,
    EventRecord,
    EventSummary,
    OutboxCounts,
    OutboxEntry,
    OutboxQuery,
    OutboxRow,
    OutgoingMessage,
    Page,
    PageRequest,
    ProcessingStatus,
    PublishOutcome,
    ReplayEntry,
    ReplayQuery,
    ReplayRequest,
    ReplayStatus,
    RowFailure,
    StatusSnapshot,
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
        self.calls: list[tuple[str, str, str | None, str]] = []
        self.errors: list[Exception] = []  # levantadas em ordem, uma por chamada

    async def fetch(
        self, chain_code: str, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        self.calls.append((chain_code, module_name, hotel_id, primary_key))
        if self.errors:
            raise self.errors.pop(0)
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
    created_at: datetime = T0
    sent_at: datetime | None = None


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
    fail_add_dlq: list[Exception] = field(default_factory=list)
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
            created_at=self.clock.now(),
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


# =============================================================== token


class FakeTokenIssuer:
    def __init__(self, clock: Clock, *, lifetime_s: float = 3600) -> None:
        self.clock = clock
        self.lifetime_s = lifetime_s
        self.issued = 0
        self.fail: list[Exception] = []

    async def issue(self) -> AccessToken:
        await asyncio.sleep(0)  # deixa chamadas concorrentes se encontrarem
        if self.fail:
            raise self.fail.pop(0)
        self.issued += 1
        return AccessToken(
            f"token-{self.issued}", self.clock.now() + timedelta(seconds=self.lifetime_s)
        )


class FakeTokenCache:
    def __init__(self) -> None:
        self.values: dict[str, tuple[AccessToken, float]] = {}
        self.broken = False

    async def get(self, key: str) -> AccessToken | None:
        if self.broken:
            raise ConnectionError("redis fora")
        entry = self.values.get(key)
        return entry[0] if entry else None

    async def put(self, key: str, token: AccessToken, ttl_s: float) -> None:
        if self.broken:
            raise ConnectionError("redis fora")
        self.values[key] = (token, ttl_s)

    async def delete(self, key: str) -> None:
        if self.broken:
            raise ConnectionError("redis fora")
        self.values.pop(key, None)


# =============================================================== lease


class MemoryLeaseStore:
    """``LeaseStore`` sobre os epochs do ``InMemoryDatabase`` (a barreira dos stores vê o mesmo
    epoch). O relógio do fake faz o papel do relógio do banco."""

    def __init__(self, db: InMemoryDatabase) -> None:
        self.db = db
        self.holders: dict[str, tuple[str | None, datetime]] = {}
        self.fail: list[Exception] = []

    def _holder(self, lease_name: str) -> tuple[str | None, datetime]:
        if self.fail:
            raise self.fail.pop(0)
        if lease_name not in self.db.leases:
            raise LeaseNotProvisionedError(lease_name)
        # Semeado com prazo já vencido (sql/003): o primeiro dono entra.
        return self.holders.get(lease_name, (None, self.db.clock.now() - timedelta(seconds=1)))

    async def acquire(self, lease_name: str, owner: str, ttl_s: float) -> int | None:
        holder, expires_at = self._holder(lease_name)
        now = self.db.clock.now()
        if not (expires_at < now or holder == owner):
            return None
        self.db.leases[lease_name] += 1
        self.holders[lease_name] = (owner, now + timedelta(seconds=ttl_s))
        return self.db.leases[lease_name]

    async def renew(self, lease_name: str, owner: str, epoch: int, ttl_s: float) -> bool:
        holder, _ = self._holder(lease_name)
        if holder != owner or self.db.leases[lease_name] != epoch:
            return False
        self.holders[lease_name] = (owner, self.db.clock.now() + timedelta(seconds=ttl_s))
        return True

    async def release(self, lease_name: str, owner: str, epoch: int) -> None:
        holder, _ = self._holder(lease_name)
        if holder == owner and self.db.leases[lease_name] == epoch:
            # No banco, o próximo acquire já vê o relógio alguns µs adiante.
            self.holders[lease_name] = (owner, self.db.clock.now() - timedelta(microseconds=1))


# =============================================================== consultas da API


def _percentile_cont(values: Sequence[float], fraction: float) -> float | None:
    """Mesma interpolação do ``PERCENTILE_CONT`` do Oracle."""
    if not values:
        return None
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _page(items: Sequence[Any], page: PageRequest, key: Callable[[Any], int]) -> Page[Any]:
    ordered = sorted(items, key=key, reverse=True)
    if page.cursor is not None:
        ordered = [i for i in ordered if key(i) < page.cursor]
    chunk = tuple(ordered[: page.limit])
    has_more = len(ordered) > page.limit
    return Page(chunk, key(chunk[-1]) if has_more and chunk else None)


class MemoryMonitoringStore:
    """``MonitoringStore`` sobre o ``InMemoryDatabase`` (mesmo contrato do adapter Oracle)."""

    def __init__(self, db: InMemoryDatabase) -> None:
        self.db = db
        self.unavailable: Exception | None = None  # injeção de falha: banco fora

    def _check(self) -> None:
        if self.unavailable is not None:
            raise self.unavailable

    async def ping(self) -> None:
        self._check()

    async def status(self) -> StatusSnapshot:
        self._check()
        db = self.db
        now = db.clock.now()
        chains = []
        for chain_code, (state, instance_id, disconnected_at, close_code) in sorted(
            db.statuses.items()
        ):
            pending = [
                o
                for o in db.outbox.values()
                if o.chain_code == chain_code and o.status == "PENDING"
            ]
            failed = [
                o for o in db.outbox.values() if o.chain_code == chain_code and o.status == "FAILED"
            ]
            recent = [
                r.event
                for r in db.raw.values()
                if r.event.chain_code == chain_code
                and r.event.received_at >= now - timedelta(minutes=5)
            ]
            lags = [(e.received_at - e.event_ts).total_seconds() for e in recent if e.event_ts]
            dlq_open = Counter(
                d.stage
                for d in db.dlq.values()
                if d.chain_code == chain_code and d.resolution is None
            )
            offset = db.offsets.get(chain_code)
            chains.append(
                ChainStatus(
                    chain_code=chain_code,
                    state=state,
                    instance_id=instance_id,
                    subscription_id=None,
                    last_offset=offset.last_offset if offset else None,
                    last_message_at=None,
                    lag_seconds_p95_5m=_percentile_cont(lags, 0.95),
                    events_last_5m=len(recent),
                    reconnects_total=0,
                    consecutive_failures=0,
                    next_attempt_at=None,
                    last_close_code=close_code,
                    last_close_reason=None,
                    last_disconnect_at=disconnected_at,
                    token_expires_at=None,
                    outbox=OutboxCounts(
                        len(pending),
                        len(failed),
                        min((o.created_at for o in pending), default=None),
                    ),
                    dlq_open={stage: dlq_open.get(stage, 0) for stage in DlqStage},
                )
            )
        return StatusSnapshot(now, tuple(chains))

    async def events(self, query: EventFilter, page: PageRequest) -> Page[EventSummary]:
        self._check()

        def matches(row: RawRow) -> bool:
            e = row.event
            return (
                (query.chain_code is None or e.chain_code == query.chain_code)
                and (query.hotel_id is None or e.hotel_id == query.hotel_id)
                and (query.event_name is None or e.event_name == query.event_name)
                and (
                    query.module_name is None or e.module_name.upper() == query.module_name.upper()
                )
                and (query.primary_key is None or e.primary_key == query.primary_key)
                and (query.received_from is None or e.received_at >= query.received_from)
                and (query.received_to is None or e.received_at < query.received_to)
                and (query.processing_status is None or row.status is query.processing_status)
            )

        rows = [self._summary(r) for r in self.db.raw.values() if matches(r)]
        return _page(rows, page, lambda e: e.raw_event_id)

    async def event(self, unique_event_id: str) -> EventRecord | None:
        self._check()
        row = self.db._raw_by_uid(unique_event_id)
        if row is None:
            return None
        return EventRecord(
            stored=StoredEvent(row.id, row.event, row.status),
            persisted_at=row.event.received_at,
            updated_at=None,
            outbox=tuple(
                self._outbox(o)
                for o in sorted(self.db.outbox.values(), key=lambda o: o.id)
                if o.raw_event_id == row.id
            ),
            dlq=tuple(
                self._dlq(d)
                for d in sorted(self.db.dlq.values(), key=lambda d: d.id)
                if d.event_raw_id == row.id
            ),
        )

    async def outbox(self, query: OutboxQuery, page: PageRequest) -> Page[OutboxEntry]:
        self._check()
        rows = [
            self._outbox(o)
            for o in self.db.outbox.values()
            if o.status == query.status
            and (query.chain_code is None or o.chain_code == query.chain_code)
        ]
        return _page(rows, page, lambda o: o.id)

    async def oldest_pending_age_s(self, chain_code: str | None) -> float | None:
        self._check()
        oldest = min(
            (
                o.created_at
                for o in self.db.outbox.values()
                if o.status == "PENDING" and (chain_code is None or o.chain_code == chain_code)
            ),
            default=None,
        )
        return None if oldest is None else (self.db.clock.now() - oldest).total_seconds()

    async def dlq(self, query: DlqQuery, page: PageRequest) -> Page[DlqEntry]:
        self._check()
        rows = [
            self._dlq(d)
            for d in self.db.dlq.values()
            if (query.stage is None or d.stage is query.stage)
            and (query.chain_code is None or d.chain_code == query.chain_code)
            and (query.resolved is None or (d.resolution is not None) == query.resolved)
        ]
        return _page(rows, page, lambda d: d.id)

    async def replays(self, query: ReplayQuery, page: PageRequest) -> Page[ReplayEntry]:
        self._check()
        rows = [
            self._replay(r)
            for r in self.db.replays.values()
            if (query.chain_code is None or r.chain_code == query.chain_code)
            and (query.status is None or r.status is query.status)
        ]
        return _page(rows, page, lambda r: r.id)

    async def replay(self, request_id: int) -> ReplayEntry | None:
        self._check()
        request = self.db.replays.get(request_id)
        return self._replay(request) if request else None

    # ----------------------------------------------------------- conversões

    @staticmethod
    def _summary(row: RawRow) -> EventSummary:
        e = row.event
        return EventSummary(
            raw_event_id=row.id,
            unique_event_id=e.unique_event_id,
            chain_code=e.chain_code,
            hotel_id=e.hotel_id,
            offset=e.offset,
            module_name=e.module_name,
            event_name=e.event_name,
            primary_key=e.primary_key,
            event_ts=e.event_ts,
            received_at=e.received_at,
            persisted_at=e.received_at,
            processing_status=row.status,
        )

    @staticmethod
    def _outbox(o: OutboxRecord) -> OutboxEntry:
        return OutboxEntry(
            id=o.id,
            event_raw_id=o.raw_event_id,
            chain_code=o.chain_code,
            unique_event_id=o.unique_event_id,
            exchange_name=o.exchange_name,
            routing_key=o.routing_key,
            status=o.status,
            attempts=o.attempts,
            next_attempt_at=o.next_attempt_at,
            last_error=o.last_error,
            created_at=o.created_at,
            sent_at=o.sent_at,
        )

    @staticmethod
    def _dlq(d: DlqRecord) -> DlqEntry:
        return DlqEntry(
            id=d.id,
            stage=d.stage,
            chain_code=d.chain_code,
            event_raw_id=d.event_raw_id,
            outbox_id=d.outbox_id,
            unique_event_id=d.unique_event_id,
            offset=d.offset,
            error_class=d.error.split(":", 1)[0],
            error_message=d.error,
            code_version=None,
            attempts=d.attempts,
            created_at=d.created_at,
            retry_requested_at=d.retry_requested_at,
            retry_requested_by=d.retry_requested_by,
            resolved_at=d.created_at if d.resolution is not None else None,
            resolved_by=d.resolved_by,
            resolution=d.resolution,
        )

    def _replay(self, r: ReplayRequest) -> ReplayEntry:
        cancelled_by = self.db.cancelled_by.get(r.id)
        return ReplayEntry(
            id=r.id,
            chain_code=r.chain_code,
            from_offset=r.from_offset,
            reason=r.reason,
            requested_by=r.requested_by,
            status=r.status,
            error_message=None,
            created_at=r.created_at,
            applied_at=None,
            cancelled_at=r.created_at if cancelled_by else None,
            cancelled_by=cancelled_by,
        )
