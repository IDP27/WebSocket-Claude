"""Consultas da API em memória (ADR-0017)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any

from ohip_streaming.application.ports import (
    ChainStatus,
    DlqEntry,
    DlqQuery,
    DlqStage,
    EventFilter,
    EventRecord,
    EventSummary,
    OutboxCounts,
    OutboxEntry,
    OutboxQuery,
    Page,
    PageRequest,
    ReplayEntry,
    ReplayQuery,
    ReplayRequest,
    StatusSnapshot,
    StoredEvent,
)
from tests.fakes.memory.database import InMemoryDatabase
from tests.fakes.memory.rows import DlqRecord, OutboxRecord, RawRow


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
        for chain_code, row in sorted(db.statuses.items()):
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
                    state=row.state,
                    instance_id=row.instance_id,
                    subscription_id=row.subscription_id,
                    last_offset=offset.last_offset if offset else None,
                    last_message_at=row.last_message_at,
                    lag_seconds_p95_5m=_percentile_cont(lags, 0.95),
                    events_last_5m=len(recent),
                    reconnects_total=row.reconnects,
                    consecutive_failures=row.consecutive_failures,
                    next_attempt_at=row.next_attempt_at,
                    last_close_code=row.last_close_code,
                    last_close_reason=row.last_close_reason,
                    last_disconnect_at=row.last_disconnect_at,
                    token_expires_at=row.token_expires_at,
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
            resolved_at=d.resolved_at,
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
