"""Consultas da API (ADR-0017)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Protocol, TypeVar

from ohip_streaming.application.ports.common import DlqStage, ProcessingStatus, StoredEvent
from ohip_streaming.application.ports.replay import ReplayStatus
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.offset import Offset


@dataclass(frozen=True, slots=True)
class PageRequest:
    """Paginação por chave: ``id`` decrescente; ``cursor`` é o último ``id`` já visto."""

    limit: int = 50
    cursor: int | None = None


ItemT = TypeVar("ItemT")


@dataclass(frozen=True, slots=True)
class Page(Generic[ItemT]):
    items: tuple[ItemT, ...]
    next_cursor: int | None  # None = última página


@dataclass(frozen=True, slots=True)
class OutboxCounts:
    pending: int = 0
    failed: int = 0
    oldest_pending_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ChainStatus:
    """Uma linha de OHIP_CONSUMER_STATUS com o offset e os agregados da chain."""

    chain_code: str
    state: ConsumerState
    instance_id: str | None
    subscription_id: str | None
    last_offset: Offset | None
    last_message_at: datetime | None
    lag_seconds_p95_5m: float | None
    events_last_5m: int
    reconnects_total: int
    consecutive_failures: int
    next_attempt_at: datetime | None
    last_close_code: int | None
    last_close_reason: str | None
    last_disconnect_at: datetime | None
    token_expires_at: datetime | None
    outbox: OutboxCounts
    dlq_open: Mapping[DlqStage, int]


@dataclass(frozen=True, slots=True)
class StatusSnapshot:
    generated_at: datetime  # relógio do banco
    chains: tuple[ChainStatus, ...]


@dataclass(frozen=True, slots=True)
class EventFilter:
    chain_code: str | None = None
    hotel_id: str | None = None
    event_name: str | None = None  # já normalizado (maiúsculas, espaços simples)
    module_name: str | None = None  # comparado sem diferenciar maiúsculas
    primary_key: str | None = None
    received_from: datetime | None = None  # inclusive
    received_to: datetime | None = None  # exclusive
    processing_status: ProcessingStatus | None = None


@dataclass(frozen=True, slots=True)
class EventSummary:
    raw_event_id: int
    unique_event_id: str
    chain_code: str
    hotel_id: str | None
    offset: Offset
    module_name: str
    event_name: str
    primary_key: str
    event_ts: datetime | None
    received_at: datetime
    persisted_at: datetime
    processing_status: ProcessingStatus


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    id: int
    event_raw_id: int
    chain_code: str
    unique_event_id: str
    exchange_name: str
    routing_key: str
    status: str  # PENDING | SENT | FAILED
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    created_at: datetime
    sent_at: datetime | None


@dataclass(frozen=True, slots=True)
class DlqEntry:
    """Item de DLQ sem ``raw_message`` nem ``stack_trace`` (podem ter dado pessoal)."""

    id: int
    stage: DlqStage
    chain_code: str | None
    event_raw_id: int | None
    outbox_id: int | None
    unique_event_id: str | None
    offset: str | None
    error_class: str
    error_message: str
    code_version: str | None
    attempts: int
    created_at: datetime
    retry_requested_at: datetime | None
    retry_requested_by: str | None
    resolved_at: datetime | None
    resolved_by: str | None
    resolution: str | None


@dataclass(frozen=True, slots=True)
class EventRecord:
    stored: StoredEvent
    persisted_at: datetime
    updated_at: datetime | None
    outbox: tuple[OutboxEntry, ...]  # em ordem de id
    dlq: tuple[DlqEntry, ...]  # em ordem de id


@dataclass(frozen=True, slots=True)
class OutboxQuery:
    status: str = "PENDING"  # PENDING | FAILED
    chain_code: str | None = None


@dataclass(frozen=True, slots=True)
class DlqQuery:
    stage: DlqStage | None = None
    chain_code: str | None = None
    resolved: bool | None = None


@dataclass(frozen=True, slots=True)
class ReplayEntry:
    id: int
    chain_code: str
    from_offset: Offset
    reason: str
    requested_by: str
    status: ReplayStatus
    error_message: str | None
    created_at: datetime
    applied_at: datetime | None
    cancelled_at: datetime | None
    cancelled_by: str | None


@dataclass(frozen=True, slots=True)
class ReplayQuery:
    chain_code: str | None = None
    status: ReplayStatus | None = None


class MonitoringStore(Protocol):
    """Leituras da API. Nenhuma escrita; páginas em ``id`` decrescente."""

    async def ping(self) -> None:
        """Levanta ``StoreUnavailableError`` se o banco não responde."""
        ...

    async def status(self) -> StatusSnapshot: ...

    async def events(self, query: EventFilter, page: PageRequest) -> Page[EventSummary]: ...

    async def event(self, unique_event_id: str) -> EventRecord | None: ...

    async def outbox(self, query: OutboxQuery, page: PageRequest) -> Page[OutboxEntry]: ...

    async def oldest_pending_age_s(self, chain_code: str | None) -> float | None:
        """Idade (s) da linha PENDING mais antiga, com o relógio do banco (o mesmo de
        ``created_at``). None se não houver PENDING."""
        ...

    async def dlq(self, query: DlqQuery, page: PageRequest) -> Page[DlqEntry]: ...

    async def replays(self, query: ReplayQuery, page: PageRequest) -> Page[ReplayEntry]: ...

    async def replay(self, request_id: int) -> ReplayEntry | None: ...
