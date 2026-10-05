"""Modelos de entrada e saída da API (docs/API.md). Viram o OpenAPI gerado pelo FastAPI.

Mudança de contrato só com ADR (RNF-15). Datas sempre em UTC.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from ohip_streaming.application.ports import (
    ChainStatus,
    DlqEntry,
    DlqStage,
    EventSummary,
    OutboxEntry,
    ProcessingStatus,
    ReplayEntry,
    ReplayStatus,
    StatusSnapshot,
)
from ohip_streaming.application.use_cases.monitoring import EventView
from ohip_streaming.domain.connection import ConsumerState

# ------------------------------------------------------------------- erros e saúde


class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str


class ErrorBody(BaseModel):
    error: ErrorDetail


class Health(BaseModel):
    status: Literal["ok"] = "ok"


class DependencyCheck(BaseModel):
    ok: bool
    latency_ms: float
    error: str | None = None


class Ready(BaseModel):
    status: Literal["ok", "fail"]
    checks: dict[str, DependencyCheck]


# ------------------------------------------------------------------- status


class LastClose(BaseModel):
    code: int | None
    reason: str | None
    at: datetime | None


class OutboxSummary(BaseModel):
    pending: int
    failed: int
    oldest_pending_seconds: float | None


class ChainStatusOut(BaseModel):
    chain_code: str
    state: ConsumerState
    instance_id: str | None
    subscription_id: str | None
    last_offset: str | None
    last_message_at: datetime | None
    lag_seconds_p95_5m: float | None
    events_per_minute: float
    reconnects_total: int
    consecutive_failures: int
    next_attempt_at: datetime | None
    last_close: LastClose | None
    token_expires_at: datetime | None
    outbox: OutboxSummary
    dlq_open: dict[DlqStage, int]


class StatusOut(BaseModel):
    generated_at: datetime
    chains: list[ChainStatusOut]


def _seconds_since(moment: datetime | None, now: datetime) -> float | None:
    return None if moment is None else round(max(0.0, (now - moment).total_seconds()), 1)


def chain_status_out(chain: ChainStatus, now: datetime) -> ChainStatusOut:
    has_close = chain.last_close_code is not None or chain.last_disconnect_at is not None
    return ChainStatusOut(
        chain_code=chain.chain_code,
        state=chain.state,
        instance_id=chain.instance_id,
        subscription_id=chain.subscription_id,
        last_offset=chain.last_offset.value if chain.last_offset else None,
        last_message_at=chain.last_message_at,
        lag_seconds_p95_5m=(
            round(chain.lag_seconds_p95_5m, 3) if chain.lag_seconds_p95_5m is not None else None
        ),
        events_per_minute=round(chain.events_last_5m / 5, 1),
        reconnects_total=chain.reconnects_total,
        consecutive_failures=chain.consecutive_failures,
        next_attempt_at=chain.next_attempt_at,
        last_close=(
            LastClose(
                code=chain.last_close_code,
                reason=chain.last_close_reason,
                at=chain.last_disconnect_at,
            )
            if has_close
            else None
        ),
        token_expires_at=chain.token_expires_at,
        outbox=OutboxSummary(
            pending=chain.outbox.pending,
            failed=chain.outbox.failed,
            oldest_pending_seconds=_seconds_since(chain.outbox.oldest_pending_at, now),
        ),
        dlq_open=dict(chain.dlq_open),
    )


def status_out(snapshot: StatusSnapshot) -> StatusOut:
    now = snapshot.generated_at
    return StatusOut(generated_at=now, chains=[chain_status_out(c, now) for c in snapshot.chains])


# ------------------------------------------------------------------- eventos


class EventSummaryOut(BaseModel):
    raw_event_id: int
    unique_event_id: str
    chain_code: str
    hotel_id: str | None
    offset: str
    module_name: str
    event_name: str
    primary_key: str
    event_ts: datetime | None
    received_at: datetime
    persisted_at: datetime
    processing_status: ProcessingStatus


class EventPage(BaseModel):
    items: list[EventSummaryOut]
    next_cursor: int | None


def event_summary_out(item: EventSummary) -> EventSummaryOut:
    return EventSummaryOut(
        raw_event_id=item.raw_event_id,
        unique_event_id=item.unique_event_id,
        chain_code=item.chain_code,
        hotel_id=item.hotel_id,
        offset=item.offset.value,
        module_name=item.module_name,
        event_name=item.event_name,
        primary_key=item.primary_key,
        event_ts=item.event_ts,
        received_at=item.received_at,
        persisted_at=item.persisted_at,
        processing_status=item.processing_status,
    )


class DetailItemOut(BaseModel):
    element_name: str
    element_type: str | None
    element_role: str | None
    element_sequence: str | None
    old_value: str | None
    new_value: str | None
    scope_from: str | None
    scope_to: str | None


class OutboxOut(BaseModel):
    id: int
    event_raw_id: int
    chain_code: str
    unique_event_id: str
    exchange_name: str
    routing_key: str
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    created_at: datetime
    sent_at: datetime | None


def outbox_out(row: OutboxEntry) -> OutboxOut:
    return OutboxOut(
        id=row.id,
        event_raw_id=row.event_raw_id,
        chain_code=row.chain_code,
        unique_event_id=row.unique_event_id,
        exchange_name=row.exchange_name,
        routing_key=row.routing_key,
        status=row.status,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        last_error=row.last_error,
        created_at=row.created_at,
        sent_at=row.sent_at,
    )


class DlqOut(BaseModel):
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


def dlq_out(item: DlqEntry) -> DlqOut:
    return DlqOut(
        id=item.id,
        stage=item.stage,
        chain_code=item.chain_code,
        event_raw_id=item.event_raw_id,
        outbox_id=item.outbox_id,
        unique_event_id=item.unique_event_id,
        offset=item.offset,
        error_class=item.error_class,
        error_message=item.error_message,
        code_version=item.code_version,
        attempts=item.attempts,
        created_at=item.created_at,
        retry_requested_at=item.retry_requested_at,
        retry_requested_by=item.retry_requested_by,
        resolved_at=item.resolved_at,
        resolved_by=item.resolved_by,
        resolution=item.resolution,
    )


class EventDetailOut(EventSummaryOut):
    subscription_id: str
    publisher_id: str | None
    action_instance_id: str | None
    updated_at: datetime | None
    routing_key: str
    masked: bool
    detail: list[DetailItemOut]
    outbox: list[OutboxOut]
    dlq: list[DlqOut]


def event_detail_out(view: EventView) -> EventDetailOut:
    record = view.record
    stored = record.stored
    event = stored.event
    return EventDetailOut(
        raw_event_id=stored.raw_event_id,
        unique_event_id=event.unique_event_id,
        chain_code=event.chain_code,
        hotel_id=event.hotel_id,
        offset=event.offset.value,
        module_name=event.module_name,
        event_name=event.event_name,
        primary_key=event.primary_key,
        event_ts=event.event_ts,
        received_at=event.received_at,
        persisted_at=record.persisted_at,
        processing_status=stored.status,
        subscription_id=event.subscription_id,
        publisher_id=event.publisher_id,
        action_instance_id=event.action_instance_id,
        updated_at=record.updated_at,
        routing_key=view.routing_key,
        masked=view.masked,
        detail=[
            DetailItemOut(
                element_name=d.element_name,
                element_type=d.element_type,
                element_role=d.element_role,
                element_sequence=d.element_sequence,
                old_value=d.old_value,
                new_value=d.new_value,
                scope_from=d.scope_from,
                scope_to=d.scope_to,
            )
            for d in view.detail
        ],
        outbox=[outbox_out(r) for r in record.outbox],
        dlq=[dlq_out(i) for i in record.dlq],
    )


# ------------------------------------------------------------------- outbox e DLQ


class OutboxPage(BaseModel):
    items: list[OutboxOut]
    next_cursor: int | None
    oldest_pending_seconds: float | None


class DlqPage(BaseModel):
    items: list[DlqOut]
    next_cursor: int | None


class DlqRetryAccepted(BaseModel):
    result: Literal["CONSUME_REQUESTED", "REPUBLISHED", "REPROCESS_ENQUEUED"]
    outbox_id: int | None


class ReprocessAccepted(BaseModel):
    outbox_id: int
    status: Literal["PENDING"] = "PENDING"


# ------------------------------------------------------------------- replay


class ReplayCreate(BaseModel):
    chain_code: str = Field(min_length=1, max_length=20)
    from_offset: str = Field(min_length=1, max_length=20)
    reason: str = Field(min_length=1, max_length=500)
    confirm: str = Field(min_length=1, max_length=20)


class ReplayAccepted(BaseModel):
    id: int
    status: ReplayStatus
    chain_code: str
    from_offset: str
    warnings: list[str]


class ReplayOut(BaseModel):
    id: int
    chain_code: str
    from_offset: str
    reason: str
    requested_by: str
    status: ReplayStatus
    error_message: str | None
    created_at: datetime
    applied_at: datetime | None
    cancelled_at: datetime | None
    cancelled_by: str | None


def replay_out(entry: ReplayEntry) -> ReplayOut:
    return ReplayOut(
        id=entry.id,
        chain_code=entry.chain_code,
        from_offset=entry.from_offset.value,
        reason=entry.reason,
        requested_by=entry.requested_by,
        status=entry.status,
        error_message=entry.error_message,
        created_at=entry.created_at,
        applied_at=entry.applied_at,
        cancelled_at=entry.cancelled_at,
        cancelled_by=entry.cancelled_by,
    )


class ReplayPage(BaseModel):
    items: list[ReplayOut]
    next_cursor: int | None
