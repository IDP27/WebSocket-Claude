"""Linhas do "banco" em memória."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ohip_streaming.application.ports import (
    DlqStage,
    ProcessingStatus,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.offset import Offset
from tests.fakes.memory.basics import T0


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
    resolved_at: datetime | None = None
    retry_requested_at: datetime | None = None
    retry_requested_by: str | None = None


@dataclass
class OffsetState:
    last_offset: Offset | None = None
    last_unique_event_id: str | None = None


@dataclass(frozen=True)
class StatusRow:
    """Uma linha de OHIP_CONSUMER_STATUS."""

    state: ConsumerState
    instance_id: str | None = None
    last_disconnect_at: datetime | None = None
    last_close_code: int | None = None
    last_close_reason: str | None = None
    subscription_id: str | None = None
    connected_at: datetime | None = None
    token_expires_at: datetime | None = None
    last_message_at: datetime | None = None
    last_ping_at: datetime | None = None
    last_pong_at: datetime | None = None
    rtt_ms: int | None = None
    reconnects: int = 0
    consecutive_failures: int = 0
    next_attempt_at: datetime | None = None
