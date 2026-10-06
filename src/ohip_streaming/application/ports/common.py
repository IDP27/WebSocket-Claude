"""Infraestrutura comum e tipos compartilhados entre contextos."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from ohip_streaming.domain.events import Event


class Clock(Protocol):
    def now(self) -> datetime:
        """Agora, com fuso (UTC)."""
        ...

    async def sleep(self, seconds: float) -> None: ...


class MetricsSink(Protocol):
    def increment(self, name: str, value: int = 1, **labels: str) -> None: ...


class ProcessingStatus(StrEnum):
    """Valores de OHIP_EVENT_RAW.processing_status (CHECK na DDL)."""

    RECEIVED = "RECEIVED"
    IGNORED = "IGNORED"
    NORMALIZED = "NORMALIZED"
    UNMAPPED = "UNMAPPED"
    ENRICHED = "ENRICHED"
    FAILED = "FAILED"


class DlqStage(StrEnum):
    CONSUME = "CONSUME"
    PUBLISH = "PUBLISH"
    NORMALIZE = "NORMALIZE"
    ENRICH = "ENRICH"


@dataclass(frozen=True, slots=True)
class StoredEvent:
    raw_event_id: int
    event: Event
    status: ProcessingStatus
