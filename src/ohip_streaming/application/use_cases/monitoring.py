"""Consultas da API de controle (ADR-0017): status, eventos, outbox, DLQ e replays.

Tudo o que sai daqui passa pela máscara LGPD (ARCHITECTURE §11). Sem máscara só quando a rota
já conferiu o perfil ``admin`` e o ator; o acesso fica registrado no log de auditoria.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ohip_streaming.application.errors import NotFoundError
from ohip_streaming.application.ports import (
    DlqEntry,
    DlqQuery,
    EventFilter,
    EventRecord,
    EventSummary,
    MonitoringStore,
    OutboxEntry,
    OutboxQuery,
    Page,
    PageRequest,
    ReplayEntry,
    ReplayQuery,
    StatusSnapshot,
)
from ohip_streaming.domain.events import EventDetail
from ohip_streaming.domain.masking import MaskingPolicy, mask_payload
from ohip_streaming.domain.messages import build_message_body, routing_key
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

EXPORT_VERSION = 1


@dataclass(frozen=True, slots=True)
class _Unmasked(MaskingPolicy):
    """Política que não mascara nada (só para ``unmasked``, já autorizado e auditado)."""

    def is_sensitive(self, element_name: str, module_name: str) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class EventView:
    record: EventRecord
    detail: tuple[EventDetail, ...]
    masked: bool
    routing_key: str


@dataclass(frozen=True, slots=True)
class OutboxView:
    page: Page[OutboxEntry]
    oldest_pending_age_s: float | None  # relógio do banco


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Monitoring:
    def __init__(
        self,
        *,
        store: MonitoringStore,
        masking: MaskingPolicy,
        module_codes: Mapping[str, str],
    ) -> None:
        self._store = store
        self._masking = masking
        self._module_codes = module_codes

    async def ping(self) -> None:
        await self._store.ping()

    async def status(self) -> StatusSnapshot:
        return await self._store.status()

    async def events(self, query: EventFilter, page: PageRequest) -> Page[EventSummary]:
        return await self._store.events(query, page)

    async def event(self, unique_event_id: str, *, unmasked_by: str | None = None) -> EventView:
        record = await self._record(unique_event_id)
        if unmasked_by is not None:
            _audit(unique_event_id, unmasked_by, "consulta")
        event = record.stored.event
        policy = self._policy(unmasked_by)
        return EventView(
            record=record,
            detail=policy.mask_detail(event.detail, event.module_name).items,
            masked=unmasked_by is None,
            routing_key=routing_key(event.module_name, event.event_name, self._module_codes),
        )

    async def export(
        self, unique_event_id: str, *, unmasked_by: str | None = None
    ) -> dict[str, Any]:
        """JSON para reproduzir o evento localmente (RF-13). Mascarado por padrão."""
        record = await self._record(unique_event_id)
        if unmasked_by is not None:
            _audit(unique_event_id, unmasked_by, "export")
        stored = record.stored
        event = stored.event
        policy = self._policy(unmasked_by)
        message, _ = build_message_body(event, raw_event_id=stored.raw_event_id, masking=policy)
        new_event = mask_payload(event.payload_json, event.module_name, policy)
        return {
            "export_version": EXPORT_VERSION,
            "masked": unmasked_by is None,
            "raw_event_id": stored.raw_event_id,
            "processing_status": stored.status.value,
            "subscription_id": event.subscription_id,
            "persisted_at": _iso(record.persisted_at),
            "updated_at": _iso(record.updated_at),
            "routing_key": routing_key(event.module_name, event.event_name, self._module_codes),
            "message": message,
            "new_event": new_event,
            "outbox": [_outbox_export(row) for row in record.outbox],
            "dlq": [_dlq_export(item) for item in record.dlq],
            "how_to_reproduce": (
                "Publique 'message' num exchange de teste com 'routing_key', ou passe "
                "'new_event' para domain.events.parse_stored_message."
            ),
        }

    async def outbox(self, query: OutboxQuery, page: PageRequest) -> OutboxView:
        rows = await self._store.outbox(query, page)
        age = await self._store.oldest_pending_age_s(query.chain_code)
        return OutboxView(rows, age)

    async def dlq(self, query: DlqQuery, page: PageRequest) -> Page[DlqEntry]:
        return await self._store.dlq(query, page)

    async def replays(self, query: ReplayQuery, page: PageRequest) -> Page[ReplayEntry]:
        return await self._store.replays(query, page)

    async def replay(self, request_id: int) -> ReplayEntry:
        entry = await self._store.replay(request_id)
        if entry is None:
            raise NotFoundError(f"pedido de replay {request_id} não encontrado")
        return entry

    # ------------------------------------------------------------------- apoio

    async def _record(self, unique_event_id: str) -> EventRecord:
        record = await self._store.event(unique_event_id)
        if record is None:
            raise NotFoundError(f"evento {unique_event_id} não encontrado")
        return record

    def _policy(self, unmasked_by: str | None) -> MaskingPolicy:
        return _Unmasked() if unmasked_by is not None else self._masking


def _audit(unique_event_id: str, actor: str, action: str) -> None:
    log.info(
        "auditoria_dado_sem_mascara", unique_event_id=unique_event_id, actor=actor, action=action
    )


def _outbox_export(row: OutboxEntry) -> dict[str, Any]:
    return {
        "id": row.id,
        "exchange_name": row.exchange_name,
        "routing_key": row.routing_key,
        "status": row.status,
        "attempts": row.attempts,
        "last_error": row.last_error,
        "created_at": _iso(row.created_at),
        "sent_at": _iso(row.sent_at),
    }


def _dlq_export(item: DlqEntry) -> dict[str, Any]:
    return {
        "id": item.id,
        "stage": item.stage.value,
        "error_class": item.error_class,
        "error_message": item.error_message,
        "code_version": item.code_version,
        "attempts": item.attempts,
        "resolution": item.resolution,
        "created_at": _iso(item.created_at),
    }
