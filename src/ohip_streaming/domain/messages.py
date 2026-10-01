"""Contrato da mensagem na fila, versão 1 (ARCHITECTURE §6) e routing key.

Mudança incompatível exige ``schema_version`` novo e ADR (RNF-15).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final

from ohip_streaming.domain.events import Event
from ohip_streaming.domain.masking import MaskingPolicy

SCHEMA_VERSION: Final = 1
_NON_SLUG_RE: Final = re.compile(r"[^A-Za-z0-9]+")


class ExchangeKind(StrEnum):
    """Destino lógico; o nome real do exchange vem da configuração."""

    EVENTS = "events"  # ohip.events — terceiros (n8n) e enricher
    REPROCESS = "reprocess"  # ohip.reprocess — só o enricher (ARCHITECTURE §4.4)


@dataclass(frozen=True, slots=True)
class QueueMessage:
    kind: ExchangeKind
    routing_key: str
    message_id: str  # = uniqueEventId: consumidores deduplicam por ele
    body: str  # JSON do contrato v1
    schema_version: int = SCHEMA_VERSION

    @property
    def headers(self) -> Mapping[str, int]:
        return {"x-schema-version": self.schema_version}


def _slug(value: str) -> str:
    return _NON_SLUG_RE.sub("_", value.strip()).strip("_")


def routing_key(module_name: str, event_name: str, module_codes: Mapping[str, str]) -> str:
    """``ohip.<modulo>.<EVENTO>`` (DV-9).

    ``module_codes`` mapeia ``moduleName`` em minúsculas para um código curto (ex.: rsv).
    Sem mapeamento, usa o próprio ``moduleName`` em minúsculas.
    """
    module = module_codes.get(module_name.strip().lower()) or _slug(module_name).lower()
    event = _slug(event_name).upper()
    return f"ohip.{module or 'unknown'}.{event or 'UNKNOWN'}"


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_message_body(
    event: Event, *, raw_event_id: int, masking: MaskingPolicy
) -> tuple[dict[str, Any], bool]:
    """Monta o corpo v1. Devolve também se houve dado de cartão no ``detail`` (alerta)."""
    masked = masking.mask_detail(event.detail, event.module_name)
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "unique_event_id": event.unique_event_id,
        "offset": event.offset.value,
        "chain_code": event.chain_code,
        "hotel_id": event.hotel_id,
        "module_name": event.module_name,
        "event_name": event.event_name,
        "primary_key": event.primary_key,
        "event_ts": _iso_utc(event.event_ts),
        "received_at": _iso_utc(event.received_at),
        "publisher_id": event.publisher_id,
        "action_instance_id": event.action_instance_id,
        "raw_event_id": raw_event_id,
        "detail": [
            {
                "element_name": item.element_name,
                "old_value": item.old_value,
                "new_value": item.new_value,
                "scope_from": item.scope_from,
                "scope_to": item.scope_to,
            }
            for item in masked.items
        ],
    }
    return body, masked.card_data_detected


def build_queue_message(
    event: Event,
    *,
    raw_event_id: int,
    masking: MaskingPolicy,
    module_codes: Mapping[str, str],
    kind: ExchangeKind = ExchangeKind.EVENTS,
) -> tuple[QueueMessage, bool]:
    body, card_detected = build_message_body(event, raw_event_id=raw_event_id, masking=masking)
    message = QueueMessage(
        kind=kind,
        routing_key=routing_key(event.module_name, event.event_name, module_codes),
        message_id=event.unique_event_id,
        body=json.dumps(body, ensure_ascii=False, separators=(",", ":")),
    )
    return message, card_detected
