"""Conversões entre o domínio e as colunas do Oracle 11g.

Colunas ``TIMESTAMP`` guardam UTC sem fuso (DDL); a aplicação sempre trabalha com
``datetime`` com fuso UTC. ``VARCHAR2(n)`` conta **bytes**: textos livres são cortados em
UTF-8 sem quebrar caractere.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from ohip_streaming.application.ports import ProcessingStatus, StoredEvent
from ohip_streaming.domain.events import Event, parse_stored_message

ERROR_MESSAGE_BYTES = 4000  # ohip_dlq.error_message, ohip_outbox.last_error
CLOSE_REASON_BYTES = 500  # ohip_consumer_status.last_close_reason
ERROR_CLASS_BYTES = 200  # ohip_dlq.error_class


def to_db(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("datetime sem fuso não pode ir ao banco (UTC explícito)")
    return value.astimezone(UTC).replace(tzinfo=None)


def from_db(value: datetime | None) -> datetime | None:
    return None if value is None else value.replace(tzinfo=UTC)


def from_db_required(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC)


def truncate_bytes(text: str, limit: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[: limit - 3].decode("utf-8", errors="ignore") + "..."


def stored_event_from_row(row: Sequence[Any]) -> StoredEvent:
    """Reconstrói o evento a partir do payload gravado (o mesmo parser do consumer).

    Colunas: id, chain_code, subscription_id, event_ts, payload, processing_status, received_at.
    """
    raw_id, chain_code, subscription_id, event_ts, payload, status, received_at = row
    parsed = parse_stored_message(
        payload,
        chain_code=chain_code,
        received_at=from_db_required(received_at),
        event_tz=ZoneInfo("UTC"),
    )
    if not isinstance(parsed, Event):
        raise ValueError(f"evento bruto {raw_id} ilegível: {parsed.reason}")
    event = replace(
        parsed,
        event_ts=from_db(event_ts),  # o valor interpretado na chegada (fuso D-3)
        subscription_id=subscription_id or parsed.subscription_id,
    )
    return StoredEvent(raw_event_id=int(raw_id), event=event, status=ProcessingStatus(status))
