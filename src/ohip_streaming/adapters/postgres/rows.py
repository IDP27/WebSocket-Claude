"""Conversões entre o domínio e as colunas do PostgreSQL (ADR-0021 §4 e §6).

- Colunas ``timestamptz``; a conexão usa ``TimeZone=UTC``. A aplicação sempre trabalha com
  ``datetime`` com fuso UTC: sem fuso não vai ao banco, e o que volta é convertido para UTC.
- Textos livres (DLQ, erros, motivos) são cortados em UTF-8 sem quebrar caractere (n bytes
  sempre cabem em ``varchar(n)``) e têm NUL e surrogates isolados trocados por ``U+FFFD``: o
  PostgreSQL recusa NUL em texto, e a gravação da própria DLQ não pode falhar por isso.
- O Oracle tratava ``''`` como ``NULL``; aqui são diferentes. Inventário da Fase 11 (ADR-0021
  §4): os opcionais do evento já chegam normalizados pelo domínio, e os do adapter já gravam
  ``NULL`` quando vazios (``last_close_reason``, ``unique_event_id``/``offset_value`` da DLQ);
  os demais textos gravados são obrigatórios.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Final
from zoneinfo import ZoneInfo

from ohip_streaming.application.ports import ProcessingStatus, StoredEvent
from ohip_streaming.domain.events import Event, parse_stored_message

ERROR_MESSAGE_BYTES = 4000  # ohip_dlq.error_message, ohip_outbox.last_error
CLOSE_REASON_BYTES = 500  # ohip_consumer_status.last_close_reason
ERROR_CLASS_BYTES = 200  # ohip_dlq.error_class

REPLACEMENT: Final = "�"
_UNSAFE_RE: Final = re.compile("[\x00\ud800-\udfff]")


def to_db(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("datetime sem fuso não pode ir ao banco (UTC explícito)")
    return value.astimezone(UTC)


def from_db(value: datetime | None) -> datetime | None:
    return None if value is None else from_db_required(value)


def from_db_required(value: datetime) -> datetime:
    if value.tzinfo is None:  # timestamp sem fuso: só por erro de DDL; o valor é UTC
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def clean_text(text: str) -> str:
    """Troca NUL e surrogates isolados por ``U+FFFD`` (ADR-0021 §6)."""
    return _UNSAFE_RE.sub(REPLACEMENT, text)


def truncate_bytes(text: str, limit: int) -> str:
    """Corta em ``limit`` bytes UTF-8 sem quebrar caractere; já limpo de NUL e surrogates."""
    cleaned = clean_text(text)
    encoded = cleaned.encode("utf-8")
    if len(encoded) <= limit:
        return cleaned
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
