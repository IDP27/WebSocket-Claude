"""Evento de negócio do OHIP e conversão da mensagem ``next`` (ARCHITECTURE §3 e §4.1).

O parser nunca levanta exceção para mensagem ruim: devolve ``RejectedMessage`` com o motivo e,
se possível, o offset e o ``uniqueEventId`` (o offset de uma mensagem rejeitada também avança,
ADR-0009). Assim a mensagem vai para a DLQ ``CONSUME`` e o fluxo segue.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, tzinfo
from typing import Any

from ohip_streaming.domain import identifiers as ids
from ohip_streaming.domain.errors import InvalidOffsetError
from ohip_streaming.domain.offset import Offset

# Formatos vistos no guia ("2021-06-03 16:45:48.000") e ISO-8601 como alternativa.
_TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True, slots=True)
class EventDetail:
    element_name: str
    old_value: str | None
    new_value: str | None
    scope_from: str | None = None
    scope_to: str | None = None
    element_type: str | None = None
    element_role: str | None = None
    element_sequence: str | None = None


@dataclass(frozen=True, slots=True)
class Event:
    chain_code: str
    unique_event_id: str
    offset: Offset
    module_name: str
    event_name: str  # normalizado (maiúsculas, espaços simples)
    primary_key: str
    hotel_id: str | None
    event_ts: datetime | None  # UTC; None se o "timestamp" não pôde ser interpretado
    publisher_id: str | None
    action_instance_id: str | None
    detail: tuple[EventDetail, ...]
    payload_json: str  # o newEvent como recebido (compacto), fonte da verdade no Oracle
    subscription_id: str
    received_at: datetime  # UTC


@dataclass(frozen=True, slots=True)
class RejectedMessage:
    """Mensagem que não virou evento. Vai para a DLQ ``CONSUME`` com o texto bruto."""

    raw: str
    reason: str
    received_at: datetime
    offset: Offset | None = None
    unique_event_id: str | None = None


@dataclass(frozen=True, slots=True)
class _Problem:
    reason: str


@dataclass(slots=True)
class _Reader:
    """Lê campos do newEvent acumulando o primeiro problema encontrado."""

    data: Mapping[str, Any]
    problem: _Problem | None = field(default=None)

    def fail(self, reason: str) -> None:
        if self.problem is None:
            self.problem = _Problem(reason)

    def text(self, key: str, max_len: int, *, required: bool) -> str | None:
        value = self.data.get(key)
        if value is None or value == "":
            if required:
                self.fail(f"campo obrigatório ausente: {key}")
            return None
        if isinstance(value, bool) or not isinstance(value, str | int):
            self.fail(f"campo {key} com tipo inesperado: {type(value).__name__}")
            return None
        text = str(value)
        if len(text) > max_len:
            self.fail(f"campo {key} excede {max_len} caracteres")
            return None
        return text


def _as_optional_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool | int | float):
        return json.dumps(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def parse_timestamp(raw: object, event_tz: tzinfo) -> datetime | None:
    """Interpreta o "timestamp" do evento. Sem fuso, assume ``event_tz`` (D-3). Devolve UTC."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    parsed: datetime | None = None
    for fmt in _TIMESTAMP_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)  # noqa: DTZ007 - fuso aplicado abaixo
            break
        except ValueError:
            continue
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=event_tz)
    return parsed.astimezone(UTC)


def _parse_detail(raw: object, reader: _Reader) -> tuple[EventDetail, ...]:
    if raw is None:
        return ()  # o detail é opcional na assinatura (orquestração via REST)
    if not isinstance(raw, list):
        reader.fail("detail não é uma lista")
        return ()
    items: list[EventDetail] = []
    for entry in raw:
        if not isinstance(entry, Mapping) or not isinstance(entry.get("elementName"), str):
            reader.fail("item de detail sem elementName")
            return ()
        items.append(
            EventDetail(
                element_name=entry["elementName"],
                old_value=_as_optional_text(entry.get("oldValue")),
                new_value=_as_optional_text(entry.get("newValue")),
                scope_from=_as_optional_text(entry.get("scopeFrom")),
                scope_to=_as_optional_text(entry.get("scopeTo")),
                element_type=_as_optional_text(entry.get("elementType")),
                element_role=_as_optional_text(entry.get("elementRole")),
                element_sequence=_as_optional_text(entry.get("elementSequence")),
            )
        )
    return tuple(items)


def _best_effort_ids(new_event: object) -> tuple[Offset | None, str | None]:
    if not isinstance(new_event, Mapping):
        return None, None
    metadata = new_event.get("metadata")
    if not isinstance(metadata, Mapping):
        return None, None
    offset: Offset | None
    try:
        offset = Offset.parse(metadata.get("offset"))
    except InvalidOffsetError:
        offset = None
    unique_id = metadata.get("uniqueEventId")
    if not isinstance(unique_id, str) or not 0 < len(unique_id) <= ids.MAX_UNIQUE_EVENT_ID:
        unique_id = None
    return offset, unique_id


def parse_new_event(
    new_event: Mapping[str, Any],
    *,
    chain_code: str,
    subscription_id: str,
    received_at: datetime,
    event_tz: tzinfo,
    raw: str,
) -> Event | RejectedMessage:
    """Converte o objeto ``newEvent`` em ``Event`` (usado também para reconstruir do Oracle)."""
    offset, unique_id = _best_effort_ids(new_event)

    def reject(reason: str) -> RejectedMessage:
        return RejectedMessage(
            raw=raw,
            reason=reason,
            received_at=received_at,
            offset=offset,
            unique_event_id=unique_id,
        )

    metadata = new_event.get("metadata")
    if not isinstance(metadata, Mapping):
        return reject("metadata ausente")
    if offset is None:
        return reject("offset ausente ou fora de ^[0-9]{1,20}$")
    if unique_id is None:
        return reject(f"uniqueEventId ausente ou com mais de {ids.MAX_UNIQUE_EVENT_ID} caracteres")

    reader = _Reader(new_event)
    module_name = reader.text("moduleName", ids.MAX_MODULE_NAME, required=True)
    event_name = reader.text("eventName", ids.MAX_EVENT_NAME, required=True)
    primary_key = reader.text("primaryKey", ids.MAX_PRIMARY_KEY, required=True)
    hotel_id = reader.text("hotelId", ids.MAX_HOTEL_ID, required=False)
    publisher_id = reader.text("publisherId", ids.MAX_PUBLISHER_ID, required=False)
    action_id = reader.text("actionInstanceId", ids.MAX_ACTION_INSTANCE_ID, required=False)
    detail = _parse_detail(new_event.get("detail"), reader)
    if reader.problem is not None:
        return reject(reader.problem.reason)
    if module_name is None or event_name is None or primary_key is None:  # já reportado acima
        return reject("campo obrigatório ausente")

    return Event(
        chain_code=chain_code,
        unique_event_id=unique_id,
        offset=offset,
        module_name=module_name,
        event_name=ids.normalize_event_name(event_name),
        primary_key=primary_key,
        hotel_id=hotel_id,
        event_ts=parse_timestamp(new_event.get("timestamp"), event_tz),
        publisher_id=publisher_id,
        action_instance_id=action_id,
        detail=detail,
        payload_json=json.dumps(new_event, ensure_ascii=False, separators=(",", ":")),
        subscription_id=subscription_id,
        received_at=received_at,
    )


def parse_next_frame(
    raw: str,
    *,
    chain_code: str,
    subscription_id: str,
    received_at: datetime,
    event_tz: tzinfo,
) -> Event | RejectedMessage:
    """Converte o texto de um frame ``next`` do graphql-transport-ws em ``Event``.

    O adapter do WebSocket já separou os frames de controle (ping, pong, complete, ack);
    aqui chegam só os ``next`` da nossa assinatura, como texto bruto.
    """

    def reject(reason: str, new_event: object = None) -> RejectedMessage:
        offset, unique_id = _best_effort_ids(new_event)
        return RejectedMessage(
            raw=raw,
            reason=reason,
            received_at=received_at,
            offset=offset,
            unique_event_id=unique_id,
        )

    try:
        frame = json.loads(raw)
    except ValueError:
        return reject("JSON inválido")
    if not isinstance(frame, Mapping):
        return reject("frame não é um objeto JSON")
    if frame.get("type") != "next":
        return reject(f"frame de tipo inesperado: {frame.get('type')!r}")
    if frame.get("id") != subscription_id:
        return reject("frame de outra assinatura (id diferente)")
    payload = frame.get("payload")
    if not isinstance(payload, Mapping):
        return reject("payload ausente")
    data = payload.get("data")
    new_event = data.get("newEvent") if isinstance(data, Mapping) else None
    if payload.get("errors"):
        return reject("payload com erros GraphQL", new_event)
    if not isinstance(new_event, Mapping):
        return reject("payload.data.newEvent ausente")
    return parse_new_event(
        new_event,
        chain_code=chain_code,
        subscription_id=subscription_id,
        received_at=received_at,
        event_tz=event_tz,
        raw=raw,
    )


STORED_SUBSCRIPTION_ID = "dlq-retry"


def parse_stored_message(
    raw: str, *, chain_code: str, received_at: datetime, event_tz: tzinfo
) -> Event | RejectedMessage:
    """Relê uma mensagem guardada na DLQ CONSUME (retry, ARCHITECTURE §4.4).

    O ``raw_message`` pode ser o frame ``next`` original (de qualquer assinatura, já encerrada)
    ou só o objeto ``newEvent``. O ``id`` da assinatura não é conferido.
    """
    try:
        data = json.loads(raw)
    except ValueError:
        return RejectedMessage(raw=raw, reason="JSON inválido", received_at=received_at)
    subscription_id = STORED_SUBSCRIPTION_ID
    if isinstance(data, Mapping) and data.get("type") == "next":
        if isinstance(data.get("id"), str):
            subscription_id = data["id"][: ids.MAX_SUBSCRIPTION_ID]
        payload = data.get("payload")
        inner = payload.get("data") if isinstance(payload, Mapping) else None
        data = inner.get("newEvent") if isinstance(inner, Mapping) else None
    if not isinstance(data, Mapping):
        return RejectedMessage(raw=raw, reason="newEvent ausente", received_at=received_at)
    return parse_new_event(
        data,
        chain_code=chain_code,
        subscription_id=subscription_id,
        received_at=received_at,
        event_tz=event_tz,
        raw=raw,
    )
