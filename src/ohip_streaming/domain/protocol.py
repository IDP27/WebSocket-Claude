"""Mensagens do graphql-transport-ws no formato do guia Oracle (ADR-0007).

Formato copiado do guia "Subscribing and Consuming Events": ``input`` escrito na própria
query (``newEvent(input: { chainCode: "X" offset: "N" })``) e ``variables.input`` só com o
``chainCode``. A query pede todos os campos que o parser lê (conferido contra o schema oficial
em ``tests/contract``); ``dataValueMapping`` não é pedido (D-12).

Os valores escritos na query passam pelos mesmos padrões do schema (chainCode, hotelCode,
offset); nenhum deles admite aspas ou barra invertida, e mesmo assim são serializados como
string JSON (compatível com string GraphQL).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from ohip_streaming.domain.identifiers import validate_chain_code, validate_hotel_codes
from ohip_streaming.domain.offset import Offset

SUBPROTOCOL: Final = "graphql-transport-ws"

# Campos pedidos na assinatura (os mesmos que domain/events.py lê).
EVENT_SELECTION: Final = (
    "metadata { offset uniqueEventId } moduleName eventName primaryKey timestamp hotelId "
    "publisherId actionInstanceId detail { elementName oldValue newValue scopeFrom scopeTo "
    "elementSequence elementType elementRole }"
)
STATUS_QUERY: Final = "query { connection { id status } }"


class FrameType(StrEnum):
    CONNECTION_ACK = "connection_ack"
    NEXT = "next"
    ERROR = "error"
    COMPLETE = "complete"
    PING = "ping"
    PONG = "pong"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class Frame:
    type: FrameType
    id: str | None
    raw: str  # texto original (o ``next`` vai inteiro para o parser de eventos)
    payload: Any = None


def _dump(message: dict[str, Any]) -> str:
    return json.dumps(message, separators=(",", ":"))


def connection_init(token: str, app_key: str) -> str:
    """Contém segredos: nunca logar o texto devolvido."""
    return _dump(
        {
            "type": "connection_init",
            "payload": {"Authorization": f"Bearer {token}", "x-app-key": app_key},
        }
    )


def subscribe(
    subscription_id: str,
    *,
    chain_code: str,
    offset: Offset | None,
    hotel_codes: Sequence[str] = (),
    delta: bool = False,
) -> str:
    chain_code = validate_chain_code(chain_code)
    arguments = [f"chainCode: {json.dumps(chain_code)}"]
    if offset is not None:  # sempre o último offset confirmado, se existir (ADR-0007)
        arguments.append(f"offset: {json.dumps(offset.value)}")
    if hotel_codes:
        arguments.append(
            f"hotelCode: {json.dumps(','.join(validate_hotel_codes(list(hotel_codes))))}"
        )
    if delta:
        arguments.append("delta: true")
    query = (
        f"subscription {{ newEvent(input: {{ {' '.join(arguments)} }}) {{ {EVENT_SELECTION} }} }}"
    )
    return _dump(
        {
            "id": subscription_id,
            "type": "subscribe",
            "payload": {
                "variables": {"input": {"chainCode": chain_code}},
                "extensions": {},
                "operationName": None,
                "query": query,
            },
        }
    )


def status_query(query_id: str) -> str:
    """Consulta de status da conexão num ``subscribe`` com id próprio (D-4)."""
    return _dump(
        {
            "id": query_id,
            "type": "subscribe",
            "payload": {
                "variables": {},
                "extensions": {},
                "operationName": None,
                "query": STATUS_QUERY,
            },
        }
    )


def complete(subscription_id: str) -> str:
    return _dump({"id": subscription_id, "type": "complete"})


PING: Final = _dump({"type": "ping"})
PONG: Final = _dump({"type": "pong"})


def classify(raw: str) -> Frame:
    """Lê só ``type``, ``id`` e (fora do ``next``) ``payload``. Nunca levanta exceção."""
    try:
        data = json.loads(raw)
    except ValueError:
        return Frame(FrameType.UNKNOWN, None, raw)
    if not isinstance(data, dict):
        return Frame(FrameType.UNKNOWN, None, raw)
    raw_type = data.get("type")
    try:
        frame_type = FrameType(raw_type) if isinstance(raw_type, str) else FrameType.UNKNOWN
    except ValueError:
        frame_type = FrameType.UNKNOWN
    frame_id = data.get("id") if isinstance(data.get("id"), str) else None
    payload = None if frame_type is FrameType.NEXT else data.get("payload")
    return Frame(frame_type, frame_id, raw, payload)


def connection_status(payload: Any) -> str | None:
    """``status`` de ``{"data": {"connection": {"status": "..."}}}`` ou None."""
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    connection = data.get("connection") if isinstance(data, dict) else None
    status = connection.get("status") if isinstance(connection, dict) else None
    return status if isinstance(status, str) else None


def error_messages(payload: Any) -> list[str]:
    """Mensagens de um frame ``error`` (lista de GraphQLError), para log (D-10)."""
    if not isinstance(payload, list):
        return []
    return [str(e.get("message")) for e in payload if isinstance(e, dict) and e.get("message")]
