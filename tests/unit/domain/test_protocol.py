"""Mensagens do graphql-transport-ws no formato do guia Oracle."""

from __future__ import annotations

import json

import pytest

from ohip_streaming.domain import protocol
from ohip_streaming.domain.errors import InvalidIdentifierError
from ohip_streaming.domain.offset import Offset


def test_connection_init_matches_the_guide() -> None:
    assert json.loads(protocol.connection_init("tok", "app")) == {
        "type": "connection_init",
        "payload": {"Authorization": "Bearer tok", "x-app-key": "app"},
    }


def test_subscribe_matches_the_guide_with_offset() -> None:
    message = json.loads(protocol.subscribe("id-1", chain_code="CHAIN1", offset=Offset("97863")))
    assert message["id"] == "id-1"
    assert message["type"] == "subscribe"
    payload = message["payload"]
    assert payload["variables"] == {"input": {"chainCode": "CHAIN1"}}
    assert payload["extensions"] == {}
    assert payload["operationName"] is None
    assert payload["query"].startswith(
        'subscription { newEvent(input: { chainCode: "CHAIN1" offset: "97863" }) { metadata '
    )


def test_subscribe_optional_arguments() -> None:
    query = json.loads(
        protocol.subscribe("i", chain_code="C1", offset=None, hotel_codes=["H1", "H2"], delta=True)
    )["payload"]["query"]
    assert 'newEvent(input: { chainCode: "C1" hotelCode: "H1,H2" delta: true })' in query
    assert "offset:" not in query


def test_subscribe_validates_values_written_in_the_query() -> None:
    with pytest.raises(InvalidIdentifierError):
        protocol.subscribe("i", chain_code='C1" }) { x', offset=None)
    with pytest.raises(InvalidIdentifierError):
        protocol.subscribe("i", chain_code="C1", offset=None, hotel_codes=['H"1'])


def test_control_messages() -> None:
    assert json.loads(protocol.PING) == {"type": "ping"}
    assert json.loads(protocol.PONG) == {"type": "pong"}
    assert json.loads(protocol.complete("id-1")) == {"id": "id-1", "type": "complete"}
    status = json.loads(protocol.status_query("q1"))
    assert status["payload"]["query"] == "query { connection { id status } }"


@pytest.mark.parametrize(
    ("raw", "kind", "frame_id"),
    [
        ('{"type":"connection_ack"}', protocol.FrameType.CONNECTION_ACK, None),
        ('{"type":"next","id":"a","payload":{"data":{}}}', protocol.FrameType.NEXT, "a"),
        ('{"type":"error","id":"a","payload":[]}', protocol.FrameType.ERROR, "a"),
        ('{"type":"complete","id":"a"}', protocol.FrameType.COMPLETE, "a"),
        ('{"type":"ping"}', protocol.FrameType.PING, None),
        ('{"type":"pong"}', protocol.FrameType.PONG, None),
        ('{"type":"ka"}', protocol.FrameType.UNKNOWN, None),
        ('{"type":7,"id":3}', protocol.FrameType.UNKNOWN, None),
        ("[1]", protocol.FrameType.UNKNOWN, None),
        ("{lixo", protocol.FrameType.UNKNOWN, None),
    ],
)
def test_classify(raw: str, kind: protocol.FrameType, frame_id: str | None) -> None:
    frame = protocol.classify(raw)
    assert (frame.type, frame.id, frame.raw) == (kind, frame_id, raw)


def test_next_payload_is_not_parsed_twice() -> None:
    assert protocol.classify('{"type":"next","id":"a","payload":{"x":1}}').payload is None


def test_connection_status_and_errors() -> None:
    assert (
        protocol.connection_status({"data": {"connection": {"status": "Inactive"}}}) == "Inactive"
    )
    bad_payloads: list[object] = [None, [], {"data": None}, {"data": {"connection": {"status": 1}}}]
    for bad in bad_payloads:
        assert protocol.connection_status(bad) is None
    assert protocol.error_messages([{"message": "offset expirado"}, {"x": 1}, "y"]) == [
        "offset expirado"
    ]
    assert protocol.error_messages({"message": "x"}) == []
