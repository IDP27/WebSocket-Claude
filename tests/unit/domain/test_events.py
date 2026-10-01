"""Conversão do frame ``next`` em Event; mensagens ruins viram RejectedMessage (nunca exceção)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, new_event, next_frame

from ohip_streaming.domain.errors import InvalidOffsetError
from ohip_streaming.domain.events import (
    STORED_SUBSCRIPTION_ID,
    Event,
    RejectedMessage,
    parse_next_frame,
    parse_stored_message,
    parse_timestamp,
)
from ohip_streaming.domain.offset import Offset

RECEIVED = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


def parse(raw: str, tz: str = "UTC") -> Event | RejectedMessage:
    return parse_next_frame(
        raw,
        chain_code="CHAIN1",
        subscription_id=SUBSCRIPTION_ID,
        received_at=RECEIVED,
        event_tz=ZoneInfo(tz),
    )


def test_parses_valid_frame() -> None:
    event = parse(next_frame(new_event("97863", "5c00f504", event_name="update  reservation")))

    assert isinstance(event, Event)
    assert event.offset == Offset("97863")
    assert event.unique_event_id == "5c00f504"
    assert event.event_name == "UPDATE RESERVATION"  # normalizado
    assert event.chain_code == "CHAIN1"
    assert event.hotel_id == "HOTEL1"
    assert event.publisher_id == "15951"
    assert event.event_ts == datetime(2026, 10, 1, 11, 59, 57, 900000, tzinfo=UTC)
    assert event.detail[0].element_name == "ARRIVAL DATE"
    assert event.detail[0].new_value == "2026-10-11"
    assert event.received_at == RECEIVED
    assert json.loads(event.payload_json)["metadata"]["offset"] == "97863"


def test_numeric_offset_is_normalized_to_string() -> None:
    event = parse(next_frame(new_event(100, "uid")))
    assert isinstance(event, Event)
    assert event.offset.value == "100"


def test_optional_fields_may_be_missing_or_null() -> None:
    raw_event = new_event(hotel_id=None, timestamp=None)
    del raw_event["detail"], raw_event["publisherId"]
    event = parse(next_frame(raw_event))

    assert isinstance(event, Event)
    assert event.hotel_id is None
    assert event.event_ts is None
    assert event.detail == ()
    assert event.publisher_id is None


def test_numeric_ids_and_detail_values_become_text() -> None:
    event = parse(
        next_frame(
            new_event(
                primary_key=123,  # type: ignore[arg-type]
                detail=[{"elementName": "NIGHTS", "oldValue": 2, "newValue": None}],
            )
        )
    )
    assert isinstance(event, Event)
    assert event.primary_key == "123"
    assert event.detail[0].old_value == "2"
    assert event.detail[0].new_value is None


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("{nao e json", "JSON inválido"),
        ("[1, 2]", "não é um objeto"),
        (json.dumps({"id": SUBSCRIPTION_ID, "type": "error"}), "tipo inesperado"),
        (next_frame(new_event(), subscription_id="outra"), "outra assinatura"),
        (json.dumps({"id": SUBSCRIPTION_ID, "type": "next"}), "payload ausente"),
        (json.dumps({"id": SUBSCRIPTION_ID, "type": "next", "payload": {}}), "newEvent ausente"),
    ],
)
def test_rejects_bad_envelopes(raw: str, reason: str) -> None:
    rejected = parse(raw)
    assert isinstance(rejected, RejectedMessage)
    assert reason in rejected.reason
    assert rejected.raw == raw
    assert rejected.received_at == RECEIVED


def test_graphql_errors_are_rejected_but_keep_offset() -> None:
    raw = json.dumps(
        {
            "id": SUBSCRIPTION_ID,
            "type": "next",
            "payload": {"errors": [{"message": "x"}], "data": {"newEvent": new_event("55", "u55")}},
        }
    )
    rejected = parse(raw)
    assert isinstance(rejected, RejectedMessage)
    assert rejected.offset == Offset("55")
    assert rejected.unique_event_id == "u55"


@pytest.mark.parametrize(
    ("changes", "reason", "keeps_offset"),
    [
        ({"metadata": None}, "metadata ausente", False),
        ({"metadata": {"offset": "abc", "uniqueEventId": "u"}}, "offset", False),
        ({"metadata": {"offset": "1" * 21, "uniqueEventId": "u"}}, "offset", False),
        ({"metadata": {"offset": "7"}}, "uniqueEventId", True),
        ({"metadata": {"offset": "7", "uniqueEventId": "u" * 65}}, "uniqueEventId", True),
        ({"moduleName": ""}, "moduleName", True),
        ({"eventName": None}, "eventName", True),
        ({"primaryKey": "p" * 101}, "primaryKey", True),
        ({"hotelId": "h" * 51}, "hotelId", True),
        ({"hotelId": {"a": 1}}, "hotelId", True),
        ({"detail": "texto"}, "detail", True),
        ({"detail": [{"oldValue": "x"}]}, "elementName", True),
    ],
)
def test_rejects_bad_events(changes: dict[str, object], reason: str, keeps_offset: bool) -> None:
    raw_event = new_event("7", "u7")
    raw_event.update(changes)
    rejected = parse(next_frame(raw_event))

    assert isinstance(rejected, RejectedMessage)
    assert reason in rejected.reason
    assert (rejected.offset == Offset("7")) is keeps_offset


@pytest.mark.parametrize(
    ("raw", "tz", "expected"),
    [
        ("2026-10-01 09:00:00.000", "UTC", datetime(2026, 10, 1, 9, 0, tzinfo=UTC)),
        ("2026-10-01 09:00:00", "America/Sao_Paulo", datetime(2026, 10, 1, 12, 0, tzinfo=UTC)),
        ("2026-10-01T09:00:00Z", "America/Sao_Paulo", datetime(2026, 10, 1, 9, 0, tzinfo=UTC)),
        ("2026-10-01T09:00:00+02:00", "UTC", datetime(2026, 10, 1, 7, 0, tzinfo=UTC)),
        ("ontem", "UTC", None),
        ("", "UTC", None),
        (None, "UTC", None),
    ],
)
def test_parse_timestamp(raw: object, tz: str, expected: datetime | None) -> None:
    assert parse_timestamp(raw, ZoneInfo(tz)) == expected


@pytest.mark.parametrize("raw", ["", "-1", "1.5", " 1", "a", "1" * 21])
def test_offset_rejects_invalid_strings(raw: str) -> None:
    with pytest.raises(InvalidOffsetError):
        Offset(raw)


@pytest.mark.parametrize("raw", [-1, True, 1.0, None])
def test_offset_parse_rejects_invalid_types(raw: object) -> None:
    with pytest.raises(InvalidOffsetError):
        Offset.parse(raw)


def test_offset_keeps_string_and_exposes_numeric_value() -> None:
    offset = Offset.parse("007")
    assert str(offset) == "007"
    assert offset.numeric() == 7


# ------------------------------------------------------------------ mensagem guardada na DLQ


def parse_stored(raw: str) -> Event | RejectedMessage:
    return parse_stored_message(
        raw, chain_code="CHAIN1", received_at=RECEIVED, event_tz=ZoneInfo("UTC")
    )


def test_stored_frame_from_an_old_subscription_is_accepted() -> None:
    event = parse_stored(next_frame(new_event("5", "uid-5"), subscription_id="assinatura-antiga"))
    assert isinstance(event, Event)
    assert event.offset == Offset("5")
    assert event.subscription_id == "assinatura-antiga"


def test_stored_bare_new_event_is_accepted() -> None:
    event = parse_stored(json.dumps(new_event("6", "uid-6")))
    assert isinstance(event, Event)
    assert event.unique_event_id == "uid-6"
    assert event.subscription_id == STORED_SUBSCRIPTION_ID
    assert json.loads(event.payload_json)["metadata"]["offset"] == "6"


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("{nao e json", "JSON inválido"),
        ("[1, 2]", "newEvent ausente"),
        (json.dumps({"type": "next", "payload": {"data": {}}}), "newEvent ausente"),
    ],
)
def test_stored_message_that_is_not_an_event_is_rejected(raw: str, reason: str) -> None:
    rejected = parse_stored(raw)
    assert isinstance(rejected, RejectedMessage)
    assert rejected.reason == reason
    assert rejected.raw == raw


def test_stored_event_still_invalid_is_rejected_with_offset_reason() -> None:
    rejected = parse_stored(json.dumps(new_event("abc", "uid-x")))
    assert isinstance(rejected, RejectedMessage)
    assert "offset" in rejected.reason.lower()
