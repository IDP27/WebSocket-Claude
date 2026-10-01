"""Máscara LGPD/cartão, routing key e contrato v1 da mensagem."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, new_event, next_frame

from ohip_streaming.domain.events import Event, EventDetail, parse_next_frame
from ohip_streaming.domain.masking import (
    MASK,
    MaskingPolicy,
    scrub_card_numbers_in_event,
    scrub_card_numbers_in_message,
    scrub_card_numbers_in_text,
)
from ohip_streaming.domain.messages import (
    SCHEMA_VERSION,
    ExchangeKind,
    build_queue_message,
    routing_key,
)

VISA_TEST = "4111 1111 1111 1111"  # número de teste (passa no Luhn)


def detail(name: str, old: str | None = "", new: str | None = "valor") -> EventDetail:
    return EventDetail(element_name=name, old_value=old, new_value=new)


def test_personal_elements_are_masked() -> None:
    masked = MaskingPolicy().mask_detail(
        [detail("First Name", "Ana", "Maria"), detail("ARRIVAL DATE", "2026-10-10", "2026-10-11")],
        "RESERVATION",
    )

    assert masked.items[0].old_value == MASK
    assert masked.items[0].new_value == MASK
    assert masked.items[1].new_value == "2026-10-11"
    assert masked.card_data_detected is False


def test_empty_values_stay_empty() -> None:
    (item,) = MaskingPolicy().mask_detail([detail("EMAIL", "", None)], "PROFILE").items
    assert item.old_value == ""
    assert item.new_value is None


def test_card_element_names_are_masked_but_only_luhn_numbers_raise_the_alert() -> None:
    masked = MaskingPolicy().mask_detail(
        [
            detail("CREDIT CARD NUMBER", "", "XXXX1111"),
            detail("CREDIT CARD EXPIRATION", "", "12/29"),
        ],
        "RESERVATION",
    )
    assert [i.new_value for i in masked.items] == [MASK, MASK]
    assert masked.card_data_detected is False  # número já truncado pelo OPERA: não é alerta

    full = MaskingPolicy().mask_detail([detail("CREDIT CARD NUMBER", "", VISA_TEST)], "RESERVATION")
    assert full.items[0].new_value == MASK
    assert full.card_data_detected is True


def test_card_numbers_in_non_personal_values_are_masked_and_flagged() -> None:
    masked = MaskingPolicy().mask_detail(
        [detail("MARKET CODE", "", f"pago com {VISA_TEST} ok")], "RESERVATION"
    )
    assert masked.items[0].new_value == f"pago com {MASK} ok"
    assert masked.card_data_detected is True


def test_long_digit_runs_that_fail_luhn_are_kept() -> None:
    value = "reserva 1234567890123 confirmada"
    masked = MaskingPolicy().mask_detail([detail("MARKET CODE", "", value)], "RESERVATION")
    assert masked.items[0].new_value == value
    assert masked.card_data_detected is False


@pytest.mark.parametrize(
    "name",
    [
        "NAME",
        "NAME2",
        "XNAME",
        "FIRST NAME",
        "XFIRST NAME",
        "LAST NAME",
        "MIDDLE",
        "INCOGNITO NAME",
        "ADDRESS1",
        "ADDRESS LINE 2",
        "CITY",
        "ZIP CODE",
        "TAX NUMBER",
        "TAX NUMBER2",
        "ID NUMBER",
        "ID PLACE",
        "ID DATE",
        "PASSPORT",
        "EMAIL",
        "phone  number",
        "MOBILE",
        "BIRTH DATE",
        "DATE OF BIRTH",
        "NATIONALITY",
        "MEMBERSHIP NUMBER",
        "COMMENTS",
        "REMARKS",
        "UDF CHAR1",
        "XTITLE",
        "CREDIT CARD NUMBER",
    ],
)
def test_personal_element_names_from_the_opera_guide(name: str) -> None:
    assert MaskingPolicy().is_sensitive(name, "RESERVATION")


@pytest.mark.parametrize(
    "name",
    ["ARRIVAL DATE", "DEPARTURE DATE", "RATE CODE", "ROOM", "MARKET CODE", "NAME TYPE", "STATUS"],
)
def test_operational_elements_stay_in_clear(name: str) -> None:
    assert not MaskingPolicy().is_sensitive(name, "RESERVATION")


def test_profile_module_denies_by_default() -> None:
    policy = MaskingPolicy()
    # Nome que nenhum padrão conhece: em PROFILE é mascarado mesmo assim.
    assert policy.is_sensitive("KEYWORD", "PROFILE")
    assert policy.is_sensitive("ALIAS", "profile")
    assert not policy.is_sensitive("ALIAS", "RESERVATION")
    # Lista de seguros passa em claro.
    assert not policy.is_sensitive("VIP STATUS", "PROFILE")
    assert not policy.is_sensitive("name type", "PROFILE")


def test_extra_elements_from_configuration() -> None:
    policy = MaskingPolicy.with_extra_elements(["vip  status"])
    masked = policy.mask_detail(
        [detail("VIP STATUS", "", "GOLD"), detail("FIRST NAME", "", "Ana")], "RESERVATION"
    )
    assert masked.items[0].new_value == MASK
    assert masked.items[1].new_value == MASK  # os padrões continuam valendo


def test_scrub_card_numbers_in_event_rewrites_detail_and_payload_only() -> None:
    raw_event = new_event(
        "4111111111111111",  # offset com 16 dígitos que passa no Luhn: não é tocado
        "uid-1",
        primary_key="4111111111111111",
        detail=[
            {"elementName": "MARKET CODE", "oldValue": VISA_TEST, "newValue": "CORP"},
            {"elementName": "ARRIVAL DATE", "oldValue": "", "newValue": "2026-10-11"},
        ],
    )
    event = parse_next_frame(
        next_frame(raw_event),
        chain_code="CHAIN1",
        subscription_id=SUBSCRIPTION_ID,
        received_at=datetime(2026, 10, 1, tzinfo=UTC),
        event_tz=ZoneInfo("UTC"),
    )
    assert isinstance(event, Event)

    scrubbed, found = scrub_card_numbers_in_event(event)

    assert found is True
    assert scrubbed.detail[0].old_value == MASK
    assert scrubbed.detail[1] == event.detail[1]
    payload = json.loads(scrubbed.payload_json)
    assert payload["detail"][0]["oldValue"] == MASK
    assert payload["metadata"]["offset"] == "4111111111111111"
    assert payload["primaryKey"] == "4111111111111111"
    assert scrubbed.offset == event.offset
    assert VISA_TEST not in scrubbed.payload_json


def test_scrub_without_card_returns_the_same_event() -> None:
    event = _event()
    assert scrub_card_numbers_in_event(event) == (event, False)


@pytest.mark.parametrize(
    "card",
    [
        "4111111111111111",
        "4111 1111 1111 1111",
        "4111-1111-1111-1111",
        "3782 822463 10005",  # Amex 4-6-5
        "4111-1111-1111-1111-003",  # 19 dígitos (Luhn ok)
    ],
)
def test_scrub_card_numbers_in_text_formats(card: str) -> None:
    assert scrub_card_numbers_in_text(f"x {card} y") == (f"x {MASK} y", True)


@pytest.mark.parametrize(
    "value",
    [
        "1234567890123",  # falha no Luhn
        "2026-10-01 2026-10-07",  # duas datas: 16 dígitos que passam no Luhn, mas não é cartão
        "411111111111111111111111",  # 24 dígitos: longo demais até para PAN + CVV colados
        "4111-1111 1111-1111",  # separadores misturados
    ],
)
def test_text_that_is_not_a_card_is_kept(value: str) -> None:
    assert scrub_card_numbers_in_text(value) == (value, False)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("4111111111111111 123", "*** 123"),  # PAN + CVV
        ("4111 1111 1111 1111 123", "*** 123"),
        ("4111-1111-1111-1111 123", "*** 123"),
        ("4111111111111111 12/28", "*** 12/28"),  # PAN + validade
        ("4111 1111 1111 1111 12/28", "*** 12/28"),
        ("cvv 123 4111111111111111", "cvv 123 ***"),  # CVV antes
        ("cvv 123 4111 1111 1111 1111 val 12 28", "cvv 123 *** val 12 28"),
        ("4111 1111 1111 1111 1111", "*** 1111"),  # outro número de 4 dígitos depois
        ("4111.1111.1111.1111", "***"),  # ponto como separador
        ("cartões 4111111111111111 e 5500 0000 0000 0004", "cartões *** e ***"),
        # digitados à mão ou colados de PDF/e-mail
        ("4111  1111  1111  1111", "***"),
        ("4111 - 1111 - 1111 - 1111", "***"),
        ("4111/1111/1111/1111", "***"),
        ("4111\u00a01111\u00a01111\u00a01111", "***"),  # NBSP
        ("cartão:\n4111\n1111\n1111\n1111\nok", "cartão:\n***\nok"),
        ("4111111111111111123", "***"),  # PAN + CVV colados
        ("41111111111111111228", "***"),  # PAN + validade colados
    ],
)
def test_card_numbers_next_to_other_numbers_are_detected(text: str, expected: str) -> None:
    assert scrub_card_numbers_in_text(text) == (expected, True)


def test_twelve_digit_maestro_only_in_card_elements() -> None:
    maestro = "675964982648"  # 12 dígitos, passa no Luhn
    assert scrub_card_numbers_in_text(maestro) == (maestro, False)
    assert scrub_card_numbers_in_text(maestro, min_digits=12) == (MASK, True)
    event = _parsed(
        new_event(
            "1",
            "uid-1",
            detail=[
                {"elementName": "CREDIT CARD NUMBER", "oldValue": "", "newValue": maestro},
                {"elementName": "MARKET CODE", "oldValue": "", "newValue": maestro},
            ],
        )
    )
    scrubbed, found = scrub_card_numbers_in_event(event)
    assert found is True
    assert [i.new_value for i in scrubbed.detail] == [MASK, maestro]
    payload_detail = json.loads(scrubbed.payload_json)["detail"]
    assert [d["newValue"] for d in payload_detail] == [MASK, maestro]


LUHN_MEMBERSHIP = "6011000990139424"  # 16 dígitos com Luhn, como muitos cartões de fidelidade
LUHN_PHONE = "5511987654325"  # celular com DDI colado que, por acaso, passa no Luhn


@pytest.mark.parametrize(
    "element",
    [
        "MEMBERSHIP NUMBER",
        "membership card number",
        "PHONE",
        "MOBILE NUMBER",
        "CONFIRMATION NO",
        "NAME ID",
        "TAX NUMBER2",
        "AR NUMBER",
        "IATA NUMBER",
        "BARCODE",
    ],
)
def test_structured_number_elements_are_never_altered_or_flagged(element: str) -> None:
    assert scrub_card_numbers_in_text(LUHN_PHONE)[1] is True  # pré-condição: passa no Luhn
    assert scrub_card_numbers_in_text(LUHN_MEMBERSHIP)[1] is True
    event = _parsed(
        new_event(
            "1",
            "uid-1",
            detail=[{"elementName": element, "oldValue": LUHN_PHONE, "newValue": LUHN_MEMBERSHIP}],
        )
    )

    assert scrub_card_numbers_in_event(event) == (event, False)  # bruto intacto
    masked = MaskingPolicy().mask_detail(event.detail, "RESERVATION")
    assert masked.card_data_detected is False  # sem alerta falso


@pytest.mark.parametrize(
    "element", ["A/R NUMBER", "CENTRAL A/R NUMBER", "MEMBERSHIP DEVICE CODE", "VIRTUAL NUMBER"]
)
def test_more_structured_elements_from_the_guide(element: str) -> None:
    event = _parsed(
        new_event(
            "1", "uid-1", detail=[{"elementName": element, "oldValue": "", "newValue": VISA_TEST}]
        )
    )
    assert scrub_card_numbers_in_event(event) == (event, False)


@pytest.mark.parametrize(
    "element", ["ACCOUNT NUMBER", "ACCOUNT NO", "EXTERNAL REFERENCE", "EXTERNAL REFERENCE TYPE"]
)
def test_elements_that_may_hold_a_typed_card_are_scanned(element: str) -> None:
    event = _parsed(
        new_event(
            "1", "uid-1", detail=[{"elementName": element, "oldValue": "", "newValue": VISA_TEST}]
        )
    )
    scrubbed, found = scrub_card_numbers_in_event(event)
    assert found is True
    assert VISA_TEST not in scrubbed.payload_json
    message_body = json.dumps(
        [i.new_value for i in MaskingPolicy().mask_detail(event.detail, "RESERVATION").items]
    )
    assert VISA_TEST not in message_body  # nem na fila/API


def test_same_luhn_number_in_free_text_is_still_scrubbed() -> None:
    event = _parsed(
        new_event(
            "1",
            "uid-1",
            detail=[
                {"elementName": "MEMBERSHIP NUMBER", "oldValue": "", "newValue": LUHN_MEMBERSHIP},
                {"elementName": "COMMENTS", "oldValue": "", "newValue": f"cc {LUHN_MEMBERSHIP}"},
            ],
        )
    )
    scrubbed, found = scrub_card_numbers_in_event(event)
    assert found is True
    payload_detail = json.loads(scrubbed.payload_json)["detail"]
    assert [d["newValue"] for d in payload_detail] == [LUHN_MEMBERSHIP, f"cc {MASK}"]


def test_structured_elements_are_still_masked_by_name_in_outputs() -> None:
    masked = MaskingPolicy().mask_detail(
        [detail("MEMBERSHIP NUMBER", "", LUHN_MEMBERSHIP), detail("PHONE", "", LUHN_PHONE)],
        "RESERVATION",
    )
    assert [i.new_value for i in masked.items] == [MASK, MASK]


def test_card_numbers_outside_detail_are_scrubbed_from_payload() -> None:
    event = _parsed(new_event("1", "uid-1", newField={"note": f"pago com {VISA_TEST}"}))
    scrubbed, found = scrub_card_numbers_in_event(event)
    assert found is True
    assert json.loads(scrubbed.payload_json)["newField"] == {"note": f"pago com {MASK}"}


def _parsed(raw_event: dict[str, object]) -> Event:
    event = parse_next_frame(
        next_frame(raw_event),
        chain_code="CHAIN1",
        subscription_id=SUBSCRIPTION_ID,
        received_at=datetime(2026, 10, 1, tzinfo=UTC),
        event_tz=ZoneInfo("UTC"),
    )
    assert isinstance(event, Event)
    return event


@pytest.mark.parametrize(
    "value",
    [
        4111111111111111,  # número JSON
        [VISA_TEST, "outro"],  # lista
        {"numero": 4111111111111111},  # objeto
    ],
)
def test_scrub_card_numbers_in_non_string_values(value: object) -> None:
    event = _parsed(
        new_event("1", "uid-1", detail=[{"elementName": "X", "oldValue": "", "newValue": value}])
    )

    scrubbed, found = scrub_card_numbers_in_event(event)

    assert found is True
    assert "4111" not in scrubbed.payload_json
    assert "4111" not in (scrubbed.detail[0].new_value or "")


def test_scrub_card_numbers_in_message_keeps_identifiers() -> None:
    luhn_offset = "4111111111111103"
    frame_text = next_frame(
        new_event(
            luhn_offset,
            VISA_TEST,  # uniqueEventId só com dígitos
            primary_key=VISA_TEST,
            detail=[{"elementName": "X", "oldValue": "", "newValue": f"cartão {VISA_TEST}"}],
        )
    )

    clean, found = scrub_card_numbers_in_message(frame_text)

    assert found is True
    data = json.loads(clean)
    event = data["payload"]["data"]["newEvent"]
    assert event["metadata"] == {"offset": luhn_offset, "uniqueEventId": VISA_TEST}
    assert event["primaryKey"] == VISA_TEST
    assert event["detail"][0]["newValue"] == f"cartão {MASK}"
    assert data["id"] == SUBSCRIPTION_ID


def test_scrub_card_numbers_in_message_without_card_returns_text_unchanged() -> None:
    text = next_frame(new_event("4111111111111103", "uid-1"))
    assert scrub_card_numbers_in_message(text) == (text, False)
    assert scrub_card_numbers_in_message("sem números") == ("sem números", False)


def test_scrub_card_numbers_in_message_that_is_not_json_uses_text() -> None:
    assert scrub_card_numbers_in_message(f"{{lixo {VISA_TEST}") == (f"{{lixo {MASK}", True)


@pytest.mark.parametrize(
    ("module", "event", "codes", "expected"),
    [
        (
            "RESERVATION",
            "UPDATE RESERVATION",
            {"reservation": "rsv"},
            "ohip.rsv.UPDATE_RESERVATION",
        ),
        ("Reservation", "new reservation", {"reservation": "rsv"}, "ohip.rsv.NEW_RESERVATION"),
        ("PROFILE", "NEW PROFILE", {}, "ohip.profile.NEW_PROFILE"),
        ("Front Office", "CHECK-IN", {}, "ohip.front_office.CHECK_IN"),
        ("***", "###", {}, "ohip.unknown.UNKNOWN"),
    ],
)
def test_routing_key(module: str, event: str, codes: dict[str, str], expected: str) -> None:
    assert routing_key(module, event, codes) == expected


def _event() -> Event:
    raw_event = new_event(
        "97863",
        "5c00f504",
        detail=[
            {
                "elementName": "FIRST NAME",
                "oldValue": "",
                "newValue": "Joe",
                "scopeFrom": "2026-10-10",
            },
            {"elementName": "ARRIVAL DATE", "oldValue": "2026-10-10", "newValue": "2026-10-11"},
        ],
    )
    event = parse_next_frame(
        next_frame(raw_event),
        chain_code="CHAIN1",
        subscription_id=SUBSCRIPTION_ID,
        received_at=datetime(2026, 10, 1, 12, 0, 0, 812000, tzinfo=UTC),
        event_tz=ZoneInfo("UTC"),
    )
    assert isinstance(event, Event)
    return event


def test_queue_message_contract_v1() -> None:
    message, card = build_queue_message(
        _event(), raw_event_id=1001, masking=MaskingPolicy(), module_codes={"reservation": "rsv"}
    )
    body = json.loads(message.body)

    assert card is False
    assert message.kind is ExchangeKind.EVENTS
    assert message.routing_key == "ohip.rsv.UPDATE_RESERVATION"
    assert message.message_id == "5c00f504"
    assert message.headers == {"x-schema-version": SCHEMA_VERSION}
    assert body == {
        "schema_version": 1,
        "unique_event_id": "5c00f504",
        "offset": "97863",
        "chain_code": "CHAIN1",
        "hotel_id": "HOTEL1",
        "module_name": "RESERVATION",
        "event_name": "UPDATE RESERVATION",
        "primary_key": "123456",
        "event_ts": "2026-10-01T11:59:57.900Z",
        "received_at": "2026-10-01T12:00:00.812Z",
        "publisher_id": "15951",
        "action_instance_id": "222222",
        "raw_event_id": 1001,
        "detail": [
            {
                "element_name": "FIRST NAME",
                "old_value": "",
                "new_value": MASK,
                "scope_from": "2026-10-10",
                "scope_to": None,
            },
            {
                "element_name": "ARRIVAL DATE",
                "old_value": "2026-10-10",
                "new_value": "2026-10-11",
                "scope_from": None,
                "scope_to": None,
            },
        ],
    }


def test_reprocess_message_uses_reprocess_exchange() -> None:
    message, _ = build_queue_message(
        _event(),
        raw_event_id=1,
        masking=MaskingPolicy(),
        module_codes={},
        kind=ExchangeKind.REPROCESS,
    )
    assert message.kind is ExchangeKind.REPROCESS


def test_dlq_message_keeps_structured_elements_and_scrubs_the_rest() -> None:
    raw = json.dumps(
        {
            "metadata": {"offset": "1"},  # sem uniqueEventId: rejeitada
            "detail": [
                {"elementName": "MEMBERSHIP NUMBER", "oldValue": None, "newValue": LUHN_MEMBERSHIP},
                {"elementName": 7, "oldValue": None, "newValue": VISA_TEST},
                {"elementName": "CREDIT CARD NUMBER", "newValue": "675964982648"},
            ],
        }
    )
    clean, found = scrub_card_numbers_in_message(raw)
    assert found is True
    detail_out = json.loads(clean)["detail"]
    assert detail_out[0]["newValue"] == LUHN_MEMBERSHIP
    assert detail_out[0]["oldValue"] is None
    assert detail_out[1]["newValue"] == MASK
    assert detail_out[2]["newValue"] == MASK  # Maestro de 12 dígitos em elemento de cartão
