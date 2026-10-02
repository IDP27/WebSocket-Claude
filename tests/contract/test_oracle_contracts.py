"""Contrato com a Oracle: o código bate com as specs oficiais copiadas em vendor/ (ADR-0012).

Ao atualizar vendor/ (scripts/sync_oracle_api_docs.sh), uma falha aqui aponta exatamente o que
mudou no lado da Oracle e precisa de decisão (ADR) antes de mexer no código.
"""

from __future__ import annotations

import base64
import json
import re
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from ohip_streaming.config import AuthMode, OhipSettings
from ohip_streaming.domain.identifiers import (
    CHAIN_CODE_RE,
    HOTEL_CODE_RE,
    HOTEL_CODES_MAX_LEN,
)
from ohip_streaming.domain.offset import OFFSET_RE

VENDOR = Path(__file__).resolve().parents[2] / "vendor" / "oracle-hospitality-api-docs"
SCHEMA = VENDOR / "graphql" / "streaming" / "StreamingGraphQLSchema.json"
OAUTH = VENDOR / "rest-api-specs" / "security" / "v1" / "publishedoauth.json"

# Campos do newEvent que o parser lê (domain/events.py). Todos precisam existir no schema.
PARSED_HEADER_FIELDS = {
    "metadata",
    "moduleName",
    "eventName",
    "primaryKey",
    "timestamp",
    "hotelId",
    "publisherId",
    "actionInstanceId",
    "detail",
}
PARSED_METADATA_FIELDS = {"offset", "uniqueEventId"}
PARSED_DETAIL_FIELDS = {
    "elementName",
    "oldValue",
    "newValue",
    "scopeFrom",
    "scopeTo",
    "elementSequence",
    "elementType",
    "elementRole",
}
# O parser rejeita a mensagem se estes faltarem; o schema precisa garanti-los (String!).
REQUIRED_BY_PARSER = {"metadata", "moduleName", "eventName", "primaryKey"}


@cache
def _types() -> dict[str, dict[str, Any]]:
    data = json.loads(SCHEMA.read_text(encoding="utf-8"))
    schema = data.get("data", data)["__schema"]
    return {t["name"]: t for t in schema["types"]}


def _unwrap(type_ref: dict[str, Any]) -> tuple[str, bool]:
    """Nome do tipo e se é não nulo (``!``) no nível externo."""
    non_null = type_ref["kind"] == "NON_NULL"
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref["name"], non_null


def _fields(type_name: str) -> dict[str, dict[str, Any]]:
    type_info = _types()[type_name]
    entries: list[dict[str, Any]] = type_info.get("fields") or type_info.get("inputFields") or []
    return {f["name"]: f for f in entries}


def _scalar_rules(scalar_name: str) -> tuple[int, re.Pattern[str]]:
    """Tamanho e regex codificados no nome: ``StringWithLength<N>AndPattern<base64>``."""
    match = re.fullmatch(r"StringWithLength(\d+)AndPattern(\w+)", scalar_name)
    assert match, f"scalar fora do formato esperado: {scalar_name}"
    encoded = match.group(2)
    pattern = base64.b64decode(encoded + "=" * (-len(encoded) % 4)).decode()
    return int(match.group(1)), re.compile(pattern)


def _input_rules(field: str) -> tuple[int, re.Pattern[str]]:
    name, _ = _unwrap(_fields("NewEventInput")[field]["type"])
    return _scalar_rules(name)


def _official_accepts(field: str, value: str) -> bool:
    max_len, pattern = _input_rules(field)
    return len(value) <= max_len and bool(pattern.fullmatch(value))


# ------------------------------------------------------------------ Streaming: assinatura


def test_subscription_is_new_event_with_new_event_input() -> None:
    new_event = _fields("Subscription")["newEvent"]
    (arg,) = new_event["args"]
    assert (arg["name"], _unwrap(arg["type"])) == ("input", ("NewEventInput", True))
    assert _unwrap(new_event["type"]) == ("EventHeader", True)
    assert set(_fields("NewEventInput")) == {
        "chainCode",
        "offset",
        "offsetType",
        "delta",
        "hotelCode",
    }


CHAIN_SAMPLES = ["OHIPCN", "CHAIN1", "A B_%#$&-9", "X" * 20, "X" * 21, "", "CHÁIN", "C,1", "C.1"]


@pytest.mark.parametrize("value", CHAIN_SAMPLES)
def test_chain_code_validation_matches_the_schema(value: str) -> None:
    ours = bool(CHAIN_CODE_RE.fullmatch(value))
    assert ours == (_official_accepts("chainCode", value) and value != "")


HOTEL_SAMPLES = ["SAND01", "H1", "H 1_%#$&-", "HÓTEL", "H.1", "H,1"]


@pytest.mark.parametrize("value", HOTEL_SAMPLES)
def test_hotel_code_validation_matches_the_schema(value: str) -> None:
    # Nosso HOTEL_CODE_RE valida um código; a vírgula é o separador da lista (D-9).
    official = _official_accepts("hotelCode", value) and "," not in value
    assert bool(HOTEL_CODE_RE.fullmatch(value)) == official


def test_hotel_code_list_limit_matches_the_schema() -> None:
    max_len, pattern = _input_rules("hotelCode")
    assert max_len == HOTEL_CODES_MAX_LEN
    assert pattern.fullmatch("H1,H2,H3")  # lista separada por vírgula é aceita


OFFSET_SAMPLES = ["0", "97863", "9" * 20, "9" * 21, "", "12a", "-1", " 1"]


@pytest.mark.parametrize("value", OFFSET_SAMPLES)
def test_offset_validation_matches_the_schema(value: str) -> None:
    # O scalar diz 20 caracteres (ADR-0006); a descrição do campo diz 10 (D-11).
    assert bool(OFFSET_RE.fullmatch(value)) == (_official_accepts("offset", value) and value != "")


def test_offset_type_only_accepts_highest() -> None:
    assert _official_accepts("offsetType", "highest")
    assert not _official_accepts("offsetType", "lowest")


def test_delta_is_boolean() -> None:
    assert _unwrap(_fields("NewEventInput")["delta"]["type"]) == ("Boolean", False)


# ------------------------------------------------------------------ Streaming: evento


def test_every_field_read_by_the_parser_exists_in_the_schema() -> None:
    assert set(_fields("EventHeader")) >= PARSED_HEADER_FIELDS
    assert set(_fields("Metadata")) >= PARSED_METADATA_FIELDS
    assert set(_fields("EventDetail")) >= PARSED_DETAIL_FIELDS


def test_new_header_fields_are_known() -> None:
    # Campo novo no schema: decidir (ADR) se entra no parser ou no contrato da fila.
    known_but_not_parsed = {"dataValueMapping"}  # D-12: fica só no payload bruto
    assert set(_fields("EventHeader")) - PARSED_HEADER_FIELDS == known_but_not_parsed


def test_fields_required_by_the_parser_are_non_null_in_the_schema() -> None:
    header = _fields("EventHeader")
    for name in REQUIRED_BY_PARSER:
        assert _unwrap(header[name]["type"])[1], f"{name} deixou de ser obrigatório no schema"
    metadata = _fields("Metadata")
    assert _unwrap(metadata["offset"]["type"]) == ("String", True)  # D-7: offset é string
    assert _unwrap(metadata["uniqueEventId"]["type"]) == ("String", True)
    assert _unwrap(_fields("EventDetail")["elementName"]["type"]) == ("String", True)


def test_connection_status_query_exists() -> None:
    # D-4: consulta opcional de status antes de assinar.
    status = _fields("Query")["connection"]
    assert _unwrap(status["type"]) == ("ConnectionStatus", True)
    assert set(_fields("ConnectionStatus")) == {"id", "status"}


# ------------------------------------------------------------------ OAuth


@cache
def _oauth() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(OAUTH.read_text(encoding="utf-8"))
    return data


def test_oauth_token_path_matches_the_spec() -> None:
    spec = _oauth()
    (path,) = spec["paths"]
    default_path = OhipSettings.model_fields["oauth_token_path"].default
    assert spec["basePath"] + path == default_path
    operation = spec["paths"][path]["post"]
    assert operation["consumes"] == ["application/x-www-form-urlencoded"]
    assert spec["securityDefinitions"] == {"basicAuth": {"type": "basic"}}


def test_oauth_grant_types_cover_our_auth_modes() -> None:
    operation = _oauth()["paths"]["/tokens"]["post"]
    grant = next(p for p in operation["parameters"] if p.get("name") == "grant_type")
    grant_types = set(grant["enum"])
    # resource_owner usa o grant "password" (usuário de integração do OPERA).
    expected = {
        AuthMode.CLIENT_CREDENTIALS: "client_credentials",
        AuthMode.RESOURCE_OWNER: "password",
    }
    assert set(expected) == set(AuthMode)
    assert set(expected.values()) == grant_types


def test_oauth_headers() -> None:
    params = _oauth()["parameters"]
    assert params["x-app-key"]["in"] == "header"
    assert params["x-app-key"]["required"] is True
    assert params["enterpriseId"]["in"] == "header"
    assert params["enterpriseId"]["required"] is False  # só client_credentials em OCIM
    assert re.fullmatch(params["enterpriseId"]["pattern"], "ENT123")


def test_subscription_selection_exists_in_the_schema() -> None:
    """Todo campo pedido no subscribe (domain/protocol.py) existe no schema oficial."""
    from ohip_streaming.domain.protocol import EVENT_SELECTION

    tokens = EVENT_SELECTION.replace("{", " { ").replace("}", " } ").split()
    stack = ["EventHeader"]
    previous: str | None = None
    for token in tokens:
        if token == "{":
            assert previous is not None
            name, _ = _unwrap(_fields(stack[-1])[previous]["type"])
            stack.append(name)
        elif token == "}":
            stack.pop()
        else:
            assert token in _fields(stack[-1]), f"{token} não existe em {stack[-1]}"
            previous = token
    assert "dataValueMapping" not in tokens  # D-12: não pedido
