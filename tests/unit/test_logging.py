"""Logs JSON: campos fixos, correlação e máscara de segredos."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
import structlog
from pydantic import SecretStr

from ohip_streaming.logging import (
    MASK,
    bind_context,
    configure_logging,
    get_logger,
    redact,
    run_in_executor,
)


@pytest.fixture(autouse=True)
def reset_logging() -> Iterator[None]:
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)


def configure(json_output: bool = True, environment: str = "homologacao") -> None:
    configure_logging(
        service="ohip-consumer",
        environment=environment,
        code_version="abc123",
        level="DEBUG",
        json_output=json_output,
    )


def json_lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line]


def test_json_log_has_fixed_fields(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    get_logger("teste").info("evento_gravado", offset="97863")

    (record,) = json_lines(capsys)
    assert record["event"] == "evento_gravado"
    assert record["offset"] == "97863"
    assert record["level"] == "info"
    assert record["logger"] == "teste"
    assert record["service"] == "ohip-consumer"
    assert record["environment"] == "homologacao"
    assert record["code_version"] == "abc123"
    assert record["timestamp"].endswith("Z")


def test_bind_context_adds_correlation_fields(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    log = get_logger()
    with bind_context(unique_event_id="5c00f504", chain_code="CHAIN1"):
        log.info("dentro")
    log.info("fora")

    inside, outside = json_lines(capsys)
    assert inside["unique_event_id"] == "5c00f504"
    assert inside["chain_code"] == "CHAIN1"
    assert "unique_event_id" not in outside


async def test_bind_context_is_isolated_between_tasks(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    log = get_logger()

    async def process(event_id: str) -> None:
        with bind_context(unique_event_id=event_id):
            await asyncio.sleep(0)
            log.info("processado")

    await asyncio.gather(process("A"), process("B"))

    assert sorted(r["unique_event_id"] for r in json_lines(capsys)) == ["A", "B"]


def test_secrets_are_masked_by_key(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    get_logger().info(
        "conectando",
        client_secret="segredo",
        headers={"Authorization": "Bearer abc.def", "x-app-key": "chave", "Accept": "json"},
        token=SecretStr("tok"),
    )

    (record,) = json_lines(capsys)
    assert record["client_secret"] == MASK
    assert record["headers"] == {"Authorization": MASK, "x-app-key": MASK, "Accept": "json"}
    assert record["token"] == MASK


def test_secrets_are_masked_inside_text(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    get_logger().info("falha com Bearer eyJhbGciOi.payload.sig em amqps://user:senha@broker/vh")

    (record,) = json_lines(capsys)
    assert "eyJhbGciOi" not in record["event"]
    assert "senha" not in record["event"]
    assert "Bearer ***" in record["event"]
    assert "amqps://user:***@broker/vh" in record["event"]


def test_library_logs_use_the_same_format(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    logging.getLogger("websockets.client").warning("conexão fechada token=%s", "Bearer xyz123")

    (record,) = json_lines(capsys)
    assert record["logger"] == "websockets.client"
    assert record["service"] == "ohip-consumer"
    assert "xyz123" not in record["event"]


def test_exceptions_are_structured_and_masked(capsys: pytest.CaptureFixture[str]) -> None:
    configure()
    try:
        raise RuntimeError("401 com Bearer segredo123")
    except RuntimeError:
        get_logger().exception("erro_no_lote")

    (record,) = json_lines(capsys)
    assert record["exception"][0]["exc_type"] == "RuntimeError"
    assert "segredo123" not in json.dumps(record)


def test_console_output_for_development(capsys: pytest.CaptureFixture[str]) -> None:
    configure(json_output=False, environment="desenvolvimento")
    get_logger().info("legivel", password="p")

    out = capsys.readouterr().out
    assert "legivel" in out
    assert "password=***" in out


def test_level_filter(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(service="s", environment="e", code_version="v", level="WARNING")
    log = get_logger()
    log.info("ignorado")
    log.warning("aparece")

    assert [r["event"] for r in json_lines(capsys)] == ["aparece"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            {"Password": "x", "nested": [{"api-key": "y"}]},
            {"Password": MASK, "nested": [{"api-key": MASK}]},
        ),
        (("Bearer abc",), (f"Bearer {MASK}",)),
        (42, 42),
        (None, None),
    ],
)
def test_redact(value: Any, expected: Any) -> None:
    assert redact(value) == expected


# --------------------------------------------------------------- achados da revisão da Fase 1


def _fail_with_sensitive_locals() -> None:
    init_payload = {"Authorization": "Bearer TOKEN-LOCAL", "x-app-key": "APPKEY-EM-CLARO"}
    detail = [{"elementName": "FIRST NAME", "newValue": "Maria CPF 123.456.789-00"}]
    assert init_payload
    assert detail
    raise RuntimeError("falhou com x-app-key: APPKEY-NA-MENSAGEM")


@pytest.mark.parametrize(
    ("json_output", "environment"), [(True, "producao"), (False, "desenvolvimento")]
)
def test_tracebacks_never_include_local_variables(
    capsys: pytest.CaptureFixture[str], json_output: bool, environment: str
) -> None:
    configure(json_output=json_output, environment=environment)
    try:
        _fail_with_sensitive_locals()
    except RuntimeError:
        get_logger().exception("erro")

    out = capsys.readouterr().out
    assert "RuntimeError" in out
    for leaked in ("APPKEY-EM-CLARO", "TOKEN-LOCAL", "123.456.789-00", "APPKEY-NA-MENSAGEM"):
        assert leaked not in out


def test_console_output_is_refused_outside_development() -> None:
    with pytest.raises(ValueError, match="desenvolvimento"):
        configure(json_output=False, environment="producao")


@pytest.mark.parametrize(
    "key",
    ["clientSecret", "accessToken", "appKey", "integrationPassword", "X-App-Key", "set-cookie"],
)
def test_camel_and_kebab_case_keys_are_masked(key: str) -> None:
    assert redact({key: "valor-secreto"}) == {key: MASK}


def test_operational_fields_are_not_masked() -> None:
    fields = {"token_expires_at": "2026-10-01T12:00:00Z", "chain_code": "CHAIN1", "offset": "1"}
    assert redact(fields) == fields


@pytest.mark.parametrize(
    ("text", "leaked"),
    [
        ("wss://gw/subscriptions?key=" + "ab" * 32, "ab" * 32),
        ("connection_init {'x-app-key': 'CHAVE123'}", "CHAVE123"),
        ('{"x-app-key":"CHAVE456"}', "CHAVE456"),
    ],
)
def test_app_key_and_its_hash_are_masked_in_text(text: str, leaked: str) -> None:
    assert leaked not in redact(text)


def test_app_key_hash_is_masked_in_params_dict() -> None:
    app_key_hash = "ab" * 32
    params = {"key": app_key_hash, "unique_event_id": "5c00f504-3844-4a23-91af-feb150b06568"}

    assert redact(params) == {"key": MASK, "unique_event_id": params["unique_event_id"]}


async def test_run_in_executor_keeps_correlation_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure()
    log = get_logger("oracle")

    def write_batch(size: int) -> int:
        log.info("lote_gravado", size=size)
        return size

    with ThreadPoolExecutor(max_workers=1) as executor, bind_context(unique_event_id="E1"):
        assert await run_in_executor(executor, write_batch, 3) == 3

    (record,) = json_lines(capsys)
    assert record["unique_event_id"] == "E1"
    assert record["size"] == 3
