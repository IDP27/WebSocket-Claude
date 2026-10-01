"""Logs JSON: campos fixos, correlação e máscara de segredos."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from typing import Any

import pytest
import structlog
from pydantic import SecretStr

from ohip_streaming.logging import MASK, bind_context, configure_logging, get_logger, redact


@pytest.fixture(autouse=True)
def reset_logging() -> Iterator[None]:
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)


def configure(json_output: bool = True) -> None:
    configure_logging(
        service="ohip-consumer",
        environment="homologacao",
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
    configure(json_output=False)
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
