"""Prontidão do broker para o ``/ready`` (ADR-0017)."""

from __future__ import annotations

from typing import Any

import aio_pika
import pytest
from aio_pika.exceptions import AMQPConnectionError

from ohip_streaming.adapters.rabbitmq import probe
from ohip_streaming.application.errors import BrokerUnavailableError


class Connection:
    closed = False

    async def close(self) -> None:
        self.closed = True


async def test_probe_opens_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = Connection()
    seen: dict[str, Any] = {}

    async def connect(url: str, **kwargs: Any) -> Connection:
        seen.update(url=url, **kwargs)
        return connection

    monkeypatch.setattr(aio_pika, "connect", connect)
    await probe.probe_broker("amqp://u:senha@h/v", timeout_s=3)
    assert connection.closed
    assert seen == {"url": "amqp://u:senha@h/v", "timeout": 3}


@pytest.mark.parametrize("error", [AMQPConnectionError("x"), OSError(), TimeoutError()])
async def test_probe_failure_hides_the_url(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    async def connect(url: str, **kwargs: Any) -> Connection:
        raise error

    monkeypatch.setattr(aio_pika, "connect", connect)
    with pytest.raises(BrokerUnavailableError) as info:
        await probe.probe_broker("amqp://u:senha@h/v", timeout_s=3)
    assert "senha" not in str(info.value)
