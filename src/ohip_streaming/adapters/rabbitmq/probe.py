"""Verificação de prontidão do broker para o ``/ready`` da API (ADR-0017).

Abre e fecha uma conexão AMQP. Erro → exceção só com o tipo (a URL tem a senha).
"""

from __future__ import annotations

import aio_pika
from aio_pika.exceptions import AMQPError

from ohip_streaming.application.errors import BrokerUnavailableError


async def probe_broker(url: str, *, timeout_s: float) -> None:
    try:
        connection = await aio_pika.connect(url, timeout=timeout_s)
    except (AMQPError, OSError, TimeoutError) as exc:
        raise BrokerUnavailableError(f"sem conexão com o broker: {type(exc).__name__}") from None
    await connection.close()
