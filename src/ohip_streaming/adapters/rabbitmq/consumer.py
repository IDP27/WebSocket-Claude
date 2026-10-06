"""Consumo da fila ``ohip.enricher`` com ``aio-pika`` (ARCHITECTURE §4.3, ADR-0019).

- Declara a fila igual ao publisher (durável, sem argumentos, ADR-0016) e liga em
  ``ohip.events`` as routing keys configuradas; o exchange é conferido passivamente (quem
  declara é o publisher). Reprocessamentos chegam pelo fanout ``ohip.reprocess``.
- Uma mensagem por vez; ``ack`` ou ``nack(requeue=True)`` conforme o ``Disposition``.
- Corpo que não é JSON ou sem ``message_id``: log e ``ack`` (a fonte da verdade é o Oracle).
- Broker fora: reconecta com backoff exponencial. Fila com argumentos diferentes ou sem
  permissão: ``BrokerMisconfiguredError`` (exige ação humana).
- Parada: termina a mensagem em curso e fecha; mensagens sem ``ack`` voltam para a fila.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
from aiormq.exceptions import ChannelAccessRefused, ChannelPreconditionFailed

from ohip_streaming.application.errors import BrokerMisconfiguredError
from ohip_streaming.application.timing import run_or_stop, sleep_or_stop
from ohip_streaming.application.use_cases.enrich_event import InboundMessage
from ohip_streaming.application.use_cases.enricher_service import Disposition
from ohip_streaming.domain.backoff import exponential_backoff
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

Handler = Callable[[InboundMessage], Awaitable[Disposition]]
Sleep = Callable[[float], Awaitable[None]]
_BROKER_ERRORS = (AMQPError, OSError, TimeoutError, ChannelInvalidStateError)


@dataclass(frozen=True, slots=True)
class ConsumerOptions:
    queue: str = "ohip.enricher"
    events_exchange: str = "ohip.events"
    reprocess_exchange: str = "ohip.reprocess"
    bindings: tuple[str, ...] = ()  # routing keys em ohip.events (vazio até a Q-1)
    prefetch: int = 5
    connect_timeout_s: float = 10.0
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 60.0


def inbound_message(
    message_id: str | None, body: bytes, exchange: str | None, reprocess_exchange: str
) -> InboundMessage | None:
    """Converte a mensagem da fila. ``None`` = descartável (sem id ou corpo ilegível)."""
    if not message_id:
        return None
    try:
        payload: Any = json.loads(body)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    return InboundMessage(message_id, payload, is_reprocess=exchange == reprocess_exchange)


@dataclass
class AioPikaQueueConsumer:
    url: str = field(repr=False)  # contém a senha
    options: ConsumerOptions = field(default_factory=ConsumerOptions)
    sleep: Sleep = asyncio.sleep

    async def run(self, handler: Handler, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            try:
                await self._consume(handler, stop)
                failures = 0
            except (ChannelPreconditionFailed, ChannelAccessRefused) as exc:
                raise BrokerMisconfiguredError(f"fila do enricher divergente: {exc}") from None
            except _BROKER_ERRORS as exc:
                failures += 1
                o = self.options
                wait = exponential_backoff(failures, o.backoff_initial_s, o.backoff_max_s)
                log.warning("enricher_broker_indisponivel", wait_s=wait, error=type(exc).__name__)
                await sleep_or_stop(wait, stop, sleep=self.sleep)

    async def _consume(self, handler: Handler, stop: asyncio.Event) -> None:
        o = self.options
        connection = await aio_pika.connect(self.url, timeout=o.connect_timeout_s)
        try:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=o.prefetch)
            queue = await channel.declare_queue(o.queue, durable=True)
            if o.bindings:
                events = await channel.get_exchange(o.events_exchange, ensure=True)
                for key in o.bindings:
                    await queue.bind(events, routing_key=key)
            log.info("enricher_consumindo", queue=o.queue, bindings=list(o.bindings))
            async with queue.iterator() as messages:
                while not stop.is_set():
                    message = await run_or_stop(_next(messages), stop, ignore=_BROKER_ERRORS)
                    if message is None:
                        return  # parada pedida
                    await self._dispatch(message, handler)
        finally:
            with contextlib.suppress(*_BROKER_ERRORS, RuntimeError):
                await connection.close()

    async def _dispatch(self, message: AbstractIncomingMessage, handler: Handler) -> None:
        inbound = inbound_message(
            message.message_id, message.body, message.exchange, self.options.reprocess_exchange
        )
        if inbound is None:
            log.error(
                "enricher_mensagem_descartada",
                message_id=message.message_id,
                exchange=message.exchange,
            )
            await message.ack()
            return
        disposition = await handler(inbound)
        if disposition is Disposition.ACK:
            await message.ack()
        else:
            await message.nack(requeue=True)


async def _next(messages: Any) -> AbstractIncomingMessage:
    try:
        message: AbstractIncomingMessage = await messages.__anext__()
    except StopAsyncIteration:
        raise ChannelInvalidStateError("fila fechada pelo broker") from None
    return message


def bindings_from(keys: Sequence[str]) -> tuple[str, ...]:
    return tuple(key.strip() for key in keys if key.strip())
