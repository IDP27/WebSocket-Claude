"""Consumo da fila ``ohip.enricher`` com aio-pika falso (ADR-0019)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import aio_pika
import pytest
from aio_pika.exceptions import AMQPConnectionError
from aiormq.exceptions import ChannelPreconditionFailed

from ohip_streaming.adapters.rabbitmq.consumer import (
    AioPikaQueueConsumer,
    ConsumerOptions,
    bindings_from,
    inbound_message,
)
from ohip_streaming.application.errors import BrokerMisconfiguredError
from ohip_streaming.application.use_cases.enrich_event import InboundMessage
from ohip_streaming.application.use_cases.enricher_service import Disposition


@dataclass
class Incoming:
    message_id: str | None
    body: bytes
    exchange: str | None = "ohip.events"
    acked: bool = False
    requeued: bool | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self.requeued = requeue


@dataclass
class Broker:
    messages: list[Incoming] = field(default_factory=list)
    connects: int = 0
    connect_errors: list[Exception] = field(default_factory=list)
    declare_error: Exception | None = None
    end_after_messages: bool = False  # o broker fecha a fila depois de entregar tudo
    qos: list[int] = field(default_factory=list)
    declared: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    bindings: list[tuple[str, str]] = field(default_factory=list)
    closed: int = 0


class Iterator:
    def __init__(self, broker: Broker) -> None:
        self.broker = broker

    async def __aenter__(self) -> AsyncIterator[Incoming]:
        return self._gen()

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def _gen(self) -> AsyncIterator[Incoming]:
        while self.broker.messages:
            yield self.broker.messages.pop(0)
        if self.broker.end_after_messages:
            return
        await asyncio.Event().wait()  # sem mais mensagens: fica esperando


class Queue:
    def __init__(self, broker: Broker, name: str) -> None:
        self.broker, self.name = broker, name

    async def bind(self, exchange: Any, routing_key: str) -> None:
        self.broker.bindings.append((exchange, routing_key))

    def iterator(self) -> Iterator:
        return Iterator(self.broker)


class Channel:
    def __init__(self, broker: Broker) -> None:
        self.broker = broker

    async def set_qos(self, prefetch_count: int) -> None:
        self.broker.qos.append(prefetch_count)

    async def declare_queue(self, name: str, **kwargs: Any) -> Queue:
        if self.broker.declare_error is not None:
            raise self.broker.declare_error
        self.broker.declared.append((name, kwargs))
        return Queue(self.broker, name)

    async def get_exchange(self, name: str, ensure: bool = True) -> str:
        assert ensure  # passivo: quem declara é o publisher
        return name


class Connection:
    def __init__(self, broker: Broker) -> None:
        self.broker = broker

    async def channel(self) -> Channel:
        return Channel(self.broker)

    async def close(self) -> None:
        self.broker.closed += 1


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> Broker:
    broker = Broker()

    async def connect(url: str, **kwargs: Any) -> Connection:
        broker.connects += 1
        if broker.connect_errors:
            raise broker.connect_errors.pop(0)
        return Connection(broker)

    monkeypatch.setattr(aio_pika, "connect", connect)
    return broker


def body(raw_event_id: int = 1) -> bytes:
    return json.dumps({"raw_event_id": raw_event_id}).encode()


async def run_until(
    consumer: AioPikaQueueConsumer,
    handled: list[InboundMessage],
    stop: asyncio.Event,
    count: int,
    disposition: Disposition = Disposition.ACK,
) -> None:
    async def handler(message: InboundMessage) -> Disposition:
        handled.append(message)
        if len(handled) >= count:
            stop.set()
        return disposition

    await asyncio.wait_for(consumer.run(handler, stop), 2)


def test_inbound_message_parsing() -> None:
    reprocess = inbound_message("u1", body(), "ohip.reprocess", "ohip.reprocess")
    assert reprocess is not None
    assert reprocess.is_reprocess
    assert reprocess.body == {"raw_event_id": 1}
    normal = inbound_message("u1", body(), "ohip.events", "ohip.reprocess")
    assert normal is not None
    assert not normal.is_reprocess
    assert inbound_message(None, body(), "x", "ohip.reprocess") is None
    assert inbound_message("u1", b"{nao", "x", "ohip.reprocess") is None
    assert inbound_message("u1", b"[1]", "x", "ohip.reprocess") is None
    assert bindings_from([" ohip.rsv.# ", "", "ohip.crm.*"]) == ("ohip.rsv.#", "ohip.crm.*")


async def test_consumes_acks_and_declares(broker: Broker) -> None:
    good = Incoming("u1", body(), "ohip.reprocess")
    broker.messages = [good]
    options = ConsumerOptions(bindings=("ohip.rsv.#",), prefetch=7)
    handled: list[InboundMessage] = []
    await run_until(
        AioPikaQueueConsumer("amqp://u:senha@h/v", options), handled, asyncio.Event(), 1
    )
    assert good.acked
    assert handled[0].is_reprocess
    assert broker.qos == [7]
    assert broker.declared == [("ohip.enricher", {"durable": True})]  # igual ao publisher
    assert broker.bindings == [("ohip.events", "ohip.rsv.#")]
    assert broker.closed == 1


async def test_requeue_and_discard(broker: Broker) -> None:
    junk = Incoming(None, b"x")
    requeued = Incoming("u2", body())
    broker.messages = [junk, requeued]
    handled: list[InboundMessage] = []
    await run_until(
        AioPikaQueueConsumer("amqp://h"), handled, asyncio.Event(), 1, Disposition.REQUEUE
    )
    assert junk.acked  # descartada com log, sem passar pelo handler
    assert [m.message_id for m in handled] == ["u2"]
    assert requeued.requeued is True
    assert not requeued.acked


async def test_reconnects_with_backoff(broker: Broker) -> None:
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    broker.connect_errors = [AMQPConnectionError("fora"), OSError("rede"), AMQPConnectionError("x")]
    broker.messages = [Incoming("u1", body())]
    options = ConsumerOptions(backoff_initial_s=1, backoff_max_s=3)
    handled: list[InboundMessage] = []
    await run_until(AioPikaQueueConsumer("amqp://h", options, sleep), handled, asyncio.Event(), 1)
    assert sleeps == [1, 2, 3]
    assert broker.connects == 4


async def test_queue_closed_by_broker_reconnects(broker: Broker) -> None:
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        broker.end_after_messages = False
        broker.messages = [Incoming("u2", body())]

    broker.end_after_messages = True
    broker.messages = [Incoming("u1", body())]
    handled: list[InboundMessage] = []
    await run_until(AioPikaQueueConsumer("amqp://h", sleep=sleep), handled, asyncio.Event(), 2)
    assert [m.message_id for m in handled] == ["u1", "u2"]
    assert broker.connects == 2


async def test_divergent_queue_is_a_configuration_error(broker: Broker) -> None:
    broker.declare_error = ChannelPreconditionFailed("PRECONDITION_FAILED")
    with pytest.raises(BrokerMisconfiguredError):
        await AioPikaQueueConsumer("amqp://h").run(_never, asyncio.Event())
    assert broker.closed == 1


async def test_stop_while_waiting(broker: Broker) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(AioPikaQueueConsumer("amqp://h").run(_never, stop))
    await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert broker.closed == 1


async def test_stop_during_backoff(broker: Broker) -> None:
    broker.connect_errors = [AMQPConnectionError("fora")]
    stop = asyncio.Event()

    async def slow(seconds: float) -> None:
        await asyncio.sleep(10)

    task = asyncio.create_task(AioPikaQueueConsumer("amqp://h", sleep=slow).run(_never, stop))
    await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 1)


def test_url_is_not_in_repr() -> None:
    assert "senha" not in repr(AioPikaQueueConsumer("amqp://u:senha@h/v"))


async def _never(message: InboundMessage) -> Disposition:
    raise AssertionError("não devia receber mensagem")
