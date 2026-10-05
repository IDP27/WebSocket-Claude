"""AioPikaPublisher com aio-pika falso: propriedades AMQP, topologia e classificação de falhas."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any, cast

import aio_pika
import pytest
from aio_pika import DeliveryMode, ExchangeType
from aio_pika.exceptions import (
    AMQPConnectionError,
    ChannelClosed,
    ChannelInvalidStateError,
    DeliveryError,
)
from aiormq.exceptions import (
    ChannelAccessRefused,
    ChannelLockedResource,
    ChannelNotFoundEntity,
    ChannelPreconditionFailed,
)
from pamqp.commands import Basic

from ohip_streaming.adapters.rabbitmq.publisher import AioPikaPublisher, Topology
from ohip_streaming.application.errors import (
    BrokerMisconfiguredError,
    BrokerUnavailableError,
    ChannelClosedError,
)
from ohip_streaming.application.ports import OutgoingMessage, PublishOutcome

MESSAGE = OutgoingMessage(
    exchange="ohip.events",
    routing_key="ohip.rsv.UPDATE_RESERVATION",
    message_id="uid-1",
    body=b'{"a":1}',
    headers={"x-schema-version": 1},
)


class FakeExchange:
    def __init__(self, broker: FakeBroker, name: str, channel: FakeChannel | None = None) -> None:
        self.broker, self.name, self.channel = broker, name, channel

    async def publish(self, message: Any, routing_key: str, **kwargs: Any) -> Any:
        self.broker.published.append((self.name, routing_key, message, kwargs))
        if self.channel is not None and self.channel.is_closed:
            raise ChannelInvalidStateError("canal fechado")
        by_message = self.broker.outcome_for.pop(message.message_id, None)
        if by_message is not None:
            outcome = by_message
        else:
            outcome = self.broker.outcomes.pop(0) if self.broker.outcomes else Basic.Ack()
        if isinstance(outcome, BaseException):
            if self.broker.close_connection_on_error:
                self.broker.connection.is_closed = True
            if isinstance(outcome, ChannelClosed) and self.channel is not None:
                self.channel.is_closed = True  # o broker fecha o canal inteiro
            raise outcome
        return outcome


class FakeQueue:
    def __init__(self, broker: FakeBroker, name: str) -> None:
        self.broker, self.name = broker, name

    async def bind(self, exchange: Any, routing_key: str | None = None) -> None:
        self.broker.bindings.append((self.name, exchange.name))


class FakeChannel:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.is_closed = False

    async def declare_exchange(self, name: str, kind: Any, **kwargs: Any) -> FakeExchange:
        if self.broker.declare_error is not None:
            raise self.broker.declare_error
        self.broker.exchanges[name] = (kind, kwargs)
        return FakeExchange(self.broker, name)

    async def declare_queue(self, name: str, **kwargs: Any) -> FakeQueue:
        self.broker.queues[name] = kwargs
        return FakeQueue(self.broker, name)

    async def get_exchange(self, name: str, ensure: bool = True) -> FakeExchange:
        return FakeExchange(self.broker, name, self)

    async def close(self) -> None:
        self.is_closed = True


class FakeConnection:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.is_closed = False

    async def channel(self, **kwargs: Any) -> FakeChannel:
        self.broker.channel_kwargs.append(kwargs)
        self.broker.channels += 1
        channel = FakeChannel(self.broker)
        self.broker.opened.append(channel)
        return channel

    async def close(self) -> None:
        self.is_closed = True


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, Any, dict[str, Any]]] = []
        self.outcomes: list[Any] = []
        self.exchanges: dict[str, Any] = {}
        self.queues: dict[str, Any] = {}
        self.bindings: list[tuple[str, str]] = []
        self.channel_kwargs: list[dict[str, Any]] = []
        self.channels = 0
        self.opened: list[FakeChannel] = []
        self.outcome_for: dict[str, Any] = {}
        self.connects = 0
        self.fail_connect = False
        self.declare_error: Exception | None = None
        self.close_connection_on_error = False
        self.connection = FakeConnection(self)

    async def connect(self, url: str, timeout: float) -> FakeConnection:  # noqa: ASYNC109 - assinatura do aio_pika.connect
        self.connects += 1
        if self.fail_connect:
            raise AMQPConnectionError("recusada")
        self.connection = FakeConnection(self)
        return self.connection


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> FakeBroker:
    fake = FakeBroker()
    monkeypatch.setattr(aio_pika, "connect", fake.connect)
    return fake


def publisher() -> AioPikaPublisher:
    return AioPikaPublisher(url="amqps://u:senha@broker/vhost", confirm_timeout_s=5)


async def test_ack_and_amqp_properties(broker: FakeBroker) -> None:
    assert await publisher().publish(MESSAGE) is PublishOutcome.ACKED
    exchange, routing_key, message, kwargs = broker.published[0]
    assert (exchange, routing_key) == ("ohip.events", "ohip.rsv.UPDATE_RESERVATION")
    assert message.message_id == "uid-1"
    assert message.content_type == "application/json"
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    assert message.headers == {"x-schema-version": 1}
    assert message.body == b'{"a":1}'
    assert kwargs == {"mandatory": True, "timeout": 5}
    assert broker.channel_kwargs == [
        {"publisher_confirms": False},  # canal só para declarar a topologia
        {"publisher_confirms": True, "on_return_raises": False},  # canal da chain
    ]


async def test_declares_our_topology_once(broker: FakeBroker) -> None:
    pub = publisher()
    await pub.publish(MESSAGE)
    await pub.publish(MESSAGE)
    assert broker.channels == 2  # topologia + um canal para a chain
    assert broker.opened[0].is_closed  # o de topologia é fechado
    assert broker.exchanges["ohip.events"] == (
        ExchangeType.TOPIC,
        {"durable": True, "arguments": {"alternate-exchange": "ohip.events.unrouted"}},
    )
    assert broker.exchanges["ohip.events.unrouted"][0] is ExchangeType.FANOUT
    assert broker.exchanges["ohip.reprocess"][0] is ExchangeType.FANOUT
    assert broker.queues["ohip.unrouted"]["arguments"] == {
        "x-max-length": 100_000,
        "x-overflow": "drop-head",
    }
    assert set(broker.bindings) == {
        ("ohip.unrouted", "ohip.events.unrouted"),
        ("ohip.enricher", "ohip.reprocess"),
    }


async def test_nack_and_returned_message_count_as_message_failure(broker: FakeBroker) -> None:
    returned = object()  # DeliveredMessage de um Basic.Return
    broker.outcomes = [
        DeliveryError(None, Basic.Nack()),
        returned,
        DeliveryError(None, Basic.Return()),
    ]
    pub = publisher()
    assert await pub.publish(MESSAGE) is PublishOutcome.NACKED
    assert await pub.publish(MESSAGE) is PublishOutcome.NACKED
    assert await pub.publish(MESSAGE) is PublishOutcome.NACKED


async def test_channel_closed_with_connection_alive(broker: FakeBroker) -> None:
    broker.outcomes = [ChannelClosed(406, "PRECONDITION_FAILED")]
    pub = publisher()
    with pytest.raises(ChannelClosedError):
        await pub.publish(MESSAGE)
    assert await pub.publish(MESSAGE) is PublishOutcome.ACKED  # canal novo
    assert broker.channels == 3
    assert broker.connects == 1


async def test_channel_closed_with_connection_lost(broker: FakeBroker) -> None:
    broker.outcomes = [ChannelClosed(320, "CONNECTION_FORCED")]
    broker.close_connection_on_error = True
    with pytest.raises(BrokerUnavailableError) as info:
        await publisher().publish(MESSAGE)
    assert not isinstance(info.value, ChannelClosedError)


@pytest.mark.parametrize(
    "error",
    [
        ChannelClosed(406, "PRECONDITION_FAILED"),
        ChannelClosed(311, "CONTENT_TOO_LARGE"),
        ChannelPreconditionFailed("PRECONDITION_FAILED"),
        ChannelNotFoundEntity("NOT_FOUND"),
        ChannelAccessRefused("ACCESS_REFUSED"),
        ChannelLockedResource("RESOURCE_LOCKED"),
    ],
)
async def test_broker_channel_error_is_attributed_to_the_message(
    broker: FakeBroker, error: Exception
) -> None:
    broker.outcomes = [error]
    with pytest.raises(ChannelClosedError):
        await publisher().publish(MESSAGE)


@pytest.mark.parametrize(
    "error", [ChannelClosed(None, None), ChannelClosed(320, "CONNECTION_FORCED")]
)
async def test_channel_close_without_channel_error_is_not_the_message(
    broker: FakeBroker, error: Exception
) -> None:
    broker.outcomes = [error]  # conexão segue de pé no fake: só o código decide
    pub = publisher()
    with pytest.raises(BrokerUnavailableError) as info:
        await pub.publish(MESSAGE)
    assert not isinstance(info.value, ChannelClosedError)
    assert await pub.publish(MESSAGE) is PublishOutcome.ACKED


async def test_channel_closed_locally_with_connection_alive_is_unavailable(
    broker: FakeBroker,
) -> None:
    pub = publisher()
    assert await pub.publish(MESSAGE) is PublishOutcome.ACKED
    broker.opened[1].is_closed = True  # o aiormq marcou o canal antes da conexão
    pub._lanes[""].channel = cast(Any, _ClosedAfterCheck(broker.opened[1]))
    with pytest.raises(BrokerUnavailableError) as info:
        await pub.publish(replace(MESSAGE, message_id="uid-2"))
    assert not isinstance(info.value, ChannelClosedError)
    assert await pub.publish(replace(MESSAGE, message_id="uid-3")) is PublishOutcome.ACKED
    assert broker.connects == 1  # conexão de pé: só o canal foi trocado


async def test_connection_drop_does_not_blame_heads_of_two_chains(broker: FakeBroker) -> None:
    """Queda de conexão com duas chains: o aiormq fecha os canais antes de a conexão constar
    como fechada. Nenhuma das cabeças pode receber ``ChannelClosedError`` (ADR-0011 nº 7)."""
    pub = publisher()
    a = replace(MESSAGE, lane="A", message_id="a1")
    b = replace(MESSAGE, lane="B", message_id="b1")
    assert await pub.publish(a) is PublishOutcome.ACKED
    assert await pub.publish(b) is PublishOutcome.ACKED
    for lane, channel in (("A", broker.opened[1]), ("B", broker.opened[2])):
        pub._lanes[lane].channel = cast(Any, _ClosedAfterCheck(channel))
    broker.opened[1].is_closed = broker.opened[2].is_closed = True

    results = await asyncio.gather(
        pub.publish(replace(a, message_id="a2")),
        pub.publish(replace(b, message_id="b2")),
        return_exceptions=True,
    )
    assert all(isinstance(r, BrokerUnavailableError) for r in results)
    assert not any(isinstance(r, ChannelClosedError) for r in results)


class _ClosedAfterCheck:
    """Canal que ainda parece aberto na checagem do ``_exchange`` e fecha antes do publish
    (a corrida que o revisor apontou)."""

    def __init__(self, inner: FakeChannel) -> None:
        self._inner = inner
        self._checks = 0

    @property
    def is_closed(self) -> bool:
        self._checks += 1
        return self._checks > 1 and self._inner.is_closed

    async def get_exchange(self, name: str, ensure: bool = True) -> FakeExchange:
        return FakeExchange(self._inner.broker, name, self._inner)

    async def close(self) -> None:
        await self._inner.close()


@pytest.mark.parametrize(
    "error", [AMQPConnectionError("caiu"), ConnectionResetError(), TimeoutError()]
)
async def test_connection_problems_are_broker_unavailable(
    broker: FakeBroker, error: Exception
) -> None:
    broker.outcomes = [error]
    pub = publisher()
    with pytest.raises(BrokerUnavailableError) as info:
        await pub.publish(MESSAGE)
    assert "senha" not in str(info.value)
    assert await pub.publish(MESSAGE) is PublishOutcome.ACKED  # reconecta
    assert broker.connects == 2


async def test_cannot_connect(broker: FakeBroker) -> None:
    broker.fail_connect = True
    with pytest.raises(BrokerUnavailableError, match="sem conexão"):
        await publisher().publish(MESSAGE)


@pytest.mark.parametrize(
    "error",
    [ChannelPreconditionFailed(406, "PRECONDITION_FAILED"), ChannelAccessRefused(403, "REFUSED")],
)
async def test_divergent_topology_is_a_configuration_error(
    broker: FakeBroker, error: Exception
) -> None:
    broker.declare_error = error
    with pytest.raises(BrokerMisconfiguredError):
        await publisher().publish(MESSAGE)


async def test_other_channel_close_during_declaration_is_transient(broker: FakeBroker) -> None:
    broker.declare_error = ChannelClosed(320, "CONNECTION_FORCED")
    with pytest.raises(BrokerUnavailableError) as info:
        await publisher().publish(MESSAGE)
    assert not isinstance(info.value, BrokerMisconfiguredError)


async def test_channel_failure_of_one_chain_does_not_touch_another(broker: FakeBroker) -> None:
    pub = publisher()
    a = replace(MESSAGE, lane="A", message_id="a1")
    b = replace(MESSAGE, lane="B", message_id="b1")
    assert await pub.publish(a) is PublishOutcome.ACKED
    assert await pub.publish(b) is PublishOutcome.ACKED
    channel_a, channel_b = broker.opened[1], broker.opened[2]

    broker.outcome_for["a2"] = ChannelClosed(406, "PRECONDITION_FAILED")  # culpa da chain A
    with pytest.raises(ChannelClosedError):
        await pub.publish(replace(a, message_id="a2"))
    assert channel_a.is_closed
    assert not channel_b.is_closed  # a chain B segue no seu canal
    assert await pub.publish(replace(b, message_id="b2")) is PublishOutcome.ACKED
    assert broker.connects == 1


async def test_stale_reset_does_not_drop_a_newer_connection(broker: FakeBroker) -> None:
    pub = publisher()
    await pub.publish(MESSAGE)
    old_generation = pub._generation
    broker.connection.is_closed = True  # a conexão caiu; outra chain reconecta
    await pub.publish(replace(MESSAGE, lane="B"))
    newer = broker.connection
    await pub._reset(old_generation)
    assert not newer.is_closed


def test_repr_hides_the_url() -> None:
    assert "senha" not in repr(publisher())


def test_topology_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from ohip_streaming.config import RabbitMQSettings

    monkeypatch.setenv("RABBITMQ_URL", "amqps://u:p@h/v")
    monkeypatch.setenv("RABBITMQ_UNROUTED_MAX_LENGTH", "500")
    topology = Topology.from_settings(RabbitMQSettings())
    assert topology.unrouted_max_length == 500
    assert AioPikaPublisher.from_settings(RabbitMQSettings()).confirm_timeout_s == 30


async def test_close_is_idempotent(broker: FakeBroker) -> None:
    pub = publisher()
    await pub.publish(MESSAGE)
    await pub.close()
    await pub.close()
    assert broker.connection.is_closed
