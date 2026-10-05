"""AioPikaPublisher contra um RabbitMQ de teste real (topologia, confirms e alternate exchange).

Só roda com ``TEST_RABBITMQ_URL`` **e** ``TEST_RABBITMQ_DISPOSABLE=sim`` (vhost descartável:
o teste declara a topologia com nomes prefixados por um id aleatório e apaga no fim).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace

import aio_pika
import pytest

from ohip_streaming.adapters.rabbitmq.publisher import AioPikaPublisher, Topology
from ohip_streaming.application.errors import BrokerUnavailableError, ChannelClosedError
from ohip_streaming.application.ports import OutgoingMessage, PublishOutcome

_URL = os.environ.get("TEST_RABBITMQ_URL")
pytestmark = [
    pytest.mark.rabbitmq,
    pytest.mark.skipif(
        not _URL or os.environ.get("TEST_RABBITMQ_DISPOSABLE") != "sim",
        reason="RabbitMQ de teste não configurado "
        "(TEST_RABBITMQ_URL e TEST_RABBITMQ_DISPOSABLE=sim)",
    ),
]


@pytest.fixture
async def topology() -> AsyncIterator[Topology]:
    run = uuid.uuid4().hex[:8]
    names = Topology(
        events_exchange=f"t{run}.events",
        unrouted_exchange=f"t{run}.unrouted",
        unrouted_queue=f"t{run}.unrouted.q",
        unrouted_max_length=10,
        reprocess_exchange=f"t{run}.reprocess",
        enricher_queue=f"t{run}.enricher",
    )
    yield names
    assert _URL is not None
    connection = await aio_pika.connect(_URL)
    channel = await connection.channel()
    for queue in (names.unrouted_queue, names.enricher_queue):
        await channel.queue_delete(queue)
    for exchange in (names.events_exchange, names.unrouted_exchange, names.reprocess_exchange):
        await channel.exchange_delete(exchange)
    await connection.close()


def message(exchange: str, routing_key: str, uid: str) -> OutgoingMessage:
    return OutgoingMessage(exchange, routing_key, uid, b'{"x":1}', {"x-schema-version": 1})


async def test_publish_routing_and_properties(topology: Topology) -> None:
    assert _URL is not None
    pub = AioPikaPublisher(url=_URL, topology=topology)
    connection = await aio_pika.connect(_URL)
    channel = await connection.channel()
    try:
        # garante a topologia antes de ligar a fila de teste
        events = message(topology.events_exchange, "ohip.rsv.UPDATE_RESERVATION", "uid-1")
        assert await pub.publish(events) is PublishOutcome.ACKED  # sem fila ligada → unrouted
        consumer_queue = await channel.declare_queue(exclusive=True)
        await consumer_queue.bind(topology.events_exchange, routing_key="ohip.rsv.#")
        assert await pub.publish(message(topology.events_exchange, "ohip.rsv.X", "uid-2")) is (
            PublishOutcome.ACKED
        )
        reprocess = message(topology.reprocess_exchange, "ohip.rsv.X", "uid-3")
        assert await pub.publish(reprocess) is PublishOutcome.ACKED

        got = await consumer_queue.get(timeout=5)
        assert got.message_id == "uid-2"
        assert got.content_type == "application/json"
        assert got.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert got.headers == {"x-schema-version": 1}
        await got.ack()

        unrouted = await channel.get_queue(topology.unrouted_queue)
        parked = await unrouted.get(timeout=5)
        assert parked.message_id == "uid-1"  # nada some em silêncio
        await parked.ack()

        enricher = await channel.get_queue(topology.enricher_queue)
        redone = await enricher.get(timeout=5)
        assert redone.message_id == "uid-3"  # fanout: chega qualquer que seja a routing key
        await redone.ack()
    finally:
        await pub.close()
        await connection.close()


async def test_connection_drop_does_not_blame_two_chains(topology: Topology) -> None:
    """A conexão cai com duas chains publicando: nenhuma cabeça recebe ``ChannelClosedError``
    (que conta tentativa, ADR-0011 nº 7); depois as duas voltam a publicar."""
    assert _URL is not None
    pub = AioPikaPublisher(url=_URL, topology=topology)
    a = replace(message(topology.events_exchange, "ohip.rsv.X", "a1"), lane="A")
    b = replace(message(topology.events_exchange, "ohip.rsv.X", "b1"), lane="B")
    try:
        assert await pub.publish(a) is PublishOutcome.ACKED
        assert await pub.publish(b) is PublishOutcome.ACKED
        connection = pub._connection
        assert connection is not None
        for _ in range(20):
            results = await asyncio.gather(
                pub.publish(replace(a, message_id=uuid.uuid4().hex)),
                pub.publish(replace(b, message_id=uuid.uuid4().hex)),
                connection.close(),
                return_exceptions=True,
            )
            for result in results[:2]:
                assert not isinstance(result, ChannelClosedError), result
                assert result is PublishOutcome.ACKED or isinstance(
                    result, BrokerUnavailableError
                ), result
            # reconecta antes da próxima volta, para fechar sempre uma conexão viva
            assert await pub.publish(replace(a, message_id=uuid.uuid4().hex)) is (
                PublishOutcome.ACKED
            )
            assert pub._connection is not None
            assert pub._connection is not connection
            connection = pub._connection
        assert await pub.publish(replace(a, message_id="a-fim")) is PublishOutcome.ACKED
        assert await pub.publish(replace(b, message_id="b-fim")) is PublishOutcome.ACKED
    finally:
        await pub.close()
