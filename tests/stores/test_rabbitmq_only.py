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


async def test_enricher_consumer_receives_reprocess_and_bound_events(topology: Topology) -> None:
    """O enricher declara a fila igual ao publisher, liga as routing keys e diferencia
    reprocessamento (ADR-0016, ADR-0019)."""
    from ohip_streaming.adapters.rabbitmq.consumer import AioPikaQueueConsumer, ConsumerOptions
    from ohip_streaming.application.use_cases.enrich_event import InboundMessage
    from ohip_streaming.application.use_cases.enricher_service import Disposition

    assert _URL is not None
    pub = AioPikaPublisher(url=_URL, topology=topology)
    try:
        assert await pub.publish(message(topology.reprocess_exchange, "ohip.rsv.X", "r1")) is (
            PublishOutcome.ACKED
        )
        options = ConsumerOptions(
            queue=topology.enricher_queue,
            events_exchange=topology.events_exchange,
            reprocess_exchange=topology.reprocess_exchange,
            bindings=("ohip.rsv.#",),
        )
        received: list[InboundMessage] = []
        stop = asyncio.Event()

        async def handler(inbound: InboundMessage) -> Disposition:
            received.append(inbound)
            if inbound.message_id == "r1":  # depois da binding, publica o evento normal
                await pub.publish(message(topology.events_exchange, "ohip.rsv.Y", "e1"))
            else:
                stop.set()
            return Disposition.ACK

        await asyncio.wait_for(AioPikaQueueConsumer(_URL, options).run(handler, stop), 15)
        assert [(m.message_id, m.is_reprocess) for m in received] == [("r1", True), ("e1", False)]
    finally:
        await pub.close()


async def test_enricher_consumer_requeues_and_discards(topology: Topology) -> None:
    """``REQUEUE`` devolve a mensagem para a fila (volta como reentrega); corpo ilegível
    leva ``ack`` e não volta (ADR-0019)."""
    from ohip_streaming.adapters.rabbitmq.consumer import AioPikaQueueConsumer, ConsumerOptions
    from ohip_streaming.application.use_cases.enrich_event import InboundMessage
    from ohip_streaming.application.use_cases.enricher_service import Disposition

    assert _URL is not None
    pub = AioPikaPublisher(url=_URL, topology=topology)
    try:
        bad = OutgoingMessage(topology.reprocess_exchange, "ohip.rsv.X", "ruim", b"{nao-json", {})
        assert await pub.publish(bad) is PublishOutcome.ACKED
        good = message(topology.reprocess_exchange, "ohip.rsv.X", "m1")
        assert await pub.publish(good) is PublishOutcome.ACKED
        options = ConsumerOptions(
            queue=topology.enricher_queue,
            events_exchange=topology.events_exchange,
            reprocess_exchange=topology.reprocess_exchange,
            prefetch=1,
        )
        seen: list[str] = []
        stop = asyncio.Event()

        async def handler(inbound: InboundMessage) -> Disposition:
            seen.append(inbound.message_id)
            if len(seen) == 1:
                return Disposition.REQUEUE
            stop.set()
            return Disposition.ACK

        await asyncio.wait_for(AioPikaQueueConsumer(_URL, options).run(handler, stop), 15)
        assert seen == ["m1", "m1"]  # "ruim" nunca chega ao handler

        connection = await aio_pika.connect(_URL)
        try:
            channel = await connection.channel()
            queue = await channel.get_queue(topology.enricher_queue)
            assert queue.declaration_result.message_count == 0  # nada ficou para trás
        finally:
            await connection.close()
    finally:
        await pub.close()


async def test_enricher_queue_with_other_arguments_is_misconfigured() -> None:
    """Fila já declarada com outros argumentos: ``BrokerMisconfiguredError`` (o processo sai
    com código 2), sem laço de reconexão (ADR-0019)."""
    from ohip_streaming.adapters.rabbitmq.consumer import AioPikaQueueConsumer, ConsumerOptions
    from ohip_streaming.application.errors import BrokerMisconfiguredError
    from ohip_streaming.application.use_cases.enricher_service import Disposition

    assert _URL is not None
    name = f"t{uuid.uuid4().hex[:8]}.enricher"
    connection = await aio_pika.connect(_URL)
    try:
        channel = await connection.channel()
        await channel.declare_queue(name, durable=True, arguments={"x-max-length": 10})

        async def handler(_: object) -> Disposition:
            raise AssertionError("não deveria consumir")

        consumer = AioPikaQueueConsumer(_URL, ConsumerOptions(queue=name))
        with pytest.raises(BrokerMisconfiguredError):
            await asyncio.wait_for(consumer.run(handler, asyncio.Event()), 15)
    finally:
        channel = await connection.channel()
        await channel.queue_delete(name)
        await connection.close()


async def test_probe_broker() -> None:
    """``/ready``: conexão abre e fecha; porta errada vira ``BrokerUnavailableError`` sem a URL
    (que tem a senha) na mensagem (ADR-0017 §8)."""
    from urllib.parse import urlsplit, urlunsplit

    from ohip_streaming.adapters.rabbitmq.probe import probe_broker

    assert _URL is not None
    await probe_broker(_URL, timeout_s=5)
    parts = urlsplit(_URL)
    credentials = parts.netloc.rpartition("@")[0]
    netloc = f"{credentials}@{parts.hostname}:1" if credentials else f"{parts.hostname}:1"
    wrong = urlunsplit(parts._replace(netloc=netloc))  # mesmo host, porta 1: conexão recusada
    with pytest.raises(BrokerUnavailableError) as caught:
        await probe_broker(wrong, timeout_s=5)
    assert wrong not in str(caught.value)
    assert (parts.password or "\0") not in str(caught.value)
