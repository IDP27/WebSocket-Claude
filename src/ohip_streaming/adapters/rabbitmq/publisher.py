"""``MessagePublisher`` com ``aio-pika`` e publisher confirms (ADR-0002, ADR-0016).

Resultado de cada publicação:

- ``Basic.Ack`` → ``ACKED``;
- ``Basic.Nack`` ou mensagem devolvida (sem rota e sem alternate exchange) → ``NACKED``: conta
  tentativa da linha;
- ``Channel.Close`` do broker com código de erro de canal (311, 403-406) e a conexão
  de pé → ``ChannelClosedError``: o caso de uso conta como falha da linha só se repetir;
- canal já fechado do nosso lado (``ChannelInvalidStateError``) ou fechado sem código de
  canal → ``BrokerUnavailableError``: a conexão pode estar caindo e a mensagem não tem culpa;
- conexão perdida, broker fora ou **confirm sem resposta no prazo** → ``BrokerUnavailableError``:
  nunca conta tentativa. Sem confirm, o resultado é desconhecido; a mensagem pode sair de novo
  e os consumidores deduplicam pelo ``message_id`` (entrega pelo menos uma vez).

**Um canal por faixa** (``OutgoingMessage.lane`` = chain), na mesma conexão. O broker fecha o
canal inteiro quando recusa uma mensagem, e o aiormq rejeita todas as publicações pendentes
daquele canal; com um canal por chain (e uma publicação por vez por chain), a queda só atinge
a chain culpada (ADR-0011 nº 7). Reabrir ou descartar um canal nunca mexe nos outros.

A conexão tem uma **geração**: um reset pedido por uma publicação só derruba a conexão em que
ela falhou, nunca uma nova que outra chain já abriu.

Propriedades AMQP (ARCHITECTURE §6): ``message_id``, ``content_type=application/json``,
``delivery_mode=persistent`` e os headers da linha (``x-schema-version``).

A topologia que é nossa (Q-13) é declarada uma vez por conexão, de forma idempotente.
PRECONDITION_FAILED ou ACCESS_REFUSED na declaração → ``BrokerMisconfiguredError``
(configuração, exige ação humana); outros fechamentos de canal → indisponível.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractExchange
from aio_pika.exceptions import (
    AMQPError,
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

from ohip_streaming.application.errors import (
    BrokerMisconfiguredError,
    BrokerUnavailableError,
    ChannelClosedError,
)
from ohip_streaming.application.ports import OutgoingMessage, PublishOutcome
from ohip_streaming.logging import get_logger

if TYPE_CHECKING:
    from ohip_streaming.config import RabbitMQSettings

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Topology:
    events_exchange: str = "ohip.events"
    unrouted_exchange: str = "ohip.events.unrouted"
    unrouted_queue: str = "ohip.unrouted"
    unrouted_max_length: int = 100_000
    reprocess_exchange: str = "ohip.reprocess"
    enricher_queue: str = "ohip.enricher"

    @classmethod
    def from_settings(cls, settings: RabbitMQSettings) -> Topology:
        return cls(
            events_exchange=settings.events_exchange,
            unrouted_exchange=settings.unrouted_exchange,
            unrouted_queue=settings.unrouted_queue,
            unrouted_max_length=settings.unrouted_max_length,
            reprocess_exchange=settings.reprocess_exchange,
            enricher_queue=settings.enricher_queue,
        )


async def declare_topology(channel: AbstractChannel, topology: Topology) -> None:
    """Exchanges e filas que são nossos (Q-13). Filas de terceiros: cada time declara.

    A fila ``ohip.enricher`` é declarada **sem argumentos**; o enricher (Fase 9) precisa
    declará-la igual (ou passivamente), senão o broker recusa a segunda declaração.
    """
    unrouted = await channel.declare_exchange(
        topology.unrouted_exchange, ExchangeType.FANOUT, durable=True
    )
    await channel.declare_exchange(
        topology.events_exchange,
        ExchangeType.TOPIC,
        durable=True,
        arguments={"alternate-exchange": topology.unrouted_exchange},
    )
    parked = await channel.declare_queue(
        topology.unrouted_queue,
        durable=True,
        arguments={"x-max-length": topology.unrouted_max_length, "x-overflow": "drop-head"},
    )
    await parked.bind(unrouted)
    # Fanout: só a fila do enricher fica ligada; a routing key (ohip.<mod>.<EVT>) não importa.
    reprocess = await channel.declare_exchange(
        topology.reprocess_exchange, ExchangeType.FANOUT, durable=True
    )
    enricher = await channel.declare_queue(topology.enricher_queue, durable=True)
    await enricher.bind(reprocess)


# Erros de canal do AMQP 0-9-1 que dizem respeito ao comando: 311 content-too-large e
# 403/404/405/406 (access-refused, not-found, resource-locked, precondition-failed).
_CHANNEL_REPLY_CODES = frozenset({311, 403, 404, 405, 406})


def _closed_by_broker(exc: ChannelClosed) -> bool:
    """``Channel.Close`` enviado pelo broker com um código de erro de canal.

    O aiormq levanta as subclasses (403, 404, 405, 406) só com o texto, e ``ChannelClosed``
    com ``(código, texto)`` para os demais códigos. Sem código (``CloseOk`` do nosso próprio
    fechamento) ou com código de conexão (320, 5xx), a culpa não é da mensagem.
    """
    if isinstance(
        exc,
        ChannelAccessRefused
        | ChannelNotFoundEntity
        | ChannelLockedResource
        | ChannelPreconditionFailed,
    ):
        return True
    code = exc.args[0] if exc.args else None
    return isinstance(code, int) and code in _CHANNEL_REPLY_CODES


@dataclass
class _Lane:
    """Canal de uma chain e os exchanges já resolvidos nele."""

    channel: AbstractChannel
    generation: int
    exchanges: dict[str, AbstractExchange] = field(default_factory=dict)


@dataclass
class AioPikaPublisher:
    url: str = field(repr=False)  # contém a senha
    topology: Topology = field(default_factory=Topology)
    connect_timeout_s: float = 10.0
    confirm_timeout_s: float = 30.0
    _connection: AbstractConnection | None = field(default=None, init=False, repr=False)
    _generation: int = field(default=0, init=False, repr=False)
    _lanes: dict[str, _Lane] = field(default_factory=dict, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    @classmethod
    def from_settings(cls, settings: RabbitMQSettings) -> AioPikaPublisher:
        return cls(
            url=settings.url.get_secret_value(),
            topology=Topology.from_settings(settings),
            connect_timeout_s=settings.connect_timeout_s,
            confirm_timeout_s=settings.confirm_timeout_s,
        )

    async def publish(self, message: OutgoingMessage) -> PublishOutcome:
        lane, exchange = await self._exchange(message.lane, message.exchange)
        amqp_message = Message(
            body=message.body,
            message_id=message.message_id,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            headers=dict(message.headers),
        )
        try:
            result: Any = await exchange.publish(
                amqp_message,
                routing_key=message.routing_key,
                mandatory=True,
                timeout=self.confirm_timeout_s,
            )
        except DeliveryError as exc:  # antes de AMQPError: nack é falha da mensagem
            if not isinstance(exc.frame, Basic.Nack):
                log.warning("rabbitmq_entrega_recusada", frame=type(exc.frame).__name__)
            return PublishOutcome.NACKED
        except TimeoutError:
            await self._reset(lane.generation)
            raise BrokerUnavailableError("confirm do broker não chegou no prazo") from None
        except ChannelInvalidStateError:
            # Canal já fechado do nosso lado (ex.: a conexão caindo antes de o aiormq marcar
            # a conexão como fechada). Não há resposta do broker sobre esta mensagem.
            await self._drop_lane(message.lane, lane)
            if not self._connection_alive(lane.generation):
                await self._reset(lane.generation)
            raise BrokerUnavailableError("canal do broker já estava fechado") from None
        except ChannelClosed as exc:
            await self._drop_lane(message.lane, lane)
            if _closed_by_broker(exc) and self._connection_alive(lane.generation):
                raise ChannelClosedError(f"canal fechado pelo broker: {exc}") from None
            await self._reset(lane.generation)
            raise BrokerUnavailableError(f"conexão com o broker perdida: {exc}") from None
        except (AMQPError, ConnectionError, OSError) as exc:
            await self._reset(lane.generation)
            raise BrokerUnavailableError(f"broker indisponível: {type(exc).__name__}") from None

        if isinstance(result, Basic.Ack):
            return PublishOutcome.ACKED
        # Mensagem devolvida (Basic.Return): sem rota. Com o alternate exchange isso não
        # deveria acontecer; indica topologia incompleta (ex.: reprocess sem fila).
        log.error("rabbitmq_mensagem_devolvida", exchange=message.exchange)
        return PublishOutcome.NACKED

    async def close(self) -> None:
        async with self._lock:
            await self._close_connection()

    # ------------------------------------------------------------------ conexão e canais

    async def _exchange(self, lane_name: str, exchange: str) -> tuple[_Lane, AbstractExchange]:
        async with self._lock:
            if self._connection is None or self._connection.is_closed:
                await self._open()
            lane = self._lanes.get(lane_name)
            if lane is None or lane.channel.is_closed or lane.generation != self._generation:
                lane = await self._open_lane(lane_name)
            if exchange not in lane.exchanges:
                try:
                    lane.exchanges[exchange] = await lane.channel.get_exchange(
                        exchange, ensure=False
                    )
                except (AMQPError, OSError) as exc:
                    self._lanes.pop(lane_name, None)
                    raise BrokerUnavailableError(
                        f"exchange {exchange}: {type(exc).__name__}"
                    ) from None
            return lane, lane.exchanges[exchange]

    async def _open(self) -> None:
        """Nova conexão (nova geração) e declaração da topologia num canal próprio."""
        await self._close_connection()
        try:
            connection = await aio_pika.connect(self.url, timeout=self.connect_timeout_s)
        except (AMQPError, OSError, TimeoutError) as exc:
            raise BrokerUnavailableError(
                f"sem conexão com o broker: {type(exc).__name__}"
            ) from None
        try:
            setup = await connection.channel(publisher_confirms=False)
            await declare_topology(setup, self.topology)
            await setup.close()
        except (ChannelPreconditionFailed, ChannelAccessRefused) as exc:
            await connection.close()
            raise BrokerMisconfiguredError(f"topologia divergente no broker: {exc}") from None
        except (AMQPError, OSError, TimeoutError) as exc:
            await connection.close()
            raise BrokerUnavailableError(f"broker indisponível: {type(exc).__name__}") from None
        self._connection = connection
        self._generation += 1
        self._lanes = {}
        log.info("rabbitmq_conectado", geracao=self._generation)

    async def _open_lane(self, lane_name: str) -> _Lane:
        if self._connection is None:
            raise BrokerUnavailableError("sem conexão com o broker")
        try:
            channel = await self._connection.channel(
                publisher_confirms=True, on_return_raises=False
            )
        except (AMQPError, OSError, TimeoutError) as exc:
            raise BrokerUnavailableError(f"sem canal com o broker: {type(exc).__name__}") from None
        lane = _Lane(channel, self._generation)
        self._lanes[lane_name] = lane
        return lane

    def _connection_alive(self, generation: int) -> bool:
        return (
            generation == self._generation
            and self._connection is not None
            and not self._connection.is_closed
        )

    async def _drop_lane(self, lane_name: str, lane: _Lane) -> None:
        """Descarta só o canal desta chain (as outras seguem nos seus)."""
        async with self._lock:
            if self._lanes.get(lane_name) is lane:
                del self._lanes[lane_name]
        if not lane.channel.is_closed:
            try:
                await lane.channel.close()
            except (AMQPError, OSError, RuntimeError):
                log.debug("rabbitmq_fechar_canal_falhou", exc_info=True)

    async def _reset(self, generation: int) -> None:
        """Derruba a conexão em que a falha aconteceu, se ainda for a atual."""
        async with self._lock:
            if generation == self._generation:
                await self._close_connection()

    async def _close_connection(self) -> None:
        connection, self._connection = self._connection, None
        self._lanes = {}
        if connection is not None and not connection.is_closed:
            try:
                await connection.close()
            except (AMQPError, OSError, RuntimeError):
                log.debug("rabbitmq_fechar_conexao_falhou", exc_info=True)
