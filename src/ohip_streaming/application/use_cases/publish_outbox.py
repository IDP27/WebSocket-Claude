"""Publica a outbox de uma chain no RabbitMQ, em ordem (ADR-0002, ARCHITECTURE §4.2).

- Cabeça da fila por chain: se a primeira linha está em backoff, a chain inteira espera.
- Uma mensagem por vez, esperando o confirm: um ``nack`` nunca deixa a seguinte já aceita.
- Conexão com o broker perdida: a exceção sobe (backoff global no entrypoint), sem contar
  tentativa. Canal fechado pelo broker repetidas vezes na mesma linha: conta como falha dela.
- ``nack``: tentativa com backoff; esgotadas, ``FAILED`` + DLQ e a chain continua.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from ohip_streaming.application.errors import ChannelClosedError
from ohip_streaming.application.ports import (
    Clock,
    MessagePublisher,
    MetricsSink,
    OutboxRow,
    OutboxStore,
    OutgoingMessage,
    PublishOutcome,
)
from ohip_streaming.domain.rules import publish_retry_delay_s
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class PublisherOptions:
    batch_limit: int = 100
    max_attempts: int = 10
    broker_max_message_bytes: int = 16 * 1024 * 1024
    channel_fail_attribution: int = 3


@dataclass(slots=True)
class ChainPublishResult:
    sent: int = 0
    failed: int = 0
    blocked_until: datetime | None = None  # cabeça em backoff: não chamar antes disso


class PublishOutbox:
    def __init__(
        self,
        *,
        store: OutboxStore,
        publisher: MessagePublisher,
        clock: Clock,
        metrics: MetricsSink,
        options: PublisherOptions,
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._clock = clock
        self._metrics = metrics
        self._options = options
        # Quedas de canal seguidas por linha (cabeças de chains diferentes, independentes).
        # Em memória: um restart zera, o que só adia a contagem (ADR-0011).
        self._channel_failures: dict[int, int] = {}

    async def publish_chain(self, chain_code: str, lease_epoch: int) -> ChainPublishResult:
        result = ChainPublishResult()
        rows = await self._store.fetch_head(chain_code, self._options.batch_limit)
        for row in rows:
            now = self._clock.now()
            if row.next_attempt_at > now:
                result.blocked_until = row.next_attempt_at
                break
            if not await self._publish_row(row, lease_epoch, result, now):
                break
        if result.sent:
            self._metrics.increment("ohip_outbox_sent_total", result.sent, chain_code=chain_code)
        if result.failed:
            self._metrics.increment(
                "ohip_outbox_failed_total", result.failed, chain_code=chain_code
            )
        return result

    async def _publish_row(
        self, row: OutboxRow, lease_epoch: int, result: ChainPublishResult, now: datetime
    ) -> bool:
        """Publica uma linha. Devolve False quando a chain deve parar nesta rodada."""
        body = row.body.encode("utf-8")
        if len(body) > self._options.broker_max_message_bytes:
            await self._fail(row, row.attempts, "mensagem acima do limite do broker", lease_epoch)
            result.failed += 1
            return True

        message = OutgoingMessage(
            exchange=row.exchange_name,
            routing_key=row.routing_key,
            message_id=row.message_id,
            body=body,
            headers={"x-schema-version": row.schema_version},
            lane=row.chain_code,
        )
        try:
            outcome = await self._publisher.publish(message)
        except ChannelClosedError as exc:
            failures = self._channel_failures.get(row.id, 0) + 1
            self._channel_failures[row.id] = failures
            if failures < self._options.channel_fail_attribution:
                raise
            log.warning("canal_caiu_repetidamente_na_linha", outbox_id=row.id, failures=failures)
            self._channel_failures.pop(row.id, None)  # recomeça a contagem para a próxima tentativa
            return await self._handle_nack(
                row, f"canal fechado pelo broker: {exc}", lease_epoch, result, now
            )

        if outcome is PublishOutcome.ACKED:
            await self._store.mark_sent(row.id, lease_epoch)
            self._channel_failures.pop(row.id, None)
            result.sent += 1
            return True
        return await self._handle_nack(row, "nack do broker", lease_epoch, result, now)

    async def _handle_nack(
        self,
        row: OutboxRow,
        error: str,
        lease_epoch: int,
        result: ChainPublishResult,
        now: datetime,
    ) -> bool:
        attempts = row.attempts + 1
        if attempts >= self._options.max_attempts:
            await self._fail(row, attempts, error, lease_epoch)
            result.failed += 1
            return True  # a chain continua (única quebra de ordem permitida)
        next_attempt_at = now + timedelta(seconds=publish_retry_delay_s(attempts))
        await self._store.record_failure(row.id, attempts, next_attempt_at, error, lease_epoch)
        result.blocked_until = next_attempt_at
        return False

    async def _fail(self, row: OutboxRow, attempts: int, error: str, lease_epoch: int) -> None:
        log.error(
            "outbox_failed",
            outbox_id=row.id,
            chain_code=row.chain_code,
            unique_event_id=row.message_id,
            attempts=attempts,
            error=error,
        )
        await self._store.mark_failed(row.id, attempts, error, lease_epoch)
        self._channel_failures.pop(row.id, None)
