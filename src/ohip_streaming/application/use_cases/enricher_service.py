"""Tratamento de cada mensagem da fila do enricher (ARCHITECTURE §4.3, ADR-0019).

- Sucesso → ``ACK``.
- Falha de infraestrutura (Oracle ou REST fora, 429): não conta tentativa; espera com backoff
  exponencial com a mensagem ainda sem ``ack`` e devolve à fila (``REQUEUE``).
- Falha da mensagem: até ``max_attempts`` tentativas; esgotou → DLQ ``NORMALIZE``/``ENRICH``
  e ``FAILED`` no bruto, ``message_id`` marcado e ``ACK``. Nenhuma mensagem é perdida: se nem a
  DLQ puder ser gravada, a mensagem volta para a fila.
- **Disjuntor**: ``systemic_failure_threshold`` mensagens seguidas esgotando as tentativas com
  a mesma falha (estágio + classe), sem sucesso no meio, é falha do sistema (403 da REST por
  falta de assinatura, tabela sem grant, regra com bug), não das mensagens: alerta crítico e
  as seguintes com essa falha voltam para a fila com backoff, em vez de esvaziá-la na DLQ.
  Um sucesso fecha o disjuntor.
- Erro inesperado (bug nosso): alerta, backoff e ``REQUEUE``. Nunca derruba o processo, que
  recebe a mesma mensagem ao voltar e cairia de novo (laço de reinício).
- Log e DLQ levam só ``EnrichmentFailedError.detail`` (sem dados pessoais).
"""

from __future__ import annotations

import asyncio
import contextlib
import traceback
from dataclasses import dataclass
from enum import StrEnum

from ohip_streaming.application.errors import (
    ApplicationError,
    EnrichmentFailedError,
    NotFoundError,
    ResourceUnavailableError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import Clock, DlqStage, EnrichmentStore, MetricsSink
from ohip_streaming.application.use_cases.enrich_event import (
    EnrichEvent,
    EnrichOutcome,
    InboundMessage,
)
from ohip_streaming.logging import bind_context, get_logger

log = get_logger(__name__)

ERROR_CLASS_MAX = 200  # ohip_dlq.error_class


class Disposition(StrEnum):
    ACK = "ACK"
    REQUEUE = "REQUEUE"


@dataclass(frozen=True, slots=True)
class EnricherOptions:
    max_attempts: int = 3
    retry_delay_s: float = 2.0
    transient_backoff_initial_s: float = 5.0
    transient_backoff_max_s: float = 60.0
    systemic_failure_threshold: int = 5  # falhas iguais seguidas que abrem o disjuntor


class EnricherService:
    def __init__(
        self,
        *,
        enrich: EnrichEvent,
        store: EnrichmentStore,
        clock: Clock,
        metrics: MetricsSink,
        options: EnricherOptions,
    ) -> None:
        self._enrich = enrich
        self._store = store
        self._clock = clock
        self._metrics = metrics
        self._options = options
        self._stop = asyncio.Event()
        self._transient_failures = 0
        # Disjuntor: assinatura (estágio, classe) da última falha esgotada e quantas seguidas.
        self._failure_signature: tuple[str, str] | None = None
        self._same_failures = 0

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop

    def request_stop(self) -> None:
        self._stop.set()

    async def handle(self, message: InboundMessage) -> Disposition:
        with bind_context(message_id=message.message_id):
            try:
                return await self._handle(message)
            except Exception as exc:  # noqa: BLE001 - qualquer bug nosso, ver abaixo
                # Bug nosso: subir derrubaria o processo, e a mesma mensagem voltaria ao
                # reiniciar. Devolve à fila com backoff (não gira em laço) e alerta.
                # Sem exc_info: o texto da exceção pode ter dados do evento. Classe e local bastam.
                log.critical(
                    "enricher_erro_inesperado", error=type(exc).__name__, where=_where(exc)
                )
                self._metrics.increment("ohip_enricher_unexpected_errors_total")
                return await self._transient(exc)

    async def _handle(self, message: InboundMessage) -> Disposition:
        attempt = 0
        while True:
            attempt += 1
            try:
                outcome = await self._enrich.execute(message)
            except (StoreUnavailableError, ResourceUnavailableError) as exc:
                return await self._transient(exc)
            except EnrichmentFailedError as exc:
                if attempt < self._options.max_attempts:
                    if self._stop.is_set():
                        return Disposition.REQUEUE  # sem DLQ antes da hora: outro processo tenta
                    log.warning(
                        "enricher_tentativa_falhou",
                        attempt=attempt,
                        raw_event_id=exc.raw_event_id,
                        error=str(exc),  # só o detalhe seguro (EnrichmentFailedError)
                    )
                    await self._sleep(self._options.retry_delay_s)
                    if self._stop.is_set():
                        return Disposition.REQUEUE  # termina a mensagem em outro processo
                    continue
                if self._is_systemic(exc):
                    return await self._systemic(exc)
                return await self._to_dlq(message, exc)
            self._transient_failures = 0
            if outcome is not EnrichOutcome.DUPLICATE:  # duplicado não exercita nada
                self._close_breaker()
            self._metrics.increment("ohip_enricher_messages_total", outcome=outcome.value)
            return Disposition.ACK

    async def _to_dlq(self, message: InboundMessage, failure: EnrichmentFailedError) -> Disposition:
        cause = failure.cause
        try:
            await self._store.add_dlq(
                failure.raw_event_id,
                DlqStage(failure.stage),
                type(cause).__name__[:ERROR_CLASS_MAX],
                failure.detail,  # vai para API e painel: nada de dados pessoais
            )
        except StoreUnavailableError as exc:
            return await self._transient(exc)
        except NotFoundError:
            # O bruto não existe mais (expurgo): não há o que reprocessar nem onde registrar.
            log.error("enricher_bruto_expurgado", raw_event_id=failure.raw_event_id)
            return Disposition.ACK
        except Exception:
            # Não deu para registrar a falha: a mensagem volta para a fila (nada se perde).
            log.exception("enricher_dlq_falhou", raw_event_id=failure.raw_event_id)
            await self._sleep(self._options.retry_delay_s)
            return Disposition.REQUEUE
        await self._enrich.mark_processed(message.message_id)
        self._metrics.increment("ohip_enricher_dlq_total", stage=failure.stage)
        log.error(
            "enricher_mensagem_na_dlq",
            raw_event_id=failure.raw_event_id,
            stage=failure.stage,
            error=str(failure),
        )
        return Disposition.ACK

    def _is_systemic(self, failure: EnrichmentFailedError) -> bool:
        signature = (failure.stage, type(failure.cause).__name__)
        if signature == self._failure_signature:
            self._same_failures += 1
        else:
            self._failure_signature, self._same_failures = signature, 1
        return self._same_failures >= self._options.systemic_failure_threshold

    async def _systemic(self, failure: EnrichmentFailedError) -> Disposition:
        stage, error_class = self._failure_signature or (failure.stage, "?")
        self._metrics.increment("ohip_enricher_systemic_failures_total", stage=stage)
        if self._same_failures == self._options.systemic_failure_threshold:
            log.critical(
                "enricher_falha_sistemica",
                stage=stage,
                error_class=error_class,
                failures=self._same_failures,
                error=str(failure),
            )
        return await self._transient(failure)

    def _close_breaker(self) -> None:
        if self._same_failures >= self._options.systemic_failure_threshold:
            log.warning("enricher_falha_sistemica_encerrada", failures=self._same_failures)
        self._failure_signature, self._same_failures = None, 0

    async def _transient(self, error: Exception) -> Disposition:
        o = self._options
        self._transient_failures += 1
        wait = min(
            o.transient_backoff_max_s,
            o.transient_backoff_initial_s * 2 ** (self._transient_failures - 1),
        )
        retry_after = getattr(error, "retry_after_s", None)
        if retry_after:
            wait = max(wait, retry_after)  # o servidor pediu (429 Retry-After)
        self._metrics.increment("ohip_enricher_dependency_unavailable_total")
        log.warning("enricher_dependencia_indisponivel", wait_s=wait, error=_safe_text(error))
        await self._sleep(wait)  # com a mensagem sem ack: nada gira em laço
        return Disposition.REQUEUE

    async def _sleep(self, seconds: float) -> None:
        if seconds <= 0 or self._stop.is_set():
            return
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, stopper):
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task


def _safe_text(error: Exception) -> str:
    """Nossos erros têm texto sem dados pessoais; de qualquer outro, só a classe."""
    return str(error) if isinstance(error, ApplicationError) else type(error).__name__


def _where(error: Exception) -> str:
    frames = traceback.extract_tb(error.__traceback__)
    if not frames:
        return "?"
    last = frames[-1]
    return f"{last.filename.rsplit('/', 1)[-1]}:{last.lineno} {last.name}"
