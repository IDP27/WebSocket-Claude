"""Laço do processo ``ohip-publisher`` (ADR-0002, ADR-0016, ARCHITECTURE §4.2).

- Um publisher ativo por ambiente: lease ``publisher`` no Oracle (ADR-0008); toda marcação de
  linha passa pela barreira desse epoch.
- A cada rodada: chains com ``PENDING`` → cada chain publica a cabeça da sua fila
  (``PublishOutbox``: uma mensagem por vez, esperando o confirm), até
  ``max_parallel_chains`` chains em paralelo. Chain com a cabeça em backoff é pulada até o
  horário dela.
- Broker fora (``BrokerUnavailableError``, inclusive canal caindo): backoff **global**
  exponencial, sem contar tentativa de nenhuma linha.
- Banco fora: espera e tenta de novo. Lease perdido: para de publicar e sai (sem liberar).
- Topologia divergente no broker: alerta crítico e sai (exige ação humana).
- Sem nada a publicar: dorme ``poll_interval_s``.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime

from ohip_streaming.application.errors import (
    BrokerMisconfiguredError,
    BrokerUnavailableError,
    LeaseLostError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import Clock, MetricsSink, OutboxStore
from ohip_streaming.application.use_cases.lease import LeaseKeeper
from ohip_streaming.application.use_cases.publish_outbox import ChainPublishResult, PublishOutbox
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class PublisherLoopOptions:
    poll_interval_s: float = 1.0
    max_parallel_chains: int = 8
    broker_backoff_initial_s: float = 1.0
    broker_backoff_max_s: float = 60.0
    store_backoff_s: float = 5.0


class PublisherService:
    def __init__(
        self,
        *,
        store: OutboxStore,
        publish: PublishOutbox,
        lease: LeaseKeeper,
        clock: Clock,
        metrics: MetricsSink,
        options: PublisherLoopOptions,
    ) -> None:
        self._store = store
        self._publish = publish
        self._lease = lease
        self._clock = clock
        self._metrics = metrics
        self._options = options
        self._stop = asyncio.Event()
        self._blocked_until: dict[str, datetime] = {}
        self._broker_failures = 0

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        epoch = await self._lease.acquire(self._stop)
        if epoch is None:
            log.info("publisher_encerrado_sem_lease")
            return
        keeper = asyncio.create_task(self._lease.keep(), name="lease-renovacao")
        try:
            while not self._stop.is_set() and not self._lease.lost:
                wait_s = await self._round(epoch)
                await self._sleep(wait_s)
        except LeaseLostError:
            log.error("publisher_perdeu_o_lease")
        except BrokerMisconfiguredError:
            log.critical("publisher_topologia_divergente", exc_info=True)
            raise
        finally:
            keeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keeper
            if not self._lease.lost:
                await self._lease.release()
            log.info("publisher_encerrado", lease_lost=self._lease.lost)

    async def _round(self, epoch: int) -> float:
        """Uma rodada. Devolve quanto esperar antes da próxima."""
        o = self._options
        try:
            chains = await self._store.chains_with_pending()
        except (StoreUnavailableError, StoreOperationError):
            log.warning("publisher_sem_banco", exc_info=True)
            return o.store_backoff_s

        now = self._clock.now()
        # Só guarda espera de chains que ainda têm o que publicar e cujo horário não passou.
        self._blocked_until = {
            c: t for c, t in self._blocked_until.items() if c in chains and t > now
        }
        ready = [c for c in chains if c not in self._blocked_until]
        self._metrics_gauge(len(chains))
        if not ready:
            return self._idle_wait(now)

        semaphore = asyncio.Semaphore(o.max_parallel_chains)

        async def one(chain: str) -> ChainPublishResult:
            async with semaphore:
                return await self._publish.publish_chain(chain, epoch)

        results = await asyncio.gather(*(one(c) for c in ready), return_exceptions=True)
        sent = 0
        broker_down: BaseException | None = None
        store_down = False
        for chain, result in zip(ready, results, strict=True):
            if isinstance(result, LeaseLostError | BrokerMisconfiguredError):
                raise result
            if isinstance(result, BrokerUnavailableError):
                broker_down = result
                continue
            if isinstance(result, StoreUnavailableError | StoreOperationError):
                log.warning("publisher_sem_banco", chain_code=chain, error=str(result))
                store_down = True
                continue
            if isinstance(result, BaseException):
                log.error("publisher_chain_falhou", chain_code=chain, exc_info=result)
                continue
            sent += result.sent
            if result.blocked_until is not None:
                self._blocked_until[chain] = result.blocked_until
            else:
                self._blocked_until.pop(chain, None)

        if broker_down is not None:
            return self._broker_backoff(broker_down)
        self._broker_failures = 0
        if store_down and not sent:
            return o.store_backoff_s  # banco fora: não gira em laço consultando o Oracle
        return 0.0 if sent else self._idle_wait(self._clock.now())

    def _broker_backoff(self, error: BaseException) -> float:
        o = self._options
        self._broker_failures += 1
        wait = min(
            o.broker_backoff_max_s, o.broker_backoff_initial_s * 2 ** (self._broker_failures - 1)
        )
        self._metrics.increment("ohip_broker_unavailable_total")
        log.warning("broker_indisponivel", wait_s=wait, error=str(error))
        return float(wait)

    def _idle_wait(self, now: datetime) -> float:
        """Dorme o intervalo de varredura, ou menos se uma cabeça sai do backoff antes."""
        wait = self._options.poll_interval_s
        future = [t for t in self._blocked_until.values() if t > now]
        if future:
            wait = min(wait, (min(future) - now).total_seconds())
        return wait

    def _metrics_gauge(self, pending_chains: int) -> None:
        if pending_chains:
            self._metrics.increment("ohip_publisher_rounds_total")

    async def _sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)  # cede a vez entre rodadas
            return
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, stopper):
                task.cancel()
