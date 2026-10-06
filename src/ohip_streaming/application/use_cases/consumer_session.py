"""Partes da sessão do consumer (refatoração 4 do /entender; ADR-0007, ADR-0020 §1).

``_Session`` (em ``consume_chain``) orquestra protocolo, leitura, gravação e drenagem; estas
peças cuidam do resto, cada uma com um só motivo para mudar:

- ``StatusReporter``: tudo o que a sessão grava em ``OHIP_CONSUMER_STATUS``, com a guarda
  "sem lease, nada é gravado na linha da chain";
- ``Liveness``: prova de vida (``pong`` ou ``next``), RTT e o laço de ``ping``;
- ``ControlLoop``: parada, lease, token perto do ``exp``, replay pendente, retry de DLQ e a
  saúde periódica, pela tarefa de controle (nunca pela leitura nem pelo heartbeat).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from ohip_streaming.application.errors import (
    LeaseLostError,
    StoreOperationError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    AccessToken,
    ConnectionHealth,
    ConsumerStatusStore,
    ReplayStore,
)
from ohip_streaming.application.timing import sleep_or_stop
from ohip_streaming.application.use_cases.lease import LeaseKeeper
from ohip_streaming.application.use_cases.process_event_batch import (
    ConsumerContext,
    RetryConsumeDlq,
)
from ohip_streaming.application.use_cases.token_provider import TokenProvider
from ohip_streaming.domain.connection import ConsumerState, RttEstimator
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

Now = Callable[[], float]  # relógio monotônico do event loop


class DrainCause(StrEnum):
    STOP = "STOP"
    TOKEN = "TOKEN"  # noqa: S105 - motivo, não segredo
    REPLAY = "REPLAY"
    POISONED = "POISONED"
    LEASE_LOST = "LEASE_LOST"
    SUBSCRIPTION_ENDED = "SUBSCRIPTION_ENDED"  # error/complete do servidor (D-10)


# ------------------------------------------------------------------ status


class StatusReporter:
    """Gravações da sessão em ``OHIP_CONSUMER_STATUS`` (ADR-0020 §1). Falha ao gravar só gera
    aviso: o status é informativo e nunca derruba a conexão."""

    def __init__(
        self,
        store: ConsumerStatusStore,
        *,
        chain_code: str,
        instance_id: str,
        lease: LeaseKeeper,
        interval_s: float,
        now: Now,
        rtt_s: Callable[[], float | None],
    ) -> None:
        self._store = store
        self._chain = chain_code
        self._instance = instance_id
        self._lease = lease
        self._interval_s = interval_s
        self._now = now
        self._rtt_s = rtt_s
        self._due = now() + interval_s
        self._written: ConnectionHealth | None = None
        # A barreira de epoch ou o lease acusou outra dona: daqui em diante, nada de status.
        self._lease_gone = False
        # Relógio do processo (UTC): informativo, fora da regra dos 10 s.
        self.last_message_at: datetime | None = None
        self.last_ping_at: datetime | None = None
        self.last_pong_at: datetime | None = None

    def lose_lease(self) -> None:
        self._lease_gone = True

    def owns(self) -> bool:
        """Sem lease, nada é gravado na linha da chain: ela é da nova dona (ADR-0008)."""
        return not (self._lease_gone or self._lease.lost)

    async def state(self, state: ConsumerState) -> None:
        if not self.owns():
            return
        try:
            await self._store.record_state(self._chain, state, self._instance)
        except StoreUnavailableError:
            log.warning("status_indisponivel")

    async def subscribed(self, subscription_id: str, token_expires_at: datetime) -> None:
        if not self.owns():
            return
        try:
            await self._store.record_subscribed(
                self._chain, self._instance, subscription_id, token_expires_at
            )
        except StoreUnavailableError:
            log.warning("status_indisponivel")

    async def health_if_due(self) -> None:
        if self._now() >= self._due:
            self._due = self._now() + self._interval_s
            await self.health()

    async def health(self) -> None:
        """Só o que mudou desde a última gravação; nada se não houver novidade."""
        srtt = self._rtt_s()
        health = ConnectionHealth(
            last_message_at=self.last_message_at,
            last_ping_at=self.last_ping_at,
            last_pong_at=self.last_pong_at,
            rtt_ms=None if srtt is None else round(srtt * 1000),
        )
        if health == self._written or health == ConnectionHealth():
            return
        if not self.owns():
            return
        try:
            await self._store.record_health(self._chain, health)
        except (StoreUnavailableError, StoreOperationError, UnknownChainError):
            log.warning("status_indisponivel", exc_info=True)
            return
        self._written = health


# ------------------------------------------------------------------ prova de vida


class Liveness:
    """Prova de vida = ``pong`` ou qualquer ``next``; prazo ``max(mínimo, 4 x SRTT + jitter)``
    (guia Oracle, Performance Considerations; ADR-0007)."""

    def __init__(
        self,
        *,
        now: Now,
        ping_interval_s: float,
        pong_timeout_min_s: float,
        rng: Callable[[], float],
    ) -> None:
        self._now = now
        self._interval_s = ping_interval_s
        self._min_timeout_s = pong_timeout_min_s
        self._rng = rng
        self.rtt = RttEstimator()
        self.last_alive = now()
        self.ping_sent_at: float | None = None

    @property
    def rtt_s(self) -> float | None:
        return self.rtt.srtt_s

    def alive(self) -> None:
        self.last_alive = self._now()

    def pong(self) -> None:
        self.alive()
        if self.ping_sent_at is not None:
            self.rtt.update(self._now() - self.ping_sent_at)
            self.ping_sent_at = None

    async def run(self, send_ping: Callable[[], Awaitable[None]], closed: asyncio.Event) -> bool:
        """``ping`` a cada intervalo até ``closed``. True = sem prova de vida além do prazo."""
        while not closed.is_set():
            await asyncio.sleep(self._interval_s)
            silence = self._now() - self.last_alive
            limit = self.rtt.liveness_timeout_s(
                min_timeout_s=self._min_timeout_s, jitter_s=self._rng() * 5
            )
            if silence > limit:
                log.error("ohip_sem_prova_de_vida", silence_s=round(silence, 1))
                return True
            if self.ping_sent_at is None:
                self.ping_sent_at = self._now()
            await send_ping()
        return False


# ------------------------------------------------------------------ controle


class ControlledSession(Protocol):
    """O que o controle usa da sessão."""

    closed: asyncio.Event
    completed: bool
    poisoned: bool
    token: AccessToken | None

    async def drain(self, cause: DrainCause) -> None: ...

    async def poison(self, why: str) -> None: ...


class ControlLoop:
    """Tarefa de controle da sessão. Prioridade: parada > lease > token > replay > retry de
    DLQ. Depois de cada passo, a saúde periódica (``StatusReporter.health_if_due``)."""

    def __init__(
        self,
        session: ControlledSession,
        *,
        stop: asyncio.Event,
        lease: LeaseKeeper,
        tokens: TokenProvider,
        replay_store: ReplayStore,
        retry_dlq: RetryConsumeDlq,
        context: ConsumerContext,
        status: StatusReporter,
        poll_interval_s: float,
        dlq_retry_limit: int,
    ) -> None:
        self._s = session
        self._stop = stop
        self._lease = lease
        self._tokens = tokens
        self._replay_store = replay_store
        self._retry_dlq = retry_dlq
        self._context = context
        self._status = status
        self._poll_interval_s = poll_interval_s
        self._dlq_retry_limit = dlq_retry_limit

    async def run(self) -> None:
        s = self._s
        while not s.closed.is_set():
            await self._wait_stop(self._poll_interval_s)
            if s.completed or s.closed.is_set():
                continue
            try:
                await self.step()
            except Exception:  # o controle nunca morre em silêncio
                log.error("controle_falhou", exc_info=True)
            await self._status.health_if_due()

    async def step(self) -> None:
        s = self._s
        if self._stop.is_set():
            await s.drain(DrainCause.STOP)
        elif self._lease.lost:
            self._status.lose_lease()
            await s.poison("lease perdido")
            await s.drain(DrainCause.LEASE_LOST)
        elif self._token_due():
            await s.drain(DrainCause.TOKEN)
        elif await self._replay_pending():
            await s.drain(DrainCause.REPLAY)
        elif not s.poisoned:
            await self._retry_consume_dlq()

    def _token_due(self) -> bool:
        """O token desta conexão entrou na margem: renovar exige reconectar (ADR-0007)."""
        token = self._s.token
        return token is not None and self._tokens.refresh_due(token)

    async def _replay_pending(self) -> bool:
        try:
            return await self._replay_store.pending_request(self._context.chain_code) is not None
        except StoreUnavailableError:
            return False

    async def _retry_consume_dlq(self) -> None:
        try:
            await self._retry_dlq.execute(self._context, limit=self._dlq_retry_limit)
        except StoreUnavailableError:
            log.warning("retry_dlq_sem_banco")
        except LeaseLostError:
            self._status.lose_lease()
            await self._s.poison("lease perdido")
            await self._s.drain(DrainCause.LEASE_LOST)

    async def _wait_stop(self, seconds: float) -> None:
        """Espera o intervalo ou o pedido de parada. Já parado: espera o intervalo inteiro
        (``Event.wait`` de um evento marcado não suspende e travaria o laço)."""
        if self._stop.is_set():
            await asyncio.sleep(seconds)
            return
        await sleep_or_stop(seconds, self._stop)
