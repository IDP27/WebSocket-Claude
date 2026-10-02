"""Liderança com prazo e epoch no Oracle (ADR-0008).

``LeaseKeeper`` adquire o lease, renova a cada ``renew_interval_s`` pela conexão de controle e
avisa quando o perdeu. O epoch vai em toda transação de escrita (barreira); quem confere de
fato a posse é o banco, nunca este objeto.

Regras:
- aquisição falhou (outro dono) → nova tentativa depois de ``ttl_s`` + jitter;
- renovação recusada → perdido na hora (outro processo assumiu);
- renovação sem resposta (banco fora) → continua tentando, mas a partir do prazo de validade
  local o lease é tratado como perdido: o processo para de gravar (DRAINING);
- ao sair: libera só se ainda for o dono; se saiu porque perdeu, não libera nada.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from ohip_streaming.application.errors import StoreUnavailableError
from ohip_streaming.application.ports import Clock, LeaseStore, MetricsSink
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class LeaseOptions:
    ttl_s: float = 30.0
    renew_interval_s: float = 10.0
    acquire_jitter_s: float = 5.0


class LeaseKeeper:
    def __init__(
        self,
        *,
        store: LeaseStore,
        clock: Clock,
        metrics: MetricsSink,
        lease_name: str,
        owner: str,
        options: LeaseOptions,
        rng: random.Random | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._metrics = metrics
        self._name = lease_name
        self._owner = owner
        self._options = options
        self._rng = rng or random.Random()  # noqa: S311 - jitter, não criptografia
        self._epoch: int | None = None
        self._valid_until: datetime | None = None
        self._lost = False

    @property
    def epoch(self) -> int | None:
        """Epoch atual, ou None se não somos donos (ou o prazo local venceu)."""
        if self._epoch is None or self._valid_until is None:
            return None
        if self._clock.now() >= self._valid_until:
            return None
        return self._epoch

    @property
    def lost(self) -> bool:
        """Tínhamos o lease e o perdemos (recusa ou prazo vencido sem renovar)."""
        return self._lost or (self._epoch is not None and self.epoch is None)

    async def acquire(self) -> int:
        """Espera até virar dono. Erros de banco também esperam e tentam de novo."""
        while True:
            started = self._clock.now()  # o banco conta o prazo durante a chamada
            try:
                epoch = await self._store.acquire(self._name, self._owner, self._options.ttl_s)
            except StoreUnavailableError:
                log.warning("lease_aquisicao_sem_banco", lease=self._name)
                epoch = None
            if epoch is not None:
                self._hold(epoch, started)
                self._metrics.increment("ohip_lease_acquired_total", lease=self._name)
                log.info("lease_adquirido", lease=self._name, epoch=epoch)
                return epoch
            await self._clock.sleep(
                self._options.ttl_s + self._rng.uniform(0, self._options.acquire_jitter_s)
            )

    async def renew_once(self) -> bool:
        """Uma renovação. False quando o lease foi perdido."""
        if self._epoch is None or self._lost:
            return False
        if self.lost:  # prazo local vencido: já parou de gravar; não ressuscita o lease
            self._lose("prazo local vencido sem renovar")
            return False
        started = self._clock.now()
        try:
            renewed = await self._store.renew(
                self._name, self._owner, self._epoch, self._options.ttl_s
            )
        except StoreUnavailableError:
            log.warning("lease_renovacao_sem_banco", lease=self._name, epoch=self._epoch)
            return not self.lost  # ainda dentro do prazo local
        if not renewed:
            self._lose("renovação recusada: outro processo assumiu")
            return False
        self._hold(self._epoch, started)  # prazo contado do início da chamada (conservador)
        return True

    async def keep(self) -> None:
        """Renova até perder. Rode como tarefa; retorna quando o lease for perdido."""
        while await self.renew_once():
            await self._clock.sleep(self._options.renew_interval_s)
        self._metrics.increment("ohip_lease_lost_total", lease=self._name)

    async def release(self) -> None:
        if self._epoch is None or self._lost:
            return  # perdido: o lease já é de outro
        try:
            await self._store.release(self._name, self._owner, self._epoch)
            log.info("lease_liberado", lease=self._name, epoch=self._epoch)
        except StoreUnavailableError:
            log.warning("lease_liberacao_sem_banco", lease=self._name)  # vence pelo prazo
        self._epoch = None
        self._valid_until = None

    def _hold(self, epoch: int, since: datetime) -> None:
        self._epoch = epoch
        self._valid_until = since + timedelta(seconds=self._options.ttl_s)

    def _lose(self, reason: str) -> None:
        self._lost = True
        log.error("lease_perdido", lease=self._name, epoch=self._epoch, reason=reason)
