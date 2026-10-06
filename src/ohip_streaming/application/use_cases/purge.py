"""Expurgo diário por retenção (ARCHITECTURE §5.1, ADR-0020 §2).

Ordem fixa (DLQ resolvida → outbox ``SENT`` → outbox ``FAILED`` já sem DLQ → bruto sem
referência), em lotes com commit por
lote, pausa entre lotes e prazo total. Prazo vencido ou parada pedida: termina o lote em
curso e para; o resto fica para a próxima execução (cada lote é independente). Em
``dry_run`` só conta.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

from ohip_streaming.application.ports import PURGE_ORDER, Clock, PurgeStore, PurgeTarget
from ohip_streaming.application.timing import sleep_or_stop
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class PurgeOptions:
    retention_days: int = 90
    batch_size: int = 5_000
    pause_s: float = 0.2
    max_runtime_s: float = 3_600.0
    dry_run: bool = True


@dataclass(frozen=True, slots=True)
class PurgeResult:
    target: PurgeTarget
    rows: int  # apagadas (ou que seriam, em dry_run)
    complete: bool  # False: parou por prazo ou parada; sobrou para a próxima execução


class PurgeExpiredData:
    def __init__(self, *, store: PurgeStore, clock: Clock, options: PurgeOptions) -> None:
        self._store = store
        self._clock = clock
        self._options = options

    async def execute(self, stop: asyncio.Event) -> list[PurgeResult]:
        o = self._options
        deadline = self._clock.now() + timedelta(seconds=o.max_runtime_s)
        results: list[PurgeResult] = []
        for target in PURGE_ORDER:
            if o.dry_run:
                rows = await self._store.count_expired(target, o.retention_days)
                log.info("expurgo_simulado", target=target.value, rows=rows)
                results.append(PurgeResult(target, rows, complete=True))
                continue
            result = await self._purge(target, deadline, stop)
            results.append(result)
            if not result.complete:
                log.warning("expurgo_interrompido", target=target.value, rows=result.rows)
                break  # prazo ou parada: as tabelas seguintes ficam para a próxima execução
        return results

    async def _purge(
        self, target: PurgeTarget, deadline: datetime, stop: asyncio.Event
    ) -> PurgeResult:
        o = self._options
        total = 0
        while True:
            if stop.is_set() or self._clock.now() >= deadline:
                return PurgeResult(target, total, complete=False)
            rows = await self._store.purge_batch(target, o.retention_days, o.batch_size)
            total += rows
            log.info("expurgo_lote", target=target.value, rows=rows, total=total)
            if rows < o.batch_size:
                return PurgeResult(target, total, complete=True)
            await self._pause(stop)

    async def _pause(self, stop: asyncio.Event) -> None:
        """Entre lotes: alivia undo/redo e a concorrência com o consumer; a parada interrompe."""
        await sleep_or_stop(self._options.pause_s, stop, sleep=self._clock.sleep)
