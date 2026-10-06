"""Processo ``ohip-purge``: expurgo por retenção, disparado pelo timer do systemd (ADR-0020 §2).

Uso: ``ohip-purge`` (``Type=oneshot``). Só Oracle, com o usuário ``ohip_purge``; sem Redis nem
RabbitMQ. Padrão ``PURGE_DRY_RUN=true``: só conta. SIGTERM/SIGINT: termina o lote em curso e
sai com sucesso (o resto fica para a próxima execução). Erro: código 1 (``OnFailure=`` da unit).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from ohip_streaming.adapters.oracle.database import PooledSession, open_pool
from ohip_streaming.adapters.oracle.purge_store import OraclePurgeStore
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.ports import PURGE_ORDER
from ohip_streaming.application.use_cases.purge import (
    PurgeExpiredData,
    PurgeOptions,
    PurgeResult,
)
from ohip_streaming.config import (
    AppSettings,
    OracleSettings,
    PurgeSettings,
    load_settings,
)
from ohip_streaming.entrypoints.runtime import setup_logging, stop_on_signals
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class PurgeConfig:
    app: AppSettings
    oracle: OracleSettings
    purge: PurgeSettings

    @classmethod
    def load(cls) -> PurgeConfig:
        return cls(
            app=load_settings(AppSettings),
            oracle=load_settings(OracleSettings),
            purge=load_settings(PurgeSettings),
        )


def purge_options(settings: PurgeSettings) -> PurgeOptions:
    return PurgeOptions(
        retention_days=settings.retention_days,
        batch_size=settings.batch_size,
        pause_s=settings.pause_ms / 1000,
        max_runtime_s=settings.max_runtime_s,
        dry_run=settings.dry_run,
    )


async def serve(config: PurgeConfig) -> list[PurgeResult]:
    stop = asyncio.Event()
    stop_on_signals(stop.set)
    pool = open_pool(config.oracle)
    try:
        session = PooledSession(pool, call_timeout_ms=config.purge.call_timeout_ms)
        use_case = PurgeExpiredData(
            store=OraclePurgeStore(session),
            clock=SystemClock(),
            options=purge_options(config.purge),
        )
        return await use_case.execute(stop)
    finally:
        pool.close(force=True)


def main() -> int:
    config = PurgeConfig.load()
    setup_logging("ohip-purge", config.app)
    p = config.purge
    log.info(
        "expurgo_iniciando",
        retention_days=p.retention_days,
        batch_size=p.batch_size,
        dry_run=p.dry_run,
    )
    try:
        results = asyncio.run(serve(config))
    except Exception:
        log.exception("expurgo_falhou")
        return 1
    log.info(
        "expurgo_concluido",
        dry_run=p.dry_run,
        rows={r.target.value: r.rows for r in results},
        complete=all(r.complete for r in results) and len(results) == len(PURGE_ORDER),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
