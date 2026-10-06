"""Montagem comum dos processos assíncronos (refatoração 6 do /entender).

O que consumer, publisher, enricher e expurgo repetiam: log estruturado com os dados do
ambiente, SIGTERM/SIGINT ligados ao pedido de parada e o snapshot periódico das métricas no
Redis, com um último retrato na saída (o lease perdido costuma vir logo antes de sair).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from ohip_streaming.adapters.redis.caches import RedisLike
from ohip_streaming.adapters.redis.metrics import (
    InMemoryMetrics,
    RedisMetricsPublisher,
    metrics_key,
)
from ohip_streaming.config import AppSettings, LogSettings, load_settings
from ohip_streaming.logging import configure_logging

METRICS_INTERVAL_S = 15.0


def setup_logging(service: str, app: AppSettings) -> None:
    logs = load_settings(LogSettings)
    configure_logging(
        service=service,
        environment=app.environment.value,
        code_version=app.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )


def stop_on_signals(request_stop: Callable[[], object]) -> None:
    """SIGTERM (systemd) e SIGINT (Ctrl+C) pedem a parada; o processo drena e sai sozinho."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, request_stop)


@asynccontextmanager
async def metrics_snapshots(
    redis: RedisLike,
    metrics: InMemoryMetrics,
    *,
    process: str,
    instance: str,
    ttl_s: int,
    interval_s: float = METRICS_INTERVAL_S,
) -> AsyncIterator[RedisMetricsPublisher]:
    """Snapshot a cada ``interval_s`` em ``ohip:metrics:<processo>:<instância>`` e um último
    ao sair (Redis fora: só log, ARCHITECTURE §10)."""
    publisher = RedisMetricsPublisher(
        redis, metrics, key=metrics_key(process, instance), ttl_s=ttl_s
    )
    task = asyncio.create_task(_publish_every(publisher, interval_s), name="metricas")
    try:
        yield publisher
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await publisher.publish()


async def _publish_every(publisher: RedisMetricsPublisher, interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        await publisher.publish()
