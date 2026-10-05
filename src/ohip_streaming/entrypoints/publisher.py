"""Processo ``ohip-publisher``: um ativo por ambiente (lease ``publisher``, ADR-0008).

Uso (systemd, Fase 10): ``ohip-publisher``. SIGTERM/SIGINT: termina a rodada em curso, libera
o lease e sai. Topologia divergente no broker: alerta crítico e saída com código 2 (exige
ação humana; reiniciar não resolve).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass

from ohip_streaming.adapters.oracle.database import PooledSession, open_pool
from ohip_streaming.adapters.oracle.lease_store import OracleLeaseStore
from ohip_streaming.adapters.oracle.outbox_store import OracleOutboxStore
from ohip_streaming.adapters.rabbitmq.publisher import AioPikaPublisher
from ohip_streaming.adapters.redis.client import open_redis
from ohip_streaming.adapters.redis.metrics import (
    InMemoryMetrics,
    RedisMetricsPublisher,
    metrics_key,
)
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.errors import BrokerMisconfiguredError
from ohip_streaming.application.use_cases.lease import LeaseKeeper, LeaseOptions
from ohip_streaming.application.use_cases.publish_outbox import PublisherOptions, PublishOutbox
from ohip_streaming.application.use_cases.publisher_service import (
    PublisherLoopOptions,
    PublisherService,
)
from ohip_streaming.config import (
    AppSettings,
    LeaseSettings,
    LogSettings,
    OracleSettings,
    PublisherSettings,
    RabbitMQSettings,
    RedisSettings,
    load_settings,
)
from ohip_streaming.entrypoints.common import AlertCounter, instance_id, preregister
from ohip_streaming.logging import configure_logging, get_logger

log = get_logger(__name__)

METRICS_INTERVAL_S = 15.0
LEASE_NAME = "publisher"


@dataclass(frozen=True)
class PublisherConfig:
    app: AppSettings
    oracle: OracleSettings
    redis: RedisSettings
    rabbitmq: RabbitMQSettings
    publisher: PublisherSettings
    lease: LeaseSettings

    @classmethod
    def load(cls) -> PublisherConfig:
        return cls(
            app=load_settings(AppSettings),
            oracle=load_settings(OracleSettings),
            redis=load_settings(RedisSettings),
            rabbitmq=load_settings(RabbitMQSettings),
            publisher=load_settings(PublisherSettings),
            lease=load_settings(LeaseSettings),
        )


def publish_options(config: PublisherConfig) -> PublisherOptions:
    return PublisherOptions(
        batch_limit=config.publisher.batch_limit,
        max_attempts=config.rabbitmq.publish_max_attempts,
        broker_max_message_bytes=config.rabbitmq.broker_max_message_bytes,
        channel_fail_attribution=config.rabbitmq.channel_fail_attribution,
    )


def loop_options(config: PublisherConfig) -> PublisherLoopOptions:
    p = config.publisher
    return PublisherLoopOptions(
        poll_interval_s=p.poll_interval_s,
        max_parallel_chains=p.max_parallel_chains,
        broker_backoff_initial_s=p.broker_backoff_initial_s,
        broker_backoff_max_s=p.broker_backoff_max_s,
    )


@asynccontextmanager
async def compose(config: PublisherConfig) -> AsyncIterator[PublisherService]:
    instance = instance_id()
    clock = SystemClock()
    metrics = InMemoryMetrics()
    preregister(metrics, alert_counters())
    pool = open_pool(config.oracle)
    executor = ThreadPoolExecutor(config.oracle.pool_max, thread_name_prefix="oracle-publisher")
    session = PooledSession(pool, call_timeout_ms=config.oracle.call_timeout_ms, executor=executor)
    broker = AioPikaPublisher.from_settings(config.rabbitmq)
    redis = open_redis(config.redis)
    try:
        store = OracleOutboxStore(session, code_version=config.app.code_version)
        service = PublisherService(
            store=store,
            publish=PublishOutbox(
                store=store,
                publisher=broker,
                clock=clock,
                metrics=metrics,
                options=publish_options(config),
            ),
            lease=LeaseKeeper(
                store=OracleLeaseStore(session),
                clock=clock,
                metrics=metrics,
                lease_name=LEASE_NAME,
                owner=instance,
                options=LeaseOptions(
                    ttl_s=config.lease.ttl_s,
                    renew_interval_s=config.lease.renew_interval_s,
                    acquire_jitter_s=config.lease.acquire_jitter_s,
                ),
            ),
            clock=clock,
            metrics=metrics,
            options=loop_options(config),
        )
        snapshots = RedisMetricsPublisher(
            redis, metrics, key=metrics_key("publisher", instance), ttl_s=config.redis.metrics_ttl_s
        )
        task = asyncio.create_task(_publish_metrics(snapshots), name="metricas")
        try:
            yield service
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await snapshots.publish()  # último retrato (ex.: lease perdido logo antes de sair)
    finally:
        await broker.close()
        await redis.aclose()
        pool.close(force=True)
        executor.shutdown(wait=False)


def alert_counters() -> list[AlertCounter]:
    """Contadores do publisher citados em deploy/prometheus/ohip-alerts.yml."""
    return [("ohip_lease_lost_total", {"lease": LEASE_NAME})]


async def _publish_metrics(publisher: RedisMetricsPublisher) -> None:
    while True:
        await asyncio.sleep(METRICS_INTERVAL_S)
        await publisher.publish()


async def serve(config: PublisherConfig) -> None:
    async with compose(config) as service:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, service.request_stop)
        await service.run()


def main() -> int:
    config = PublisherConfig.load()
    logs = load_settings(LogSettings)
    configure_logging(
        service="ohip-publisher",
        environment=config.app.environment.value,
        code_version=config.app.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )
    log.info("publisher_iniciando")
    try:
        asyncio.run(serve(config))
    except BrokerMisconfiguredError:
        return 2  # já logado como crítico; não adianta reiniciar
    except Exception:
        log.exception("publisher_falhou")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
