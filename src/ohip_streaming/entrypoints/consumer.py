"""Processo ``ohip-consumer``: um por chain, fora de Uvicorn/Gunicorn (ADR-0001).

Uso (systemd, Fase 10): ``python -m ohip_streaming.entrypoints.consumer``. A chain vem de
``OHIP_CHAIN_CODE``. SIGTERM/SIGINT: ``complete`` → drena → grava a desconexão → libera o
lease → sai (ADR-0007, ADR-0008).

Composição: Oracle (escrita dedicada de 1 thread + sessão de controle), Redis (token
compartilhado, dedup rápido, métricas), OAuth (httpx) e WebSocket (websockets).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import httpx

from ohip_streaming.adapters.ohip_rest.oauth import OhipTokenIssuer, credentials_from_settings
from ohip_streaming.adapters.ohip_ws.client import WebsocketsConnector
from ohip_streaming.adapters.oracle.database import DedicatedSession, PooledSession, open_pool
from ohip_streaming.adapters.oracle.event_store import OracleEventStore
from ohip_streaming.adapters.oracle.lease_store import OracleLeaseStore
from ohip_streaming.adapters.oracle.replay_store import OracleReplayStore
from ohip_streaming.adapters.oracle.status_store import OracleConsumerStatusStore
from ohip_streaming.adapters.redis.caches import RedisSeenCache, RedisTokenCache
from ohip_streaming.adapters.redis.client import open_redis
from ohip_streaming.adapters.redis.metrics import (
    InMemoryMetrics,
    RedisMetricsPublisher,
    metrics_key,
)
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.use_cases.consume_chain import (
    ChainConsumer,
    ConsumerDeps,
    ConsumerOptions,
)
from ohip_streaming.application.use_cases.lease import LeaseKeeper, LeaseOptions
from ohip_streaming.application.use_cases.process_event_batch import (
    BatchOptions,
    ProcessEventBatch,
    RetryConsumeDlq,
)
from ohip_streaming.application.use_cases.replay import ApplyReplay
from ohip_streaming.application.use_cases.token_provider import TokenProvider, token_cache_key
from ohip_streaming.config import (
    AppSettings,
    ConsumerSettings,
    LeaseSettings,
    LogSettings,
    OhipSettings,
    OracleSettings,
    RabbitMQSettings,
    RedisSettings,
    load_settings,
)
from ohip_streaming.domain.connection import ReconnectPolicy
from ohip_streaming.domain.messages import ExchangeKind
from ohip_streaming.logging import configure_logging, get_logger

log = get_logger(__name__)

METRICS_INTERVAL_S = 15.0
OAUTH_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class ConsumerConfig:
    app: AppSettings
    ohip: OhipSettings
    oracle: OracleSettings
    redis: RedisSettings
    consumer: ConsumerSettings
    lease: LeaseSettings
    rabbitmq: RabbitMQSettings

    @classmethod
    def load(cls) -> ConsumerConfig:
        return cls(
            app=load_settings(AppSettings),
            ohip=load_settings(OhipSettings),
            oracle=load_settings(OracleSettings),
            redis=load_settings(RedisSettings),
            consumer=load_settings(ConsumerSettings),
            lease=load_settings(LeaseSettings),
            rabbitmq=load_settings(RabbitMQSettings),
        )


def instance_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def consumer_options(config: ConsumerConfig, instance: str) -> ConsumerOptions:
    o, c = config.ohip, config.consumer
    return ConsumerOptions(
        chain_code=o.chain_code,
        instance_id=instance,
        event_tz=ZoneInfo(o.event_timezone),
        hotel_codes=tuple(o.hotel_codes),
        delta=o.delta,
        ping_interval_s=o.ping_interval_s,
        ack_timeout_s=o.ack_timeout_s,
        pong_timeout_min_s=o.pong_timeout_min_s,
        drain_timeout_s=o.drain_timeout_s,
        intake_max_items=c.intake_queue_max,
        intake_max_bytes=c.intake_queue_max_bytes,
        intake_stall_timeout_s=c.intake_stall_timeout_s,
        batch_max_events=c.batch_max_events,
        batch_max_wait_s=c.batch_max_wait_ms / 1000,
        control_poll_interval_s=c.control_poll_interval_s,
        status_check_enabled=o.status_check_enabled,
        policy=ReconnectPolicy(
            min_gap_s=o.reconnect_min_gap_s,
            lockout_4409_s=o.lockout_4409_s,
            backoff_4504_s=o.backoff_4504_s,
            backoff_initial_s=o.backoff_initial_s,
            backoff_max_s=o.backoff_max_s,
            oversize_max_repeats=o.oversize_max_repeats,
        ),
    )


def exchange_names(rabbitmq: RabbitMQSettings) -> dict[ExchangeKind, str]:
    return {
        ExchangeKind.EVENTS: rabbitmq.events_exchange,
        ExchangeKind.REPROCESS: rabbitmq.reprocess_exchange,
    }


@asynccontextmanager
async def compose(config: ConsumerConfig) -> AsyncIterator[ChainConsumer]:
    """Monta o consumer com os adapters reais e fecha tudo na saída."""
    o = config.ohip
    instance = instance_id()
    clock = SystemClock()
    metrics = InMemoryMetrics()
    pool = open_pool(config.oracle)
    writer = DedicatedSession(pool, call_timeout_ms=config.oracle.call_timeout_ms)
    control = PooledSession(pool, call_timeout_ms=config.oracle.call_timeout_ms)
    redis = open_redis(config.redis)
    http = httpx.AsyncClient(timeout=OAUTH_TIMEOUT_S)
    try:
        events = OracleEventStore(
            writer,
            exchange_names=exchange_names(config.rabbitmq),
            code_version=config.app.code_version,
        )
        replay = OracleReplayStore(control)
        seen = RedisSeenCache(redis, ttl_s=config.redis.seen_ttl_s)
        batch_options = BatchOptions(
            allowlist=frozenset(o.event_allowlist),
            module_codes=o.module_codes,
            max_retries=config.consumer.batch_max_retries,
        )
        tokens = TokenProvider(
            issuer=OhipTokenIssuer(http, credentials_from_settings(o), clock),
            cache=RedisTokenCache(redis),
            clock=clock,
            metrics=metrics,
            cache_key=token_cache_key(config.app.environment.value, o.chain_code),
            margin_s=o.token_refresh_margin_s,
        )
        lease = LeaseKeeper(
            store=OracleLeaseStore(control),
            clock=clock,
            metrics=metrics,
            lease_name=f"consumer:{o.chain_code}",
            owner=instance,
            options=LeaseOptions(
                ttl_s=config.lease.ttl_s,
                renew_interval_s=config.lease.renew_interval_s,
                acquire_jitter_s=config.lease.acquire_jitter_s,
            ),
        )
        deps = ConsumerDeps(
            connector=WebsocketsConnector.from_settings(o),
            tokens=tokens,
            lease=lease,
            batch=ProcessEventBatch(
                store=events, seen=seen, clock=clock, metrics=metrics, options=batch_options
            ),
            retry_dlq=RetryConsumeDlq(store=events, seen=seen, clock=clock, options=batch_options),
            replay_store=replay,
            apply_replay=ApplyReplay(store=replay),
            status=OracleConsumerStatusStore(control),
            clock=clock,
            metrics=metrics,
            app_key=o.app_key.get_secret_value(),
        )
        publisher = RedisMetricsPublisher(
            redis,
            metrics,
            key=metrics_key(f"consumer-{o.chain_code}", instance),
            ttl_s=config.redis.metrics_ttl_s,
        )
        snapshots = asyncio.create_task(_publish_metrics(publisher), name="metricas")
        try:
            yield ChainConsumer(deps, consumer_options(config, instance))
        finally:
            snapshots.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await snapshots
    finally:
        writer.close()
        await http.aclose()
        await redis.aclose()
        pool.close(force=True)


async def _publish_metrics(publisher: RedisMetricsPublisher) -> None:
    while True:
        await asyncio.sleep(METRICS_INTERVAL_S)
        await publisher.publish()


async def serve(config: ConsumerConfig) -> None:
    async with compose(config) as consumer:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, consumer.request_stop)
        await consumer.run()


def main() -> int:
    config = ConsumerConfig.load()
    logs = load_settings(LogSettings)
    configure_logging(
        service="ohip-consumer",
        environment=config.app.environment.value,
        code_version=config.app.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )
    log.info("consumer_iniciando", chain_code=config.ohip.chain_code)
    try:
        asyncio.run(serve(config))
    except Exception:  # traceback no JSON estruturado, com a chain (o systemd reinicia)
        log.exception("consumer_falhou", chain_code=config.ohip.chain_code)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
