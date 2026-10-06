"""Processo ``ohip-enricher``: consome ``ohip.enricher`` e normaliza no Oracle (ADR-0019).

Uso (systemd, Fase 10): ``ohip-enricher``; N instâncias em paralelo (MERGE condicional, sem
lease). SIGTERM/SIGINT: termina a mensagem em curso e sai; o que não teve ``ack`` volta para
a fila. Fila com argumentos divergentes: alerta crítico e saída com código 2.

**Sem regras até a Q-1** (CLAUDE.md): todo evento termina ``UNMAPPED``.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx

from ohip_streaming.adapters.ohip_rest.oauth import OhipTokenIssuer, credentials_from_settings
from ohip_streaming.adapters.ohip_rest.resources import OhipResourceFetcher, RateLimiter
from ohip_streaming.adapters.oracle.database import PooledSession, open_pool
from ohip_streaming.adapters.oracle.enrichment_store import OracleEnrichmentStore
from ohip_streaming.adapters.rabbitmq.consumer import (
    AioPikaQueueConsumer,
    ConsumerOptions,
    bindings_from,
)
from ohip_streaming.adapters.redis.caches import (
    RedisQueueDedup,
    RedisResourceCache,
    RedisTokenCache,
)
from ohip_streaming.adapters.redis.client import open_redis
from ohip_streaming.adapters.redis.metrics import (
    InMemoryMetrics,
    RedisMetricsPublisher,
    metrics_key,
)
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.errors import BrokerMisconfiguredError, ResourceRejectedError
from ohip_streaming.application.ports import DlqStage, ResourceFetcher
from ohip_streaming.application.use_cases.enrich_event import EnrichEvent, RuleRegistry
from ohip_streaming.application.use_cases.enricher_service import (
    EnricherOptions,
    EnricherService,
)
from ohip_streaming.application.use_cases.token_provider import TokenProvider, token_cache_key
from ohip_streaming.config import (
    AppSettings,
    EnricherSettings,
    LogSettings,
    OhipSettings,
    OracleSettings,
    RabbitMQSettings,
    RedisSettings,
    load_settings,
)
from ohip_streaming.entrypoints.common import AlertCounter, instance_id, preregister
from ohip_streaming.logging import configure_logging, get_logger

log = get_logger(__name__)

METRICS_INTERVAL_S = 15.0


@dataclass(frozen=True)
class EnricherConfig:
    app: AppSettings
    oracle: OracleSettings
    redis: RedisSettings
    rabbitmq: RabbitMQSettings
    enricher: EnricherSettings
    ohip: OhipSettings | None  # só com ENRICHER_REST_ENABLED=true (credenciais do OHIP)

    @classmethod
    def load(cls) -> EnricherConfig:
        enricher = load_settings(EnricherSettings)
        return cls(
            app=load_settings(AppSettings),
            oracle=load_settings(OracleSettings),
            redis=load_settings(RedisSettings),
            rabbitmq=load_settings(RabbitMQSettings),
            enricher=enricher,
            ohip=load_settings(OhipSettings) if enricher.rest_enabled else None,
        )


def build_rules() -> RuleRegistry:
    # TODO(Q-1): regras por eventName e tabelas de domínio só depois da resposta da Q-1
    # (CLAUDE.md). Sem regras, todo evento termina UNMAPPED (ohip_events_unmapped_total).
    return RuleRegistry(())


class DisabledFetcher:
    """REST desligada (Q-2): uma regra que peça recurso falha a mensagem, com o motivo."""

    async def fetch(
        self, chain_code: str, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        raise ResourceRejectedError("enriquecimento REST desligado (ENRICHER_REST_ENABLED=false)")


def consumer_options(config: EnricherConfig) -> ConsumerOptions:
    r, e = config.rabbitmq, config.enricher
    return ConsumerOptions(
        queue=r.enricher_queue,
        events_exchange=r.events_exchange,
        reprocess_exchange=r.reprocess_exchange,
        bindings=bindings_from(e.bindings),
        prefetch=e.prefetch,
        connect_timeout_s=r.connect_timeout_s,
    )


def enricher_options(config: EnricherConfig) -> EnricherOptions:
    e = config.enricher
    return EnricherOptions(
        max_attempts=e.max_attempts,
        retry_delay_s=e.retry_delay_s,
        transient_backoff_initial_s=e.transient_backoff_initial_s,
        transient_backoff_max_s=e.transient_backoff_max_s,
        systemic_failure_threshold=e.systemic_failure_threshold,
    )


@dataclass
class EnricherRuntime:
    service: EnricherService
    consumer: AioPikaQueueConsumer


@asynccontextmanager
async def compose(config: EnricherConfig) -> AsyncIterator[EnricherRuntime]:
    instance = instance_id()
    clock = SystemClock()
    metrics = InMemoryMetrics()
    preregister(metrics, alert_counters())
    pool = open_pool(config.oracle)
    executor = ThreadPoolExecutor(config.oracle.pool_max, thread_name_prefix="oracle-enricher")
    session = PooledSession(pool, call_timeout_ms=config.oracle.call_timeout_ms, executor=executor)
    redis = open_redis(config.redis)
    http: httpx.AsyncClient | None = None
    try:
        store = OracleEnrichmentStore(session, code_version=config.app.code_version)
        fetcher: ResourceFetcher = DisabledFetcher()
        if config.ohip is not None:
            o, e = config.ohip, config.enricher
            http = httpx.AsyncClient(base_url=o.gateway_url, timeout=e.rest_timeout_s)
            tokens = TokenProvider(
                issuer=OhipTokenIssuer(http, credentials_from_settings(o), clock),
                cache=RedisTokenCache(redis),
                clock=clock,
                metrics=metrics,
                cache_key=token_cache_key(config.app.environment.value, o.chain_code),
                margin_s=o.token_refresh_margin_s,
            )
            fetcher = OhipResourceFetcher(
                http,
                tokens={o.chain_code: tokens},
                app_key=o.app_key.get_secret_value(),
                paths=e.resource_paths,
                limiter=RateLimiter(rate_per_s=e.rest_rate_per_s, burst=e.rest_burst, clock=clock),
                cache=RedisResourceCache(redis, ttl_s=e.rest_cache_ttl_s),
                clock=clock,
                metrics=metrics,
                auth_rejected_alert=e.rest_auth_rejected_alert,
            )
        service = EnricherService(
            enrich=EnrichEvent(
                store=store,
                dedup=RedisQueueDedup(redis, ttl_s=config.enricher.dedup_ttl_s),
                fetcher=fetcher,
                rules=build_rules(),
                metrics=metrics,
            ),
            store=store,
            clock=clock,
            metrics=metrics,
            options=enricher_options(config),
        )
        consumer = AioPikaQueueConsumer(
            url=config.rabbitmq.url.get_secret_value(), options=consumer_options(config)
        )
        snapshots = RedisMetricsPublisher(
            redis, metrics, key=metrics_key("enricher", instance), ttl_s=config.redis.metrics_ttl_s
        )
        task = asyncio.create_task(_publish_metrics(snapshots), name="metricas")
        try:
            yield EnricherRuntime(service, consumer)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await snapshots.publish()  # último retrato antes de sair
    finally:
        if http is not None:
            await http.aclose()
        await redis.aclose()
        pool.close(force=True)
        executor.shutdown(wait=False)


def alert_counters() -> list[AlertCounter]:
    """Contadores do enricher citados em deploy/prometheus/ohip-alerts.yml."""
    return [
        ("ohip_enricher_systemic_failures_total", {"stage": DlqStage.NORMALIZE.value}),
        ("ohip_enricher_systemic_failures_total", {"stage": DlqStage.ENRICH.value}),
        ("ohip_enricher_unexpected_errors_total", {}),
        ("ohip_rest_auth_rejected_total", {}),
    ]


async def _publish_metrics(publisher: RedisMetricsPublisher) -> None:
    while True:
        await asyncio.sleep(METRICS_INTERVAL_S)
        await publisher.publish()


async def serve(config: EnricherConfig) -> None:
    async with compose(config) as runtime:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, runtime.service.request_stop)
        try:
            await runtime.consumer.run(runtime.service.handle, runtime.service.stop_event)
        except BrokerMisconfiguredError:
            log.critical("enricher_fila_divergente", exc_info=True)
            raise
        log.info("enricher_encerrado")


def main() -> int:
    config = EnricherConfig.load()
    logs = load_settings(LogSettings)
    configure_logging(
        service="ohip-enricher",
        environment=config.app.environment.value,
        code_version=config.app.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )
    log.info("enricher_iniciando", rest_enabled=config.enricher.rest_enabled)
    try:
        asyncio.run(serve(config))
    except BrokerMisconfiguredError:
        return 2  # já logado como crítico; não adianta reiniciar
    except Exception:
        log.exception("enricher_falhou")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
