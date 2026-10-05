"""Fábrica da app FastAPI (ADR-0017).

- ``build_app(services)``: a app com rotas, erros e middleware; os testes passam serviços com
  fakes.
- ``create_app()``: fábrica do Uvicorn em produção. Carrega a configuração e, no lifespan,
  abre o pool Oracle e o Redis do worker e monta os serviços (``compose``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from ohip_streaming.adapters.oracle.database import InlineSession, Pool, PooledSession, open_pool
from ohip_streaming.adapters.oracle.monitoring_store import OracleMonitoringStore
from ohip_streaming.adapters.oracle.operations_store import OracleOperationsStore
from ohip_streaming.adapters.oracle.replay_store import OracleReplayStore
from ohip_streaming.adapters.rabbitmq.probe import probe_broker
from ohip_streaming.adapters.redis.api_cache import RedisApiCache, open_sync_redis
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.use_cases.monitoring import Monitoring
from ohip_streaming.application.use_cases.operations import (
    MessagingOptions,
    ReprocessEvent,
    RetryDlqItem,
)
from ohip_streaming.application.use_cases.replay import CancelReplay, RequestReplay
from ohip_streaming.config import (
    ApiSettings,
    AppSettings,
    Environment,
    LogSettings,
    OhipRoutingSettings,
    OracleSettings,
    RabbitMQSettings,
    RedisSettings,
    load_settings,
)
from ohip_streaming.domain.masking import MaskingPolicy
from ohip_streaming.domain.messages import ExchangeKind
from ohip_streaming.entrypoints.api.errors import RequestContextMiddleware, install_error_handlers
from ohip_streaming.entrypoints.api.routes import api, internal
from ohip_streaming.entrypoints.api.runtime import run_sync
from ohip_streaming.entrypoints.api.security import Role
from ohip_streaming.entrypoints.api.services import (
    ApiServices,
    MetricsService,
    Readiness,
    StatusService,
    TtlCache,
    status_payload,
)
from ohip_streaming.logging import configure_logging, get_logger

log = get_logger(__name__)

TITLE = "ohip-api"
DESCRIPTION = "API de controle do consumidor OHIP Streaming (docs/API.md)."
VERSION = "1"

Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def build_app(
    services: ApiServices | None = None,
    *,
    lifespan: Lifespan | None = None,
    expose_docs: bool = True,
) -> FastAPI:
    """``expose_docs=False`` (produção) desliga ``/docs``, ``/redoc`` e ``/openapi.json``."""
    app = FastAPI(
        title=TITLE,
        description=DESCRIPTION,
        version=VERSION,
        lifespan=lifespan,
        docs_url="/docs" if expose_docs else None,
        redoc_url="/redoc" if expose_docs else None,
        openapi_url="/openapi.json" if expose_docs else None,
    )
    if services is not None:
        app.state.services = services
    install_error_handlers(app)
    app.add_middleware(RequestContextMiddleware)
    app.include_router(internal)
    app.include_router(api)
    return app


@dataclass(frozen=True)
class ApiConfig:
    app: AppSettings
    api: ApiSettings
    oracle: OracleSettings
    redis: RedisSettings
    rabbitmq: RabbitMQSettings
    routing: OhipRoutingSettings

    @classmethod
    def load(cls) -> ApiConfig:
        return cls(
            app=load_settings(AppSettings),
            api=load_settings(ApiSettings),
            oracle=load_settings(OracleSettings),
            redis=load_settings(RedisSettings),
            rabbitmq=load_settings(RabbitMQSettings),
            routing=load_settings(OhipRoutingSettings),
        )


def compose(config: ApiConfig, pool: Pool, redis: Any) -> ApiServices:
    """Serviços do worker sobre o pool Oracle e o Redis síncrono."""
    # Prazo próprio da API (API_ORACLE_CALL_TIMEOUT_MS), não o do consumer.
    session = InlineSession(PooledSession(pool, call_timeout_ms=config.api.oracle_call_timeout_ms))
    module_codes = config.routing.module_codes
    monitoring = Monitoring(
        store=OracleMonitoringStore(session), masking=MaskingPolicy(), module_codes=module_codes
    )
    messaging = MessagingOptions(
        exchange_names={
            ExchangeKind.EVENTS: config.rabbitmq.events_exchange,
            ExchangeKind.REPROCESS: config.rabbitmq.reprocess_exchange,
        },
        module_codes=module_codes,
    )
    operations = OracleOperationsStore(session)
    replays = OracleReplayStore(session)
    cache = RedisApiCache(redis, status_ttl_s=config.redis.api_status_ttl_s)
    status = StatusService(
        cache=cache, compute=status_payload(monitoring), ttl_s=config.redis.api_status_ttl_s
    )
    broker_url = config.rabbitmq.url.get_secret_value()
    readiness = Readiness(
        {
            "oracle": lambda: run_sync(monitoring.ping()),
            "redis": cache.ping,
            "rabbitmq": lambda: run_sync(
                probe_broker(broker_url, timeout_s=config.rabbitmq.connect_timeout_s)
            ),
        }
    )
    metrics = MetricsService(monitoring=monitoring, snapshots=cache.process_snapshots)
    return ApiServices(
        tokens={digest: Role(role) for digest, role in config.api.service_tokens.items()},
        monitoring=monitoring,
        request_replay=RequestReplay(store=replays, clock=SystemClock()),
        cancel_replay=CancelReplay(store=replays),
        reprocess=ReprocessEvent(store=operations, options=messaging),
        retry_dlq=RetryDlqItem(store=operations, options=messaging),
        status=status,
        ready=TtlCache(readiness.check, config.api.ready_cache_s),
        metrics=TtlCache(metrics.render, config.api.metrics_cache_s),
    )


def create_app() -> FastAPI:
    """Fábrica do Uvicorn (``--factory``): uma por worker."""
    config = ApiConfig.load()
    logs = load_settings(LogSettings)
    configure_logging(
        service="ohip-api",
        environment=config.app.environment.value,
        code_version=config.app.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )
    if not config.api.service_tokens:
        log.warning("api_sem_tokens_de_servico")  # toda rota /api/v1 responderá 401

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = open_pool(config.oracle)
        redis = open_sync_redis(config.redis)
        try:
            app.state.services = compose(config, pool, redis)
            log.info("api_iniciada")
            yield
        finally:
            redis.close()
            pool.close(force=True)
            log.info("api_encerrada")

    # Em produção a documentação interativa fica desligada (o contrato está no API.md e no
    # OpenAPI gerado nos testes).
    return build_app(
        lifespan=lifespan, expose_docs=config.app.environment is not Environment.PRODUCAO
    )
