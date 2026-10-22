"""Fábrica da app FastAPI (ADR-0017).

- ``build_app(services)``: a app com rotas, erros e middleware; os testes passam serviços com
  fakes.
- ``create_app()``: fábrica do Uvicorn em produção. Carrega a configuração e, no lifespan,
  abre o pool do PostgreSQL e o Redis do worker e monta os serviços (``compose``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from ohip_streaming.adapters.postgres.database import InlineSession, Pool, PooledSession, open_pool
from ohip_streaming.adapters.postgres.monitoring_store import PostgresMonitoringStore
from ohip_streaming.adapters.postgres.operations_store import PostgresOperationsStore
from ohip_streaming.adapters.postgres.replay_store import PostgresReplayStore
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
    MaskingSettings,
    OhipRoutingSettings,
    PostgresSettings,
    RabbitMQSettings,
    RedisSettings,
    load_settings,
)
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
from ohip_streaming.entrypoints.common import masking_policy
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
    db: PostgresSettings
    redis: RedisSettings
    rabbitmq: RabbitMQSettings
    routing: OhipRoutingSettings
    masking: MaskingSettings

    @classmethod
    def load(cls) -> ApiConfig:
        return cls(
            app=load_settings(AppSettings),
            api=load_settings(ApiSettings),
            db=load_settings(PostgresSettings),
            redis=load_settings(RedisSettings),
            rabbitmq=load_settings(RabbitMQSettings),
            routing=load_settings(OhipRoutingSettings),
            masking=load_settings(MaskingSettings),
        )


def open_api_pool(config: ApiConfig) -> Pool:
    """Pool do worker com o prazo da API, menor que o do consumer: uma busca cara não segura
    uma conexão do pool (``POSTGRES_POOL_MAX`` por worker) por muito tempo (ADR-0017)."""
    return open_pool(
        config.db,
        environment=config.app.environment,
        application_name="ohip-api",
        statement_timeout_ms=config.api.db_statement_timeout_ms,
    )


def compose(config: ApiConfig, pool: Pool, redis: Any) -> ApiServices:
    """Serviços do worker sobre o pool do banco e o Redis síncrono. O pool já vem com o prazo
    próprio da API (``API_DB_STATEMENT_TIMEOUT_MS``, ``open_api_pool``)."""
    masking = masking_policy(config.masking)  # LGPD (Q-9): igual à fila
    session = InlineSession(PooledSession(pool))
    module_codes = config.routing.module_codes
    monitoring = Monitoring(
        store=PostgresMonitoringStore(session), masking=masking, module_codes=module_codes
    )
    messaging = MessagingOptions(
        exchange_names={
            ExchangeKind.EVENTS: config.rabbitmq.events_exchange,
            ExchangeKind.REPROCESS: config.rabbitmq.reprocess_exchange,
        },
        module_codes=module_codes,
        masking=masking,
    )
    operations = PostgresOperationsStore(session)
    replays = PostgresReplayStore(session)
    cache = RedisApiCache(redis, status_ttl_s=config.redis.api_status_ttl_s)
    status = StatusService(
        cache=cache, compute=status_payload(monitoring), ttl_s=config.redis.api_status_ttl_s
    )
    broker_url = config.rabbitmq.url.get_secret_value()
    readiness = Readiness(
        {
            "postgres": lambda: run_sync(monitoring.ping()),
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
        pool = open_api_pool(config)
        redis = open_sync_redis(config.redis)
        try:
            app.state.services = compose(config, pool, redis)
            log.info("api_iniciada")
            yield
        finally:
            redis.close()
            pool.close()
            log.info("api_encerrada")

    # Em produção a documentação interativa fica desligada (o contrato está no API.md e no
    # OpenAPI gerado nos testes).
    return build_app(
        lifespan=lifespan, expose_docs=config.app.environment is not Environment.PRODUCAO
    )
