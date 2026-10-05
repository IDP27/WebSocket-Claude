"""Composição do processo ohip-enricher (sem rede nem banco), ADR-0019."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from tests.fakes.oracle_driver import FakeOracle
from tests.unit.adapters.test_redis_adapters import FakeRedis
from tests.unit.test_config import OHIP_MIN_ENV, set_env

from ohip_streaming.adapters.ohip_rest.resources import OhipResourceFetcher
from ohip_streaming.adapters.rabbitmq.consumer import AioPikaQueueConsumer
from ohip_streaming.application.errors import BrokerMisconfiguredError, ResourceRejectedError
from ohip_streaming.config import EnricherSettings, load_settings
from ohip_streaming.entrypoints import enricher as entry

ENV = {
    "ORACLE_DSN": "db:1521/svc",
    "ORACLE_USER": "ohip_app",
    "ORACLE_PASSWORD": "senha",
    "REDIS_URL": "redis://:senha@localhost:6379/0",
    "RABBITMQ_URL": "amqps://u:senha@broker/vhost",
    "ENRICHER_BINDINGS": "ohip.rsv.#, ohip.crm.*",
    "ENRICHER_MAX_ATTEMPTS": "4",
}


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> entry.EnricherConfig:
    set_env(monkeypatch, **ENV)
    load_settings.cache_clear()
    return entry.EnricherConfig.load()


def test_rest_disabled_needs_no_ohip_credentials(config: entry.EnricherConfig) -> None:
    assert config.ohip is None
    assert entry.consumer_options(config).bindings == ("ohip.rsv.#", "ohip.crm.*")
    assert entry.consumer_options(config).queue == "ohip.enricher"
    assert entry.enricher_options(config).max_attempts == 4


def test_rest_enabled_requires_ohip_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    set_env(monkeypatch, **ENV, ENRICHER_REST_ENABLED="true")
    load_settings.cache_clear()
    with pytest.raises(ValidationError):
        entry.EnricherConfig.load()
    set_env(monkeypatch, **OHIP_MIN_ENV)
    load_settings.cache_clear()
    assert entry.EnricherConfig.load().ohip is not None


def test_no_rules_before_q1() -> None:
    assert entry.build_rules().get("UPDATE RESERVATION") is None


async def test_disabled_fetcher_explains_itself() -> None:
    with pytest.raises(ResourceRejectedError, match="ENRICHER_REST_ENABLED"):
        await entry.DisabledFetcher().fetch("C1", "RESERVATION", "H1", "1")


async def compose_and_check(
    config: entry.EnricherConfig, monkeypatch: pytest.MonkeyPatch
) -> tuple[Any, list[str]]:
    closed: list[str] = []

    class ClosingRedis(FakeRedis):
        async def aclose(self) -> None:
            closed.append("redis")

    pool = FakeOracle()
    monkeypatch.setattr(entry, "open_pool", lambda settings: pool)
    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosingRedis())
    monkeypatch.setattr(pool, "close", lambda force=False: closed.append("oracle"))
    async with entry.compose(config) as runtime:
        assert isinstance(runtime.consumer, AioPikaQueueConsumer)
        fetcher = runtime.service._enrich._fetcher
    return fetcher, closed


async def test_compose_without_rest(
    config: entry.EnricherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetcher, closed = await compose_and_check(config, monkeypatch)
    assert isinstance(fetcher, entry.DisabledFetcher)
    assert closed == ["redis", "oracle"]


async def test_compose_with_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    set_env(monkeypatch, **ENV, **OHIP_MIN_ENV, ENRICHER_REST_ENABLED="true")
    load_settings.cache_clear()
    fetcher, closed = await compose_and_check(entry.EnricherConfig.load(), monkeypatch)
    assert isinstance(fetcher, OhipResourceFetcher)
    assert closed == ["redis", "oracle"]


async def test_serve_runs_the_consumer_with_the_service_stop(
    config: entry.EnricherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Any] = []

    async def run(self: Any, handler: Any, stop: Any) -> None:
        calls.append((handler, stop))

    class ClosableRedis(FakeRedis):
        async def aclose(self) -> None:
            return None

    pool = FakeOracle()
    monkeypatch.setattr(entry, "open_pool", lambda settings: pool)
    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosableRedis())
    monkeypatch.setattr(AioPikaQueueConsumer, "run", run)
    await entry.serve(config)
    ((handler, stop),) = calls
    assert handler.__name__ == "handle"
    assert not stop.is_set()


@pytest.mark.parametrize(
    ("error", "code"), [(None, 0), (BrokerMisconfiguredError("x"), 2), (RuntimeError("x"), 1)]
)
def test_main_exit_codes(
    config: entry.EnricherConfig,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception | None,
    code: int,
) -> None:
    import logging

    import structlog

    async def serve(_: Any) -> None:
        if error is not None:
            raise error

    monkeypatch.setattr(entry, "serve", serve)
    try:
        assert entry.main() == code
    finally:
        structlog.reset_defaults()
        root = logging.getLogger()
        root.handlers = []
        root.setLevel(logging.WARNING)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ENRICHER_BINDINGS", "ohip.rsv.#, tem espaço"),
        ("ENRICHER_RESOURCE_PATHS", '{"reservation": "sem-barra/{primaryKey}"}'),
        ("ENRICHER_RESOURCE_PATHS", '{"reservation": "/rsv/{hotelId}"}'),
        ("ENRICHER_RESOURCE_PATHS", '{"reservation": "/rsv/{primaryKey}/{outro}"}'),
        ("ENRICHER_TRANSIENT_BACKOFF_MAX_S", "1"),
    ],
)
def test_enricher_settings_validation(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        EnricherSettings()


def test_enricher_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "ENRICHER_RESOURCE_PATHS", '{" Reservation ": "/rsv/{hotelId}/{primaryKey}"}'
    )
    settings = EnricherSettings()
    assert settings.rest_enabled is False
    assert settings.bindings == []
    assert settings.resource_paths == {"reservation": "/rsv/{hotelId}/{primaryKey}"}
