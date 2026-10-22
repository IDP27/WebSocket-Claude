"""Composição do processo ohip-publisher (sem rede nem banco)."""

from __future__ import annotations

from typing import Any

import pytest
from tests.fakes.postgres_driver import FakePool
from tests.unit.adapters.test_redis_adapters import FakeRedis
from tests.unit.test_config import set_env

from ohip_streaming.application.use_cases.publisher_service import PublisherService
from ohip_streaming.config import load_settings
from ohip_streaming.entrypoints import publisher as entry

ENV = {
    "POSTGRES_HOST": "db",
    "POSTGRES_DB": "ohip",
    "POSTGRES_USER": "ohip_app",
    "POSTGRES_PASSWORD": "senha",
    "REDIS_URL": "redis://:senha@localhost:6379/0",
    "RABBITMQ_URL": "amqps://u:senha@broker/vhost",
    "RABBITMQ_PUBLISH_MAX_ATTEMPTS": "7",
    "PUBLISHER_MAX_PARALLEL_CHAINS": "3",
}


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> entry.PublisherConfig:
    set_env(monkeypatch, **ENV)
    load_settings.cache_clear()
    return entry.PublisherConfig.load()


def test_options_follow_the_settings(config: entry.PublisherConfig) -> None:
    assert entry.publish_options(config).max_attempts == 7
    assert entry.loop_options(config).max_parallel_chains == 3


async def test_compose_builds_and_closes(
    config: entry.PublisherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    closed: list[str] = []

    class ClosingRedis(FakeRedis):
        async def aclose(self) -> None:
            closed.append("redis")

    pool = FakePool()
    monkeypatch.setattr(entry, "open_pool", lambda settings, **kw: pool)
    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosingRedis())
    monkeypatch.setattr(pool, "close", lambda timeout=5.0: closed.append("postgres"))
    async with entry.compose(config) as service:
        assert isinstance(service, PublisherService)
        lease: Any = service._lease
        assert lease.name == "publisher"
    assert closed == ["redis", "postgres"]
    assert entry.alert_counters() == [("ohip_lease_lost_total", {"lease": "publisher"})]
