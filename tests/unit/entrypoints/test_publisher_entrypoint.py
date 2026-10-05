"""Composição do processo ohip-publisher (sem rede nem banco)."""

from __future__ import annotations

from typing import Any

import pytest
from tests.fakes.oracle_driver import FakeOracle
from tests.unit.adapters.test_redis_adapters import FakeRedis
from tests.unit.test_config import set_env

from ohip_streaming.application.use_cases.publisher_service import PublisherService
from ohip_streaming.config import load_settings
from ohip_streaming.entrypoints import publisher as entry

ENV = {
    "ORACLE_DSN": "db:1521/svc",
    "ORACLE_USER": "ohip_app",
    "ORACLE_PASSWORD": "senha",
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

    pool = FakeOracle()
    monkeypatch.setattr(entry, "open_pool", lambda settings: pool)
    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosingRedis())
    monkeypatch.setattr(pool, "close", lambda force=False: closed.append("oracle"))
    async with entry.compose(config) as service:
        assert isinstance(service, PublisherService)
        lease: Any = service._lease
        assert lease._name == "publisher"
    assert closed == ["redis", "oracle"]
