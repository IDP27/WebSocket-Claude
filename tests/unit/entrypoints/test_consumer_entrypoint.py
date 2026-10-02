"""Composição do processo ohip-consumer (sem rede nem banco: pool e Redis falsos)."""

from __future__ import annotations

from typing import Any

import pytest
from tests.fakes.oracle_driver import FakeOracle
from tests.unit.adapters.test_redis_adapters import FakeRedis
from tests.unit.test_config import OHIP_MIN_ENV, set_env

from ohip_streaming.application.use_cases.consume_chain import ChainConsumer
from ohip_streaming.config import load_settings
from ohip_streaming.domain.messages import ExchangeKind
from ohip_streaming.entrypoints import consumer as entry

ENV = {
    **OHIP_MIN_ENV,
    "OHIP_HOTEL_CODES": "H1,H2",
    "OHIP_EVENT_TIMEZONE": "America/Sao_Paulo",
    "ORACLE_DSN": "db:1521/svc",
    "ORACLE_USER": "ohip_app",
    "ORACLE_PASSWORD": "senha",
    "REDIS_URL": "redis://:senha@localhost:6379/0",
    "RABBITMQ_URL": "amqps://u:senha@broker/vhost",
    "CONSUMER_BATCH_MAX_WAIT_MS": "250",
    "LEASE_TTL_S": "40",
}


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> entry.ConsumerConfig:
    set_env(monkeypatch, **ENV)
    load_settings.cache_clear()
    return entry.ConsumerConfig.load()


def test_options_follow_the_settings(config: entry.ConsumerConfig) -> None:
    options = entry.consumer_options(config, "vm1:1:abc")
    assert options.chain_code == "CHAIN1"
    assert options.hotel_codes == ("H1", "H2")
    assert str(options.event_tz) == "America/Sao_Paulo"
    assert options.batch_max_wait_s == 0.25
    assert options.ping_interval_s == 15
    assert options.policy.min_gap_s == 10
    assert options.policy.lockout_4409_s == 120
    assert entry.exchange_names(config.rabbitmq) == {
        ExchangeKind.EVENTS: "ohip.events",
        ExchangeKind.REPROCESS: "ohip.reprocess",
    }


def test_instance_id_is_unique() -> None:
    assert entry.instance_id() != entry.instance_id()
    assert entry.instance_id().count(":") == 2


async def test_compose_builds_and_closes_everything(
    config: entry.ConsumerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = FakeOracle()
    closed: list[str] = []

    class ClosingRedis(FakeRedis):
        async def aclose(self) -> None:
            closed.append("redis")

    monkeypatch.setattr(entry, "open_pool", lambda settings: pool)
    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosingRedis())
    original_close = pool.close

    def close_pool(force: bool = False) -> None:
        closed.append("oracle")
        original_close(force)

    monkeypatch.setattr(pool, "close", close_pool)

    async with entry.compose(config) as consumer:
        assert isinstance(consumer, ChainConsumer)
        deps: Any = consumer._deps
        assert deps.app_key == "app-key-secreta"
        assert "app-key-secreta" not in repr(deps)  # segredo fora do repr
        assert deps.lease._options.ttl_s == 40
    assert closed == ["redis", "oracle"]
