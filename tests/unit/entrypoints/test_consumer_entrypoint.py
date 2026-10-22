"""Composição do processo ohip-consumer (sem rede nem banco: pool e Redis falsos)."""

from __future__ import annotations

from typing import Any

import pytest
from tests.fakes.postgres_driver import FakePool
from tests.unit.adapters.test_redis_adapters import FakeRedis
from tests.unit.test_config import OHIP_MIN_ENV, set_env

from ohip_streaming.application.use_cases.consume_chain import ChainConsumer
from ohip_streaming.config import load_settings
from ohip_streaming.domain.masking import MaskingPolicy
from ohip_streaming.domain.messages import ExchangeKind
from ohip_streaming.entrypoints import consumer as entry
from ohip_streaming.entrypoints.common import instance_id

ENV = {
    **OHIP_MIN_ENV,
    "OHIP_HOTEL_CODES": "H1,H2",
    "OHIP_EVENT_TIMEZONE": "America/Sao_Paulo",
    "POSTGRES_HOST": "db",
    "POSTGRES_DB": "ohip",
    "POSTGRES_USER": "ohip_app",
    "POSTGRES_PASSWORD": "senha",
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
    assert instance_id() != instance_id()
    assert instance_id().count(":") == 2


async def test_compose_builds_and_closes_everything(
    config: entry.ConsumerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = FakePool()
    closed: list[str] = []

    class ClosingRedis(FakeRedis):
        async def aclose(self) -> None:
            closed.append("redis")

    monkeypatch.setattr(entry, "open_pool", lambda settings, **kw: pool)
    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosingRedis())
    original_close = pool.close

    def close_pool(timeout: float = 5.0) -> None:
        closed.append("postgres")
        original_close(timeout)

    monkeypatch.setattr(pool, "close", close_pool)

    async with entry.compose(config) as consumer:
        assert isinstance(consumer, ChainConsumer)
        deps: Any = consumer._deps
        assert deps.app_key == "app-key-secreta"
        assert "app-key-secreta" not in repr(deps)  # segredo fora do repr
        assert deps.lease._options.ttl_s == 40
        # Caracterização (refatoração 10): o nome que o LeaseKeeper disputa é o mesmo da
        # barreira de epoch do adapter (test_pg_event_store: "consumer:<chain>").
        assert deps.lease.name == "consumer:CHAIN1"
        # Caracterização (refatoração 11): sem lista extra, a máscara padrão da Q-9 na fila.
        assert deps.batch._options.masking == MaskingPolicy()
        assert deps.retry_dlq._options.masking == MaskingPolicy()
    assert closed == ["redis", "postgres"]
    assert ("ohip_lease_lost_total", {"lease": "consumer:CHAIN1"}) in entry.alert_counters("CHAIN1")


async def test_extra_masked_elements_reach_the_queue_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MASK_EXTRA_ELEMENTS (Q-9) chega à máscara da fila e do retry de DLQ."""
    set_env(monkeypatch, **ENV, MASK_EXTRA_ELEMENTS="room number")
    load_settings.cache_clear()
    config = entry.ConsumerConfig.load()
    monkeypatch.setattr(entry, "open_pool", lambda settings, **kw: FakePool())

    class ClosingRedis(FakeRedis):
        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(entry, "open_redis", lambda settings: ClosingRedis())
    async with entry.compose(config) as consumer:
        deps: Any = consumer._deps
        for policy in (deps.batch._options.masking, deps.retry_dlq._options.masking):
            assert policy.is_sensitive("ROOM NUMBER", "RESERVATION")
            assert not policy.is_sensitive("ARRIVAL DATE", "RESERVATION")
