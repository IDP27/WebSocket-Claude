"""Redis síncrono da API: cache do status e snapshots de métricas (ADR-0017)."""

from __future__ import annotations

import json

import pytest
from tests.fakes.redis_sync import FakeSyncRedis

from ohip_streaming.adapters.redis.api_cache import (
    STATUS_KEY,
    RedisApiCache,
    open_sync_redis,
)


def test_status_cache_round_trip() -> None:
    redis = FakeSyncRedis()
    cache = RedisApiCache(redis, status_ttl_s=5)
    assert cache.get_status() is None
    cache.put_status(b'{"chains":[]}')
    assert cache.get_status() == b'{"chains":[]}'
    assert redis.expires[STATUS_KEY.encode()] == 5


def test_status_cache_never_raises() -> None:
    redis = FakeSyncRedis()
    redis.fail = True
    cache = RedisApiCache(redis, status_ttl_s=5)
    cache.put_status(b"x")
    assert cache.get_status() is None
    with pytest.raises(ConnectionError):
        cache.ping()  # o /ready precisa ver a falha


def test_process_snapshots_come_from_the_index() -> None:
    redis = FakeSyncRedis()
    counters = [{"name": "ohip_events_total", "labels": {"chain_code": "C1"}, "value": 3}]
    live = b"ohip:metrics:consumer:vm1:123:abcd"
    redis.data[live] = json.dumps(counters).encode()
    redis.data[b"ohip:metrics:publisher:vm2:9:ef"] = b"nao-e-json"
    redis.data[b"ohip:metrics:quebrado"] = b"[]"
    redis.data[b"ohip:metrics:fora-do-indice:vm9:1:aa"] = b"[]"  # não é varrido
    expired = b"ohip:metrics:enricher:vm3:1:aa"  # TTL venceu: some do índice
    redis.sadd(
        "ohip:metrics_index",
        live,
        b"ohip:metrics:publisher:vm2:9:ef",
        b"ohip:metrics:quebrado",
        expired,
    )

    (snapshot,) = RedisApiCache(redis, status_ttl_s=5).process_snapshots()

    assert (snapshot.process, snapshot.instance) == ("consumer", "vm1:123:abcd")
    assert snapshot.counters == tuple(counters)
    assert expired not in redis.sets[b"ohip:metrics_index"]
    assert live in redis.sets[b"ohip:metrics_index"]


def test_no_snapshots() -> None:
    assert RedisApiCache(FakeSyncRedis(), status_ttl_s=5).process_snapshots() == []


def test_open_sync_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    from ohip_streaming.config import RedisSettings

    monkeypatch.setenv("REDIS_URL", "redis://:senha@localhost:6379/0")
    client = open_sync_redis(RedisSettings())
    try:
        assert client.connection_pool.connection_kwargs["socket_timeout"] == 2.0
    finally:
        client.close()
