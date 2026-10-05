"""Adapters de Redis com um Redis falso (o comportamento real é coberto pelos testes @redis)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import SecretStr

from ohip_streaming.adapters.redis.caches import (
    RedisQueueDedup,
    RedisSeenCache,
    RedisTokenCache,
)
from ohip_streaming.adapters.redis.client import open_redis
from ohip_streaming.adapters.redis.metrics import (
    InMemoryMetrics,
    RedisMetricsPublisher,
    metrics_key,
)
from ohip_streaming.application.ports import AccessToken
from ohip_streaming.config import RedisSettings


class FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.ttls: dict[str, int | None] = {}
        self.sets: dict[str, set[str]] = {}
        self.broken = False

    def _check(self) -> None:
        if self.broken:
            raise ConnectionError("redis fora")

    async def mget(self, keys: Sequence[str]) -> list[Any]:
        self._check()
        return [self.data.get(k) for k in keys]

    async def get(self, name: str) -> Any:
        self._check()
        return self.data.get(name)

    async def set(self, name: str, value: Any, ex: int | None = None) -> Any:
        self._check()
        self.data[name] = value
        self.ttls[name] = ex
        return True

    async def exists(self, *names: str) -> int:
        self._check()
        return sum(1 for n in names if n in self.data)

    async def delete(self, *names: str) -> int:
        self._check()
        return sum(1 for n in names if self.data.pop(n, None) is not None)

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self.redis = redis
        self.ops: list[tuple[str, Any, int | None]] = []
        self.adds: list[tuple[str, tuple[str, ...]]] = []

    def set(self, name: str, value: Any, ex: int | None = None) -> FakePipeline:
        self.ops.append((name, value, ex))
        return self

    def sadd(self, name: str, *values: str) -> FakePipeline:
        self.adds.append((name, values))
        return self

    async def execute(self) -> list[Any]:
        results = [await self.redis.set(n, v, ex=e) for n, v, e in self.ops]
        for name, values in self.adds:
            self.redis._check()
            self.redis.sets.setdefault(name, set()).update(values)
        return results


async def test_seen_cache() -> None:
    redis = FakeRedis()
    cache = RedisSeenCache(redis, ttl_s=86_400)
    await cache.mark_seen(["a", "b"])
    assert redis.ttls["ohip:seen:a"] == 86_400
    assert await cache.filter_seen(["a", "c", "b"]) == {"a", "b"}
    assert await cache.filter_seen([]) == set()
    await cache.mark_seen([])  # nada a fazer


async def test_seen_cache_failures_propagate_to_the_use_case() -> None:
    redis = FakeRedis()
    redis.broken = True
    with pytest.raises(ConnectionError):  # ProcessEventBatch trata como "sem atalho"
        await RedisSeenCache(redis, ttl_s=60).filter_seen(["a"])


async def test_queue_dedup() -> None:
    redis = FakeRedis()
    dedup = RedisQueueDedup(redis, ttl_s=3600)
    assert not await dedup.is_processed("m1")
    await dedup.mark_processed("m1")
    assert await dedup.is_processed("m1")
    assert redis.ttls["ohip:processed:m1"] == 3600


async def test_token_cache_round_trip() -> None:
    redis = FakeRedis()
    cache = RedisTokenCache(redis)
    token = AccessToken("tok", datetime(2026, 10, 1, 13, 0, tzinfo=UTC), lifetime_s=3600.0)
    await cache.put("ohip:token:h:C1", token, 3299.7)
    assert redis.ttls["ohip:token:h:C1"] == 3299
    assert await cache.get("ohip:token:h:C1") == token
    await cache.delete("ohip:token:h:C1")
    assert await cache.get("ohip:token:h:C1") is None
    await cache.put("k", token, 0.2)
    assert redis.ttls["k"] == 1  # nunca 0 (o Redis recusaria)


async def test_token_cache_ignores_garbage() -> None:
    redis = FakeRedis()
    redis.data["k"] = b"{lixo"
    assert await RedisTokenCache(redis).get("k") is None
    redis.data["k"] = json.dumps({"value": "x"}).encode()
    assert await RedisTokenCache(redis).get("k") is None
    old_format = {"value": "x", "expires_at": "2026-10-01T13:00:00+00:00"}
    redis.data["k"] = json.dumps(old_format).encode()
    cached = await RedisTokenCache(redis).get("k")
    assert cached is not None
    assert cached.lifetime_s is None  # sem vida conhecida: margem cheia


async def test_metrics_snapshot() -> None:
    redis = FakeRedis()
    metrics = InMemoryMetrics()
    metrics.increment("ohip_events_inserted_total", 3, chain_code="C1")
    metrics.increment("ohip_events_inserted_total", 2, chain_code="C1")
    metrics.increment("ohip_token_issued_total")
    key = metrics_key("consumer", "vm1:42")
    publisher = RedisMetricsPublisher(redis, metrics, key=key, ttl_s=60)

    assert await publisher.publish()
    assert key == "ohip:metrics:consumer:vm1:42"
    assert redis.ttls[key] == 60
    assert redis.sets["ohip:metrics_index"] == {key}  # a API lê pelo índice, sem SCAN
    assert json.loads(redis.data[key]) == [
        {"name": "ohip_events_inserted_total", "labels": {"chain_code": "C1"}, "value": 5},
        {"name": "ohip_token_issued_total", "labels": {}, "value": 1},
    ]
    redis.broken = True
    assert not await publisher.publish()  # Redis fora: só log


async def test_open_redis_uses_short_timeouts() -> None:
    client = open_redis(RedisSettings(url=SecretStr("redis://:senha@localhost:6379/0")))
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == 2.0
    assert kwargs["password"] == "senha"
    await client.aclose()
