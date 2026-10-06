"""``entrypoints/runtime.py``: snapshot periódico e último retrato (refatoração 6)."""

from __future__ import annotations

import asyncio
import json

from tests.unit.adapters.test_redis_adapters import FakeRedis

from ohip_streaming.adapters.redis.metrics import InMemoryMetrics
from ohip_streaming.entrypoints.runtime import metrics_snapshots


async def test_publishes_periodically_and_once_more_on_exit() -> None:
    redis, metrics = FakeRedis(), InMemoryMetrics()
    key = "ohip:metrics:teste:i1"
    async with metrics_snapshots(
        redis, metrics, process="teste", instance="i1", ttl_s=60, interval_s=0.01
    ):
        await asyncio.sleep(0.035)
        assert key in redis.data  # snapshots periódicos já saíram
        metrics.increment("ohip_lease_lost_total", lease="publisher")  # logo antes de sair
    (counter,) = json.loads(redis.data[key])
    assert counter["value"] == 1  # o último retrato pega o que aconteceu no fim
    assert redis.ttls[key] == 60


async def test_redis_down_on_exit_does_not_raise() -> None:
    redis, metrics = FakeRedis(), InMemoryMetrics()
    redis.broken = True
    async with metrics_snapshots(redis, metrics, process="teste", instance="i1", ttl_s=60):
        pass  # sai sem erro: Redis é atalho (ARCHITECTURE §10)
