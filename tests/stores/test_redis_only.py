"""Adapters de Redis contra um Redis de teste real.

Só roda com ``TEST_REDIS_URL`` **e** ``TEST_REDIS_DISPOSABLE=sim`` (Redis descartável; os
testes gravam chaves ``ohip:*`` com sufixos aleatórios e TTL curto).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr
from redis.asyncio import Redis

from ohip_streaming.adapters.redis.caches import RedisSeenCache, RedisTokenCache
from ohip_streaming.adapters.redis.client import open_redis
from ohip_streaming.application.ports import AccessToken
from ohip_streaming.config import RedisSettings

_URL = os.environ.get("TEST_REDIS_URL")
pytestmark = [
    pytest.mark.redis,
    pytest.mark.skipif(
        not _URL or os.environ.get("TEST_REDIS_DISPOSABLE") != "sim",
        reason="Redis de teste não configurado (TEST_REDIS_URL e TEST_REDIS_DISPOSABLE=sim)",
    ),
]


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    assert _URL is not None
    client = open_redis(RedisSettings(url=SecretStr(_URL)))
    try:
        yield client
    finally:
        await client.aclose()


async def test_seen_cache_round_trip(redis: Redis) -> None:
    run = uuid.uuid4().hex
    cache = RedisSeenCache(redis, ttl_s=60)
    await cache.mark_seen([f"{run}-a", f"{run}-b"])
    assert await cache.filter_seen([f"{run}-a", f"{run}-x", f"{run}-b"]) == {f"{run}-a", f"{run}-b"}
    assert 0 < await redis.ttl(f"ohip:seen:{run}-a") <= 60


async def test_token_cache_round_trip(redis: Redis) -> None:
    key = f"ohip:token:teste:{uuid.uuid4().hex}"
    cache = RedisTokenCache(redis)
    token = AccessToken("tok", datetime.now(UTC) + timedelta(hours=1))
    await cache.put(key, token, 30)
    assert await cache.get(key) == token
    await cache.delete(key)
    assert await cache.get(key) is None
