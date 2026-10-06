"""Adapters de Redis contra um Redis de teste real.

Só roda com ``TEST_REDIS_URL`` **e** ``TEST_REDIS_DISPOSABLE=sim`` (Redis descartável; os
testes gravam chaves ``ohip:*`` com sufixos aleatórios e TTL curto; ``ohip:api:status`` e o
índice ``ohip:metrics_index`` têm nome fixo, e os testes só tiram de lá o que puseram).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr
from redis.asyncio import Redis

from ohip_streaming.adapters.redis.api_cache import STATUS_KEY, RedisApiCache, open_sync_redis
from ohip_streaming.adapters.redis.caches import (
    RedisQueueDedup,
    RedisResourceCache,
    RedisSeenCache,
    RedisTokenCache,
)
from ohip_streaming.adapters.redis.client import open_redis
from ohip_streaming.adapters.redis.metrics import (
    METRICS_INDEX_KEY,
    InMemoryMetrics,
    RedisMetricsPublisher,
    metrics_key,
)
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


async def test_queue_dedup_marks_with_ttl(redis: Redis) -> None:
    """``ohip:processed:<message_id>`` só existe depois do ``mark_processed`` e vence (ADR-0019)."""
    message_id = uuid.uuid4().hex
    dedup = RedisQueueDedup(redis, ttl_s=60)
    assert await dedup.is_processed(message_id) is False
    await dedup.mark_processed(message_id)
    assert await dedup.is_processed(message_id) is True
    assert 0 < await redis.ttl(f"ohip:processed:{message_id}") <= 60
    await redis.delete(f"ohip:processed:{message_id}")


async def test_resource_cache_keeps_value_and_not_found(redis: Redis) -> None:
    """O recurso e o "não encontrado" (``null``) voltam do Redis com TTL; chave ausente é
    *miss* (ADR-0019)."""
    run = uuid.uuid4().hex
    found, missing, absent = (f"ohip:rest:ZZT1:RESERVATION:H1:{run}-{s}" for s in "fna")
    cache = RedisResourceCache(redis, ttl_s=60)
    await cache.put(found, {"id": "1", "nome": "ação"})
    await cache.put(missing, None)
    assert await cache.get(found) == (True, {"id": "1", "nome": "ação"})
    assert await cache.get(missing) == (True, None)
    assert await cache.get(absent) == (False, None)
    assert 0 < await redis.ttl(found) <= 60
    await redis.delete(found, missing)


async def test_metrics_snapshot_read_by_api(redis: Redis) -> None:
    """Processo grava o snapshot e registra a chave no índice; a API (cliente síncrono) lê
    pelo índice e tira dele as chaves vencidas, sem ``SCAN`` (ADR-0017)."""
    assert _URL is not None
    instance = f"teste:{uuid.uuid4().hex}"
    metrics = InMemoryMetrics()
    metrics.increment("ohip_events_unmapped_total", 2, event_name="X")
    key = metrics_key("enricher", instance)
    assert await RedisMetricsPublisher(redis, metrics, key=key, ttl_s=60).publish() is True
    assert 0 < await redis.ttl(key) <= 60
    expired = metrics_key("consumer", f"teste:{uuid.uuid4().hex}")
    await redis.sadd(METRICS_INDEX_KEY, expired)  # nome no índice sem valor: TTL vencido

    sync = open_sync_redis(RedisSettings(url=SecretStr(_URL)))
    try:
        api = RedisApiCache(sync, status_ttl_s=5)
        api.ping()
        mine = [s for s in api.process_snapshots() if s.instance == instance]
        assert [(s.process, list(s.counters)) for s in mine] == [
            (
                "enricher",
                [{"name": "ohip_events_unmapped_total", "labels": {"event_name": "X"}, "value": 2}],
            )
        ]
        members = await redis.smembers(METRICS_INDEX_KEY)
        assert expired.encode() not in members
        assert key.encode() in members
    finally:
        sync.close()
        await redis.srem(METRICS_INDEX_KEY, key, expired)
        await redis.delete(key)


def test_api_status_cache_round_trip() -> None:
    """``ohip:api:status`` com o TTL configurado (ADR-0017 §6)."""
    assert _URL is not None
    sync = open_sync_redis(RedisSettings(url=SecretStr(_URL)))
    try:
        api = RedisApiCache(sync, status_ttl_s=5)
        api.put_status(b'{"chains":[]}')
        assert api.get_status() == b'{"chains":[]}'
        assert 0 < sync.ttl(STATUS_KEY) <= 5
    finally:
        sync.delete(STATUS_KEY)
        sync.close()
