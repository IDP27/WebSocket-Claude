"""Caches no Redis: dedup rápido do consumer, dedup do enricher e token OAuth.

Chaves (ARCHITECTURE §7): ``ohip:seen:<uniqueEventId>`` (24 h, gravada só depois do commit),
``ohip:processed:<message_id>`` (enricher) e ``ohip:token:<ambiente>:<chain>``.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol

from ohip_streaming.application.ports import AccessToken

SEEN_PREFIX = "ohip:seen:"
PROCESSED_PREFIX = "ohip:processed:"


class RedisLike(Protocol):
    """O subconjunto de ``redis.asyncio.Redis`` usado aqui (os testes usam um fake).

    Os métodos do redis-py são tipados como "awaitable ou valor"; por isso o retorno é ``Any``
    e os adapters fazem ``await``.
    """

    def mget(self, keys: Any, *args: Any) -> Any: ...

    def get(self, name: Any) -> Any: ...

    def set(self, name: Any, value: Any, *args: Any, **kwargs: Any) -> Any: ...

    def exists(self, *names: Any) -> Any: ...

    def delete(self, *names: Any) -> Any: ...

    def pipeline(self, *args: Any, **kwargs: Any) -> Any: ...


class RedisSeenCache:
    """``SeenCache`` (ADR-0009): atalho; o UNIQUE do Oracle é quem garante."""

    def __init__(self, redis: RedisLike, *, ttl_s: int) -> None:
        self._redis = redis
        self._ttl_s = ttl_s

    async def filter_seen(self, unique_event_ids: Sequence[str]) -> set[str]:
        if not unique_event_ids:
            return set()
        values = await self._redis.mget([SEEN_PREFIX + uid for uid in unique_event_ids])
        return {uid for uid, value in zip(unique_event_ids, values, strict=True) if value}

    async def mark_seen(self, unique_event_ids: Sequence[str]) -> None:
        if not unique_event_ids:
            return
        pipe = self._redis.pipeline(transaction=False)
        for uid in unique_event_ids:
            pipe.set(SEEN_PREFIX + uid, b"1", ex=self._ttl_s)
        await pipe.execute()


class RedisQueueDedup:
    """``QueueDedup`` do enricher: ``message_id`` marcado só depois do sucesso."""

    def __init__(self, redis: RedisLike, *, ttl_s: int) -> None:
        self._redis = redis
        self._ttl_s = ttl_s

    async def is_processed(self, message_id: str) -> bool:
        return bool(await self._redis.exists(PROCESSED_PREFIX + message_id))

    async def mark_processed(self, message_id: str) -> None:
        await self._redis.set(PROCESSED_PREFIX + message_id, b"1", ex=self._ttl_s)


class RedisTokenCache:
    """``TokenCache``: token com validade até ``exp - margem`` (Q-12)."""

    def __init__(self, redis: RedisLike) -> None:
        self._redis = redis

    async def get(self, key: str) -> AccessToken | None:
        raw = await self._redis.get(key)
        if not raw:
            return None
        try:
            data = json.loads(raw)
            lifetime = data.get("lifetime_s")
            return AccessToken(
                value=data["value"],
                expires_at=datetime.fromisoformat(data["expires_at"]),
                lifetime_s=float(lifetime) if isinstance(lifetime, int | float) else None,
            )
        except (ValueError, KeyError, TypeError):
            return None  # valor estranho: emite um novo

    async def put(self, key: str, token: AccessToken, ttl_s: float) -> None:
        payload = json.dumps(
            {
                "value": token.value,
                "expires_at": token.expires_at.isoformat(),
                "lifetime_s": token.lifetime_s,
            }
        )
        await self._redis.set(key, payload.encode(), ex=max(1, int(ttl_s)))

    async def delete(self, key: str) -> None:
        await self._redis.delete(key)
