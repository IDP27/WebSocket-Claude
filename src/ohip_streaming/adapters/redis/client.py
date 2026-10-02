"""Cliente Redis assíncrono. Redis é só atalho: token, caches e métricas (ADR-0008, DV-12).

Timeouts curtos: um Redis lento não pode atrasar a ingestão. Os adapters pegam as exceções
do Redis e as tratam como "sem atalho"; quem decide é o caso de uso (falha nunca afeta a
correção).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from redis.asyncio import Redis

if TYPE_CHECKING:
    from ohip_streaming.config import RedisSettings

SOCKET_TIMEOUT_S = 2.0


def open_redis(settings: RedisSettings) -> Redis:
    return Redis.from_url(
        settings.url.get_secret_value(),
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=SOCKET_TIMEOUT_S,
        health_check_interval=30,
        decode_responses=False,
    )
