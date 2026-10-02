"""Métricas: contadores em memória e snapshot periódico no Redis (ARCHITECTURE §10).

Chave ``ohip:metrics:<processo>:<instancia>`` com TTL de 60 s, regravada a cada 15 s. A API
(Fase 7) junta os snapshots no ``/metrics``. Redis fora: os contadores continuam em memória.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from ohip_streaming.adapters.redis.caches import RedisLike
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


class InMemoryMetrics:
    """``MetricsSink`` do processo. Rótulos viram parte da chave do contador."""

    def __init__(self) -> None:
        self._counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        self._counters[(name, tuple(sorted(labels.items())))] += value

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {"name": name, "labels": dict(labels), "value": value}
            for (name, labels), value in sorted(self._counters.items())
        ]


def metrics_key(process: str, instance: str) -> str:
    return f"ohip:metrics:{process}:{instance}"


class RedisMetricsPublisher:
    def __init__(self, redis: RedisLike, metrics: InMemoryMetrics, *, key: str, ttl_s: int):
        self._redis = redis
        self._metrics = metrics
        self._key = key
        self._ttl_s = ttl_s

    async def publish(self) -> bool:
        """Grava o snapshot. False se o Redis falhou (só log; tenta de novo no próximo ciclo)."""
        payload = json.dumps(self._metrics.snapshot(), separators=(",", ":"))
        try:
            await self._redis.set(self._key, payload.encode(), ex=self._ttl_s)
        except Exception:  # Redis é atalho
            log.warning("metricas_redis_indisponivel", exc_info=True)
            return False
        return True
