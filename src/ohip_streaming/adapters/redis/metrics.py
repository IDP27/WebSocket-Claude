"""Métricas: contadores em memória e snapshot periódico no Redis (ARCHITECTURE §10).

Chave ``ohip:metrics:<processo>:<instancia>`` com TTL de 60 s, regravada a cada 15 s, e o nome
da chave no conjunto ``ohip:metrics_index``. A API (Fase 7) lê o índice e junta os snapshots no
``/metrics`` sem varrer o keyspace (``SCAN`` percorreria também as chaves ``ohip:seen:*``,
ADR-0017). Redis fora: os contadores continuam em memória.
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


METRICS_INDEX_KEY = "ohip:metrics_index"


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
            pipe = self._redis.pipeline(transaction=False)
            pipe.set(self._key, payload.encode(), ex=self._ttl_s)
            pipe.sadd(METRICS_INDEX_KEY, self._key)  # a API limpa as chaves vencidas
            await pipe.execute()
        except Exception:  # Redis é atalho
            log.warning("metricas_redis_indisponivel", exc_info=True)
            return False
        return True
