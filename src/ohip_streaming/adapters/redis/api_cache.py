"""Redis **síncrono** da API de controle (ADR-0017).

As rotas da API rodam em threads do threadpool, cada requisição com o seu ``asyncio.run``; um
cliente ``redis.asyncio`` ficaria preso ao event loop em que foi criado. Por isso a API usa o
cliente síncrono do mesmo pacote.

- ``ohip:api:status``: cache do ``GET /api/v1/status`` (ARCHITECTURE §7), protege o Oracle do
  polling do painel. Falha do Redis = sem cache (nunca derruba a rota).
- ``ohip:metrics:<processo>:<instancia>``: snapshots dos contadores dos processos, lidos pelo
  ``/metrics`` a partir do índice ``ohip:metrics_index`` (sem ``SCAN``: o keyspace tem uma
  chave ``ohip:seen:*`` por evento). Nomes vencidos saem do índice na leitura.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from redis import Redis

from ohip_streaming.adapters.redis.client import SOCKET_TIMEOUT_S
from ohip_streaming.adapters.redis.metrics import METRICS_INDEX_KEY
from ohip_streaming.logging import get_logger

if TYPE_CHECKING:
    from ohip_streaming.config import RedisSettings

log = get_logger(__name__)

STATUS_KEY = "ohip:api:status"


class SyncRedisLike(Protocol):
    """O subconjunto de ``redis.Redis`` usado aqui (os testes usam um fake)."""

    def get(self, name: Any) -> Any: ...

    def set(self, name: Any, value: Any, *args: Any, **kwargs: Any) -> Any: ...

    def mget(self, keys: Any, *args: Any) -> Any: ...

    def smembers(self, name: Any) -> Any: ...

    def srem(self, name: Any, *values: Any) -> Any: ...

    def ping(self, **kwargs: Any) -> Any: ...


def open_sync_redis(settings: RedisSettings) -> Redis:
    return Redis.from_url(
        settings.url.get_secret_value(),
        socket_timeout=SOCKET_TIMEOUT_S,
        socket_connect_timeout=SOCKET_TIMEOUT_S,
        health_check_interval=30,
        decode_responses=False,
    )


@dataclass(frozen=True, slots=True)
class ProcessSnapshot:
    """Contadores de um processo: ``[{"name", "labels", "value"}, ...]`` (InMemoryMetrics)."""

    process: str
    instance: str
    counters: tuple[Mapping[str, Any], ...]


class RedisApiCache:
    def __init__(self, redis: SyncRedisLike, *, status_ttl_s: int) -> None:
        self._redis = redis
        self._status_ttl_s = status_ttl_s

    def ping(self) -> None:
        """Levanta a exceção do Redis (``/ready``)."""
        self._redis.ping()

    def get_status(self) -> bytes | None:
        try:
            value = self._redis.get(STATUS_KEY)
        except Exception:  # Redis é atalho
            log.warning("api_cache_status_indisponivel", exc_info=True)
            return None
        return bytes(value) if value is not None else None

    def put_status(self, payload: bytes) -> None:
        try:
            self._redis.set(STATUS_KEY, payload, ex=self._status_ttl_s)
        except Exception:
            log.warning("api_cache_status_indisponivel", exc_info=True)

    def process_snapshots(self) -> list[ProcessSnapshot]:
        """Snapshots vivos (TTL de 60 s). Levanta a exceção do Redis; snapshot ilegível é
        ignorado com log; chave vencida sai do índice."""
        keys = sorted(self._redis.smembers(METRICS_INDEX_KEY))
        if not keys:
            return []
        snapshots: list[ProcessSnapshot] = []
        expired: list[Any] = []
        for key, value in zip(keys, self._redis.mget(keys), strict=True):
            if value is None:  # o processo parou de gravar: o TTL venceu
                expired.append(key)
                continue
            parsed = _parse(key, value)
            if parsed is not None:
                snapshots.append(parsed)
        if expired:
            self._redis.srem(METRICS_INDEX_KEY, *expired)
        return snapshots


def _parse(key: Any, value: Any) -> ProcessSnapshot | None:
    name = key.decode() if isinstance(key, bytes) else str(key)
    # ohip:metrics:<processo>:<instancia>; a instância tem ':' (host:pid:aleatório).
    parts = name.split(":", 3)
    try:
        counters = json.loads(value)
        if len(parts) != 4 or not isinstance(counters, list):
            raise ValueError("formato inesperado")
        items = tuple(c for c in counters if isinstance(c, dict))
    except ValueError:
        log.warning("api_snapshot_metricas_ilegivel", key=name)
        return None
    return ProcessSnapshot(process=parts[2], instance=parts[3], counters=items)
