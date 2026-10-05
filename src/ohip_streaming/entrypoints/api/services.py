"""Serviços que as rotas usam, montados uma vez por worker (ADR-0017).

Em produção, ``app.compose`` monta com Oracle, Redis e RabbitMQ; os testes montam com os
fakes em memória.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from ohip_streaming.adapters.redis.api_cache import ProcessSnapshot
from ohip_streaming.application.ports import StatusSnapshot
from ohip_streaming.application.use_cases.monitoring import Monitoring
from ohip_streaming.application.use_cases.operations import ReprocessEvent, RetryDlqItem
from ohip_streaming.application.use_cases.replay import CancelReplay, RequestReplay
from ohip_streaming.entrypoints.api import metrics
from ohip_streaming.entrypoints.api.runtime import run_sync
from ohip_streaming.entrypoints.api.schemas import DependencyCheck, Ready, status_out
from ohip_streaming.entrypoints.api.security import Role
from ohip_streaming.logging import get_logger

log = get_logger(__name__)
T = TypeVar("T")


class StatusCache(Protocol):
    def get_status(self) -> bytes | None: ...

    def put_status(self, payload: bytes) -> None: ...


class TtlCache(Generic[T]):
    """Valor calculado no máximo uma vez a cada ``ttl_s`` por worker (thread-safe)."""

    def __init__(
        self,
        compute: Callable[[], T],
        ttl_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._compute = compute
        self._ttl_s = ttl_s
        self._clock = clock
        self._lock = threading.Lock()
        self._value: T | None = None
        self._expires = 0.0

    def get(self) -> T:
        with self._lock:
            now = self._clock()
            if self._value is None or now >= self._expires:
                self._value = self._compute()
                self._expires = now + self._ttl_s
            return self._value


class Readiness:
    """Confere as dependências. Cada verificação levanta em caso de falha; o erro mostra só o
    tipo da exceção (a mensagem pode ter URL com senha)."""

    def __init__(self, checks: Mapping[str, Callable[[], object]]) -> None:
        self._checks = checks

    def check(self) -> Ready:
        results: dict[str, DependencyCheck] = {}
        for name, probe in self._checks.items():
            started = time.perf_counter()
            error: str | None = None
            try:
                probe()
            except Exception as exc:  # noqa: BLE001 - qualquer falha = dependência fora
                error = type(exc).__name__
                log.warning("api_dependencia_indisponivel", dependency=name, error=error)
            results[name] = DependencyCheck(
                ok=error is None,
                latency_ms=round((time.perf_counter() - started) * 1000, 1),
                error=error,
            )
        ok = all(r.ok for r in results.values())
        return Ready(status="ok" if ok else "fail", checks=results)


class StatusService:
    """``GET /api/v1/status`` (ARCHITECTURE §7, ADR-0017).

    - Cache compartilhado entre workers no Redis (``ohip:api:status``) e uma cópia local, as
      duas com o mesmo prazo; a cópia local cobre o Redis fora.
    - Um único cálculo por worker de cada vez (*single-flight*): quem chega enquanto o status
      está sendo calculado espera e reaproveita o resultado, em vez de repetir as consultas.
    """

    def __init__(
        self,
        *,
        cache: StatusCache,
        compute: Callable[[], bytes],
        ttl_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._cache = cache
        self._compute = compute
        self._ttl_s = ttl_s
        self._clock = clock
        self._lock = threading.Lock()
        self._local: tuple[float, bytes] | None = None  # (vence em, corpo)

    def payload(self) -> bytes:
        cached = self._fresh()
        if cached is not None:
            return cached
        with self._lock:
            cached = self._fresh()  # outro thread pode ter calculado enquanto esperávamos
            if cached is not None:
                return cached
            body = self._compute()
            self._cache.put_status(body)
            self._local = (self._clock() + self._ttl_s, body)
            return body

    def _fresh(self) -> bytes | None:
        local = self._local
        if local is not None and self._clock() < local[0]:
            return local[1]
        return self._cache.get_status()


def status_payload(monitoring: Monitoring) -> Callable[[], bytes]:
    def compute() -> bytes:
        return status_out(run_sync(monitoring.status())).model_dump_json().encode()

    return compute


class MetricsService:
    def __init__(
        self, *, monitoring: Monitoring, snapshots: Callable[[], list[ProcessSnapshot]]
    ) -> None:
        self._monitoring = monitoring
        self._snapshots = snapshots

    def render(self) -> str:
        status: StatusSnapshot | None
        try:
            status = run_sync(self._monitoring.status())
        except Exception:
            log.warning("metricas_oracle_indisponivel", exc_info=True)
            status = None
        processes: list[ProcessSnapshot] | None
        try:
            processes = self._snapshots()
        except Exception:
            log.warning("metricas_redis_indisponivel", exc_info=True)
            processes = None
        return metrics.render(status, processes)


@dataclass(frozen=True)
class ApiServices:
    tokens: Mapping[str, Role]  # SHA-256 do token → perfil
    monitoring: Monitoring
    request_replay: RequestReplay
    cancel_replay: CancelReplay
    reprocess: ReprocessEvent
    retry_dlq: RetryDlqItem
    status: StatusService
    ready: TtlCache[Ready]
    metrics: TtlCache[str]
