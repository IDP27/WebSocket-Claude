"""``ResourceFetcher``: GET do recurso completo na REST do OHIP (RF-09, ADR-0019).

Chamadas e headers conferidos nas specs oficiais (docs/OHIP_APIS.md §4, ADR-0012):
``Authorization``, ``x-app-key``, ``x-hotelid`` (obrigatório em todas) e ``x-request-id``.

- Caminho por ``moduleName`` (``ENRICHER_RESOURCE_PATHS``), com ``{hotelId}`` e
  ``{primaryKey}`` escapados.
- 200 → JSON; 204/404 → ``None``; 401 → token novo e uma nova tentativa, depois transitória;
  429 → transitória com ``Retry-After`` (pausa o fetcher inteiro); 5xx, rede e timeout →
  transitória; 403 e outros 4xx → falha da mensagem.
- Rate limit do lado do cliente (balde de fichas) e cache no Redis (``ohip:rest:*``).
- OAuth recusando as credenciais segue transitória (a assinatura pode ser corrigida no
  Developer Portal sem reiniciar), mas ``auth_rejected_alert`` recusas seguidas geram alerta
  crítico e ``ohip_rest_auth_rejected_total``; o backoff do serviço limita os pedidos de token.
- Nunca loga token, app key nem o caminho (tem o ``primaryKey``); loga o ``x-request-id``.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Protocol
from urllib.parse import quote

import httpx

from ohip_streaming.application.errors import (
    AuthRejectedError,
    AuthUnavailableError,
    ResourceRejectedError,
    ResourceUnavailableError,
)
from ohip_streaming.application.ports import AccessToken, Clock, MetricsSink
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

DEFAULT_RETRY_AFTER_S = 30.0
MAX_RETRY_AFTER_S = 600.0


class TokenSource(Protocol):
    """O que o fetcher usa do ``TokenProvider``."""

    async def get(self) -> AccessToken: ...

    async def invalidate(self) -> None: ...


class ResourceCache(Protocol):
    async def get(self, key: str) -> tuple[bool, Mapping[str, Any] | None]:
        """(achou, valor). Falha do cache = (False, None)."""
        ...

    async def put(self, key: str, value: Mapping[str, Any] | None) -> None: ...


def resource_cache_key(chain: str, module: str, hotel: str, primary_key: str) -> str:
    return f"ohip:rest:{chain}:{module}:{hotel}:{primary_key}"


class RateLimiter:
    """Balde de fichas: até ``burst`` chamadas seguidas, ``rate_per_s`` em média. Um 429
    pausa todas as chamadas até o horário do ``Retry-After``."""

    def __init__(self, *, rate_per_s: float, burst: int, clock: Clock) -> None:
        self._rate = rate_per_s
        self._burst = float(burst)
        self._clock = clock
        self._tokens = float(burst)
        self._updated: datetime | None = None
        self._paused_until: datetime | None = None
        self._lock = asyncio.Lock()

    def pause_for(self, seconds: float) -> None:
        until = self._clock.now() + timedelta(seconds=seconds)
        if self._paused_until is None or until > self._paused_until:
            self._paused_until = until

    async def acquire(self) -> None:
        async with self._lock:  # quem chega depois espera a vez (ordem de chegada)
            while True:
                now = self._clock.now()
                if self._paused_until is not None and now < self._paused_until:
                    await self._clock.sleep((self._paused_until - now).total_seconds())
                    continue
                if self._updated is not None:
                    elapsed = (now - self._updated).total_seconds()
                    self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await self._clock.sleep((1 - self._tokens) / self._rate)


class OhipResourceFetcher:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        tokens: Mapping[str, TokenSource],
        app_key: str,
        paths: Mapping[str, str],
        limiter: RateLimiter,
        cache: ResourceCache,
        clock: Clock,
        metrics: MetricsSink,
        auth_rejected_alert: int = 5,
    ) -> None:
        self._http = http
        self._tokens = tokens
        self._app_key = app_key
        self._paths = {module.strip().lower(): path for module, path in paths.items()}
        self._limiter = limiter
        self._cache = cache
        self._clock = clock
        self._metrics = metrics
        self._auth_rejected_alert = auth_rejected_alert
        self._auth_rejections = 0  # seguidas; zera quando a REST aceita um token

    async def fetch(
        self, chain_code: str, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        module = module_name.strip().lower()
        template = self._paths.get(module)
        if template is None:
            raise ResourceRejectedError(f"sem chamada REST configurada para o módulo {module_name}")
        if not hotel_id:
            # TODO(confirmar-doc): perfil compartilhado sem hotelId; x-hotelid é obrigatório
            # nas specs (D-13).
            raise ResourceRejectedError("evento sem hotelId: x-hotelid é obrigatório na REST")
        tokens = self._tokens.get(chain_code)
        if tokens is None:
            raise ResourceRejectedError(f"o enricher não tem credenciais da chain {chain_code}")

        key = resource_cache_key(chain_code, module, hotel_id, primary_key)
        hit, cached = await self._cache.get(key)
        if hit:
            return cached
        path = template.format(
            hotelId=quote(hotel_id, safe=""), primaryKey=quote(primary_key, safe="")
        )
        resource = await self._get(path, hotel_id, module, tokens)
        await self._cache.put(key, resource)
        return resource

    async def _get(
        self, path: str, hotel_id: str, module: str, tokens: TokenSource
    ) -> Mapping[str, Any] | None:
        for attempt in (1, 2):
            await self._limiter.acquire()
            token = await self._token(tokens)
            request_id = str(uuid.uuid4())
            headers = {
                "Authorization": f"Bearer {token.value}",
                "x-app-key": self._app_key,
                "x-hotelid": hotel_id,
                "x-request-id": request_id,
                "Accept": "application/json",
            }
            try:
                response = await self._http.get(path, headers=headers)
            except httpx.HTTPError as exc:
                raise ResourceUnavailableError(
                    f"REST do OHIP sem resposta: {type(exc).__name__}"
                ) from None
            status = response.status_code
            log.info("ohip_rest_chamada", module=module, status=status, request_id=request_id)
            if status != 401:
                self._auth_rejections = 0  # o token foi aceito
            if status == 200:
                return _json_object(response)
            if status in (204, 404):
                return None
            if status == 401 and attempt == 1:
                await tokens.invalidate()  # token revogado ou vencido antes da hora
                continue
            if status == 401:
                self._auth_rejected("REST do OHIP recusou o token novo (401)")
                raise ResourceUnavailableError("REST do OHIP recusou o token novo (401)")
            if status == 429:
                wait = _retry_after_s(response.headers.get("Retry-After"), self._clock.now())
                self._limiter.pause_for(wait)
                raise ResourceUnavailableError("REST do OHIP: 429", retry_after_s=wait)
            if status >= 500:
                raise ResourceUnavailableError(f"REST do OHIP respondeu {status}")
            raise ResourceRejectedError(f"REST do OHIP recusou o pedido: HTTP {status}")
        raise AssertionError("inalcançável")  # pragma: no cover

    async def _token(self, tokens: TokenSource) -> AccessToken:
        try:
            return await tokens.get()
        except AuthRejectedError as exc:
            self._auth_rejected(f"OAuth recusou as credenciais: {exc}")
            # Credenciais afetam todas as mensagens: infraestrutura, não falha da mensagem.
            raise ResourceUnavailableError(f"sem token do OHIP: {exc.code}") from None
        except AuthUnavailableError as exc:
            raise ResourceUnavailableError(f"sem token do OHIP: {exc.code}") from None

    def _auth_rejected(self, reason: str) -> None:
        self._auth_rejections += 1
        self._metrics.increment("ohip_rest_auth_rejected_total")
        if self._auth_rejections >= self._auth_rejected_alert:
            # Exige ação humana: credenciais (.env) ou assinatura no Developer Portal.
            log.critical(
                "ohip_rest_credenciais_recusadas", rejections=self._auth_rejections, reason=reason
            )


def _json_object(response: httpx.Response) -> Mapping[str, Any]:
    try:
        body = response.json()
    except ValueError:
        raise ResourceUnavailableError("REST do OHIP devolveu corpo que não é JSON") from None
    if not isinstance(body, dict):
        raise ResourceUnavailableError("REST do OHIP devolveu JSON fora do formato esperado")
    return body


def _retry_after_s(value: str | None, now: datetime) -> float:
    """``Retry-After`` em segundos ou data HTTP; ausente ou ilegível → padrão."""
    if not value:
        return DEFAULT_RETRY_AFTER_S
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = (parsedate_to_datetime(value) - now).total_seconds()
        except (TypeError, ValueError):
            return DEFAULT_RETRY_AFTER_S
    return min(MAX_RETRY_AFTER_S, max(1.0, seconds))
