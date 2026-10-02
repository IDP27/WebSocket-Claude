"""Token OAuth do OHIP com cache e renovação antecipada (OHIP_APIS.md §4, DV-2).

- O token é por (ambiente, chain) e fica em memória e no Redis (``ohip:token:<amb>:<chain>``),
  compartilhado pelos processos: pedir token é cobrado para parceiros.
- Renova ``margin_s`` antes do ``exp`` (padrão 300 s; a Oracle pede ao menos 120 s).
- Uma só emissão por vez (chamadas concorrentes esperam a mesma).
- Redis fora não impede a emissão: só perde o compartilhamento.
- ``invalidate()`` depois de um 4401 força um token novo na próxima chamada.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace

from ohip_streaming.application.ports import (
    AccessToken,
    Clock,
    MetricsSink,
    TokenCache,
    TokenIssuer,
)
from ohip_streaming.domain.connection import token_refresh_due
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


def token_cache_key(environment: str, chain_code: str) -> str:
    return f"ohip:token:{environment}:{chain_code}"


class TokenProvider:
    def __init__(
        self,
        *,
        issuer: TokenIssuer,
        cache: TokenCache,
        clock: Clock,
        metrics: MetricsSink,
        cache_key: str,
        margin_s: float,
    ) -> None:
        self._issuer = issuer
        self._cache = cache
        self._clock = clock
        self._metrics = metrics
        self._key = cache_key
        self._margin_s = margin_s
        self._token: AccessToken | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> AccessToken:
        if token := self._usable(self._token):
            return token
        async with self._lock:  # quem chegou junto reaproveita a emissão de quem entrou antes
            if token := self._usable(self._token):
                return token
            token = self._usable(await self._from_cache()) or await self._issue()
            self._token = token
            return token

    def refresh_due(self, token: AccessToken) -> bool:
        """O token entrou na margem de renovação (a mesma regra do ``get``)."""
        return self._usable(token) is None

    async def invalidate(self) -> None:
        """O servidor recusou o token (4401): descarta em memória e no cache.

        Sob o mesmo lock do ``get``: uma busca em curso não devolve o token recusado depois.
        """
        async with self._lock:
            self._token = None
            try:
                await self._cache.delete(self._key)
            except Exception:  # Redis é atalho
                log.warning("token_cache_indisponivel", exc_info=True)

    def _usable(self, token: AccessToken | None) -> AccessToken | None:
        if token is None or token_refresh_due(
            now=self._clock.now(), expires_at=token.expires_at, margin_s=self._margin_for(token)
        ):
            return None
        return token

    def _margin_for(self, token: AccessToken) -> float:
        """Margem efetiva: no máximo metade da vida do token (evita emitir a cada chamada)."""
        if token.lifetime_s is None:
            return self._margin_s
        return min(self._margin_s, token.lifetime_s / 2)

    async def _from_cache(self) -> AccessToken | None:
        try:
            return await self._cache.get(self._key)
        except Exception:
            log.warning("token_cache_indisponivel", exc_info=True)
            return None

    async def _issue(self) -> AccessToken:
        token = await self._issuer.issue()  # AuthRejected/AuthUnavailable sobem para quem chamou
        self._metrics.increment("ohip_token_issued_total")
        lifetime = (token.expires_at - self._clock.now()).total_seconds()
        token = replace(token, lifetime_s=lifetime)
        if lifetime <= self._margin_s:
            self._metrics.increment("ohip_token_short_lived_total")
            log.error("token_vida_menor_que_margem", lifetime_s=lifetime, margin_s=self._margin_s)
        ttl = lifetime - self._margin_for(token)
        if ttl > 0:
            try:
                await self._cache.put(self._key, token, ttl)
            except Exception:
                log.warning("token_cache_indisponivel", exc_info=True)
        log.info("token_emitido", expires_at=token.expires_at.isoformat())
        return token
