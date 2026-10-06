"""Token OAuth do OHIP (ADR-0014)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AccessToken:
    value: str = field(repr=False)  # nunca em repr/log
    expires_at: datetime  # UTC
    # Vida total na emissão; viaja com o token pelo cache para todo processo calcular a
    # mesma margem efetiva (ADR-0014). None = desconhecida (margem cheia).
    lifetime_s: float | None = None


class TokenIssuer(Protocol):
    async def issue(self) -> AccessToken:
        """POST /oauth/v1/tokens. Levanta ``AuthRejectedError`` ou ``AuthUnavailableError``."""
        ...


class TokenCache(Protocol):
    """Cache compartilhado entre processos (Redis). Falhas aqui nunca impedem a emissão."""

    async def get(self, key: str) -> AccessToken | None: ...

    async def put(self, key: str, token: AccessToken, ttl_s: float) -> None: ...

    async def delete(self, key: str) -> None: ...
