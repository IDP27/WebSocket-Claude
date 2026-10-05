"""Autenticação por token de serviço e auditoria do ator (API.md, ADR-0004, ADR-0017).

- ``Authorization: Bearer <token>``: o SHA-256 do token é procurado em ``API_SERVICE_TOKENS``
  (hash → perfil). O token em claro nunca é guardado nem logado.
- Ações ``admin`` exigem ``X-Actor`` (usuário final repassado pelo painel); vai para
  ``requested_by``/``resolved_by``/``cancelled_by``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from ohip_streaming.entrypoints.api.errors import ApiError

ACTOR_MAX_BYTES = 100  # requested_by, resolved_by, cancelled_by: VARCHAR2(100)


class Role(StrEnum):
    READ = "read"
    ADMIN = "admin"


@dataclass(frozen=True, slots=True)
class Principal:
    role: Role
    actor: str | None


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def authenticate(token: str | None, tokens: Mapping[str, Role]) -> Role:
    """Perfil do token (``Authorization: Bearer``). Ausente ou desconhecido → 401."""
    if not token or not token.strip():
        raise ApiError(401, "UNAUTHENTICATED", "token de serviço ausente")
    role = tokens.get(token_hash(token.strip()))
    if role is None:
        raise ApiError(401, "UNAUTHENTICATED", "token de serviço inválido")
    return role


def validate_actor(raw: str | None) -> str | None:
    """``X-Actor`` aparado; None se ausente. Inválido → 400."""
    if raw is None or not raw.strip():
        return None
    actor = raw.strip()
    if len(actor.encode("utf-8")) > ACTOR_MAX_BYTES or not actor.isprintable():
        raise ApiError(
            400,
            "VALIDATION_ERROR",
            f"X-Actor deve ter até {ACTOR_MAX_BYTES} bytes, sem caracteres de controle",
        )
    return actor


def require_admin(principal: Principal) -> str:
    """Devolve o ator de uma ação ``admin``."""
    if principal.role is not Role.ADMIN:
        raise ApiError(403, "FORBIDDEN", "ação exige o perfil admin")
    if principal.actor is None:
        raise ApiError(403, "FORBIDDEN", "ação admin exige o header X-Actor")
    return principal.actor
