"""``TokenIssuer``: ``POST {gateway}/oauth/v1/tokens`` (spec oficial ``publishedoauth.json``).

- ``Authorization: Basic base64(ClientID:ClientSecret)``, ``x-app-key``, ``X-Request-Id`` (GUID);
  ``enterpriseId`` só com ``client_credentials`` (ambientes OCIM).
- Corpo ``application/x-www-form-urlencoded``: ``grant_type=client_credentials`` + ``scope``, ou
  ``grant_type=password`` + usuário de integração (resource owner).
- Validade: ``expires_in`` da resposta; se o JWT trouxer ``exp`` anterior, vale o menor.
- 400/401/403 → ``AuthRejectedError`` (não repetir em seguida); rede, 429 e 5xx →
  ``AuthUnavailableError``. Nenhum segredo vai para log ou mensagem de erro.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from ohip_streaming.application.errors import AuthRejectedError, AuthUnavailableError
from ohip_streaming.application.ports import AccessToken, Clock
from ohip_streaming.logging import get_logger

if TYPE_CHECKING:
    from ohip_streaming.config import OhipSettings

log = get_logger(__name__)

DEFAULT_EXPIRES_IN_S = 3600  # "typically 3600" (spec); usado se a resposta não trouxer


@dataclass(frozen=True, slots=True)
class OAuthCredentials:
    token_url: str  # gateway + caminho (OHIP_OAUTH_TOKEN_PATH)
    client_id: str
    client_secret: str = field(repr=False)
    app_key: str = field(repr=False)
    grant_type: str  # "client_credentials" | "password"
    scope: str | None = None
    enterprise_id: str | None = None
    username: str | None = None
    password: str | None = field(default=None, repr=False)


def credentials_from_settings(settings: OhipSettings) -> OAuthCredentials:
    """``OHIP_AUTH_MODE``: ``client_credentials`` ou ``resource_owner`` (grant ``password``)."""
    client_credentials = settings.auth_mode.value == "client_credentials"
    password = settings.integration_password
    return OAuthCredentials(
        token_url=settings.gateway_url.rstrip("/") + settings.oauth_token_path,
        client_id=settings.client_id,
        client_secret=settings.client_secret.get_secret_value(),
        app_key=settings.app_key.get_secret_value(),
        grant_type="client_credentials" if client_credentials else "password",
        scope=settings.oauth_scope if client_credentials else None,
        enterprise_id=settings.enterprise_id if client_credentials else None,
        username=None if client_credentials else settings.integration_username,
        password=None if client_credentials or password is None else password.get_secret_value(),
    )


class OhipTokenIssuer:
    def __init__(self, client: httpx.AsyncClient, credentials: OAuthCredentials, clock: Clock):
        self._client = client
        self._credentials = credentials
        self._clock = clock

    async def issue(self) -> AccessToken:
        c = self._credentials
        request_id = str(uuid.uuid4())
        headers = {
            "x-app-key": c.app_key,
            "X-Request-Id": request_id,
            "Accept": "application/json",
        }
        form = {"grant_type": c.grant_type}
        if c.grant_type == "client_credentials":
            if c.scope:
                form["scope"] = c.scope
            if c.enterprise_id:
                headers["enterpriseId"] = c.enterprise_id
        else:
            form["username"] = c.username or ""
            form["password"] = c.password or ""

        try:
            response = await self._client.post(
                c.token_url, data=form, headers=headers, auth=(c.client_id, c.client_secret)
            )
        except httpx.HTTPError as exc:
            raise AuthUnavailableError(f"OAuth sem resposta: {type(exc).__name__}") from None

        status = response.status_code
        if status in (400, 401, 403):
            log.error("oauth_recusado", status=status, request_id=request_id)
            raise AuthRejectedError(f"OAuth recusou as credenciais (HTTP {status})")
        if status != 200:
            log.warning("oauth_indisponivel", status=status, request_id=request_id)
            raise AuthUnavailableError(f"OAuth respondeu HTTP {status}")
        return self._parse(response)

    def _parse(self, response: httpx.Response) -> AccessToken:
        try:
            body: dict[str, Any] = response.json()
            value = body["access_token"]
        except (ValueError, KeyError, TypeError):
            raise AuthUnavailableError("resposta do OAuth sem access_token") from None
        if not isinstance(value, str) or not value:
            raise AuthUnavailableError("resposta do OAuth sem access_token")
        now = self._clock.now()
        expires_in = body.get("expires_in")
        seconds = expires_in if isinstance(expires_in, int) and expires_in > 0 else None
        expires_at = now + timedelta(seconds=seconds or DEFAULT_EXPIRES_IN_S)
        jwt_exp = jwt_expiry(value)
        if jwt_exp is not None and jwt_exp < expires_at:
            expires_at = jwt_exp
        return AccessToken(value=value, expires_at=expires_at)


def jwt_expiry(token: str) -> datetime | None:
    """``exp`` do payload do JWT, sem validar assinatura (só para agendar a renovação)."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
        exp = claims.get("exp") if isinstance(claims, dict) else None
    except (ValueError, binascii.Error):
        return None
    if not isinstance(exp, int | float) or isinstance(exp, bool):
        return None
    try:
        return datetime.fromtimestamp(exp, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None
