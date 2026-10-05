"""Cliente síncrono da ``ohip-api`` para o painel (ADR-0004, ADR-0018).

- Escolhe o token de serviço pelo perfil do usuário e repassa o usuário em ``X-Actor``.
- Repassa o ``X-Request-ID`` da requisição do painel, para correlacionar os logs.
- Erro da API → ``ApiError`` com ``status``, ``code``, ``message`` e ``request_id`` do corpo
  padrão. API fora ou lenta → ``ApiUnavailableError``. Nunca guarda nem loga o token.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from urllib.parse import quote

import httpx

from ohip_streaming.logging import get_logger

log = get_logger(__name__)


class PanelRole(StrEnum):
    READ = "read"
    ADMIN = "admin"


@dataclass(frozen=True, slots=True)
class Caller:
    """Quem está usando o painel (vindo dos headers do Nginx)."""

    user: str
    role: PanelRole
    request_id: str


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, request_id: str | None) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.request_id = request_id


class ApiUnavailableError(ApiError):
    """Sem resposta da API (fora, lenta ou resposta que não é do contrato)."""

    def __init__(self, message: str, request_id: str | None = None) -> None:
        super().__init__(503, "API_UNAVAILABLE", message, request_id)


@dataclass(frozen=True)
class Download:
    content: bytes
    filename: str


class ApiClient:
    def __init__(self, http: httpx.Client, *, tokens: Mapping[PanelRole, str]) -> None:
        self._http = http
        self._tokens = dict(tokens)

    # ------------------------------------------------------------------- leitura

    def status(self, caller: Caller) -> dict[str, Any]:
        return self._json("GET", "/api/v1/status", caller)

    def events(self, caller: Caller, params: Mapping[str, str]) -> dict[str, Any]:
        return self._json("GET", "/api/v1/events", caller, params=params)

    def event(self, caller: Caller, unique_event_id: str) -> dict[str, Any]:
        return self._json("GET", f"/api/v1/events/{_segment(unique_event_id)}", caller)

    def outbox(self, caller: Caller, params: Mapping[str, str]) -> dict[str, Any]:
        return self._json("GET", "/api/v1/outbox", caller, params=params)

    def dlq(self, caller: Caller, params: Mapping[str, str]) -> dict[str, Any]:
        return self._json("GET", "/api/v1/dlq", caller, params=params)

    def replays(self, caller: Caller, params: Mapping[str, str]) -> dict[str, Any]:
        return self._json("GET", "/api/v1/replay", caller, params=params)

    # ------------------------------------------------------------------- ações (admin)

    def export(self, caller: Caller, unique_event_id: str) -> Download:
        response = self._send("GET", f"/api/v1/events/{_segment(unique_event_id)}/export", caller)
        disposition = response.headers.get("Content-Disposition", "")
        filename = "evento.json"
        if 'filename="' in disposition:
            filename = disposition.split('filename="', 1)[1].split('"', 1)[0] or filename
        return Download(response.content, filename)

    def reprocess(self, caller: Caller, unique_event_id: str) -> dict[str, Any]:
        return self._json("POST", f"/api/v1/events/{_segment(unique_event_id)}/reprocess", caller)

    def request_replay(
        self, caller: Caller, *, chain_code: str, from_offset: str, reason: str, confirm: str
    ) -> dict[str, Any]:
        body = {
            "chain_code": chain_code,
            "from_offset": from_offset,
            "reason": reason,
            "confirm": confirm,
        }
        return self._json("POST", "/api/v1/replay", caller, json=body)

    def cancel_replay(self, caller: Caller, request_id: int) -> dict[str, Any]:
        return self._json("DELETE", f"/api/v1/replay/{int(request_id)}", caller)

    def retry_dlq(self, caller: Caller, item_id: int) -> dict[str, Any]:
        return self._json("POST", f"/api/v1/dlq/{int(item_id)}/retry", caller)

    # ------------------------------------------------------------------- apoio

    def _json(self, method: str, path: str, caller: Caller, **kwargs: Any) -> dict[str, Any]:
        response = self._send(method, path, caller, **kwargs)
        try:
            body = response.json()
        except ValueError:
            raise ApiUnavailableError("resposta da API não é JSON", caller.request_id) from None
        if not isinstance(body, dict):
            raise ApiUnavailableError("resposta da API fora do contrato", caller.request_id)
        return body

    def _send(self, method: str, path: str, caller: Caller, **kwargs: Any) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {self._tokens[caller.role]}",
            "X-Request-ID": caller.request_id,
        }
        if caller.role is PanelRole.ADMIN:
            headers["X-Actor"] = caller.user
        try:
            response = self._http.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            log.warning("painel_api_indisponivel", path=path, error=type(exc).__name__)
            raise ApiUnavailableError(
                f"sem resposta da API ({type(exc).__name__})", caller.request_id
            ) from None
        if response.is_success:
            return response
        raise _error(response, caller.request_id)


def _segment(value: str) -> str:
    """Um segmento de caminho seguro (o uid vem da URL do painel): ``/``, ``?`` e ``#``
    não mudam a rota chamada na API."""
    return quote(value, safe="")


def _error(response: httpx.Response, fallback_request_id: str) -> ApiError:
    try:
        detail = response.json()["error"]
        code, message = str(detail["code"]), str(detail["message"])
        request_id = str(detail.get("request_id") or fallback_request_id)
    except (ValueError, KeyError, TypeError):
        if response.status_code >= 500:
            return ApiUnavailableError(f"API respondeu {response.status_code}", fallback_request_id)
        return ApiError(response.status_code, f"HTTP_{response.status_code}", "erro da API", None)
    return ApiError(response.status_code, code, message, request_id)
