"""Cabeçalhos de segurança, log por requisição e páginas de erro.

Os nomes com sublinhado daqui são internos ao pacote ``admin``, não ao módulo: os módulos
irmãos (e ``app.py``) os importam. Antes de renomear ou remover, procure o uso no pacote."""

from __future__ import annotations

import time

from flask import (
    Response,
    g,
    render_template,
    request,
)
from werkzeug.exceptions import HTTPException

from ohip_streaming.entrypoints.admin.api_client import (
    ApiError,
    ApiUnavailableError,
    Caller,
)
from ohip_streaming.entrypoints.admin.context import PANEL_LOGGER
from ohip_streaming.logging import get_logger

log = get_logger(PANEL_LOGGER)


def _finish(response: Response) -> Response:
    headers = response.headers
    headers["X-Request-ID"] = g.get("request_id", "")
    headers["Content-Security-Policy"] = (
        "default-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'"
    )
    headers["X-Frame-Options"] = "DENY"
    headers["X-Content-Type-Options"] = "nosniff"
    headers["Referrer-Policy"] = "no-referrer"
    if request.endpoint != "static":
        headers["Cache-Control"] = "no-store"
    caller: Caller | None = g.get("caller")
    log.info(
        "painel_requisicao",
        method=request.method,
        path=request.path,  # sem query string
        status=response.status_code,
        duration_ms=round((time.perf_counter() - g.get("started", time.perf_counter())) * 1000, 1),
        user=caller.user if caller else None,
        role=caller.role.value if caller else None,
        request_id=g.get("request_id"),
    )
    return response


_HTTP_MESSAGES = {
    400: "Requisição inválida (formulário expirado? recarregue a página).",
    401: "Usuário não identificado: o acesso deve passar pelo Nginx/SSO.",
    403: "Seu grupo não tem acesso a esta ação.",
    404: "Página não encontrada.",
    405: "Método não permitido.",
    413: "Formulário grande demais.",
}


def _error_page(status: int, title: str, message: str, request_id: str | None) -> Response:
    body = render_template(
        "error.html", status=status, title=title, message=message, request_id=request_id
    )
    return Response(body, status=status)


def _http_error_page(exc: HTTPException) -> Response:
    status = exc.code or 500
    return _error_page(
        status, f"Erro {status}", _HTTP_MESSAGES.get(status, "Erro."), g.get("request_id")
    )


def _api_status(exc: ApiError) -> tuple[int, str]:
    if isinstance(exc, ApiUnavailableError) or exc.status >= 500:
        return 503, "A API de controle não respondeu. Tente de novo em instantes."
    if exc.status in (401, 403):
        # O painel usa os tokens certos por perfil; recusa aqui é configuração.
        return 502, "A API recusou o token do painel (confira ADMIN_API_*_TOKEN)."
    return exc.status, exc.message


def _api_error_page(exc: ApiError) -> Response:
    status, message = _api_status(exc)
    log.warning("painel_erro_api", status=exc.status, code=exc.code, api_request_id=exc.request_id)
    return _error_page(status, f"Erro {exc.code}", message, exc.request_id or g.get("request_id"))


def _unexpected_error_page(exc: Exception) -> Response:
    log.exception("painel_erro_inesperado", path=request.path)
    return _error_page(500, "Erro interno", "Erro inesperado no painel.", g.get("request_id"))
