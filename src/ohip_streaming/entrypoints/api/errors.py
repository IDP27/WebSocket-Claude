"""Formato de erro, mapeamento das exceções e middleware de requisição (API.md, ADR-0017).

Erro: ``{"error": {"code", "message", "request_id"}}``. Erro inesperado vira 500 com
mensagem genérica; o detalhe fica só no log.
"""

from __future__ import annotations

import re
import time
import uuid
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ohip_streaming.application.errors import (
    ApplicationError,
    InvalidOperationError,
    NotFoundError,
    ReplayAlreadyPendingError,
    ReplayNotPendingError,
    StoreUnavailableError,
)
from ohip_streaming.domain.errors import (
    DomainError,
    ReplayConfirmationError,
    ReplayError,
    ReplayForwardNotAllowedError,
    ReplayOffsetInvalidError,
    ReplayReasonRequiredError,
)
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_HTTP_CODES = {401: "UNAUTHENTICATED", 403: "FORBIDDEN", 404: "NOT_FOUND"}
_DOMAIN_MESSAGES: dict[type[DomainError], str] = {
    ReplayOffsetInvalidError: "from_offset deve casar com ^[0-9]+$ e ter até 20 caracteres",
    ReplayConfirmationError: "confirm deve repetir o chain_code",
    ReplayReasonRequiredError: "motivo obrigatório, com até 500 caracteres",
    ReplayForwardNotAllowedError: (
        "from_offset não pode ser maior que o último offset confirmado da chain"
    ),
}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def request_id_of(scope_or_request: Scope | Request) -> str:
    scope = scope_or_request.scope if isinstance(scope_or_request, Request) else scope_or_request
    state = scope.get("state") or {}
    return str(state.get("request_id", ""))


def error_response(request: Request, status: int, code: str, message: str) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "request_id": request_id_of(request)}}
    return JSONResponse(body, status_code=status)


def _application_error(exc: ApplicationError) -> tuple[int, str, str]:
    if isinstance(exc, NotFoundError):
        return 404, "NOT_FOUND", str(exc)
    if isinstance(exc, ReplayAlreadyPendingError):
        return 409, exc.code, f"já há replay pendente na chain {exc}"
    if isinstance(exc, ReplayNotPendingError):
        return 409, exc.code, str(exc)
    if isinstance(exc, InvalidOperationError):
        return 409, exc.code, str(exc)
    if isinstance(exc, StoreUnavailableError):
        return 503, exc.code, "banco de dados indisponível"
    return 500, "INTERNAL_ERROR", "erro interno"


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> JSONResponse:
        return error_response(request, exc.status, exc.code, exc.message)

    @app.exception_handler(ApplicationError)
    async def _app_error(request: Request, exc: ApplicationError) -> JSONResponse:
        status, code, message = _application_error(exc)
        if status >= 500:
            log.error("api_erro_aplicacao", error_code=exc.code, error=str(exc))
        return error_response(request, status, code, message)

    @app.exception_handler(DomainError)
    async def _domain_error(request: Request, exc: DomainError) -> JSONResponse:
        # Mensagem fixa por tipo: a do domínio pode repetir o valor recebido (API.md).
        message = _DOMAIN_MESSAGES.get(type(exc), "parâmetro com formato inválido")
        if isinstance(exc, ReplayForwardNotAllowedError):
            return error_response(request, 422, exc.code, message)
        # Só os códigos de replay estão no contrato (API.md); o resto é parâmetro inválido.
        code = exc.code if isinstance(exc, ReplayError) else "VALIDATION_ERROR"
        return error_response(request, 400, code, message)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Sem ecoar o valor recebido (pode ser dado pessoal ou token).
        problems = "; ".join(
            f"{'.'.join(str(p) for p in error.get('loc', ()))}: {error.get('msg', '')}"
            for error in exc.errors()
        )
        return error_response(request, 400, "VALIDATION_ERROR", problems or "requisição inválida")

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _HTTP_CODES.get(exc.status_code, f"HTTP_{exc.status_code}")
        return error_response(request, exc.status_code, code, str(exc.detail))


class RequestContextMiddleware:
    """``request_id`` em todo log e no header da resposta, uma linha de log por requisição
    (sem query string) e 500 no formato padrão para erro inesperado.

    O access log do Uvicorn fica desligado: ele grava a query string (ex.: ``primary_key``).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = _incoming_request_id(scope) or uuid.uuid4().hex
        state: dict[str, Any] = scope.setdefault("state", {})
        state["request_id"] = request_id
        started = time.perf_counter()
        status = 500
        response_started = False

        async def send_with_id(message: Message) -> None:
            nonlocal status, response_started
            if message["type"] == "http.response.start":
                response_started = True
                status = int(message["status"])
                headers = list(message.get("headers", []))
                headers.append((REQUEST_ID_HEADER.lower().encode(), request_id.encode()))
                message = {**message, "headers": headers}
            await send(message)

        with structlog.contextvars.bound_contextvars(request_id=request_id):
            try:
                await self.app(scope, receive, send_with_id)
            except Exception:
                log.exception("api_erro_inesperado", path=scope.get("path"))
                if response_started:
                    raise
                body = {
                    "error": {
                        "code": "INTERNAL_ERROR",
                        "message": "erro interno",
                        "request_id": request_id,
                    }
                }
                await JSONResponse(body, status_code=500)(scope, receive, send_with_id)
            finally:
                log.info(
                    "api_requisicao",
                    method=scope.get("method"),
                    path=scope.get("path"),
                    status=status,
                    duration_ms=round((time.perf_counter() - started) * 1000, 1),
                    role=state.get("role"),
                    actor=state.get("actor"),
                )


def _incoming_request_id(scope: Scope) -> str | None:
    for name, value in scope.get("headers", []):
        if name == b"x-request-id":
            text = value.decode("latin-1")
            return text if _REQUEST_ID_RE.fullmatch(text) else None
    return None
