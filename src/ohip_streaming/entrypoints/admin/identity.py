"""Identidade (headers do Nginx/SSO, segredo do proxy), perfil e CSRF (ADR-0018).

Os nomes com sublinhado daqui são internos ao pacote ``admin``, não ao módulo: os módulos
irmãos (e ``app.py``) os importam. Antes de renomear ou remover, procure o uso no pacote."""

from __future__ import annotations

import hmac
import re
import secrets
import time
import uuid

from flask import (
    Response,
    abort,
    g,
    request,
    session,
)

from ohip_streaming.entrypoints.admin.api_client import (
    Caller,
    PanelRole,
)
from ohip_streaming.entrypoints.admin.context import PANEL_LOGGER, _settings
from ohip_streaming.entrypoints.admin.errors import _error_page
from ohip_streaming.logging import get_logger

log = get_logger(PANEL_LOGGER)


_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


# O usuário vai à API em X-Actor (header HTTP, só ASCII) e é gravado em VARCHAR2(100).
_USER_RE = re.compile(r"^[\x21-\x7e](?:[\x20-\x7e]{0,98}[\x21-\x7e])?$")


_PUBLIC_ENDPOINTS = frozenset({"admin.healthz", "static"})


CSRF_FIELD = "csrf_token"


CSRF_HEADER = "X-CSRF-Token"


def _identify() -> Response | None:
    incoming = request.headers.get("X-Request-ID", "")
    g.request_id = incoming if _REQUEST_ID_RE.fullmatch(incoming) else uuid.uuid4().hex
    g.started = time.perf_counter()
    g.caller = None
    if request.endpoint in _PUBLIC_ENDPOINTS:
        return None
    settings = _settings()
    if settings.proxy_secret is not None:
        sent = request.headers.get(settings.proxy_secret_header, "")
        if not _same_secret(sent, settings.proxy_secret.get_secret_value()):
            log.warning("painel_sem_segredo_do_proxy", path=request.path)
            abort(403)  # não veio pelo Nginx
    user = (request.headers.get(settings.user_header) or "").strip()
    if not user:
        if request.headers.get("HX-Request") == "true":
            # Sessão do SSO vencida durante a atualização automática: recarrega a página
            # inteira (passa de novo pelo login) em vez de trocar só o bloco.
            response = Response("", status=401)
            response.headers["HX-Refresh"] = "true"
            return response
        abort(401)  # o Nginx deveria ter autenticado e definido o usuário
    if not _USER_RE.fullmatch(user):
        return _error_page(
            400,
            "Usuário não suportado",
            "O identificador do usuário enviado pelo login deve ter até 100 caracteres "
            "ASCII (sem acentos). Ajuste o header de usuário no Nginx/SSO (Q-5).",
            g.request_id,
        )
    groups = {
        group.strip()
        for group in (request.headers.get(settings.groups_header) or "").split(",")
        if group.strip()
    }
    if settings.admin_group in groups:
        role = PanelRole.ADMIN
    elif settings.read_group in groups:
        role = PanelRole.READ
    else:
        abort(403)
    g.caller = Caller(user=user, role=role, request_id=g.request_id)
    if request.method == "POST":
        if role is not PanelRole.ADMIN:  # defesa em profundidade: a API confere de novo
            abort(403)
        _check_csrf()
    return None


def _csrf_token() -> str:
    token = session.get("csrf")
    if not isinstance(token, str):
        token = secrets.token_urlsafe(32)
        session["csrf"] = token
    return token


def _same_secret(sent: str, expected: str) -> bool:
    """Comparação em tempo constante. Em bytes: com ``str``, ``compare_digest`` levanta
    ``TypeError`` se houver caractere não ASCII (o Werkzeug decodifica headers em latin-1),
    e um valor forjado viraria 500 em vez de recusa."""
    return hmac.compare_digest(sent.encode("utf-8"), expected.encode("utf-8"))


def _check_csrf() -> None:
    expected = session.get("csrf")
    sent = request.form.get(CSRF_FIELD) or request.headers.get(CSRF_HEADER) or ""
    if not isinstance(expected, str) or not _same_secret(sent, expected):
        log.warning("painel_csrf_recusado", path=request.path)
        abort(400)


def _caller() -> Caller:
    caller: Caller = g.caller
    return caller


def _require_admin() -> Caller:
    caller = _caller()
    if caller.role is not PanelRole.ADMIN:
        abort(403)
    return caller
