"""Painel operacional ``ohip-admin`` (docs/UI.md, ADR-0004, ADR-0018).

- Usuário e grupos vêm de headers definidos pelo Nginx/SSO (Q-5); o perfil escolhe o token de
  serviço da API, e o usuário vai em ``X-Actor``.
- Todo POST é ação ``admin``: perfil e CSRF conferidos no servidor antes de chamar a API.
- Sem acesso ao Oracle nem ao Redis: tudo passa pela API (import-linter).
"""

from __future__ import annotations

import hmac
import re
import secrets
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, TypeVar

import httpx
from flask import (
    Blueprint,
    Flask,
    Response,
    abort,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.exceptions import HTTPException
from werkzeug.wrappers.response import Response as WerkzeugResponse

from ohip_streaming.entrypoints.admin.api_client import (
    ApiClient,
    ApiError,
    ApiUnavailableError,
    Caller,
    PanelRole,
)
from ohip_streaming.entrypoints.admin.settings import (
    AdminSettings,
    PanelAppSettings,
    PanelEnvironment,
    PanelLogSettings,
    load_panel_settings,
)
from ohip_streaming.logging import configure_logging, get_logger

log = get_logger(__name__)
T = TypeVar("T")

_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
# O usuário vai à API em X-Actor (header HTTP, só ASCII) e é gravado em VARCHAR2(100).
_USER_RE = re.compile(r"^[\x21-\x7e](?:[\x20-\x7e]{0,98}[\x21-\x7e])?$")
_PUBLIC_ENDPOINTS = frozenset({"admin.healthz", "static"})
# htmx 2.0.11 servido pelo painel; hash publicado em htmx.org (static/HTMX_PROVENANCE.md).
HTMX_INTEGRITY = "sha384-2OatzQy1H+Zd/IIrjr1TcuDGqLXeHhbooAyJY1KdQMKnr4LZ22k31GBLdYKHmVjg"
CSRF_FIELD = "csrf_token"
CSRF_HEADER = "X-CSRF-Token"

# Filtros repassados à API (o resto da query string é ignorado).
EVENT_FILTERS = (
    "chain_code",
    "hotel_id",
    "event_name",
    "module_name",
    "primary_key",
    "processing_status",
    "from",
    "to",
    "cursor",
)
DLQ_FILTERS = ("stage", "chain_code", "resolved", "cursor")
REPLAY_FILTERS = ("chain_code", "status", "cursor")
# Filtros que o link de nova tentativa de cada fragmento pode levar.
_FRAGMENT_FILTERS: dict[str, tuple[str, ...]] = {
    "admin.fragment_overview": (),
    "admin.fragment_dlq": DLQ_FILTERS,
    "admin.fragment_replays": REPLAY_FILTERS,
}
STATE_LABELS = {
    "SUBSCRIBED": ("CONECTADO", "ok"),
    "WAITING": ("AGUARDANDO", "warn"),
    "STOPPED": ("PARADO", "bad"),
    "DRAINING": ("ENCERRANDO", "warn"),
    "ACQUIRING": ("SEM LEASE", "warn"),
    "CONNECTING": ("CONECTANDO", "warn"),
    "INIT_SENT": ("CONECTANDO", "warn"),
    "STATUS_CHECK": ("CONECTANDO", "warn"),
}

admin = Blueprint("admin", __name__, url_prefix="/admin")


# ======================================================================= montagem


def build_app(
    *,
    settings: AdminSettings,
    app_settings: PanelAppSettings,
    api: ApiClient,
) -> Flask:
    app = Flask(__name__, static_url_path="/admin/static")
    app.config.update(
        SECRET_KEY=settings.secret_key.get_secret_value(),
        SESSION_COOKIE_NAME="ohip_admin",
        SESSION_COOKIE_PATH="/admin",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Strict",
        SESSION_COOKIE_SECURE=app_settings.environment is not PanelEnvironment.DESENVOLVIMENTO,
        MAX_CONTENT_LENGTH=64 * 1024,  # só formulários pequenos
    )
    app.extensions["ohip_admin"] = {"settings": settings, "api": api, "app": app_settings}
    app.register_blueprint(admin)
    app.jinja_env.filters["when"] = _when
    app.jinja_env.filters["ago"] = _ago
    app.jinja_env.globals["state_label"] = _state_label

    app.before_request(_identify)
    app.after_request(_finish)
    app.register_error_handler(ApiError, _api_error_page)
    app.register_error_handler(HTTPException, _http_error_page)
    app.register_error_handler(Exception, _unexpected_error_page)
    app.context_processor(_template_globals)
    return app


def create_app() -> Flask:
    """Fábrica do Gunicorn (uma por worker)."""
    settings = load_panel_settings(AdminSettings)
    app_settings = load_panel_settings(PanelAppSettings)
    logs = load_panel_settings(PanelLogSettings)
    configure_logging(
        service="ohip-admin",
        environment=app_settings.environment.value,
        code_version=app_settings.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )
    http = httpx.Client(base_url=settings.api_base_url, timeout=settings.api_timeout_s)
    api = ApiClient(
        http,
        tokens={
            PanelRole.READ: settings.api_read_token.get_secret_value(),
            PanelRole.ADMIN: settings.api_admin_token.get_secret_value(),
        },
    )
    return build_app(settings=settings, app_settings=app_settings, api=api)


def _ext(key: str) -> Any:
    from flask import current_app

    return current_app.extensions["ohip_admin"][key]


def _api() -> ApiClient:
    client: ApiClient = _ext("api")
    return client


def _settings() -> AdminSettings:
    settings: AdminSettings = _ext("settings")
    return settings


# ======================================================================= identidade e CSRF


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


# ======================================================================= resposta e erros


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


def _template_globals() -> dict[str, Any]:
    app_settings: PanelAppSettings = _ext("app")
    return {
        "csrf_token": _csrf_token,
        "caller": g.get("caller"),
        "is_admin": bool(g.get("caller")) and g.caller.role is PanelRole.ADMIN,
        "environment": app_settings.environment.value,
        "refresh_s": _settings().refresh_s,
        "htmx_integrity": HTMX_INTEGRITY,
        "now": datetime.now(UTC),
    }


def _action(run: Callable[[], T], success: Callable[[T], str]) -> None:
    """Ação admin: flash com o resultado ou com o erro da API (sem quebrar a página)."""
    try:
        flash(success(run()), "ok")
    except ApiError as exc:
        status, message = _api_status(exc)
        flash(f"{message} [{exc.code}; request_id {exc.request_id or g.request_id}]", "bad")
        log.warning("painel_acao_recusada", status=status, code=exc.code)


# ======================================================================= filtros de template


def _parse(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _when(value: Any) -> str:
    parsed = _parse(value)
    if parsed is None:
        return "—"
    text = parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"{text} UTC"


def _ago(value: Any, now: datetime | None = None) -> str:
    parsed = _parse(value)
    if parsed is None:
        return "—"
    seconds = int(((now or datetime.now(UTC)) - parsed).total_seconds())
    if seconds < 0:
        return "agora"
    if seconds < 120:
        return f"há {seconds} s"
    if seconds < 7200:
        return f"há {seconds // 60} min"
    return f"há {seconds // 3600} h"


def _state_label(state: str) -> tuple[str, str]:
    return STATE_LABELS.get(state, (state, "warn"))


def _filters(names: tuple[str, ...]) -> dict[str, str]:
    return {name: value for name in names if (value := request.args.get(name, "").strip())}


# ======================================================================= páginas


@admin.get("/healthz")
def healthz() -> Response:
    return Response("ok", mimetype="text/plain")


def _overview_context() -> dict[str, Any]:
    status = _api().status(_caller())
    chains = status.get("chains", [])
    totals = {
        "pending": sum(c["outbox"]["pending"] for c in chains),
        "failed": sum(c["outbox"]["failed"] for c in chains),
        "oldest": max((c["outbox"]["oldest_pending_seconds"] or 0 for c in chains), default=0)
        or None,
        "dlq": {
            stage: sum(c["dlq_open"].get(stage, 0) for c in chains)
            for stage in ("CONSUME", "PUBLISH", "NORMALIZE", "ENRICH")
        },
    }
    return {"status": status, "chains": chains, "totals": totals}


@admin.get("/")
def overview() -> str:
    return render_template("overview.html", **_overview_context())


@admin.get("/fragments/overview")
def fragment_overview() -> str | Response:
    try:
        return render_template("_overview.html", **_overview_context())
    except ApiError as exc:
        return _fragment_error(exc, "admin.fragment_overview")


@admin.get("/events")
def events() -> str:
    filters = _filters(EVENT_FILTERS)
    page = _api().events(_caller(), filters)
    next_args = {**filters, "cursor": str(page["next_cursor"])} if page["next_cursor"] else None
    return render_template("events.html", page=page, filters=filters, next_args=next_args)


@admin.get("/events/<path:unique_event_id>")
def event(unique_event_id: str) -> str:
    return render_template("event.html", event=_api().event(_caller(), unique_event_id))


@admin.get("/events/<path:unique_event_id>/export")
def export_event(unique_event_id: str) -> Response:
    download = _api().export(_require_admin(), unique_event_id)
    response = Response(download.content, mimetype="application/json")
    response.headers["Content-Disposition"] = f'attachment; filename="{download.filename}"'
    return response


@admin.post("/events/<path:unique_event_id>/reprocess")
def reprocess_event(unique_event_id: str) -> WerkzeugResponse:
    caller = _require_admin()
    _action(
        lambda: _api().reprocess(caller, unique_event_id),
        lambda r: f"Reprocessamento enfileirado na outbox (#{r['outbox_id']}).",
    )
    return redirect(url_for("admin.event", unique_event_id=unique_event_id))


def _dlq_context() -> dict[str, Any]:
    filters = _filters(DLQ_FILTERS)
    filters.setdefault("resolved", "false")
    if filters["resolved"] == "all":
        filters.pop("resolved")
        shown = "all"
    else:
        shown = filters["resolved"]
    page = _api().dlq(_caller(), filters)
    next_args = (
        {**filters, "resolved": shown, "cursor": str(page["next_cursor"])}
        if page["next_cursor"]
        else None
    )
    return {"page": page, "filters": {**filters, "resolved": shown}, "next_args": next_args}


@admin.get("/dlq")
def dlq() -> str:
    return render_template("dlq.html", **_dlq_context())


@admin.get("/fragments/dlq")
def fragment_dlq() -> str | Response:
    try:
        return render_template("_dlq.html", **_dlq_context())
    except ApiError as exc:
        return _fragment_error(exc, "admin.fragment_dlq")


@admin.post("/dlq/<int:item_id>/retry")
def retry_dlq(item_id: int) -> WerkzeugResponse:
    caller = _require_admin()
    messages = {
        "CONSUME_REQUESTED": "Retry pedido: o consumer da chain vai reprocessar a mensagem.",
        "REPUBLISHED": "Mensagem copiada para o fim da fila da outbox.",
        "REPROCESS_ENQUEUED": "Mensagem enviada ao enricher pela outbox.",
    }
    _action(
        lambda: _api().retry_dlq(caller, item_id),
        lambda r: f"Item {item_id}: {messages.get(r['result'], r['result'])}",
    )
    return redirect(url_for("admin.dlq"))


def _operations_context() -> dict[str, Any]:
    filters = _filters(REPLAY_FILTERS)
    replays = _api().replays(_caller(), filters)
    next_args = (
        {**filters, "cursor": str(replays["next_cursor"])} if replays["next_cursor"] else None
    )
    return {"replays": replays, "replay_filters": filters, "next_args": next_args}


@admin.get("/operations")
def operations() -> str:
    status = _api().status(_caller())
    return render_template(
        "operations.html", chains=status.get("chains", []), **_operations_context()
    )


@admin.get("/fragments/replays")
def fragment_replays() -> str | Response:
    try:
        return render_template("_replays.html", **_operations_context())
    except ApiError as exc:
        return _fragment_error(exc, "admin.fragment_replays")


def _replay_form() -> dict[str, str]:
    return {
        "chain_code": request.form.get("chain_code", "").strip(),
        "from_offset": request.form.get("from_offset", "").strip(),
        "reason": request.form.get("reason", "").strip(),
    }


@admin.post("/operations/replay")
def replay_review() -> str | WerkzeugResponse:
    """Primeiro passo: confere o preenchimento e mostra a confirmação (sem chamar a API)."""
    _require_admin()
    form = _replay_form()
    if not all(form.values()):
        flash("Preencha chain, offset e motivo.", "bad")
        return redirect(url_for("admin.operations"))
    return render_template("replay_confirm.html", form=form)


@admin.post("/operations/replay/confirm")
def replay_confirm() -> str | WerkzeugResponse:
    """Segundo passo: o operador digita o código da chain (a API confere de novo)."""
    caller = _require_admin()
    form = _replay_form()
    confirm = request.form.get("confirm", "").strip()
    if not all(form.values()):
        flash("Pedido incompleto; comece de novo.", "bad")
        return redirect(url_for("admin.operations"))
    if confirm != form["chain_code"]:
        flash(f"Digite exatamente {form['chain_code']} para confirmar.", "bad")
        return render_template("replay_confirm.html", form=form)

    def done(result: Mapping[str, Any]) -> str:
        text = (
            f"Replay #{result['id']} pedido "
            f"({result['chain_code']} a partir de {result['from_offset']})."
        )
        warnings = result.get("warnings") or []
        return " ".join([text, *("Aviso: " + w for w in warnings)])

    _action(lambda: _api().request_replay(caller, confirm=confirm, **form), done)
    return redirect(url_for("admin.operations"))


@admin.post("/operations/replay/<int:request_id>/cancel")
def cancel_replay(request_id: int) -> WerkzeugResponse:
    caller = _require_admin()
    _action(
        lambda: _api().cancel_replay(caller, request_id),
        lambda r: f"Pedido de replay #{r['id']} cancelado.",
    )
    return redirect(url_for("admin.operations"))


def _fragment_error(exc: ApiError, endpoint: str) -> Response:
    """Fragmento HTMX com o aviso de erro; continua se atualizando."""
    _, message = _api_status(exc)
    log.warning("painel_fragmento_sem_api", code=exc.code, api_request_id=exc.request_id)
    body = render_template(
        "_fragment_error.html",
        message=message,
        request_id=exc.request_id or g.request_id,
        endpoint=endpoint,
        args=_filters(_FRAGMENT_FILTERS[endpoint]),  # nunca _external, _scheme etc.
    )
    return Response(body, status=200)
