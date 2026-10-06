"""Rotas do painel (blueprint ``admin``): páginas, fragmentos HTMX e ações admin."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, TypeVar

from flask import (
    Blueprint,
    Response,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
)
from werkzeug.wrappers.response import Response as WerkzeugResponse

from ohip_streaming.entrypoints.admin.api_client import (
    ApiError,
)
from ohip_streaming.entrypoints.admin.context import PANEL_LOGGER, _api
from ohip_streaming.entrypoints.admin.errors import _api_status
from ohip_streaming.entrypoints.admin.identity import _caller, _require_admin
from ohip_streaming.logging import get_logger

log = get_logger(PANEL_LOGGER)


T = TypeVar("T")


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


admin = Blueprint("admin", __name__, url_prefix="/admin")


def _action(run: Callable[[], T], success: Callable[[T], str]) -> None:
    """Ação admin: flash com o resultado ou com o erro da API (sem quebrar a página)."""
    try:
        flash(success(run()), "ok")
    except ApiError as exc:
        status, message = _api_status(exc)
        flash(f"{message} [{exc.code}; request_id {exc.request_id or g.request_id}]", "bad")
        log.warning("painel_acao_recusada", status=status, code=exc.code)


def _filters(names: tuple[str, ...]) -> dict[str, str]:
    return {name: value for name in names if (value := request.args.get(name, "").strip())}


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
