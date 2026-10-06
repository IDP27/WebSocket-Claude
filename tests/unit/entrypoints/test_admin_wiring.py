"""Montagem do painel (refatoração 7 do /entender, caracterização).

Escrito antes de separar ``admin/app.py`` em identidade, erros, templates e rotas: as mesmas
rotas (caminho, métodos e endpoint), os mesmos ganchos de requisição, handlers de erro,
filtros de template e configuração de sessão. O comportamento de cada rota já é coberto por
``test_admin_panel.py`` e ``test_admin_client.py``, que não mudam.
"""

from __future__ import annotations

from pathlib import Path

from tests.unit.entrypoints.test_admin_panel import make_panel

from ohip_streaming.entrypoints.admin import app as panel_module

ROUTES = [
    ("/admin/", ("GET",), "admin.overview"),
    ("/admin/dlq", ("GET",), "admin.dlq"),
    ("/admin/dlq/<int:item_id>/retry", ("POST",), "admin.retry_dlq"),
    ("/admin/events", ("GET",), "admin.events"),
    ("/admin/events/<path:unique_event_id>", ("GET",), "admin.event"),
    ("/admin/events/<path:unique_event_id>/export", ("GET",), "admin.export_event"),
    ("/admin/events/<path:unique_event_id>/reprocess", ("POST",), "admin.reprocess_event"),
    ("/admin/fragments/dlq", ("GET",), "admin.fragment_dlq"),
    ("/admin/fragments/overview", ("GET",), "admin.fragment_overview"),
    ("/admin/fragments/replays", ("GET",), "admin.fragment_replays"),
    ("/admin/healthz", ("GET",), "admin.healthz"),
    ("/admin/operations", ("GET",), "admin.operations"),
    ("/admin/operations/replay", ("POST",), "admin.replay_review"),
    ("/admin/operations/replay/<int:request_id>/cancel", ("POST",), "admin.cancel_replay"),
    ("/admin/operations/replay/confirm", ("POST",), "admin.replay_confirm"),
    ("/admin/static/<path:filename>", ("GET",), "static"),
]


def test_routes_are_unchanged() -> None:
    app = make_panel().app
    rules = sorted(
        (r.rule, tuple(sorted((r.methods or set()) - {"HEAD", "OPTIONS"})), r.endpoint)
        for r in app.url_map.iter_rules()
    )
    assert rules == ROUTES


def test_hooks_handlers_and_template_helpers() -> None:
    app = make_panel().app
    assert [f.__name__ for f in app.before_request_funcs[None]] == ["_identify"]
    assert [f.__name__ for f in app.after_request_funcs[None]] == ["_finish"]
    handlers = app.error_handler_spec[None][None]
    assert {e.__name__: h.__name__ for e, h in handlers.items()} == {
        "ApiError": "_api_error_page",
        "HTTPException": "_http_error_page",
        "Exception": "_unexpected_error_page",
    }
    assert [f.__name__ for f in app.template_context_processors[None]][-1] == "_template_globals"
    assert {"when", "ago"} <= set(app.jinja_env.filters)
    assert "state_label" in app.jinja_env.globals


def test_session_cookie_and_static_files() -> None:
    app = make_panel().app
    assert (app.config["SESSION_COOKIE_NAME"], app.config["SESSION_COOKIE_PATH"]) == (
        "ohip_admin",
        "/admin",
    )
    assert app.config["SESSION_COOKIE_HTTPONLY"] is True
    assert app.config["SESSION_COOKIE_SAMESITE"] == "Strict"
    assert app.config["MAX_CONTENT_LENGTH"] == 64 * 1024
    # Templates e estáticos continuam na pasta do pacote admin.
    package = Path(panel_module.__file__).parent
    assert Path(app.root_path) == package
    assert (package / "templates").is_dir()
    assert (package / "static" / "htmx.min.js").is_file()
