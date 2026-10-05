"""Cliente da API do painel, configuração e processo ``ohip-admin`` (ADR-0018)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from ohip_streaming.entrypoints.admin import app as panel_app
from ohip_streaming.entrypoints.admin import server
from ohip_streaming.entrypoints.admin.api_client import (
    ApiClient,
    ApiError,
    ApiUnavailableError,
    Caller,
    PanelRole,
)
from ohip_streaming.entrypoints.admin.settings import AdminSettings, load_panel_settings

TOKENS = {PanelRole.READ: "t-read", PanelRole.ADMIN: "t-admin"}
READER = Caller("rui", PanelRole.READ, "req-1")
ADMIN = Caller("ana", PanelRole.ADMIN, "req-2")


def client(handler: Any) -> tuple[ApiClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        result: httpx.Response = handler(request)
        return result

    http = httpx.Client(transport=httpx.MockTransport(record), base_url="http://api")
    return ApiClient(http, tokens=TOKENS), seen


def ok(body: Any = None, **kwargs: Any) -> Any:
    return lambda request: httpx.Response(200, json=body if body is not None else {}, **kwargs)


def test_token_by_role_and_actor_only_for_admin() -> None:
    api, seen = client(ok())
    api.status(READER)
    api.reprocess(ADMIN, "uid-1")
    read, write = seen
    assert read.headers["Authorization"] == "Bearer t-read"
    assert "X-Actor" not in read.headers
    assert read.headers["X-Request-ID"] == "req-1"
    assert write.headers["Authorization"] == "Bearer t-admin"
    assert (write.method, write.url.path) == ("POST", "/api/v1/events/uid-1/reprocess")
    assert write.headers["X-Actor"] == "ana"


def test_unique_event_id_cannot_change_the_route() -> None:
    api, seen = client(ok())
    api.event(READER, "../replay?x=1#y")
    assert seen[0].url.raw_path == b"/api/v1/events/..%2Freplay%3Fx%3D1%23y"


def test_requests_and_params() -> None:
    api, seen = client(ok({"items": [], "next_cursor": None}))
    api.events(READER, {"chain_code": "C1", "from": "2026-10-01T00:00"})
    api.outbox(READER, {"status": "FAILED"})
    api.dlq(READER, {"resolved": "false"})
    api.replays(READER, {})
    api.request_replay(ADMIN, chain_code="C1", from_offset="9", reason="r", confirm="C1")
    api.cancel_replay(ADMIN, 7)
    api.retry_dlq(ADMIN, 3)
    calls = [(r.method, r.url.path, dict(r.url.params)) for r in seen]
    assert calls[0] == ("GET", "/api/v1/events", {"chain_code": "C1", "from": "2026-10-01T00:00"})
    assert calls[4][:2] == ("POST", "/api/v1/replay")
    assert calls[5][:2] == ("DELETE", "/api/v1/replay/7")
    assert calls[6][:2] == ("POST", "/api/v1/dlq/3/retry")


def test_export_filename() -> None:
    headers = {"Content-Disposition": 'attachment; filename="evento-7.json"'}
    api, _ = client(lambda r: httpx.Response(200, content=b"{}", headers=headers))
    download = api.export(ADMIN, "uid-1")
    assert (download.filename, download.content) == ("evento-7.json", b"{}")
    api, _ = client(lambda r: httpx.Response(200, content=b"{}"))
    assert api.export(ADMIN, "uid-1").filename == "evento.json"


def test_api_error_body_is_parsed() -> None:
    body = {"error": {"code": "NOT_FOUND", "message": "evento x", "request_id": "api-9"}}
    api, _ = client(lambda r: httpx.Response(404, json=body))
    with pytest.raises(ApiError) as info:
        api.event(READER, "x")
    assert (info.value.status, info.value.code, info.value.request_id) == (
        404,
        "NOT_FOUND",
        "api-9",
    )


@pytest.mark.parametrize(
    ("response", "unavailable"),
    [
        (httpx.Response(502, text="bad gateway"), True),
        (httpx.Response(400, text="?"), False),
        (httpx.Response(200, text="não é json"), True),
        (httpx.Response(200, json=[1, 2]), True),
    ],
)
def test_responses_outside_the_contract(response: httpx.Response, unavailable: bool) -> None:
    api, _ = client(lambda r: response)
    with pytest.raises(ApiError) as info:
        api.status(READER)
    assert isinstance(info.value, ApiUnavailableError) is unavailable


def test_network_error_is_unavailable_without_token() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lenta", request=request)

    api, _ = client(fail)
    with pytest.raises(ApiUnavailableError) as info:
        api.status(READER)
    assert "t-read" not in str(info.value)
    assert info.value.request_id == "req-1"


# ------------------------------------------------------------------ filtros de template


def test_time_filters() -> None:
    now = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert panel_app._when("2026-10-01T11:59:58.812Z") == "2026-10-01 11:59:58.812 UTC"
    assert panel_app._when(None) == "—"
    assert panel_app._when("lixo") == "—"
    assert panel_app._ago((now - timedelta(seconds=3)).isoformat(), now) == "há 3 s"
    assert panel_app._ago((now - timedelta(minutes=5)).isoformat(), now) == "há 5 min"
    assert panel_app._ago((now - timedelta(hours=3)).isoformat(), now) == "há 3 h"
    assert panel_app._ago((now + timedelta(seconds=3)).isoformat(), now) == "agora"
    assert panel_app._ago("2026-10-01T11:59:58", now) == "há 2 s"  # sem fuso = UTC
    assert panel_app._state_label("SUBSCRIBED") == ("CONECTADO", "ok")
    assert panel_app._state_label("NOVO") == ("NOVO", "warn")


# ------------------------------------------------------------------ configuração e processo


@pytest.fixture
def panel_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_READ_TOKEN", "token-leitura")
    monkeypatch.setenv("ADMIN_API_ADMIN_TOKEN", "token-admin")
    monkeypatch.setenv("ADMIN_SECRET_KEY", "k" * 40)


@pytest.mark.usefixtures("panel_env")
def test_settings_defaults() -> None:
    settings = AdminSettings()
    assert settings.api_base_url == "http://127.0.0.1:8080"
    assert (settings.user_header, settings.groups_header) == (
        "X-Forwarded-User",
        "X-Forwarded-Groups",
    )
    assert "token-admin" not in repr(settings)


@pytest.mark.usefixtures("panel_env")
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ADMIN_SECRET_KEY", "curta"),
        ("ADMIN_API_BASE_URL", "ftp://api"),
        ("ADMIN_READ_GROUP", "ohip-admin"),
        ("ADMIN_API_ADMIN_TOKEN", "token-leitura"),
    ],
)
def test_settings_validation(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError) as info:
        AdminSettings()
    assert "token-leitura" not in str(info.value)


@pytest.mark.usefixtures("panel_env")
def test_create_app_and_server(monkeypatch: pytest.MonkeyPatch) -> None:
    import logging

    import structlog

    try:
        app = panel_app.create_app()
        assert app.config["SESSION_COOKIE_SECURE"] is False  # desenvolvimento
        assert app.test_client().get("/admin/healthz").status_code == 200

        options = server.gunicorn_options(load_panel_settings(AdminSettings))
        assert options["bind"] == "127.0.0.1:8081"
        assert options["accesslog"] is None
        assert options["preload_app"] is False

        ran: list[dict[str, Any]] = []
        monkeypatch.setattr(server.PanelServer, "run", lambda self: ran.append(self.options))
        assert server.main() == 0
        assert ran == [options]
        assert server.PanelServer(options).load().name == app.name
    finally:
        structlog.reset_defaults()
        root = logging.getLogger()
        root.handlers = []
        root.setLevel(logging.WARNING)
