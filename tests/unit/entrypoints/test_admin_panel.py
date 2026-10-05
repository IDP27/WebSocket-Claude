"""Painel Flask de ponta a ponta: painel → API da Fase 7 → fakes em memória (ADR-0018)."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import structlog
from flask import Flask
from flask.testing import FlaskClient
from tests.unit.entrypoints.test_api_routes import ADMIN_TOKEN, READ_TOKEN, Env, make_env

from ohip_streaming.entrypoints.admin.api_client import ApiClient, PanelRole
from ohip_streaming.entrypoints.admin.app import build_app
from ohip_streaming.entrypoints.admin.settings import (
    AdminSettings,
    PanelAppSettings,
    PanelEnvironment,
)
from ohip_streaming.logging import configure_logging

ADMIN_USER = {"X-Forwarded-User": "ana@empresa", "X-Forwarded-Groups": "outros, ohip-admin"}
READ_USER = {"X-Forwarded-User": "rui@empresa", "X-Forwarded-Groups": "ohip-read"}
SECRET = "x" * 40


def settings(**overrides: Any) -> AdminSettings:
    values: dict[str, Any] = {
        "api_read_token": READ_TOKEN,
        "api_admin_token": ADMIN_TOKEN,
        "secret_key": SECRET,
    }
    values.update(overrides)
    return AdminSettings(**values)


@dataclass
class Panel:
    api: Env
    app: Flask
    client: FlaskClient

    def csrf(self, path: str = "/admin/operations", headers: dict[str, str] = ADMIN_USER) -> str:
        page = self.client.get(path, headers=headers).get_data(as_text=True)
        match = re.search(r'name="csrf_token" value="([^"]+)"', page)
        assert match, "página sem token CSRF"
        return match.group(1)

    def text(self, path: str, headers: dict[str, str] = READ_USER) -> str:
        response = self.client.get(path, headers=headers)
        assert response.status_code == 200, response.get_data(as_text=True)
        return response.get_data(as_text=True)


def make_panel(http: httpx.Client | None = None, api: Env | None = None) -> Panel:
    api = api or make_env()
    client = ApiClient(
        http or api.client, tokens={PanelRole.READ: READ_TOKEN, PanelRole.ADMIN: ADMIN_TOKEN}
    )
    app = build_app(settings=settings(), app_settings=PanelAppSettings(), api=client)
    app.config["TESTING"] = True
    return Panel(api, app, app.test_client())


@pytest.fixture(autouse=True)
def json_logs() -> Iterator[None]:
    configure_logging(service="ohip-admin", environment="homologacao", code_version="t")
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)


@pytest.fixture
def panel() -> Panel:
    return make_panel()


# ------------------------------------------------------------------ identidade e segurança


def test_healthz_needs_no_user(panel: Panel) -> None:
    response = panel.client.get("/admin/healthz")
    assert (response.status_code, response.get_data(as_text=True)) == (200, "ok")


def test_user_comes_from_the_proxy_headers(panel: Panel) -> None:
    assert panel.client.get("/admin/").status_code == 401
    no_group = {"X-Forwarded-User": "ze", "X-Forwarded-Groups": "outros"}
    assert panel.client.get("/admin/", headers=no_group).status_code == 403
    assert panel.client.get("/admin/", headers=READ_USER).status_code == 200


def test_security_headers(panel: Panel) -> None:
    response = panel.client.get("/admin/", headers={**READ_USER, "X-Request-ID": "abc-1"})
    headers = response.headers
    assert "default-src 'self'" in headers["Content-Security-Policy"]
    assert headers["X-Frame-Options"] == "DENY"
    assert headers["Cache-Control"] == "no-store"
    assert headers["X-Request-ID"] == "abc-1"


def test_post_requires_admin_and_csrf(panel: Panel) -> None:
    token = panel.csrf()
    path = "/admin/events/uid-90/reprocess"
    assert panel.client.post(path, headers=ADMIN_USER).status_code == 400  # sem CSRF
    assert panel.client.post(path, headers=ADMIN_USER, data={"csrf_token": "x"}).status_code == 400
    assert panel.client.post(path, headers=READ_USER, data={"csrf_token": token}).status_code == 403
    assert len(panel.api.db.outbox) == 3  # nada chegou à API


def test_request_log_has_no_query_string(panel: Panel, caplog: pytest.LogCaptureFixture) -> None:
    caplog.clear()
    panel.client.get("/admin/events?primary_key=VALOR-SENSIVEL", headers=READ_USER)
    records = [r.msg for r in caplog.records if isinstance(r.msg, dict)]
    (line,) = [r for r in records if r["event"] == "painel_requisicao"]
    assert (line["path"], line["user"], line["role"]) == ("/admin/events", "rui@empresa", "read")
    assert "VALOR-SENSIVEL" not in json.dumps(records)


# ------------------------------------------------------------------ páginas


def test_overview_and_fragment(panel: Panel) -> None:
    page = panel.text("/admin/")
    assert "CHAIN1" in page
    assert "PARADO" in page  # estado com texto, não só cor
    assert "Pendentes 3" in page
    assert "every 15s" in page
    fragment = panel.text("/admin/fragments/overview")
    assert fragment.startswith('<div id="overview"')
    assert "<html" not in fragment


def test_events_list_filters_and_next_page(panel: Panel) -> None:
    page = panel.text("/admin/events?event_name=checkin+reservation&ignorado=1")
    assert "CHECKIN RESERVATION" in page
    assert "UPDATE RESERVATION" not in page
    paged = panel.text("/admin/events?limit=1")  # o painel não repassa limit
    assert "Próxima página" not in paged


def test_event_detail_is_masked_and_actions_are_admin_only(panel: Panel) -> None:
    page = panel.text("/admin/events/uid-90")
    assert "FIRST NAME" in page
    assert "***" in page
    assert "Maria" not in page
    assert "Reprocessar" not in page  # perfil read
    assert "Reprocessar" in panel.text("/admin/events/uid-90", ADMIN_USER)


def test_reprocess_action(panel: Panel) -> None:
    token = panel.csrf()
    response = panel.client.post(
        "/admin/events/uid-90/reprocess",
        headers=ADMIN_USER,
        data={"csrf_token": token},
        follow_redirects=True,
    )
    assert "Reprocessamento enfileirado na outbox" in response.get_data(as_text=True)
    assert any(o.exchange_name == "ohip.reprocess" for o in panel.api.db.outbox.values())


def test_export_downloads_masked_json(panel: Panel) -> None:
    assert panel.client.get("/admin/events/uid-90/export", headers=READ_USER).status_code == 403
    response = panel.client.get("/admin/events/uid-90/export", headers=ADMIN_USER)
    assert response.status_code == 200
    assert "attachment" in response.headers["Content-Disposition"]
    assert json.loads(response.data)["masked"] is True
    assert b"Maria" not in response.data


def test_event_not_found_page(panel: Panel) -> None:
    response = panel.client.get("/admin/events/nao-existe", headers=READ_USER)
    assert response.status_code == 404
    assert "request_id" in response.get_data(as_text=True)


def test_dlq_page_and_retry(panel: Panel) -> None:
    epoch = panel.api.db.acquire("publisher")
    import asyncio

    asyncio.run(panel.api.db.mark_failed(1, 10, "NACK do broker", epoch))
    page = panel.text("/admin/dlq")
    assert "NACK do broker" in page
    assert "Reprocessar" not in page  # perfil read
    token = panel.csrf("/admin/dlq")
    (item_id,) = panel.api.db.dlq
    response = panel.client.post(
        f"/admin/dlq/{item_id}/retry",
        headers=ADMIN_USER,
        data={"csrf_token": token},
        follow_redirects=True,
    )
    assert "copiada para o fim da fila" in response.get_data(as_text=True)
    assert panel.api.db.dlq[item_id].resolved_by == "ana@empresa"  # X-Actor = usuário
    again = panel.client.post(
        f"/admin/dlq/{item_id}/retry",
        headers=ADMIN_USER,
        data={"csrf_token": token},
        follow_redirects=True,
    )
    assert "INVALID_STATE" in again.get_data(as_text=True)  # erro vira mensagem, não 500
    assert "RETRIED por ana@empresa" in panel.text("/admin/fragments/dlq?resolved=all")


def test_replay_needs_typed_confirmation(panel: Panel) -> None:
    token = panel.csrf()
    form = {"csrf_token": token, "chain_code": "CHAIN1", "from_offset": "90", "reason": "INC-9"}
    review = panel.client.post("/admin/operations/replay", headers=ADMIN_USER, data=form)
    assert "Digite <strong>CHAIN1</strong>" in review.get_data(as_text=True)
    assert panel.api.db.replays == {}  # o primeiro passo não chama a API

    wrong = panel.client.post(
        "/admin/operations/replay/confirm", headers=ADMIN_USER, data={**form, "confirm": "chain1"}
    )
    assert "Digite exatamente CHAIN1" in wrong.get_data(as_text=True)
    assert panel.api.db.replays == {}

    done = panel.client.post(
        "/admin/operations/replay/confirm",
        headers=ADMIN_USER,
        data={**form, "confirm": "CHAIN1"},
        follow_redirects=True,
    )
    assert "Replay #1 pedido" in done.get_data(as_text=True)
    (request,) = panel.api.db.replays.values()
    assert request.requested_by == "ana@empresa"

    history = panel.text("/admin/fragments/replays", ADMIN_USER)
    assert "PENDING" in history
    cancelled = panel.client.post(
        f"/admin/operations/replay/{request.id}/cancel",
        headers=ADMIN_USER,
        data={"csrf_token": token},
        follow_redirects=True,
    )
    assert "cancelado" in cancelled.get_data(as_text=True)


def test_replay_errors_from_the_api_become_messages(panel: Panel) -> None:
    token = panel.csrf()
    form = {
        "csrf_token": token,
        "chain_code": "CHAIN1",
        "from_offset": "999",
        "reason": "x",
        "confirm": "CHAIN1",
    }
    response = panel.client.post(
        "/admin/operations/replay/confirm", headers=ADMIN_USER, data=form, follow_redirects=True
    )
    assert "REPLAY_FORWARD_NOT_ALLOWED" in response.get_data(as_text=True)
    incomplete = panel.client.post(
        "/admin/operations/replay",
        headers=ADMIN_USER,
        data={"csrf_token": token, "chain_code": "CHAIN1"},
        follow_redirects=True,
    )
    assert "Preencha chain, offset e motivo" in incomplete.get_data(as_text=True)


# ------------------------------------------------------------------ API fora ou recusando


def broken_api(status: int | None = None) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if status is None:
            raise httpx.ConnectError("recusada", request=request)
        return httpx.Response(
            status, json={"error": {"code": "X", "message": "m", "request_id": "r"}}
        )

    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://api")


def test_api_down_is_503_page_and_fragment_keeps_polling() -> None:
    panel = make_panel(broken_api())
    page = panel.client.get("/admin/", headers=READ_USER)
    assert page.status_code == 503
    fragment = panel.client.get("/admin/fragments/overview", headers=READ_USER)
    assert fragment.status_code == 200
    body = fragment.get_data(as_text=True)
    assert "não respondeu" in body
    assert 'hx-get="/admin/fragments/overview"' in body


def test_api_rejecting_panel_token_is_502() -> None:
    panel = make_panel(broken_api(401))
    response = panel.client.get("/admin/", headers=READ_USER)
    assert response.status_code == 502
    assert "ADMIN_API_" in response.get_data(as_text=True)


def test_unexpected_error_is_500_page(panel: Panel, monkeypatch: pytest.MonkeyPatch) -> None:
    api: Any = panel.app.extensions["ohip_admin"]["api"]

    def boom(*args: Any) -> Any:
        raise RuntimeError("detalhe interno")

    monkeypatch.setattr(api, "status", boom)
    response = panel.client.get("/admin/", headers=READ_USER)
    assert response.status_code == 500
    assert "detalhe interno" not in response.get_data(as_text=True)


def test_session_cookie_flags() -> None:
    app = build_app(
        settings=settings(),
        app_settings=PanelAppSettings(environment=PanelEnvironment.PRODUCAO),
        api=ApiClient(broken_api(), tokens={}),
    )
    assert app.config["SESSION_COOKIE_SECURE"] is True
    assert app.config["SESSION_COOKIE_SAMESITE"] == "Strict"
    assert app.config["SESSION_COOKIE_HTTPONLY"] is True


# ------------------------------------------------------------------ correções da revisão


@pytest.mark.parametrize("user", ["joão", "joÃ£o", "a" * 101, "ana\tsilva", " "])
def test_unsupported_user_is_400_not_500(panel: Panel, user: str) -> None:
    headers = {"X-Forwarded-User": user, "X-Forwarded-Groups": "ohip-admin"}
    response = panel.client.get("/admin/events/uid-90", headers=headers)
    assert response.status_code in (400, 401)  # " " vira vazio: 401
    if user.strip():
        assert "ASCII" in response.get_data(as_text=True)


def test_longest_ascii_user_is_accepted(panel: Panel) -> None:
    headers = {"X-Forwarded-User": "a" * 100, "X-Forwarded-Groups": "ohip-admin"}
    assert panel.client.get("/admin/", headers=headers).status_code == 200


def test_fragment_retry_link_keeps_only_known_filters() -> None:
    panel = make_panel(broken_api())
    query = "?stage=ENRICH&_external=1&_scheme=http&_anchor=x&cursor=5"
    body = panel.client.get(f"/admin/fragments/dlq{query}", headers=READ_USER).get_data(
        as_text=True
    )
    (link,) = re.findall(r'hx-get="([^"]+)"', body)
    assert link.startswith("/admin/fragments/dlq?")
    assert "_external" not in link
    assert "_scheme" not in link
    assert "stage=ENRICH" in link
    assert "cursor=5" in link


def test_csrf_token_from_another_session_is_refused(panel: Panel) -> None:
    other = make_panel(api=panel.api)
    foreign = other.csrf()
    panel.csrf()  # esta sessão tem o seu próprio token
    response = panel.client.post(
        "/admin/events/uid-90/reprocess", headers=ADMIN_USER, data={"csrf_token": foreign}
    )
    assert response.status_code == 400


def test_export_route_with_slash_in_uid() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"{}")

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://api")
    panel = make_panel(http)
    response = panel.client.get("/admin/events/a/b/export", headers=ADMIN_USER)
    assert response.status_code == 200
    assert seen[0].url.raw_path == b"/api/v1/events/a%2Fb/export"


def test_htmx_poll_with_expired_login_reloads_the_page(panel: Panel) -> None:
    response = panel.client.get("/admin/fragments/overview", headers={"HX-Request": "true"})
    assert response.status_code == 401
    assert response.headers["HX-Refresh"] == "true"


def test_proxy_secret_is_required_when_configured() -> None:
    api = make_env()
    client = ApiClient(
        api.client, tokens={PanelRole.READ: READ_TOKEN, PanelRole.ADMIN: ADMIN_TOKEN}
    )
    app = build_app(
        settings=settings(host="0.0.0.0", proxy_secret="s" * 40),  # noqa: S104 - teste
        app_settings=PanelAppSettings(),
        api=client,
    )
    test_client = app.test_client()
    assert test_client.get("/admin/", headers=READ_USER).status_code == 403
    wrong = {**READ_USER, "X-Admin-Proxy-Secret": "errado"}
    assert test_client.get("/admin/", headers=wrong).status_code == 403
    right = {**READ_USER, "X-Admin-Proxy-Secret": "s" * 40}
    assert test_client.get("/admin/", headers=right).status_code == 200
    assert test_client.get("/admin/healthz").status_code == 200


def test_open_bind_without_proxy_secret_is_refused() -> None:
    with pytest.raises(ValueError, match="ADMIN_PROXY_SECRET"):
        settings(host="0.0.0.0")  # noqa: S104 - teste
    assert settings(host="::1").host == "::1"


def test_htmx_file_matches_the_published_hash(panel: Panel) -> None:
    import base64
    import hashlib
    from pathlib import Path

    from ohip_streaming.entrypoints.admin import app as panel_module

    static = Path(panel_module.__file__).parent / "static" / "htmx.min.js"
    digest = base64.b64encode(hashlib.sha384(static.read_bytes()).digest()).decode()
    assert f"sha384-{digest}" == panel_module.HTMX_INTEGRITY
    page = panel.text("/admin/")
    assert f'integrity="{panel_module.HTMX_INTEGRITY}"' in page
    with panel.client.get("/admin/static/htmx.min.js") as served:  # fecha o arquivo
        assert served.status_code == 200
        assert served.data.startswith(b"var htmx=")


def test_non_ascii_secrets_are_refused_not_500(panel: Panel) -> None:
    """``compare_digest`` com ``str`` não ASCII levantaria TypeError (500)."""
    panel.csrf()
    response = panel.client.post(
        "/admin/events/uid-90/reprocess", headers=ADMIN_USER, data={"csrf_token": "tökên"}
    )
    assert response.status_code == 400

    api = make_env()
    client = ApiClient(
        api.client, tokens={PanelRole.READ: READ_TOKEN, PanelRole.ADMIN: ADMIN_TOKEN}
    )
    app = build_app(
        settings=settings(proxy_secret="s" * 40), app_settings=PanelAppSettings(), api=client
    )
    headers = {**READ_USER, "X-Admin-Proxy-Secret": "sëgrêdo"}
    assert app.test_client().get("/admin/", headers=headers).status_code == 403
