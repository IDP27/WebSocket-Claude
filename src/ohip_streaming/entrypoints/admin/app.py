"""Painel operacional ``ohip-admin`` (docs/UI.md, ADR-0004, ADR-0018).

- Usuário e grupos vêm de headers definidos pelo Nginx/SSO (Q-5); o perfil escolhe o token de
  serviço da API, e o usuário vai em ``X-Actor``.
- Todo POST é ação ``admin``: perfil e CSRF conferidos no servidor antes de chamar a API.
- Sem acesso ao Oracle nem ao Redis: tudo passa pela API (import-linter).

Montagem (``build_app``/``create_app``); identidade, erros, templates e rotas ficam em
módulos próprios (refatoração 7 do /entender)."""

from __future__ import annotations

import httpx
from flask import (
    Flask,
)
from werkzeug.exceptions import HTTPException

from ohip_streaming.entrypoints.admin.api_client import (
    ApiClient,
    ApiError,
    PanelRole,
)
from ohip_streaming.entrypoints.admin.errors import (
    _api_error_page,
    _finish,
    _http_error_page,
    _unexpected_error_page,
)
from ohip_streaming.entrypoints.admin.identity import _identify
from ohip_streaming.entrypoints.admin.settings import (
    AdminSettings,
    PanelAppSettings,
    PanelEnvironment,
    PanelLogSettings,
    load_panel_settings,
)
from ohip_streaming.entrypoints.admin.templating import (
    HTMX_INTEGRITY,
    _ago,
    _state_label,
    _template_globals,
    _when,
)
from ohip_streaming.entrypoints.admin.views import admin
from ohip_streaming.logging import configure_logging


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


# Reexportados: testes e quem importava do módulo único anterior.
__all__ = ["HTMX_INTEGRITY", "_ago", "_state_label", "_when", "build_app", "create_app"]
