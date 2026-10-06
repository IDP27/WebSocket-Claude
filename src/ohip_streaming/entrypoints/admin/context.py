"""Acesso ao que ``build_app`` guarda na aplicação (configuração e cliente da API).

Os nomes com sublinhado daqui são internos ao pacote ``admin``, não ao módulo: os módulos
irmãos (e ``app.py``) os importam. Antes de renomear ou remover, procure o uso no pacote."""

from __future__ import annotations

from typing import Any

from ohip_streaming.entrypoints.admin.api_client import (
    ApiClient,
)
from ohip_streaming.entrypoints.admin.settings import (
    AdminSettings,
)

# Um só logger para o painel: o nome sai em todo log (add_logger_name) e não muda
# com a divisão do módulo (refatoração 7 do /entender).
PANEL_LOGGER = "ohip_streaming.entrypoints.admin.app"


def _ext(key: str) -> Any:
    from flask import current_app

    return current_app.extensions["ohip_admin"][key]


def _api() -> ApiClient:
    client: ApiClient = _ext("api")
    return client


def _settings() -> AdminSettings:
    settings: AdminSettings = _ext("settings")
    return settings
