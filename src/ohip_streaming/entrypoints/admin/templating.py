"""Filtros e variáveis dos templates.

Os nomes com sublinhado daqui são internos ao pacote ``admin``, não ao módulo: os módulos
irmãos (e ``app.py``) os importam. Antes de renomear ou remover, procure o uso no pacote."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from flask import (
    g,
)

from ohip_streaming.entrypoints.admin.api_client import (
    PanelRole,
)
from ohip_streaming.entrypoints.admin.context import _ext, _settings
from ohip_streaming.entrypoints.admin.identity import _csrf_token
from ohip_streaming.entrypoints.admin.settings import (
    PanelAppSettings,
)

# htmx 2.0.11 servido pelo painel; hash publicado em htmx.org (static/HTMX_PROVENANCE.md).
HTMX_INTEGRITY = "sha384-2OatzQy1H+Zd/IIrjr1TcuDGqLXeHhbooAyJY1KdQMKnr4LZ22k31GBLdYKHmVjg"


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
