"""Processo ``ohip-admin``: Gunicorn com a fábrica ``create_app`` (ADR-0018).

Uso (systemd, Fase 10): ``ohip-admin``. Escuta em ``ADMIN_HOST``:``ADMIN_PORT`` (padrão
``127.0.0.1:8081``, atrás do Nginx, que autentica e define os headers do usuário) com
``ADMIN_WORKERS`` workers síncronos. Access log do Gunicorn desligado: ele grava a query
string; o painel loga a sua linha por requisição.
"""

from __future__ import annotations

from typing import Any

from flask import Flask
from gunicorn.app.base import BaseApplication

from ohip_streaming.entrypoints.admin.app import create_app
from ohip_streaming.entrypoints.admin.settings import AdminSettings, load_panel_settings


class PanelServer(BaseApplication):  # type: ignore[misc]  # base sem tipos
    def __init__(self, options: dict[str, Any]) -> None:
        self.options = options
        super().__init__()

    def load_config(self) -> None:
        for key, value in self.options.items():
            self.cfg.set(key, value)

    def load(self) -> Flask:
        return create_app()  # em cada worker (com preload desligado)


def gunicorn_options(settings: AdminSettings) -> dict[str, Any]:
    return {
        "bind": f"{settings.host}:{settings.port}",
        "workers": settings.workers,
        "worker_class": "sync",
        "accesslog": None,
        "errorlog": "-",
        "forwarded_allow_ips": "127.0.0.1",  # só o Nginx local
        "proxy_allow_ips": "127.0.0.1",
        "timeout": 30,
        "graceful_timeout": 20,
        "preload_app": False,
    }


def main() -> int:
    PanelServer(gunicorn_options(load_panel_settings(AdminSettings))).run()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
