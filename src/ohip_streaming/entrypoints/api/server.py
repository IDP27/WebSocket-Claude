"""Processo ``ohip-api``: Uvicorn com a fábrica ``create_app`` (ADR-0017).

Uso (systemd, Fase 10): ``ohip-api``. Escuta em ``API_HOST``:``API_PORT`` (padrão
``127.0.0.1:8080``, atrás do Nginx) com ``API_WORKERS`` workers. O consumer nunca roda aqui
(ADR-0001).
"""

from __future__ import annotations

import uvicorn

from ohip_streaming.config import ApiSettings, AppSettings, LogSettings, load_settings
from ohip_streaming.logging import configure_logging, get_logger

log = get_logger(__name__)

APP_FACTORY = "ohip_streaming.entrypoints.api.app:create_app"


def main() -> int:
    settings = load_settings(ApiSettings)
    app = load_settings(AppSettings)
    logs = load_settings(LogSettings)
    # Log do processo supervisor; cada worker configura o seu na fábrica.
    configure_logging(
        service="ohip-api",
        environment=app.environment.value,
        code_version=app.code_version,
        level=logs.level,
        json_output=logs.json_output,
    )
    log.info("api_iniciando", host=settings.host, port=settings.port, workers=settings.workers)
    uvicorn.run(
        APP_FACTORY,
        factory=True,
        host=settings.host,
        port=settings.port,
        workers=settings.workers,
        access_log=False,  # grava a query string; a API loga a sua linha por requisição
        log_config=None,  # logs pelo structlog (configure_logging na fábrica)
        proxy_headers=True,
        forwarded_allow_ips="127.0.0.1",  # só o Nginx local
        server_header=False,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
