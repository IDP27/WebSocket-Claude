"""Logs estruturados em JSON com structlog (RNF-09).

- Todo registro leva ``service``, ``environment`` e ``code_version``.
- Contexto de correlação (``unique_event_id``, ``chain_code``, ``offset``...) via
  ``bind_context``: vale para tudo que for logado dentro do bloco, inclusive em corrotinas.
- Logs de bibliotecas (uvicorn, websockets, aio-pika) passam pelo mesmo formatador.
- Segredos são mascarados por nome de campo e por padrão (``Bearer ...``) antes de sair.
  Dados pessoais do ``detail`` são mascarados no domínio (LGPD), não aqui.
"""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

import structlog
from pydantic import SecretStr
from structlog.types import EventDict, Processor, WrappedLogger

MASK = "***"

# Comparação em minúsculas, com "-" tratado como "_".
_SENSITIVE_KEYS = frozenset(
    {
        "authorization",
        "token",
        "access_token",
        "refresh_token",
        "id_token",
        "client_secret",
        "app_key",
        "x_app_key",
        "password",
        "passwd",
        "secret",
        "api_key",
        "service_token",
        "integration_password",
    }
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]+=*")
_URL_CREDENTIALS_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]*):([^/\s@]+)@")

_LIBRARY_LOGGERS = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "websockets",
    "aio_pika",
    "aiormq",
)


def _is_sensitive(key: str) -> bool:
    return key.lower().replace("-", "_") in _SENSITIVE_KEYS


def _redact_text(text: str) -> str:
    text = _BEARER_RE.sub(f"Bearer {MASK}", text)
    return _URL_CREDENTIALS_RE.sub(rf"\1\2:{MASK}@", text)


def redact(value: Any) -> Any:
    """Mascara segredos em qualquer estrutura (dicts, listas, textos, SecretStr)."""
    if isinstance(value, SecretStr):
        return MASK
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, Mapping):
        return {
            key: (MASK if isinstance(key, str) and _is_sensitive(key) else redact(item))
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return type(value)(redact(item) for item in value)
    return value


def _redact_processor(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    return redact(event_dict)  # type: ignore[no-any-return]


def _static_fields(**fields: str) -> Processor:
    def add_fields(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        for key, value in fields.items():
            event_dict.setdefault(key, value)
        return event_dict

    return add_fields


def configure_logging(
    *,
    service: str,
    environment: str,
    code_version: str,
    level: str = "INFO",
    json_output: bool = True,
) -> None:
    """Configura structlog e o logging padrão. Chamar uma vez, no início do entrypoint."""
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _static_fields(service=service, environment=environment, code_version=code_version),
        structlog.processors.StackInfoRenderer(),
    ]
    renderer: Processor
    if json_output:
        shared.append(structlog.processors.dict_tracebacks)
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)
    # A máscara roda por último, depois de os tracebacks virarem dados.
    shared.append(_redact_processor)

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for name in _LIBRARY_LOGGERS:
        library_logger = logging.getLogger(name)
        library_logger.handlers = []
        library_logger.propagate = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.stdlib.get_logger(name)


@contextmanager
def bind_context(**fields: Any) -> Iterator[None]:
    """Adiciona campos de correlação a todos os logs do bloco (seguro com asyncio)."""
    with structlog.contextvars.bound_contextvars(**fields):
        yield
