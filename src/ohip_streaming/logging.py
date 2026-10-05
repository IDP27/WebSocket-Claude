"""Logs estruturados em JSON com structlog (RNF-09).

- Todo registro leva ``service``, ``environment`` e ``code_version``.
- Contexto de correlação (``unique_event_id``, ``chain_code``, ``offset``...) via
  ``bind_context``: vale para tudo que for logado dentro do bloco, inclusive em corrotinas.
- Logs de bibliotecas (uvicorn, websockets, aio-pika) passam pelo mesmo formatador.
- Segredos são mascarados por nome de campo (snake, kebab ou camelCase) e por padrão no texto
  (``Bearer ...``, senha em URL, ``?key=<hash>``, ``x-app-key: ...``) antes de sair.
- Tracebacks **nunca** levam variáveis locais: elas podem conter o ``connection_init`` com a
  app key, tokens ou o ``detail`` com dados pessoais, e o ``repr`` delas escapa da máscara.
  O traceback vira dado/texto antes da máscara, que então também cobre a mensagem da exceção.
- Dados pessoais do ``detail`` são mascarados no domínio (LGPD), não aqui.
- Código síncrono em threads (Oracle, ADR-0003) deve rodar via ``run_in_executor`` deste
  módulo, que leva o contexto de correlação para a thread.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import re
import sys
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Executor
from contextlib import contextmanager
from typing import Any, ParamSpec, TypeVar

import structlog
from pydantic import SecretStr
from structlog.types import EventDict, Processor, WrappedLogger

MASK = "***"

# Nomes de campo comparados só com letras e dígitos minúsculos: "clientSecret", "client_secret"
# e "client-secret" viram "clientsecret". Sensível = nome exato ou terminação abaixo
# (ex.: "accessToken", mas não "token_expires_at").
_SENSITIVE_NAMES = frozenset({"authorization", "cookie", "setcookie"})
_SENSITIVE_SUFFIXES = ("secret", "password", "passwd", "token", "appkey", "apikey")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]")

_TEXT_PATTERNS = (
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]+=*"), f"Bearer {MASK}"),
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]*):([^/\s@]+)@"), rf"\1\2:{MASK}@"),
    # Hash SHA-256 da app key (o guia manda proteger também o hash): em qualquer texto, inclusive
    # quando vem sozinho num dict de parâmetros ({"key": "<sha256>"}). O serviço não loga outros
    # hashes de 64 hex; uniqueEventId é UUID e não casa.
    (re.compile(r"(?i)(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])"), MASK),
    (re.compile(r"(?i)(x-app-key[\"']?\s*[:=]\s*[\"']?)[^\s\"',}]+"), rf"\g<1>{MASK}"),
)

_LIBRARY_LOGGERS = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "websockets",
    "aio_pika",
    "aiormq",
)
# Só WARNING para cima: o log INFO do httpx grava a URL completa, com a query string
# (pode ter primary_key ou outros filtros do painel).
_QUIET_LOGGERS = ("httpx", "httpcore")


def _is_sensitive(key: str) -> bool:
    name = _NON_ALNUM_RE.sub("", key.lower())
    return name in _SENSITIVE_NAMES or name.endswith(_SENSITIVE_SUFFIXES)


def _redact_text(text: str) -> str:
    for pattern, replacement in _TEXT_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


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
    """Configura structlog e o logging padrão. Chamar uma vez, no início do entrypoint.

    A saída legível (``json_output=False``) só é aceita em desenvolvimento.
    """
    if not json_output and environment != "desenvolvimento":
        raise ValueError("LOG_JSON_OUTPUT=false só é permitido em APP_ENVIRONMENT=desenvolvimento")
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
        # show_locals=False: o padrão do structlog serializa as variáveis locais de cada frame.
        shared.append(
            structlog.processors.ExceptionRenderer(
                structlog.tracebacks.ExceptionDictTransformer(show_locals=False, use_rich=False)
            )
        )
        renderer = structlog.processors.JSONRenderer()
    else:
        # Traceback em texto simples (sem locais) antes da máscara; o renderer só imprime.
        shared.append(structlog.processors.format_exc_info)
        renderer = structlog.dev.ConsoleRenderer(
            colors=False, exception_formatter=structlog.dev.plain_traceback
        )
    # A máscara roda por último, depois de os tracebacks virarem dados ou texto.
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
    for name in _QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.stdlib.get_logger(name)


@contextmanager
def bind_context(**fields: Any) -> Iterator[None]:
    """Adiciona campos de correlação a todos os logs do bloco (seguro com asyncio)."""
    with structlog.contextvars.bound_contextvars(**fields):
        yield


P = ParamSpec("P")
R = TypeVar("R")


async def run_in_executor(
    executor: Executor | None, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
) -> R:
    """Como ``loop.run_in_executor``, mas levando o contexto de correlação para a thread.

    ``loop.run_in_executor`` não copia contextvars; sem isso, os logs do adapter Oracle
    perderiam ``unique_event_id``/``chain_code`` (RF-13).
    """
    context = contextvars.copy_context()
    call = functools.partial(context.run, func, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(executor, call)
