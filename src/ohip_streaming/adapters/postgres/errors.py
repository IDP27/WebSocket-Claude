"""Tradução dos erros do psycopg para os erros da aplicação (ADR-0021 §5, ADR-0009).

- **Indisponível** (``StoreUnavailableError``): conexão, servidor fora ou subindo, prazo do
  comando estourado, failover para standby, sessão encerrada por ociosidade, lock não obtido
  e **falta de recurso** (disco, memória, conexões). São falhas que atingiriam qualquer lote:
  o consumer envenena a conexão em vez de isolar mensagens na DLQ.
- **Duplicado**: ``23505`` numa das duas constraints de deduplicação do evento bruto.
- **Erro de dado da linha**: classes ``22``/``23`` e o que o driver recusa antes de enviar
  (NUL em texto, surrogate isolado). Vira DLQ no lote do consumer (ADR-0022).
- Qualquer outro erro: a operação foi recusada (``BatchFailedError`` no lote do consumer,
  ``StoreOperationError`` nas demais).

A classificação usa só o SQLSTATE e o ``diag`` do driver, nunca o texto da mensagem.
"""

from __future__ import annotations

from typing import Final

import psycopg
from psycopg_pool import PoolTimeout

from ohip_streaming.application.errors import ApplicationError, StoreUnavailableError

# Classes inteiras de SQLSTATE tratadas como indisponibilidade.
_UNAVAILABLE_CLASSES: Final = frozenset(
    {
        "08",  # conexão
        "53",  # falta de recurso: disco (53100), memória (53200), conexões (53300)
        "58",  # erro de E/S do servidor
    }
)
_UNAVAILABLE_STATES: Final = frozenset(
    {
        "57P01",  # servidor desligando (admin shutdown)
        "57P02",  # crash de outro processo do servidor
        "57P03",  # servidor ainda não aceita conexões
        "57P05",  # sessão ociosa encerrada (idle_session_timeout)
        "57014",  # statement_timeout estourado (equivale ao call_timeout do Oracle)
        "40001",  # falha de serialização (transiente)
        "40P01",  # deadlock (transiente)
        "25006",  # transação somente leitura: conexão num standby após failover
        "25P03",  # sessão encerrada por ociosidade em transação
        "55P03",  # lock não obtido (lock_timeout)
    }
)
DEDUP_CONSTRAINTS: Final = frozenset({"ohip_event_raw_uq_evt", "ohip_event_raw_uq_off"})
UNIQUE_VIOLATION: Final = "23505"


def sqlstate(exc: BaseException) -> str | None:
    return getattr(exc, "sqlstate", None) if isinstance(exc, psycopg.Error) else None


def is_unavailable(exc: BaseException) -> bool:
    if isinstance(exc, PoolTimeout):
        return True
    if not isinstance(exc, psycopg.Error):
        return False
    state = exc.sqlstate
    if state is None:
        # Sem SQLSTATE: o erro nasceu no cliente. Conexão perdida/fechada é indisponível;
        # dado recusado pelo driver (NUL em texto) é erro de dado.
        return isinstance(exc, psycopg.OperationalError | psycopg.InterfaceError)
    return state[:2] in _UNAVAILABLE_CLASSES or state in _UNAVAILABLE_STATES


def violated_constraint(exc: BaseException) -> str | None:
    """Nome da constraint de um ``23505`` (minúsculas, como o PostgreSQL devolve), ou None."""
    if sqlstate(exc) != UNIQUE_VIOLATION:
        return None
    name = exc.diag.constraint_name if isinstance(exc, psycopg.Error) else None
    return name.lower() if name else None


def is_dedup_violation(exc: BaseException) -> bool:
    return violated_constraint(exc) in DEDUP_CONSTRAINTS


def is_row_data_error(exc: BaseException) -> bool:
    """Erro causado pelos valores de uma linha (vai para a DLQ, ADR-0022)."""
    if isinstance(exc, UnicodeEncodeError):
        return True  # surrogate isolado: recusado na codificação, do lado do cliente
    if not isinstance(exc, psycopg.Error) or is_unavailable(exc):
        return False
    state = exc.sqlstate
    if state is None:
        return isinstance(exc, psycopg.DataError)  # ex.: NUL em texto, recusado pelo driver
    return state[:2] in {"22", "23"}


def error_code(exc: BaseException) -> str:
    """Código curto do erro: SQLSTATE, ou o nome do tipo quando nasceu no cliente."""
    return sqlstate(exc) or type(exc).__name__


def describe(exc: BaseException) -> str:
    """Código + primeira linha da mensagem (o servidor anexa detalhes nas linhas seguintes)."""
    if isinstance(exc, UnicodeEncodeError):
        return f"{type(exc).__name__}: texto com caractere que não codifica em UTF-8"
    text = str(exc).strip()
    first = text.splitlines()[0] if text else ""
    code = error_code(exc)
    return f"{code}: {first}" if first else code


def translate(exc: BaseException, failure: type[ApplicationError]) -> ApplicationError:
    """``StoreUnavailableError`` para indisponibilidade; ``failure`` para o resto."""
    if is_unavailable(exc):
        return StoreUnavailableError(describe(exc))
    return failure(describe(exc))
