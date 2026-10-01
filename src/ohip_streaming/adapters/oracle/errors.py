"""Tradução dos erros do driver para os erros da aplicação (ADR-0011 §7, ADR-0009).

- **Indisponível** (``StoreUnavailableError``): rede, sessão, instância fora, tempo de chamada
  estourado e **falta de recurso** (tablespace, cota, undo, temp, archiver). São falhas que
  atingiriam qualquer lote: o consumer envenena a conexão em vez de isolar mensagens na DLQ.
- **Duplicado**: ORA-00001 numa das duas constraints de deduplicação do evento bruto.
- Qualquer outro erro: a operação foi recusada (``BatchFailedError`` no lote do consumer,
  ``StoreOperationError`` nas demais).
"""

from __future__ import annotations

import re
from typing import Final, Protocol

import oracledb

from ohip_streaming.application.errors import ApplicationError, StoreUnavailableError

# ORA-nnnnn tratados como indisponibilidade.
_UNAVAILABLE_ORA: Final = frozenset(
    {
        # sessão, rede e instância
        28,  # sessão encerrada pelo DBA
        1012,  # not logged on
        1033,  # instância subindo ou descendo
        1034,  # instância indisponível
        1089,  # shutdown imediato em andamento
        1090,  # shutdown em andamento
        1092,  # instância encerrada
        2396,  # tempo ocioso máximo excedido
        3113,  # end-of-file on communication channel
        3114,  # not connected
        3135,  # connection lost contact
        3156,  # tempo de chamada excedido
        12153,
        12170,
        12514,
        12528,
        12537,
        12541,
        12543,
        12545,
        12547,
        12560,
        12571,
        25408,  # não é seguro repetir a chamada
        25402,  # a transação precisa ser desfeita (failover)
        # falta de recurso: falha sistêmica (base do disjuntor, ADR-0011 §7)
        257,  # archiver travado
        1000,  # cursores abertos esgotados (vazamento)
        1536,  # cota do tablespace excedida
        1628,  # máximo de extents (rollback segment)
        1631,  # máximo de extents (tabela)
        1632,  # máximo de extents (índice)
        1650,  # rollback segment
        1651,  # save undo
        1652,  # temp
        1653,  # tabela
        1654,  # índice
        1658,  # extent inicial
        1659,  # MINEXTENTS
        1683,  # partição de índice
        1688,  # partição de tabela
        1691,  # LOB
        1692,  # partição de LOB
        4030,  # memória do processo
        4031,  # shared pool
        30036,  # undo
    }
)
# Erros do driver (DPI-/DPY-) de conexão fechada ou tempo de chamada.
_UNAVAILABLE_DRIVER: Final = frozenset(
    {"DPI-1010", "DPI-1067", "DPI-1080", "DPY-1001", "DPY-4011", "DPY-4024"}
)
DEDUP_CONSTRAINTS: Final = frozenset({"OHIP_EVENT_RAW_UQ_EVT", "OHIP_EVENT_RAW_UQ_OFF"})
_CONSTRAINT_RE: Final = re.compile(r"\(\s*(?:[^.()]+\.)?([^.()]+)\s*\)")


class DriverError(Protocol):
    """O que usamos de ``oracledb._Error`` (também nos erros por linha de ``getbatcherrors``)."""

    @property
    def code(self) -> int: ...

    @property
    def full_code(self) -> str: ...

    @property
    def message(self) -> str: ...

    @property
    def isrecoverable(self) -> bool: ...


def driver_error(exc: oracledb.Error) -> DriverError | None:
    detail = exc.args[0] if exc.args else None
    return detail if hasattr(detail, "full_code") else None


def is_unavailable(error: DriverError) -> bool:
    if error.full_code in _UNAVAILABLE_DRIVER or error.isrecoverable:
        return True
    return error.full_code.startswith("ORA-") and error.code in _UNAVAILABLE_ORA


def violated_constraint(error: DriverError) -> str | None:
    """Nome da constraint de um ORA-00001 (sem o schema), ou None."""
    if error.full_code != "ORA-00001":
        return None
    match = _CONSTRAINT_RE.search(error.message)
    return match.group(1).upper() if match else None


def is_dedup_violation(error: DriverError) -> bool:
    return violated_constraint(error) in DEDUP_CONSTRAINTS


def describe(error: DriverError) -> str:
    """Código + primeira linha da mensagem (o driver anexa URL de ajuda nas linhas seguintes)."""
    return error.message.splitlines()[0] if error.message else error.full_code


def translate(exc: oracledb.Error, failure: type[ApplicationError]) -> ApplicationError:
    """``StoreUnavailableError`` para indisponibilidade; ``failure`` para o resto."""
    error = driver_error(exc)
    if error is None:  # sem detalhe do driver: trate como fora (descarta a conexão)
        return StoreUnavailableError(f"{type(exc).__name__}: {exc}")
    if is_unavailable(error):
        return StoreUnavailableError(describe(error))
    return failure(describe(error))
