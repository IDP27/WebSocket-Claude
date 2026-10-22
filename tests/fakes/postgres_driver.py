"""Driver falso roteirizado para os testes unitários do adapter PostgreSQL (ADR-0021 §9).

Imita o subconjunto do psycopg 3 que o adapter usa: ``connection.cursor()``,
``connection.transaction()`` (aninhado = savepoint), ``cursor.execute``/``executemany``
(com ``returning`` e ``results()``), ``fetchone``/``fetchall``/``rowcount`` e o estado da
transação. Cada comando passa por ``script(sql, params)``, que devolve as linhas (ou um
``rowcount``) ou levanta o erro do roteiro. ``log`` guarda tudo, inclusive savepoints,
commits e rollbacks, para conferir a ordem.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import psycopg

Script = Callable[[str, Any], Any]


@dataclass
class Result:
    """O que o roteiro devolve: linhas e ``rowcount``."""

    rows: list[tuple[Any, ...]] = field(default_factory=list)
    rowcount: int = -1


def unique_violation(constraint: str) -> psycopg.errors.UniqueViolation:
    """``23505`` com o nome da constraint no ``diag`` (como o servidor manda)."""

    class _Unique(psycopg.errors.UniqueViolation):
        @property
        def diag(self) -> Any:
            return SimpleNamespace(constraint_name=constraint)

    return _Unique(f'duplicate key value violates unique constraint "{constraint}"')


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.rowcount = -1
        self._rows: list[tuple[Any, ...]] = []
        self._results: list[list[tuple[Any, ...]]] = []
        self.closed = False

    def _run(self, sql: str, params: Any) -> Result:
        self.connection.log.append(("execute", sql, params))
        self.connection.touch()
        outcome = self.connection.script(sql, params)
        if isinstance(outcome, BaseException):
            self.connection.fail()
            raise outcome
        if outcome is None:
            return Result(rowcount=1)
        if isinstance(outcome, Result):
            return outcome
        rows = [tuple(row) for row in outcome]
        return Result(rows=rows, rowcount=len(rows))

    def execute(self, query: Any, params: Any = None) -> FakeCursor:
        result = self._run(str(query), params)
        self._rows, self.rowcount = list(result.rows), result.rowcount
        return self

    def executemany(self, query: Any, params_seq: Any, *, returning: bool = False) -> None:
        self._results = []
        for params in params_seq:
            result = self._run(str(query), params)
            if returning:
                self._results.append(list(result.rows))
        self.rowcount = len(self._results) if returning else -1

    def results(self) -> Iterator[Any]:
        for rows in self._results:
            yield SimpleNamespace(fetchall=lambda rows=rows: list(rows))

    def fetchone(self) -> Any:
        return self._rows.pop(0) if self._rows else None

    def fetchall(self) -> list[Any]:
        rows, self._rows = self._rows, []
        return rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Conexão com ``autocommit``: fora de ``transaction()`` cada comando é imediato."""

    def __init__(self, script: Script | None = None) -> None:
        self.script: Script = script or (lambda sql, params: None)
        self.log: list[tuple[Any, ...]] = []
        self.depth = 0
        self.aborted = False  # erro dentro do bloco: só rollback resolve
        self.closed = False
        self.info = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE)

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def touch(self) -> None:
        if self.aborted:
            raise psycopg.errors.InFailedSqlTransaction("transação abortada")

    def fail(self) -> None:
        if self.depth:
            self.aborted = True

    @contextmanager
    def transaction(self) -> Iterator[None]:
        name = "BEGIN" if self.depth == 0 else "SAVEPOINT"
        self.log.append((name, self.depth))
        self.depth += 1
        self.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        try:
            yield
        except BaseException:
            self.depth -= 1
            self.aborted = False
            self.log.append(("ROLLBACK" if name == "BEGIN" else "ROLLBACK TO", self.depth))
            self._settle()
            raise
        self.depth -= 1
        self.log.append(("COMMIT" if name == "BEGIN" else "RELEASE", self.depth))
        self._settle()

    def _settle(self) -> None:
        if self.depth == 0:
            self.info.transaction_status = psycopg.pq.TransactionStatus.IDLE

    def close(self) -> None:
        self.closed = True

    def statements(self) -> list[str]:
        """Só os comandos SQL, na ordem (primeira linha útil de cada um)."""
        return [_head(entry[1]) for entry in self.log if entry[0] == "execute"]


def _head(sql: str) -> str:
    return next(line.strip() for line in sql.strip().splitlines() if line.strip())


class FakePool:
    """Pool falso. Conexões novas usam as respostas de ``rows`` (SQL → linhas); o resto do
    roteiro é "afeta 1 linha". ``statements()`` junta os comandos de todas as conexões."""

    def __init__(self, connections: Sequence[FakeConnection] = ()) -> None:
        self._free = list(connections)
        self.given: list[FakeConnection] = []
        self.returned: list[FakeConnection] = []
        self.error: BaseException | None = None
        self.answers: dict[str, Any] = {}

    def rows(self, statement: str, rows: list[tuple[Any, ...]]) -> None:
        self.answers[statement] = rows

    def _script(self, statement: str, params: Any) -> Any:
        return self.answers.get(statement)

    def getconn(self, timeout: float | None = None) -> FakeConnection:
        if self.error is not None:
            raise self.error
        connection = self._free.pop(0) if self._free else FakeConnection(self._script)
        self.given.append(connection)
        return connection

    def putconn(self, conn: FakeConnection) -> None:
        self.returned.append(conn)

    def close(self, timeout: float = 5.0) -> None:
        pass

    def statements(self) -> list[str]:
        return [entry[1] for c in self.given for entry in c.log if entry[0] == "execute"]
