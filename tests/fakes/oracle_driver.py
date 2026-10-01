"""Driver Oracle falso e roteirizado para os testes unitários do adapter.

Não simula SQL: cada comando (o texto exato da constante do adapter) tem um *handler* que o
teste define, e todas as chamadas ficam registradas em ``FakeOracle.calls`` para conferir a
ordem (ex.: barreira de epoch por último, rollback depois de erro). A semântica real do
Oracle é coberta pelos cenários de ``tests/stores`` contra um banco de teste.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import oracledb


@dataclass
class FakeDriverError:
    """Mesmos atributos de ``oracledb._Error`` usados pelo adapter."""

    code: int
    full_code: str
    message: str
    isrecoverable: bool = False
    offset: int = 0


def ora(
    code: int, text: str = "erro", *, offset: int = 0, recoverable: bool = False
) -> FakeDriverError:
    full_code = f"ORA-{code:05d}"
    return FakeDriverError(code, full_code, f"{full_code}: {text}", recoverable, offset)


def driver(full_code: str, text: str = "erro") -> FakeDriverError:
    return FakeDriverError(0, full_code, f"{full_code}: {text}")


def db_error(
    detail: FakeDriverError, cls: type[oracledb.Error] = oracledb.DatabaseError
) -> oracledb.Error:
    return cls(detail)


def unique_violation(constraint: str, *, offset: int = 0) -> FakeDriverError:
    return ora(1, f"unique constraint (OHIP_APP.{constraint}) violated", offset=offset)


class FakeVar:
    def __init__(self, typ: Any) -> None:
        self.typ = typ
        self.values: list[Any] = []

    def getvalue(self) -> list[Any]:
        return self.values


@dataclass
class Call:
    kind: str  # execute | executemany | commit | rollback | acquire | release | drop
    statement: str = ""
    parameters: Any = None
    input_sizes: dict[str, Any] = field(default_factory=dict)


Handler = Callable[["FakeCursor", Any], None]


class FakeCursor:
    def __init__(self, oracle: FakeOracle, connection: FakeConnection) -> None:
        self._oracle = oracle
        self.connection = connection
        self.rowcount = 0
        self.arraysize = 100
        self.outputtypehandler: Any = None
        self.rows: list[tuple[Any, ...]] = []
        self.batch_errors: list[FakeDriverError] = []
        self._input_sizes: dict[str, Any] = {}
        self.closed = False

    def setinputsizes(self, *args: Any, **kwargs: Any) -> None:
        self._input_sizes = dict(kwargs)

    def var(self, typ: Any, *args: Any, **kwargs: Any) -> FakeVar:
        return FakeVar(typ)

    def execute(self, statement: str, parameters: Any = None) -> None:
        self._run("execute", statement, parameters)

    def executemany(self, statement: str, parameters: Any, *, batcherrors: bool = False) -> None:
        self.batch_errors = []
        self._run("executemany", statement, parameters)
        if not batcherrors and self.batch_errors:
            raise db_error(self.batch_errors[0])

    def getbatcherrors(self) -> list[FakeDriverError]:
        return self.batch_errors

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows.pop(0) if self.rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows, self.rows = self.rows, []
        return rows

    def close(self) -> None:
        self.closed = True

    def _run(self, kind: str, statement: str, parameters: Any) -> None:
        self._oracle.calls.append(Call(kind, statement, parameters, self._input_sizes))
        self._input_sizes = {}
        self.rowcount = 0
        self.rows = []
        handler = self._oracle.handlers.get(statement)
        if handler is None:
            self.rowcount = len(parameters) if kind == "executemany" else 1
            return
        handler(self, parameters)


class FakeConnection:
    def __init__(self, oracle: FakeOracle, number: int) -> None:
        self._oracle = oracle
        self.number = number
        self.call_timeout = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._oracle, self)

    def commit(self) -> None:
        self._oracle.calls.append(Call("commit"))
        if self._oracle.fail_commit:
            raise self._oracle.fail_commit.pop(0)

    def rollback(self) -> None:
        self._oracle.calls.append(Call("rollback"))
        if self._oracle.fail_rollback:
            raise self._oracle.fail_rollback.pop(0)

    def close(self) -> None:
        pass


class FakeOracle:
    """Pool + registro de chamadas + handlers por comando."""

    def __init__(self) -> None:
        self.handlers: dict[str, Handler] = {}
        self.calls: list[Call] = []
        self.fail_commit: list[oracledb.Error] = []
        self.fail_rollback: list[oracledb.Error] = []
        self.fail_acquire: list[oracledb.Error] = []
        self.connections = 0

    def on(self, statement: str, handler: Handler) -> None:
        self.handlers[statement] = handler

    def rows(self, statement: str, rows: list[tuple[Any, ...]]) -> None:
        def handler(cursor: FakeCursor, _parameters: Any) -> None:
            cursor.rows = list(rows)

        self.on(statement, handler)

    def rowcount(self, statement: str, count: int) -> None:
        def handler(cursor: FakeCursor, _parameters: Any) -> None:
            cursor.rowcount = count

        self.on(statement, handler)

    def fail(self, statement: str, error: oracledb.Error) -> None:
        def handler(_cursor: FakeCursor, _parameters: Any) -> None:
            raise error

        self.on(statement, handler)

    # ----------------------------------------------------------------- pool

    def acquire(self) -> FakeConnection:
        if self.fail_acquire:
            raise self.fail_acquire.pop(0)
        self.connections += 1
        self.calls.append(Call("acquire", parameters=self.connections))
        return FakeConnection(self, self.connections)

    def release(self, connection: FakeConnection) -> None:
        self.calls.append(Call("release", parameters=connection.number))

    def drop(self, connection: FakeConnection) -> None:
        self.calls.append(Call("drop", parameters=connection.number))

    def close(self, force: bool = False) -> None:
        pass

    # ----------------------------------------------------------------- consultas

    def kinds(self) -> list[str]:
        return [c.kind for c in self.calls]

    def statements(self) -> list[str]:
        return [c.statement for c in self.calls if c.kind in ("execute", "executemany")]

    def last(self, statement: str) -> Call:
        return next(c for c in reversed(self.calls) if c.statement == statement)

    def count(self, statement: str) -> int:
        return sum(1 for c in self.calls if c.statement == statement)
