"""Sessões, transação e conexão do adapter PostgreSQL com driver falso (ADR-0021 §2-§3)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import errors as pg_errors
from pydantic import SecretStr
from tests.fakes.postgres_driver import FakeConnection, FakePool, Result

from ohip_streaming.adapters.postgres import database as db
from ohip_streaming.application.errors import (
    LeaseLostError,
    NotFoundError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.config import Environment, PostgresSettings


def settings(**kw: Any) -> PostgresSettings:
    values: dict[str, Any] = {
        "host": "db",
        "db": "ohip",
        "user": "ohip_app",
        "password": SecretStr("segredo"),
    }
    values.update(kw)
    return PostgresSettings(**values)


def test_connection_kwargs() -> None:
    kwargs = db.connection_kwargs(
        settings(sslrootcert=Path("/etc/ca.pem")),
        environment=Environment.PRODUCAO,
        application_name="ohip-consumer",
    )
    assert kwargs["autocommit"] is True
    assert kwargs["options"] == "-c TimeZone=UTC -c statement_timeout=60000"
    assert (kwargs["sslmode"], kwargs["sslrootcert"]) == ("verify-full", "/etc/ca.pem")
    assert kwargs["application_name"] == "ohip-consumer"
    api = db.connection_kwargs(
        settings(), environment=Environment.PRODUCAO, application_name="x", statement_timeout_ms=0
    )
    assert api["options"].endswith("statement_timeout=0")
    assert "sslrootcert" not in api


@pytest.mark.parametrize("mode", ["disable", "allow", "prefer", "require"])
def test_unverified_tls_only_in_development(mode: str) -> None:
    """RNF-07: fora de desenvolvimento, só TLS com o certificado do servidor conferido."""
    weak = settings(sslmode=mode)
    for environment in (Environment.HOMOLOGACAO, Environment.PRODUCAO):
        with pytest.raises(ValueError, match="verify-full ou verify-ca"):
            db.connection_kwargs(weak, environment=environment, application_name="x")
    kwargs = db.connection_kwargs(
        weak, environment=Environment.DESENVOLVIMENTO, application_name="x"
    )
    assert kwargs["sslmode"] == mode


def test_verified_tls_is_accepted_everywhere() -> None:
    for mode in ("verify-ca", "verify-full"):
        kwargs = db.connection_kwargs(
            settings(sslmode=mode), environment=Environment.PRODUCAO, application_name="x"
        )
        assert kwargs["sslmode"] == mode


def test_transaction_commits_and_leaves_no_transaction_open() -> None:
    connection = FakeConnection(lambda sql, params: [(1,)])
    assert db.transaction(
        connection,
        lambda cursor: cursor.execute("SELECT 1").fetchone(),
        failure=StoreOperationError,
    ) == (1,)
    assert [entry[0] for entry in connection.log] == ["BEGIN", "execute", "COMMIT"]
    assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE


def test_transaction_rolls_back_and_translates() -> None:
    connection = FakeConnection(lambda sql, params: pg_errors.DiskFull("disco"))
    with pytest.raises(StoreUnavailableError, match="53100"):
        db.transaction(connection, lambda c: c.execute("INSERT"), failure=StoreOperationError)
    assert connection.log[-1] == ("ROLLBACK", 0)

    other = FakeConnection(lambda sql, params: pg_errors.UndefinedTable("x"))
    with pytest.raises(StoreOperationError, match="42P01"):
        db.transaction(other, lambda c: c.execute("INSERT"), failure=StoreOperationError)


def test_transaction_keeps_application_and_python_errors() -> None:
    connection = FakeConnection()

    def not_found(cursor: Any) -> None:
        cursor.execute("UPDATE")
        raise NotFoundError("x")

    with pytest.raises(NotFoundError):
        db.transaction(connection, not_found, failure=StoreOperationError)

    def bug(cursor: Any) -> None:
        raise KeyError("bug")

    with pytest.raises(KeyError):
        db.transaction(connection, bug, failure=StoreOperationError)
    assert [e[0] for e in connection.log if e[0] in {"COMMIT", "ROLLBACK"}] == [
        "ROLLBACK",
        "ROLLBACK",
    ]


def test_cursor_on_a_closed_connection_is_translated() -> None:
    class Closed(FakeConnection):
        def cursor(self) -> Any:
            raise psycopg.OperationalError("the connection is closed")

    with pytest.raises(StoreUnavailableError):
        db.query(Closed(), "SELECT", {}, failure=StoreOperationError)
    with pytest.raises(StoreUnavailableError):
        db.transaction(Closed(), lambda c: None, failure=StoreOperationError)


def test_query_runs_outside_a_transaction() -> None:
    connection = FakeConnection(lambda sql, params: [(1,), (2,)])
    assert db.query(connection, "SELECT", {}, failure=StoreOperationError) == [(1,), (2,)]
    assert [entry[0] for entry in connection.log] == ["execute"]  # sem BEGIN
    broken = FakeConnection(lambda sql, params: psycopg.OperationalError("caiu"))
    with pytest.raises(StoreUnavailableError):
        db.query(broken, "SELECT", {}, failure=StoreOperationError)


def test_executemany_checks_one_result_per_row() -> None:
    connection = FakeConnection(lambda sql, params: [(params["n"],)] if params["n"] else [])
    cursor = connection.cursor()
    rows = [{"n": 1}, {"n": 0}, {"n": 3}]
    assert db.executemany(cursor, "INSERT", rows, returning=True) == [[(1,)], [], [(3,)]]
    assert db.executemany(cursor, "INSERT", rows) == []

    cursor.results = lambda: iter([])  # type: ignore[method-assign]
    with pytest.raises(psycopg.ProgrammingError, match="0 resultados para 1"):
        db.executemany(cursor, "INSERT", [{"n": 1}], returning=True)


def test_fence() -> None:
    ok = FakeConnection(lambda sql, params: Result(rowcount=1))
    db.fence(ok.cursor(), "consumer:X", 3)
    assert ok.log[-1][2] == {"lease_name": "consumer:X", "epoch": 3}
    lost = FakeConnection(lambda sql, params: Result(rowcount=0))
    with pytest.raises(LeaseLostError):
        db.fence(lost.cursor(), "consumer:X", 3)


def test_savepoint_rolls_back_only_the_block() -> None:
    connection = FakeConnection(lambda sql, params: pg_errors.StringDataRightTruncation("x"))
    with connection.transaction():
        cursor = connection.cursor()
        with pytest.raises(pg_errors.StringDataRightTruncation), db.savepoint(cursor):
            cursor.execute("INSERT")
        assert not connection.aborted
    assert [entry[0] for entry in connection.log] == [
        "BEGIN",
        "SAVEPOINT",
        "execute",
        "ROLLBACK TO",
        "COMMIT",
    ]


async def test_dedicated_session_keeps_the_connection_and_discards_on_failure() -> None:
    first, second = FakeConnection(), FakeConnection()
    pool = FakePool([first, second])
    with ThreadPoolExecutor(1) as executor:
        session = db.DedicatedSession(pool, executor=executor)
        assert await session.run(lambda c: c) is first
        assert await session.run(lambda c: c) is first

        def app_error(c: Any) -> None:
            raise NotFoundError("x")

        with pytest.raises(NotFoundError):
            await session.run(app_error)
        assert await session.run(lambda c: c) is first  # erro da aplicação: conexão boa

        def unavailable(c: Any) -> None:
            raise StoreUnavailableError("caiu")

        with pytest.raises(StoreUnavailableError):
            await session.run(unavailable)
        assert first.closed
        assert pool.returned == [first]
        assert await session.run(lambda c: c) is second

        def bug(c: Any) -> None:
            raise KeyError("bug")

        with pytest.raises(KeyError):
            await session.run(bug)
        assert second.closed
        session.close()  # sem conexão: nada a devolver
        await session.run(lambda c: c)
        session.close()
    assert len(pool.returned) == 3


async def test_pooled_and_inline_sessions() -> None:
    pool = FakePool()
    pooled = db.PooledSession(pool)
    assert isinstance(await pooled.run(lambda c: c), FakeConnection)
    assert isinstance(await db.InlineSession(pooled).run(lambda c: c), FakeConnection)

    def unavailable(c: Any) -> None:
        raise StoreUnavailableError("caiu")

    with pytest.raises(StoreUnavailableError):
        pooled.call(unavailable)
    assert pool.returned[-1].closed  # descartada

    def other(c: Any) -> None:
        raise StoreOperationError("x")

    with pytest.raises(StoreOperationError):
        pooled.call(other)
    kept = pool.returned[-1]
    assert not kept.closed  # devolvida inteira

    pool.error = TimeoutError("pool esgotado")
    with pytest.raises(StoreUnavailableError):
        pooled.call(lambda c: c)


def test_drop_tolerates_a_lost_connection() -> None:
    class Lost(FakeConnection):
        def close(self) -> None:
            raise psycopg.OperationalError("já perdida")

    class BadPool(FakePool):
        def putconn(self, conn: Any) -> None:
            raise RuntimeError("pool fechado")

    db._drop(BadPool(), Lost())  # só loga
