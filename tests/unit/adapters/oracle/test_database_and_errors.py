"""Classificação de erros do driver, sessões, transação e abertura do pool."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import oracledb
import pytest
from pydantic import SecretStr
from tests.fakes.oracle_driver import FakeOracle, db_error, driver, ora, unique_violation

from ohip_streaming.adapters.oracle import database
from ohip_streaming.adapters.oracle.database import (
    DedicatedSession,
    PooledSession,
    new_cursor,
    query,
    transaction,
)
from ohip_streaming.adapters.oracle.errors import (
    is_dedup_violation,
    is_unavailable,
    translate,
    violated_constraint,
)
from ohip_streaming.adapters.oracle.rows import from_db, to_db, truncate_bytes
from ohip_streaming.application.errors import (
    BatchFailedError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.config import OracleSettings

# ------------------------------------------------------------------ classificação


NETWORK_AND_SESSION = [3113, 3114, 3135, 12541, 12514, 1033, 1034, 1089, 28, 2396, 3156, 25402]
NO_RESOURCES = [1653, 1654, 1688, 1691, 30036, 1536, 1652, 257, 4031]
NO_RESOURCES += [1628, 1631, 1632, 1650, 1651, 1658, 1659, 1683, 1692, 1000, 4030]


@pytest.mark.parametrize("code", NETWORK_AND_SESSION + NO_RESOURCES)
def test_unavailable_ora_codes(code: int) -> None:
    assert is_unavailable(ora(code))
    assert isinstance(translate(db_error(ora(code)), BatchFailedError), StoreUnavailableError)


@pytest.mark.parametrize("full_code", ["DPI-1080", "DPI-1010", "DPI-1067", "DPY-4011", "DPY-1001"])
def test_unavailable_driver_codes(full_code: str) -> None:
    assert is_unavailable(driver(full_code))


@pytest.mark.parametrize("code", [1, 600, 12899, 1400, 2291, 904])
def test_other_errors_are_failures(code: int) -> None:
    assert not is_unavailable(ora(code))
    translated = translate(db_error(ora(code, "detalhe\nhttps://docs")), BatchFailedError)
    assert type(translated) is BatchFailedError
    assert str(translated) == f"ORA-{code:05d}: detalhe"


def test_recoverable_flag_counts_as_unavailable() -> None:
    assert is_unavailable(ora(600, recoverable=True))


def test_error_without_driver_detail_is_unavailable() -> None:
    translated = translate(oracledb.InterfaceError("fechada"), StoreOperationError)
    assert isinstance(translated, StoreUnavailableError)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "ORA-00001: unique constraint (OHIP_APP.OHIP_EVENT_RAW_UQ_EVT) violated",
            "OHIP_EVENT_RAW_UQ_EVT",
        ),
        ("ORA-00001: unique constraint (ohip_event_raw_uq_off) violated", "OHIP_EVENT_RAW_UQ_OFF"),
        ("ORA-00001: unique constraint violated", None),
    ],
)
def test_violated_constraint(message: str, expected: str | None) -> None:
    error = ora(1)
    error.message = message
    assert violated_constraint(error) == expected


def test_dedup_violation_only_for_dedup_constraints() -> None:
    assert is_dedup_violation(unique_violation("OHIP_EVENT_RAW_UQ_EVT"))
    assert is_dedup_violation(unique_violation("OHIP_EVENT_RAW_UQ_OFF"))
    assert not is_dedup_violation(unique_violation("OHIP_EVENT_RAW_PK"))
    assert not is_dedup_violation(ora(12899))


def test_empty_message_uses_code() -> None:
    error = ora(600)
    error.message = ""
    assert str(translate(db_error(error), BatchFailedError)) == "ORA-00600"


# ------------------------------------------------------------------ conversões


def test_datetime_conversions() -> None:
    aware = datetime(2026, 10, 1, 9, 0, tzinfo=timezone(timedelta(hours=-3)))
    utc = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    db_value = utc.replace(tzinfo=None)  # o que o Oracle guarda e devolve
    assert to_db(aware) == db_value
    assert to_db(None) is None
    assert from_db(db_value) == utc
    assert from_db(None) is None
    with pytest.raises(ValueError, match="sem fuso"):
        to_db(datetime(2026, 10, 1, tzinfo=UTC).replace(tzinfo=None))


def test_truncate_bytes_never_splits_a_character() -> None:
    text = "ç" * 3000  # 6000 bytes
    cut = truncate_bytes(text, 4000)
    assert len(cut.encode()) <= 4000
    assert cut.endswith("...")
    assert truncate_bytes("curto", 10) == "curto"


# ------------------------------------------------------------------ sessões e transação


def test_new_cursor_reads_clob_as_text() -> None:
    oracle = FakeOracle()
    cursor = new_cursor(oracle.acquire())
    handler = cursor.outputtypehandler
    clob = SimpleNamespace(type_code=oracledb.DB_TYPE_CLOB)
    number = SimpleNamespace(type_code=oracledb.DB_TYPE_NUMBER)
    assert handler(cursor, clob).typ is oracledb.DB_TYPE_LONG
    assert handler(cursor, number) is None


def test_query_translates_errors() -> None:
    oracle = FakeOracle()
    oracle.fail("SELECT 1 FROM dual", db_error(ora(904, "invalid identifier")))
    with pytest.raises(StoreOperationError):
        query(oracle.acquire(), "SELECT 1 FROM dual", {}, failure=StoreOperationError)


def test_transaction_translates_and_rolls_back() -> None:
    oracle = FakeOracle()
    connection = oracle.acquire()

    def body(_cursor: Any) -> None:
        raise db_error(ora(1400, "cannot insert NULL"))

    with pytest.raises(StoreOperationError, match="ORA-01400"):
        transaction(connection, body, failure=StoreOperationError)
    assert oracle.kinds()[-1] == "rollback"


def test_transaction_rolls_back_on_unexpected_python_errors() -> None:
    oracle = FakeOracle()
    connection = oracle.acquire()

    def body(cursor: Any) -> None:
        cursor.execute("INSERT pendente")
        raise KeyError("bug no meio do corpo")

    with pytest.raises(KeyError):
        transaction(connection, body, failure=StoreOperationError)
    assert oracle.kinds()[-2:] == ["execute", "rollback"]  # nada fica pendente na conexão


async def test_dedicated_session_discards_connection_after_unexpected_error() -> None:
    oracle = FakeOracle()
    with ThreadPoolExecutor(1) as executor:
        session = DedicatedSession(oracle, call_timeout_ms=1, executor=executor)

        def bug(_connection: Any) -> None:
            raise KeyError("x")

        with pytest.raises(KeyError):
            await session.run(bug)
        assert oracle.kinds()[-1] == "drop"

        def refused(_connection: Any) -> None:
            raise BatchFailedError("recusado")

        with pytest.raises(BatchFailedError):
            await session.run(refused)
        assert oracle.kinds()[-1] == "acquire"  # erro da aplicação: conexão mantida
        assert oracle.connections == 2


async def test_pooled_session_releases_or_drops() -> None:
    oracle = FakeOracle()
    session = PooledSession(oracle, call_timeout_ms=1234)

    assert await session.run(lambda connection: connection.call_timeout) == 1234
    assert oracle.kinds()[-1] == "release"

    def unavailable(_connection: Any) -> None:
        raise StoreUnavailableError("fora")

    with pytest.raises(StoreUnavailableError):
        await session.run(unavailable)
    assert oracle.kinds()[-1] == "drop"

    def other(_connection: Any) -> None:
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError):
        await session.run(other)
    assert oracle.kinds()[-1] == "release"


async def test_acquire_failure_is_unavailable() -> None:
    oracle = FakeOracle()
    oracle.fail_acquire = [db_error(driver("DPY-4005", "pool timeout"))]
    with pytest.raises(StoreUnavailableError):
        await PooledSession(oracle, call_timeout_ms=1).run(lambda _c: None)


async def test_dedicated_session_keeps_one_connection() -> None:
    oracle = FakeOracle()
    with ThreadPoolExecutor(1) as executor:
        session = DedicatedSession(oracle, call_timeout_ms=1, executor=executor)
        await session.run(lambda _c: None)
        await session.run(lambda _c: None)
        assert oracle.connections == 1
        session.close()
        assert oracle.kinds()[-1] == "release"
        session.close()  # idempotente


async def test_dedicated_session_survives_drop_failure() -> None:
    class DropFails(FakeOracle):
        def drop(self, connection: Any) -> None:
            raise db_error(ora(3113))

    oracle = DropFails()
    with ThreadPoolExecutor(1) as executor:
        session = DedicatedSession(oracle, call_timeout_ms=1, executor=executor)

        def unavailable(_connection: Any) -> None:
            raise StoreUnavailableError("fora")

        with pytest.raises(StoreUnavailableError):
            await session.run(unavailable)
        await session.run(lambda _c: None)  # reconecta mesmo assim
        assert oracle.connections == 2


async def test_pooled_session_survives_drop_failure() -> None:
    class DropFails(FakeOracle):
        def drop(self, connection: Any) -> None:
            raise db_error(ora(3113))

    def unavailable(_connection: Any) -> None:
        raise StoreUnavailableError("fora")

    with pytest.raises(StoreUnavailableError):
        await PooledSession(DropFails(), call_timeout_ms=1).run(unavailable)


def test_open_pool_initializes_thick_mode_once(monkeypatch: pytest.MonkeyPatch) -> None:
    inits: list[str | None] = []
    pools: list[dict[str, Any]] = []
    monkeypatch.setattr(database, "_client_initialized", False)
    monkeypatch.setattr(oracledb, "init_oracle_client", lambda lib_dir=None: inits.append(lib_dir))

    def create_pool(**kwargs: Any) -> FakeOracle:
        pools.append(kwargs)
        return FakeOracle()

    monkeypatch.setattr(oracledb, "create_pool", create_pool)
    settings = OracleSettings(
        dsn="db:1521/svc",
        user="ohip_app",
        password=SecretStr("segredo"),
        client_lib_dir=Path("/opt/oracle/ic19"),
    )

    database.open_pool(settings)
    database.open_pool(settings)

    assert inits == ["/opt/oracle/ic19"]  # modo Thick, uma vez por processo
    assert pools[0]["password"] == "segredo"
    assert pools[0]["getmode"] == oracledb.POOL_GETMODE_TIMEDWAIT
    assert pools[0]["wait_timeout"] == 10_000
