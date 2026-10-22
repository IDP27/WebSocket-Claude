"""Classificação de erros por SQLSTATE e conversões de linha do adapter PostgreSQL (ADR-0021)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import psycopg
import pytest
from psycopg import errors as pg_errors
from psycopg_pool import PoolTimeout
from tests.fakes.postgres_driver import unique_violation

from ohip_streaming.adapters.postgres import errors, rows
from ohip_streaming.application.errors import BatchFailedError, StoreUnavailableError

UNAVAILABLE = [
    pg_errors.DiskFull("disco"),  # 53100
    pg_errors.OutOfMemory("memória"),  # 53200
    pg_errors.TooManyConnections("conexões"),  # 53300
    pg_errors.ConnectionFailure("rede"),  # 08006
    pg_errors.IoError("e/s"),  # 58030
    pg_errors.AdminShutdown("desligando"),  # 57P01
    pg_errors.CrashShutdown("crash"),  # 57P02
    pg_errors.CannotConnectNow("subindo"),  # 57P03
    pg_errors.IdleSessionTimeout("ociosa"),  # 57P05
    pg_errors.QueryCanceled("statement_timeout"),  # 57014
    pg_errors.SerializationFailure("serialização"),  # 40001
    pg_errors.DeadlockDetected("deadlock"),  # 40P01
    pg_errors.ReadOnlySqlTransaction("standby"),  # 25006
    pg_errors.IdleInTransactionSessionTimeout("ociosa em transação"),  # 25P03
    pg_errors.LockNotAvailable("lock"),  # 55P03
    psycopg.OperationalError("conexão perdida"),  # sem SQLSTATE
    psycopg.InterfaceError("conexão fechada"),
    PoolTimeout("pool esgotado"),
]


@pytest.mark.parametrize("exc", UNAVAILABLE, ids=lambda e: errors.error_code(e))
def test_unavailable_errors(exc: BaseException) -> None:
    assert errors.is_unavailable(exc)
    assert not errors.is_row_data_error(exc)
    translated = errors.translate(exc, BatchFailedError)
    assert isinstance(translated, StoreUnavailableError)


@pytest.mark.parametrize(
    "exc",
    [
        pg_errors.StringDataRightTruncation("value too long"),  # 22001
        pg_errors.NotNullViolation("null"),  # 23502
        pg_errors.CheckViolation("check"),  # 23514
        psycopg.DataError("PostgreSQL text fields cannot contain NUL (0x00) bytes"),
        UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogates not allowed"),
        unique_violation("ohip_event_raw_pk"),
    ],
    ids=lambda e: errors.error_code(e),
)
def test_row_data_errors(exc: BaseException) -> None:
    assert errors.is_row_data_error(exc)
    assert not errors.is_unavailable(exc)


def test_other_errors_are_failures() -> None:
    exc = pg_errors.UndefinedTable('relation "x" does not exist\nDETAIL: valor')
    assert not errors.is_unavailable(exc)
    assert not errors.is_row_data_error(exc)
    translated = errors.translate(exc, BatchFailedError)
    assert type(translated) is BatchFailedError
    assert str(translated) == '42P01: relation "x" does not exist'  # só a primeira linha
    assert not errors.is_row_data_error(ValueError("python"))


def test_dedup_constraints_by_name() -> None:
    for name in ("ohip_event_raw_uq_evt", "ohip_event_raw_uq_off"):
        exc = unique_violation(name)
        assert errors.violated_constraint(exc) == name
        assert errors.is_dedup_violation(exc)
    pk = unique_violation("ohip_event_raw_pk")
    assert not errors.is_dedup_violation(pk)  # conflito na PK nunca é duplicado (ADR-0022 §2)
    assert errors.violated_constraint(pg_errors.CheckViolation("x")) is None
    assert errors.violated_constraint(ValueError("x")) is None


def test_describe_and_code() -> None:
    assert errors.error_code(psycopg.DataError("NUL")) == "DataError"
    assert errors.describe(pg_errors.DiskFull("")) == "53100"
    surrogate = UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogates not allowed")
    assert errors.describe(surrogate).startswith("UnicodeEncodeError:")
    assert errors.sqlstate(ValueError("x")) is None


# ----------------------------------------------------------------------- linhas


def test_datetimes_are_utc_both_ways() -> None:
    local = datetime(2026, 10, 1, 9, 0, tzinfo=timezone(timedelta(hours=-3)))
    assert rows.to_db(local) == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert rows.to_db(None) is None
    with pytest.raises(ValueError, match="sem fuso"):
        rows.to_db(datetime(2026, 10, 1))  # noqa: DTZ001 - o teste é justamente sem fuso
    assert rows.from_db(local) == datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    assert rows.from_db(None) is None
    naive = datetime(2026, 10, 1, 12, 0)  # noqa: DTZ001 - só por erro de DDL
    assert rows.from_db_required(naive).tzinfo is UTC


def test_free_text_is_cleaned_and_cut_in_bytes() -> None:
    assert rows.clean_text("a\x00b\ud800c") == "a�b�c"
    assert rows.truncate_bytes("ação\x00", 100) == "ação�"
    cut = rows.truncate_bytes("é" * 10, 10)
    assert cut.endswith("...")
    assert len(cut.encode("utf-8")) <= 10
