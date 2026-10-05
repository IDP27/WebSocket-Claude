"""``OracleEnrichmentStore`` com driver falso: MERGE condicional e DLQ (ADR-0019)."""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from tests.fakes.frames import new_event
from tests.fakes.oracle_driver import FakeCursor, FakeOracle, db_error, ora
from tests.unit.adapters.oracle.test_monitoring_store import NAIVE

from ohip_streaming.adapters.oracle import enrichment_store as sql
from ohip_streaming.adapters.oracle.database import PooledSession
from ohip_streaming.adapters.oracle.enrichment_store import OracleEnrichmentStore, merge_sql
from ohip_streaming.adapters.oracle.operations_store import EVENT_BY_ID_SQL
from ohip_streaming.application.errors import (
    NotFoundError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import DlqStage, DomainWrite, ProcessingStatus


@pytest.fixture
def oracle() -> FakeOracle:
    return FakeOracle()


@pytest.fixture
def store(oracle: FakeOracle) -> OracleEnrichmentStore:
    return OracleEnrichmentStore(PooledSession(oracle, call_timeout_ms=5_000), code_version="v9")


def write(**overrides: Any) -> DomainWrite:
    values: dict[str, Any] = {
        "table": "OHIP_RSV_RESERVATION",
        "key": {"chain_code": "C1", "hotel_key": "H1", "primary_key": "123"},
        "values": {"status": "RESERVED", "arrival": "2026-10-11"},
        "source_event_raw_id": 7,
        "source_offset_num": 100,
    }
    values.update(overrides)
    return DomainWrite(**values)


def test_merge_is_conditional_on_the_offset() -> None:
    statement, params = merge_sql(write())
    assert "MERGE INTO ohip_rsv_reservation t" in statement
    assert "USING (SELECT :k1 AS chain_code, :k2 AS hotel_key, :k3 AS primary_key" in statement
    assert "ON (t.chain_code = s.chain_code AND t.hotel_key = s.hotel_key" in statement
    assert "UPDATE SET t.status = :v1, t.arrival = :v2," in statement
    assert "WHERE t.source_offset_num <= :source_offset_num" in statement
    assert (
        "INSERT (chain_code, hotel_key, primary_key, status, arrival, source_event_raw_id"
        in statement
    )
    assert "VALUES (s.chain_code, s.hotel_key, s.primary_key, :v1, :v2," in statement
    assert params == {
        "k1": "C1",
        "k2": "H1",
        "k3": "123",
        "v1": "RESERVED",
        "v2": "2026-10-11",
        "source_event_raw_id": 7,
        "source_offset_num": 100,
    }
    assert "RESERVED" not in statement  # valores só por bind


def test_thirty_character_columns_keep_binds_within_the_11g_limit() -> None:
    key_column, value_column = "k" * 30, "v" * 30  # limite exato do 11g
    statement, params = merge_sql(write(key={key_column: "C1"}, values={value_column: "x"}))
    binds = set(re.findall(r":([A-Za-z0-9_]+)", statement))
    assert binds == set(params)
    assert max(len(bind) for bind in binds) <= 30  # ORA-00972 acima disso
    assert f":k1 AS {key_column}" in statement


def test_null_key_is_refused() -> None:
    # t.col = NULL nunca casa: o MERGE inseriria uma linha nova a cada evento.
    with pytest.raises(StoreOperationError, match=r"chave nula.*\['hotel_key'\]"):
        merge_sql(write(key={"chain_code": "C1", "hotel_key": None, "primary_key": "1"}))


@pytest.mark.parametrize(
    "overrides",
    [
        {"table": "t; DROP TABLE x"},
        {"table": "a" * 31},  # limite do 11g
        {"key": {"chain code": "C1"}},
        {"values": {'status"': "x"}},
        {"key": {}},
        {"key": {"status": "1"}},  # chave repetida nos valores
        {"values": {"source_offset_num": 1}},
    ],
)
def test_invalid_writes_are_refused(overrides: dict[str, Any]) -> None:
    with pytest.raises(StoreOperationError):
        merge_sql(write(**overrides))


async def test_apply_merges_and_updates_status(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    newer, _ = merge_sql(write())
    older_write = write(table="OHIP_CRM_PROFILE")
    older, _ = merge_sql(older_write)
    oracle.rowcount(older, 0)  # existia com offset maior: a condição barrou
    skipped = await store.apply(7, [write(), older_write], ProcessingStatus.NORMALIZED)

    assert skipped == 1
    assert oracle.statements() == [newer, older, sql.UPDATE_STATUS_SQL]
    assert oracle.last(sql.UPDATE_STATUS_SQL).parameters == {
        "status": "NORMALIZED",
        "raw_event_id": 7,
    }
    assert oracle.kinds()[-2:] == ["commit", "release"]


async def test_apply_without_writes_only_marks_status(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    assert await store.apply(7, (), ProcessingStatus.UNMAPPED) == 0
    assert oracle.statements() == [sql.UPDATE_STATUS_SQL]


async def test_apply_rolls_back_when_raw_is_gone(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    oracle.rowcount(sql.UPDATE_STATUS_SQL, 0)
    with pytest.raises(NotFoundError):
        await store.apply(7, [write()], ProcessingStatus.NORMALIZED)
    assert "rollback" in oracle.kinds()
    assert "commit" not in oracle.kinds()


async def test_invalid_write_never_opens_a_transaction(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    with pytest.raises(StoreOperationError):
        await store.apply(7, [write(table="x y")], ProcessingStatus.NORMALIZED)
    assert oracle.calls == []


async def test_merge_race_is_an_operation_error(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    statement, _ = merge_sql(write())
    oracle.fail(statement, db_error(ora(1, "unique constraint (APP.OHIP_RSV_UK) violated")))
    with pytest.raises(StoreOperationError):  # conta tentativa; na próxima o MERGE casa
        await store.apply(7, [write()], ProcessingStatus.NORMALIZED)


async def test_database_down_is_unavailable(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    oracle.fail(sql.UPDATE_STATUS_SQL, db_error(ora(3113, "end-of-file")))
    with pytest.raises(StoreUnavailableError):
        await store.apply(7, (), ProcessingStatus.UNMAPPED)


async def test_add_dlq_copies_the_raw_and_marks_failed(
    store: OracleEnrichmentStore, oracle: FakeOracle
) -> None:
    await store.add_dlq(7, DlqStage.ENRICH, "ResourceRejectedError", "HTTP 403 " + "x" * 5000)
    assert oracle.statements() == [sql.INSERT_DLQ_SQL, sql.UPDATE_STATUS_SQL]
    params = oracle.last(sql.INSERT_DLQ_SQL).parameters
    assert (params["stage"], params["raw_event_id"], params["code_version"]) == ("ENRICH", 7, "v9")
    assert len(params["error_message"].encode()) <= 4000
    assert oracle.last(sql.UPDATE_STATUS_SQL).parameters["status"] == "FAILED"


async def test_add_dlq_for_missing_raw(store: OracleEnrichmentStore, oracle: FakeOracle) -> None:
    oracle.rowcount(sql.INSERT_DLQ_SQL, 0)
    with pytest.raises(NotFoundError):
        await store.add_dlq(7, DlqStage.NORMALIZE, "ValueError", "x")
    with pytest.raises(ValueError, match="NORMALIZE/ENRICH"):
        await store.add_dlq(7, DlqStage.PUBLISH, "x", "y")


async def test_get_event(store: OracleEnrichmentStore, oracle: FakeOracle) -> None:
    payload = json.dumps(new_event("100", "uid-7"))
    oracle.rows(EVENT_BY_ID_SQL, [(7, "C1", "sub", NAIVE, payload, "RECEIVED", NAIVE)])
    stored = await store.get_event(7)
    assert stored is not None
    assert stored.event.unique_event_id == "uid-7"

    def empty(cursor: FakeCursor, _: Any) -> None:
        cursor.rows = []

    oracle.on(EVENT_BY_ID_SQL, empty)
    assert await store.get_event(8) is None
