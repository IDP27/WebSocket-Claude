"""OracleLeaseStore com driver falso: SQL do ADR-0008, outro dono ou linha ausente."""

from __future__ import annotations

from typing import Any

import pytest
from tests.fakes.oracle_driver import FakeCursor, FakeOracle, db_error, ora

from ohip_streaming.adapters.oracle import lease_store as sql
from ohip_streaming.adapters.oracle.database import PooledSession
from ohip_streaming.adapters.oracle.lease_store import OracleLeaseStore
from ohip_streaming.application.errors import LeaseNotProvisionedError, StoreUnavailableError


@pytest.fixture
def oracle() -> FakeOracle:
    return FakeOracle()


@pytest.fixture
def store(oracle: FakeOracle) -> OracleLeaseStore:
    return OracleLeaseStore(PooledSession(oracle, call_timeout_ms=1000))


async def test_acquire_returns_new_epoch(store: OracleLeaseStore, oracle: FakeOracle) -> None:
    def returning(cursor: FakeCursor, params: Any) -> None:
        cursor.rowcount = 1
        params["epoch"].values = [8]

    oracle.on(sql.ACQUIRE_SQL, returning)
    assert await store.acquire("consumer:C1", "vm1:42:abc", 30) == 8
    params = oracle.last(sql.ACQUIRE_SQL).parameters
    assert (params["lease_name"], params["owner"], params["ttl_s"]) == (
        "consumer:C1",
        "vm1:42:abc",
        30,
    )
    assert oracle.kinds()[-2:] == ["commit", "release"]


async def test_acquire_with_other_owner(store: OracleLeaseStore, oracle: FakeOracle) -> None:
    oracle.rowcount(sql.ACQUIRE_SQL, 0)
    oracle.rows(sql.EXISTS_SQL, [(1,)])
    assert await store.acquire("publisher", "vm2", 30) is None


async def test_acquire_without_row(store: OracleLeaseStore, oracle: FakeOracle) -> None:
    oracle.rowcount(sql.ACQUIRE_SQL, 0)
    oracle.rows(sql.EXISTS_SQL, [(0,)])
    with pytest.raises(LeaseNotProvisionedError):
        await store.acquire("consumer:X", "vm1", 30)


async def test_renew_and_release(store: OracleLeaseStore, oracle: FakeOracle) -> None:
    assert await store.renew("publisher", "vm1", 3, 30)
    assert oracle.last(sql.RENEW_SQL).parameters == {
        "lease_name": "publisher",
        "owner": "vm1",
        "epoch": 3,
        "ttl_s": 30,
    }
    oracle.rowcount(sql.RENEW_SQL, 0)
    assert not await store.renew("publisher", "vm1", 3, 30)
    await store.release("publisher", "vm1", 3)
    assert oracle.last(sql.RELEASE_SQL).parameters == {
        "lease_name": "publisher",
        "owner": "vm1",
        "epoch": 3,
    }


async def test_database_down(store: OracleLeaseStore, oracle: FakeOracle) -> None:
    oracle.fail(sql.RENEW_SQL, db_error(ora(3113)))
    with pytest.raises(StoreUnavailableError):
        await store.renew("publisher", "vm1", 3, 30)
