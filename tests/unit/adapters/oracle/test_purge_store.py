"""``OraclePurgeStore`` com driver falso: SQL do expurgo e commit por lote (ADR-0020 §2)."""

from __future__ import annotations

import pytest
from tests.fakes.oracle_driver import FakeOracle

from ohip_streaming.adapters.oracle.database import PooledSession
from ohip_streaming.adapters.oracle.purge_store import OraclePurgeStore, count_sql, delete_sql
from ohip_streaming.application.ports import PurgeTarget


@pytest.fixture
def oracle() -> FakeOracle:
    return FakeOracle()


@pytest.fixture
def store(oracle: FakeOracle) -> OraclePurgeStore:
    return OraclePurgeStore(PooledSession(oracle, call_timeout_ms=300_000))


CUTOFF = "SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL(:days, 'DAY')"


def test_filters_keep_referenced_rows() -> None:
    dlq, outbox, failed, raw = (delete_sql(t) for t in PurgeTarget)
    assert "o.status = 'FAILED' AND o.created_at <" in failed
    assert "NOT EXISTS (SELECT 1 FROM ohip_dlq d WHERE d.outbox_id = o.id)" in failed
    assert dlq.startswith("DELETE FROM ohip_dlq d WHERE d.resolved_at < ")
    assert dlq.endswith(" AND ROWNUM <= :n")
    assert "o.status = 'SENT' AND o.sent_at <" in outbox
    assert "NOT EXISTS (SELECT 1 FROM ohip_dlq d WHERE d.outbox_id = o.id)" in outbox
    assert "NOT EXISTS (SELECT 1 FROM ohip_outbox o WHERE o.event_raw_id = r.id)" in raw
    assert "NOT EXISTS (SELECT 1 FROM ohip_dlq d WHERE d.event_raw_id = r.id)" in raw
    for target in PurgeTarget:
        assert CUTOFF in delete_sql(target)  # relógio do banco
        assert count_sql(target).startswith("SELECT COUNT(*) FROM ohip_")
        assert "ROWNUM" not in count_sql(target)


async def test_each_batch_is_its_own_transaction(
    store: OraclePurgeStore, oracle: FakeOracle
) -> None:
    oracle.rowcount(delete_sql(PurgeTarget.RAW), 5000)
    assert await store.purge_batch(PurgeTarget.RAW, 90, 5000) == 5000
    assert oracle.last(delete_sql(PurgeTarget.RAW)).parameters == {"days": 90, "n": 5000}
    assert oracle.kinds()[-2:] == ["commit", "release"]


async def test_count_expired(store: OraclePurgeStore, oracle: FakeOracle) -> None:
    oracle.rows(count_sql(PurgeTarget.DLQ), [(42,)])
    assert await store.count_expired(PurgeTarget.DLQ, 90) == 42
    assert oracle.last(count_sql(PurgeTarget.DLQ)).parameters == {"days": 90}
    assert "commit" not in oracle.kinds()
