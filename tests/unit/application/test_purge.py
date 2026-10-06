"""Expurgo por retenção: ordem, lotes, prazo, parada e dry run (ADR-0020 §2)."""

from __future__ import annotations

import asyncio

import pytest
from tests.fakes.memory import FakeClock

from ohip_streaming.application.errors import StoreUnavailableError
from ohip_streaming.application.ports import PurgeTarget
from ohip_streaming.application.use_cases.purge import (
    PurgeExpiredData,
    PurgeOptions,
    PurgeResult,
)

DLQ, OUTBOX, RAW = PurgeTarget.DLQ, PurgeTarget.OUTBOX, PurgeTarget.RAW
FAILED = PurgeTarget.OUTBOX_FAILED


class ScriptedStore:
    """Linhas expiradas por tabela; cada lote apaga até ``batch_size``."""

    def __init__(self, clock: FakeClock, **rows: int) -> None:
        self.clock = clock
        self.rows = {t: rows.get(t.value.lower(), 0) for t in PurgeTarget}
        self.calls: list[tuple[PurgeTarget, int, int]] = []
        self.batch_cost_s = 0.0
        self.fail: Exception | None = None

    async def purge_batch(self, target: PurgeTarget, retention_days: int, batch_size: int) -> int:
        if self.fail is not None:
            raise self.fail
        self.calls.append((target, retention_days, batch_size))
        self.clock.advance(self.batch_cost_s)
        deleted = min(batch_size, self.rows[target])
        self.rows[target] -= deleted
        return deleted

    async def count_expired(self, target: PurgeTarget, retention_days: int) -> int:
        self.calls.append((target, retention_days, 0))
        return self.rows[target]


def purge(store: ScriptedStore, **options: object) -> PurgeExpiredData:
    defaults: dict[str, object] = {"dry_run": False, "batch_size": 10, "pause_s": 1}
    defaults.update(options)
    return PurgeExpiredData(
        store=store,
        clock=store.clock,
        options=PurgeOptions(**defaults),  # type: ignore[arg-type]
    )


async def test_purges_in_foreign_key_order_and_batches() -> None:
    clock = FakeClock()
    store = ScriptedStore(clock, dlq=5, outbox=25, outbox_failed=1, raw=10)
    results = await purge(store, retention_days=120).execute(asyncio.Event())
    assert results == [
        PurgeResult(DLQ, 5, complete=True),
        PurgeResult(OUTBOX, 25, complete=True),
        PurgeResult(FAILED, 1, complete=True),
        PurgeResult(RAW, 10, complete=True),
    ]
    assert [(t, n) for t, _, n in store.calls] == [
        (DLQ, 10),
        (OUTBOX, 10),
        (OUTBOX, 10),
        (OUTBOX, 10),
        (FAILED, 10),
        (RAW, 10),
        (RAW, 10),  # lote cheio: confere se sobrou; o vazio encerra
    ]
    assert {days for _, days, _ in store.calls} == {120}
    assert clock.sleeps == [1, 1, 1]  # pausa só depois de lote cheio


async def test_dry_run_only_counts() -> None:
    store = ScriptedStore(FakeClock(), dlq=1, outbox=2, raw=3)
    results = await purge(store, dry_run=True).execute(asyncio.Event())
    assert [(r.target, r.rows) for r in results] == [(DLQ, 1), (OUTBOX, 2), (FAILED, 0), (RAW, 3)]
    assert store.rows == {DLQ: 1, OUTBOX: 2, FAILED: 0, RAW: 3}  # nada apagado
    assert all(n == 0 for _, _, n in store.calls)


def test_dry_run_is_the_default() -> None:
    assert PurgeOptions().dry_run is True


async def test_deadline_stops_after_the_current_batch() -> None:
    clock = FakeClock()
    store = ScriptedStore(clock, dlq=100, outbox=5)
    store.batch_cost_s = 30
    results = await purge(store, max_runtime_s=60, pause_s=0).execute(asyncio.Event())
    # 2 lotes de 30 s cabem no prazo; a outbox e o bruto ficam para a próxima execução.
    assert results == [PurgeResult(DLQ, 20, complete=False)]
    assert store.rows[OUTBOX] == 5


async def test_stop_interrupts_the_pause_and_ends_cleanly() -> None:
    class SlowClock(FakeClock):
        async def sleep(self, seconds: float) -> None:
            await asyncio.sleep(10)

    clock = SlowClock()
    store = ScriptedStore(clock, dlq=100)
    stop = asyncio.Event()
    task = asyncio.create_task(purge(store, pause_s=5).execute(stop))
    await asyncio.sleep(0.01)
    stop.set()
    results = await asyncio.wait_for(task, 1)
    assert results == [PurgeResult(DLQ, 10, complete=False)]


async def test_database_errors_propagate_after_committed_batches() -> None:
    store = ScriptedStore(FakeClock(), dlq=5)
    store.fail = StoreUnavailableError("ORA-03113")
    with pytest.raises(StoreUnavailableError):
        await purge(store).execute(asyncio.Event())
