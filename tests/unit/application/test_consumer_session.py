"""Partes da sessão do consumer isoladas (refatoração 4 do /entender)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from tests.fakes.memory import FakeClock, FakeMetrics, InMemoryDatabase, MemoryLeaseStore

from ohip_streaming.application.errors import LeaseLostError, StoreUnavailableError
from ohip_streaming.application.ports import AccessToken
from ohip_streaming.application.use_cases.consumer_session import (
    ControlLoop,
    DrainCause,
    Liveness,
    StatusReporter,
)
from ohip_streaming.application.use_cases.lease import LeaseKeeper, LeaseOptions
from ohip_streaming.application.use_cases.process_event_batch import ConsumerContext
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.offset import Offset

CHAIN = "CHAIN1"
T0 = datetime(2026, 10, 1, 12, tzinfo=UTC)


class Ticker:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


async def owned_lease(db: InMemoryDatabase) -> LeaseKeeper:
    lease = LeaseKeeper(
        store=MemoryLeaseStore(db),
        clock=db.clock,
        metrics=FakeMetrics(),
        lease_name=f"consumer:{CHAIN}",
        owner="vm1",
        options=LeaseOptions(ttl_s=30, renew_interval_s=10),
    )
    await lease.acquire()
    return lease


def reporter(
    db: InMemoryDatabase, lease: LeaseKeeper, now: Ticker, rtt: list[float | None]
) -> StatusReporter:
    return StatusReporter(
        db,
        chain_code=CHAIN,
        instance_id="vm1:1",
        lease=lease,
        interval_s=15,
        now=now,
        rtt_s=lambda: rtt[0],
    )


# ------------------------------------------------------------------ StatusReporter


async def test_reporter_writes_health_only_when_due_and_changed() -> None:
    db = InMemoryDatabase(clock=FakeClock())
    db.provision_chain(CHAIN)
    now = Ticker()
    rtt: list[float | None] = [None]
    r = reporter(db, await owned_lease(db), now, rtt)
    r.last_message_at = T0
    await r.health_if_due()
    assert db.statuses[CHAIN].last_message_at is None  # ainda não venceu o intervalo
    now.t = 15
    await r.health_if_due()
    assert db.statuses[CHAIN].last_message_at == T0
    calls: list[Any] = []
    original = db.record_health

    async def spy(*args: Any) -> None:
        calls.append(args)
        await original(*args)

    db.record_health = spy  # type: ignore[method-assign,assignment]
    await r.health()  # nada mudou
    assert calls == []
    rtt[0] = 0.042
    await r.health()
    assert db.statuses[CHAIN].rtt_ms == 42


async def test_reporter_without_lease_writes_nothing() -> None:
    db = InMemoryDatabase(clock=FakeClock())
    db.provision_chain(CHAIN)
    r = reporter(db, await owned_lease(db), Ticker(), [0.01])
    before = db.statuses[CHAIN]
    r.lose_lease()
    r.last_message_at = T0
    await r.state(ConsumerState.DRAINING)
    await r.subscribed("sub", T0)
    await r.health()
    assert db.statuses[CHAIN] == before


async def test_reporter_tolerates_a_database_outage() -> None:
    db = InMemoryDatabase(clock=FakeClock())
    db.provision_chain(CHAIN)
    r = reporter(db, await owned_lease(db), Ticker(), [None])
    db.fail_status = [StoreUnavailableError("fora")] * 2
    await r.state(ConsumerState.CONNECTING)  # só aviso
    await r.subscribed("sub", T0)
    assert db.statuses[CHAIN].state is ConsumerState.STOPPED


# ------------------------------------------------------------------ Liveness


async def test_liveness_times_out_without_pong_or_next() -> None:
    now = Ticker()
    live = Liveness(now=now, ping_interval_s=0, pong_timeout_min_s=180, rng=lambda: 0.0)
    pings: list[float] = []

    async def ping() -> None:
        pings.append(now.t)
        now.t += 100  # 100 s entre pings, sem resposta

    assert await live.run(ping, asyncio.Event()) is True
    assert pings == [0, 100]  # 1º ping em 0; aos 200 s de silêncio (> 180) desiste
    assert live.ping_sent_at == 0  # nunca respondeu


async def test_pong_updates_rtt_and_keeps_alive() -> None:
    now = Ticker()
    live = Liveness(now=now, ping_interval_s=0, pong_timeout_min_s=180, rng=lambda: 0.0)
    closed = asyncio.Event()

    async def ping() -> None:
        now.t += 0.2
        live.pong()
        closed.set()

    assert await live.run(ping, closed) is False
    assert live.rtt_s == pytest.approx(0.2)
    assert live.ping_sent_at is None


# ------------------------------------------------------------------ ControlLoop


class FakeSession:
    def __init__(self) -> None:
        self.closed = asyncio.Event()
        self.completed = False
        self.poisoned = False
        self.token: AccessToken | None = None
        self.actions: list[str] = []

    async def drain(self, cause: DrainCause) -> None:
        self.actions.append(f"drain:{cause.value}")
        self.completed = True

    async def poison(self, why: str) -> None:
        self.actions.append("poison")
        self.poisoned = True


class Tokens:
    def __init__(self, due: bool) -> None:
        self.due = due

    def refresh_due(self, token: AccessToken) -> bool:
        return self.due


class RetryDlq:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls = 0
        self.error = error

    async def execute(self, context: ConsumerContext, *, limit: int) -> None:
        self.calls += 1
        if self.error is not None:
            raise self.error


async def control(
    *,
    stop: bool = False,
    lost: bool = False,
    token_due: bool = False,
    replay: bool = False,
    retry: RetryDlq | None = None,
) -> tuple[ControlLoop, FakeSession, InMemoryDatabase, StatusReporter]:
    db = InMemoryDatabase(clock=FakeClock())
    db.provision_chain(CHAIN)
    lease = await owned_lease(db)
    if lost:
        lease._lose("teste")
    if replay:
        await db.create_request(
            CHAIN,
            Offset("1"),
            "x",
            "ana",
        )
    session = FakeSession()
    session.token = AccessToken("t", T0)
    status = reporter(db, lease, Ticker(), [None])
    event = asyncio.Event()
    if stop:
        event.set()
    loop = ControlLoop(
        session,
        stop=event,
        lease=lease,
        tokens=Tokens(token_due),  # type: ignore[arg-type]
        replay_store=db,
        retry_dlq=retry or RetryDlq(),  # type: ignore[arg-type]
        context=ConsumerContext(CHAIN, "sub", 1, UTC),
        status=status,
        poll_interval_s=0.01,
        dlq_retry_limit=20,
    )
    return loop, session, db, status


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ({"stop": True, "lost": True, "token_due": True}, ["drain:STOP"]),
        ({"lost": True, "token_due": True, "replay": True}, ["poison", "drain:LEASE_LOST"]),
        ({"token_due": True, "replay": True}, ["drain:TOKEN"]),
        ({"replay": True}, ["drain:REPLAY"]),
        ({}, []),
    ],
)
async def test_control_priority(flags: dict[str, bool], expected: list[str]) -> None:
    loop, session, _, status = await control(**flags)  # type: ignore[arg-type]
    await loop.step()
    assert session.actions == expected
    assert status.owns() is not flags.get("lost", False)


async def test_control_retries_dlq_only_when_idle_and_healthy() -> None:
    retry = RetryDlq()
    loop, session, _, _ = await control(retry=retry)
    await loop.step()
    assert retry.calls == 1
    session.poisoned = True
    await loop.step()
    assert retry.calls == 1  # envenenada: nada desta conexão é gravado


async def test_lease_lost_during_dlq_retry_stops_status_writes() -> None:
    loop, session, _, status = await control(retry=RetryDlq(LeaseLostError("epoch")))
    await loop.step()
    assert session.actions == ["poison", "drain:LEASE_LOST"]
    assert status.owns() is False


async def test_control_loop_keeps_running_after_a_failing_step() -> None:
    loop, session, _, _ = await control(retry=RetryDlq(RuntimeError("bug")))
    task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.05)  # vários passos falhando: o laço segue
    session.closed.set()
    await asyncio.wait_for(task, 0.5)


async def test_database_down_while_checking_replay_keeps_the_connection() -> None:
    retry = RetryDlq()
    loop, session, db, _ = await control(replay=True, retry=retry)

    async def down(chain_code: str) -> None:
        raise StoreUnavailableError("ORA-03113")

    db.pending_request = down  # type: ignore[method-assign]
    await loop.step()
    # Banco fora não é motivo para derrubar a conexão: segue para o retry de DLQ.
    assert session.actions == []
    assert not session.poisoned
    assert retry.calls == 1


async def test_database_down_during_dlq_retry_only_warns() -> None:
    retry = RetryDlq(StoreUnavailableError("ORA-03113"))
    loop, session, _, status = await control(retry=retry)
    await loop.step()
    await loop.step()
    assert retry.calls == 2  # tenta de novo no próximo passo
    assert session.actions == []
    assert not session.poisoned
    assert status.owns()
