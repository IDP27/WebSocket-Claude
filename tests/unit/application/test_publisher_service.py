"""PublisherService: lease, chains em paralelo, backoff do broker e cabeça em backoff."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from tests.fakes.memory import (
    FakeClock,
    FakeMetrics,
    FakePublisher,
    InMemoryDatabase,
    MemoryLeaseStore,
)

from ohip_streaming.application.errors import (
    BrokerMisconfiguredError,
    BrokerUnavailableError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import PublishOutcome
from ohip_streaming.application.use_cases.lease import LeaseKeeper, LeaseOptions
from ohip_streaming.application.use_cases.publish_outbox import PublisherOptions, PublishOutbox
from ohip_streaming.application.use_cases.publisher_service import (
    PublisherLoopOptions,
    PublisherService,
)
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage

ACK, NACK = PublishOutcome.ACKED, PublishOutcome.NACKED


class Harness:
    def __init__(self, script: list[PublishOutcome | Exception] | None = None) -> None:
        self.clock = FakeClock()
        self.db = InMemoryDatabase(clock=self.clock)
        self.db.leases["publisher"] = 0
        self.leases = MemoryLeaseStore(self.db)
        self.metrics = FakeMetrics()
        self.publisher = FakePublisher(script or [])
        self.lease = LeaseKeeper(
            store=self.leases,
            clock=self.clock,
            metrics=self.metrics,
            lease_name="publisher",
            owner="vm1",
            options=LeaseOptions(ttl_s=30, renew_interval_s=10),
        )
        self.service = PublisherService(
            store=self.db,
            publish=PublishOutbox(
                store=self.db,
                publisher=self.publisher,
                clock=self.clock,
                metrics=self.metrics,
                options=PublisherOptions(max_attempts=3),
            ),
            lease=self.lease,
            clock=self.clock,
            metrics=self.metrics,
            options=PublisherLoopOptions(
                poll_interval_s=1, broker_backoff_initial_s=1, broker_backoff_max_s=8
            ),
        )

    def add(self, chain: str, uid: str) -> int:
        message = QueueMessage(ExchangeKind.EVENTS, "ohip.rsv.X", uid, "{}")
        record = self.db._outbox_record(chain, 0, message)
        record.id = self.db._next_id("outbox")
        self.db.outbox[record.id] = record
        return record.id

    def loop_sleeps(self) -> list[float]:
        """Esperas do laço (sem as de 10 s da renovação do lease, no mesmo relógio)."""
        return [s for s in self.clock.sleeps if s != 10]

    def statuses(self) -> list[str]:
        return [o.status for o in sorted(self.db.outbox.values(), key=lambda o: o.id)]

    async def run_until(self, condition: Callable[[], bool], max_steps: int = 2000) -> None:
        task = asyncio.create_task(self.service.run())
        for _ in range(max_steps):
            if condition() or task.done():
                break
            await asyncio.sleep(0)
        self.service.request_stop()
        await asyncio.wait_for(task, 2)


async def test_publishes_every_chain_in_order() -> None:
    h = Harness()
    for chain in ("C1", "C2"):
        for n in range(3):
            h.add(chain, f"{chain}-{n}")
    await h.run_until(lambda: h.statuses().count("SENT") == 6)
    assert h.statuses() == ["SENT"] * 6
    by_chain = [m.message_id for m in h.publisher.published if m.message_id.startswith("C1")]
    assert by_chain == ["C1-0", "C1-1", "C1-2"]
    assert h.lease.epoch is None  # lease liberado ao sair
    assert not h.lease.lost


async def test_head_in_backoff_does_not_block_other_chains() -> None:
    h = Harness([NACK])  # a primeira publicação (cabeça de C1) leva nack
    h.add("C1", "c1-a")
    h.add("C1", "c1-b")
    h.add("C2", "c2-a")
    await h.run_until(lambda: h.db.outbox[3].status == "SENT")
    assert h.db.outbox[1].status == "PENDING"
    assert h.db.outbox[1].attempts == 1
    assert h.db.outbox[2].status == "PENDING"  # não pula a cabeça de C1


async def test_broker_outage_backs_off_globally_without_counting_attempts() -> None:
    down = BrokerUnavailableError("broker fora")
    h = Harness([down, down, down])
    h.add("C1", "a")
    await h.run_until(lambda: h.db.outbox[1].status == "SENT")
    assert h.db.outbox[1].attempts == 0
    assert h.loop_sleeps()[:3] == [1, 2, 4]  # exponencial
    assert h.metrics.total("ohip_broker_unavailable_total") == 3


async def test_database_outage_waits_and_retries() -> None:
    h = Harness()
    h.add("C1", "a")
    calls = {"n": 0}
    real = h.db.chains_with_pending

    async def flaky() -> list[str]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise StoreUnavailableError("oracle fora")
        return await real()

    h.db.chains_with_pending = flaky  # type: ignore[method-assign]
    await h.run_until(lambda: h.db.outbox[1].status == "SENT")
    assert 5.0 in h.loop_sleeps()


async def test_lease_lost_stops_without_releasing(monkeypatch: pytest.MonkeyPatch) -> None:
    h = Harness()
    h.add("C1", "a")
    real_acquire = h.leases.acquire

    async def acquire_then_lose(name: str, owner: str, ttl_s: float) -> int | None:
        epoch = await real_acquire(name, owner, ttl_s)
        h.db.leases["publisher"] += 1  # outro publisher assumiu logo em seguida
        return epoch

    monkeypatch.setattr(h.leases, "acquire", acquire_then_lose)
    await asyncio.wait_for(h.service.run(), 2)  # a barreira recusa a marcação e o laço sai
    assert h.db.outbox[1].status == "PENDING"
    assert h.db.leases["publisher"] == 2  # o lease do outro ficou intacto


async def test_misconfigured_broker_is_fatal() -> None:
    h = Harness([BrokerMisconfiguredError("PRECONDITION_FAILED")])
    h.add("C1", "a")
    with pytest.raises(BrokerMisconfiguredError):
        await asyncio.wait_for(h.service.run(), 2)
    assert h.lease.epoch is None  # liberado


async def test_idle_sleeps_the_poll_interval() -> None:
    h = Harness()
    await h.run_until(lambda: len(h.loop_sleeps()) >= 2)
    assert h.loop_sleeps()[:2] == [1, 1]


async def test_stop_while_waiting_for_the_lease() -> None:
    h = Harness()
    h.leases.holders["publisher"] = ("outro", h.clock.now().replace(year=2030))
    task = asyncio.create_task(h.service.run())
    await asyncio.sleep(0)
    h.service.request_stop()
    await asyncio.wait_for(task, 2)
    assert h.publisher.attempts == []


async def test_database_down_during_a_round_does_not_spin(monkeypatch: pytest.MonkeyPatch) -> None:
    h = Harness()
    h.add("C1", "a")
    h.service._blocked_until["C1"] = h.clock.now()
    calls = {"n": 0}
    real = h.service._publish.publish_chain

    async def flaky(chain: str, epoch: int) -> Any:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise StoreUnavailableError("oracle fora")
        return await real(chain, epoch)

    monkeypatch.setattr(h.service._publish, "publish_chain", flaky)
    await h.run_until(lambda: h.db.outbox[1].status == "SENT")
    assert h.loop_sleeps()[:2] == [5.0, 5.0]  # espera do banco, não laço sem pausa
