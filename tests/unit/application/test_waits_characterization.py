"""Caracterização das esperas interrompíveis e dos backoffs (refatoração 1 do /entender).

Escrito **antes** da troca pelo utilitário único (RNF-15): fixa o comportamento que precisa
continuar igual. Os casos de estouro com muitas falhas seguidas são correção e ficam em
``test_timing.py``.
"""

from __future__ import annotations

import asyncio

import pytest
from tests.fakes.memory import FakeClock
from tests.unit.application.test_token_and_lease import LeaseHarness

from ohip_streaming.domain import connection as c
from ohip_streaming.domain.rules import publish_retry_delay_s


class SlowClock(FakeClock):
    """Relógio cujo sleep demora de verdade: só a parada tira a espera do lugar."""

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        await asyncio.sleep(10)


def test_publish_retry_table() -> None:
    assert [publish_retry_delay_s(n) for n in range(0, 9)] == [
        10, 10, 20, 40, 80, 160, 300, 300, 300,
    ]  # fmt: skip


def test_reconnect_backoff_table() -> None:
    policy = c.ReconnectPolicy(min_gap_s=1, backoff_initial_s=2, backoff_max_s=50)
    waits = [c.decide_on_close(None, n, policy, lambda: 0.0).wait_s for n in range(0, 8)]
    assert waits == [2, 2, 4, 8, 16, 32, 50, 50]


async def test_lease_wait_is_interrupted_by_stop() -> None:
    h = LeaseHarness()
    await h.keeper("vm1").acquire()  # a outra VM é dona
    passive = h.keeper("vm2")
    passive._clock = SlowClock()
    stop = asyncio.Event()
    task = asyncio.create_task(passive.acquire(stop))
    await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(task, 0.5) is None


async def test_lease_with_stop_already_set_does_not_try() -> None:
    h = LeaseHarness()
    stop = asyncio.Event()
    stop.set()
    assert await asyncio.wait_for(h.keeper("vm1").acquire(stop), 0.5) is None
    assert h.db.leases.get("consumer:CHAIN1", 0) == 0  # nem tentou adquirir


@pytest.mark.parametrize("seconds", [0, -1])
async def test_publish_retry_never_negative(seconds: int) -> None:
    assert publish_retry_delay_s(seconds) == 10
