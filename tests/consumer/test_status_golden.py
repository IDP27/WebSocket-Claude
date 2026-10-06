"""Sequência de gravações de status de uma sessão (golden; refatoração 4 do /entender).

Escrito **antes** de extrair o ``StatusReporter``, o heartbeat e o controle de ``_Session``:
a ordem e o conteúdo das escritas em ``OHIP_CONSUMER_STATUS`` não podem mudar.
``status_interval_s`` alto: a saúde só é gravada no fim da sessão (sequência determinística).

Prazos folgados (``GOLDEN``): com os do harness (ping a cada 50 ms, ack/pong/drenagem em
0,4-0,5 s, sessão saudável após 0,3 s), uma pausa da máquina muda a sequência sem nenhum bug:
ex.: 80 ms entre o fim dos eventos e a parada fazem um ``pong`` entrar e a saúde final levar RTT
(reproduzido na revisão da refatoração 4). O que se fixa aqui é o status, não o heartbeat.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from tests.consumer.test_consumer_e2e import CHAIN, Harness, server  # noqa: F401 - fixture
from tests.fakes.fake_ohip_server import Behavior, FakeOhipServer

from ohip_streaming.domain.offset import Offset

GOLDEN: dict[str, float] = {
    "status_interval_s": 3600,
    "ping_interval_s": 30,
    "pong_timeout_min_s": 60,
    "ack_timeout_s": 5,
    "drain_timeout_s": 5,
    "healthy_after_s": 30,
}


def record_calls(h: Harness) -> list[tuple[Any, ...]]:
    calls: list[tuple[Any, ...]] = []

    def wrap(name: str) -> None:
        original = getattr(h.db, name)

        async def spy(*args: Any, **kwargs: Any) -> Any:
            calls.append(summarize(name, args, kwargs))
            return await original(*args, **kwargs)

        setattr(h.db, name, spy)

    for name in ("record_state", "record_subscribed", "record_health", "record_disconnect"):
        wrap(name)
    return calls


def summarize(name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
    if name == "record_state":
        return (name, args[1].value)
    if name == "record_subscribed":
        return (name, args[1])  # instance_id
    if name == "record_health":
        health = args[1]
        return (name, health.last_message_at is not None, health.rtt_ms is not None)
    state, code = args[1].value, args[2]
    wait = kwargs.get("next_attempt_in_s")
    return (
        name,
        state,
        code,
        kwargs.get("consecutive_failures"),
        kwargs.get("reconnect"),
        None if wait is None else round(wait, 3),
    )


async def test_consume_then_stop(server: FakeOhipServer) -> None:  # noqa: F811
    h = Harness(server, **GOLDEN)
    calls = record_calls(h)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await asyncio.sleep(0.2)  # pausa da máquina: com os prazos folgados, a sequência não muda
    await h.stop()
    assert calls == [
        ("record_state", "WAITING"),  # regra dos 10 s na partida
        ("record_state", "CONNECTING"),
        ("record_state", "INIT_SENT"),
        ("record_subscribed", "vm1:1"),
        ("record_state", "DRAINING"),
        ("record_health", True, False),  # ping a cada 30 s: nenhum no teste, sem RTT
        ("record_disconnect", "WAITING", 1000, 0, False, None),
    ]


async def test_reconnect_after_4504(server: FakeOhipServer) -> None:  # noqa: F811
    server.behaviors = [Behavior(close_after_subscribe=4504)]
    h = Harness(server, **GOLDEN)
    calls = record_calls(h)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    assert calls == [
        ("record_state", "WAITING"),
        ("record_state", "CONNECTING"),
        ("record_state", "INIT_SENT"),
        ("record_subscribed", "vm1:1"),
        ("record_disconnect", "WAITING", 4504, 1, True, 0.1),  # sem evento: sem saúde
        ("record_state", "CONNECTING"),
        ("record_state", "INIT_SENT"),
        ("record_subscribed", "vm1:1"),
        ("record_state", "DRAINING"),
        ("record_health", True, False),
        ("record_disconnect", "WAITING", 1000, 0, False, None),
    ]


async def test_lost_lease_writes_nothing_after_the_loss(server: FakeOhipServer) -> None:  # noqa: F811
    h = Harness(server, **GOLDEN)
    calls = record_calls(h)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    before = list(calls)
    h.db.leases[f"consumer:{CHAIN}"] += 1
    h.lease._lose("teste")
    assert h.task is not None
    await h.task
    assert before == [
        ("record_state", "WAITING"),
        ("record_state", "CONNECTING"),
        ("record_state", "INIT_SENT"),
        ("record_subscribed", "vm1:1"),
    ]
    assert calls == before


@pytest.mark.parametrize("interval", [0.05])
async def test_periodic_health_comes_from_the_control_task(
    server: FakeOhipServer,  # noqa: F811
    interval: float,
) -> None:
    h = Harness(server, status_interval_s=interval)
    calls = record_calls(h)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.until(lambda: any(c[0] == "record_health" and c[2] for c in calls))  # com RTT
    await h.stop()
    # A periódica sai assinada, antes da drenagem; a final só se algo mudou desde a última.
    first_health = next(i for i, c in enumerate(calls) if c[0] == "record_health")
    assert first_health < calls.index(("record_state", "DRAINING"))
    assert calls[-1][0] == "record_disconnect"
