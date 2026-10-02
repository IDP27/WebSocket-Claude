"""Fila interna: limites por quantidade e bytes, espera quando cheia, lotes e descarte."""

from __future__ import annotations

import asyncio

from tests.fakes.memory import T0

from ohip_streaming.application.intake import IntakeItem, IntakeQueue


def item(n: int, size: int = 10) -> IntakeItem:
    return IntakeItem(f"m{n}", T0, size)


async def test_batches_up_to_max_items() -> None:
    queue = IntakeQueue(max_items=10, max_bytes=1000)
    for n in range(5):
        await queue.put(item(n))
    batch = await queue.get_batch(3, 0.01)
    assert [i.raw for i in batch] == ["m0", "m1", "m2"]
    assert len(queue) == 2
    assert queue.bytes == 20


async def test_put_waits_when_full_and_never_drops() -> None:
    queue = IntakeQueue(max_items=2, max_bytes=1000)
    await queue.put(item(1))
    await queue.put(item(2))
    blocked = asyncio.create_task(queue.put(item(3)))
    await asyncio.sleep(0.01)
    assert not blocked.done()  # cheia: espera
    await queue.get_batch(1, 0)
    await asyncio.wait_for(blocked, 1)
    assert len(queue) == 2


async def test_byte_limit() -> None:
    queue = IntakeQueue(max_items=100, max_bytes=25)
    await queue.put(item(1, 20))
    blocked = asyncio.create_task(queue.put(item(2, 10)))
    await asyncio.sleep(0.01)
    assert not blocked.done()
    await queue.get_batch(10, 0)
    await asyncio.wait_for(blocked, 1)


async def test_oversized_item_enters_an_empty_queue() -> None:
    queue = IntakeQueue(max_items=10, max_bytes=5)
    await asyncio.wait_for(queue.put(item(1, 50)), 1)
    assert len(queue) == 1


async def test_batch_waits_briefly_for_more_items() -> None:
    queue = IntakeQueue(max_items=10, max_bytes=1000)
    await queue.put(item(1))

    async def late() -> None:
        await asyncio.sleep(0.01)
        await queue.put(item(2))

    producer = asyncio.create_task(late())
    batch = await queue.get_batch(2, 1.0)
    await producer
    assert [i.raw for i in batch] == ["m1", "m2"]


async def test_close_drains_then_returns_empty() -> None:
    queue = IntakeQueue(max_items=10, max_bytes=1000)
    await queue.put(item(1))
    await queue.close()
    await queue.put(item(2))  # fechada: nada mais entra
    assert [i.raw for i in await queue.get_batch(10, 0.01)] == ["m1"]
    assert await queue.get_batch(10, 0.01) == []


async def test_discard_drops_everything_and_unblocks() -> None:
    queue = IntakeQueue(max_items=1, max_bytes=1000)
    await queue.put(item(1))
    blocked = asyncio.create_task(queue.put(item(2)))
    await asyncio.sleep(0.01)
    assert await queue.discard() == 1
    await asyncio.wait_for(blocked, 1)
    assert len(queue) == 0
    assert await queue.get_batch(10, 0.01) == []
