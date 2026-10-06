"""Utilitários de espera interrompível e backoff (refatoração 1 do /entender)."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from ohip_streaming.application.timing import run_or_stop, sleep_or_stop
from ohip_streaming.domain import connection as c
from ohip_streaming.domain.backoff import exponential_backoff
from ohip_streaming.domain.rules import publish_retry_delay_s


class Recorder:
    def __init__(self, real_s: float = 0.0) -> None:
        self.calls: list[float] = []
        self.real_s = real_s

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        await asyncio.sleep(self.real_s)


# ------------------------------------------------------------------ backoff


def test_backoff_doubles_up_to_the_cap() -> None:
    assert [exponential_backoff(n, 5, 60) for n in range(0, 7)] == [5, 5, 10, 20, 40, 60, 60]


@pytest.mark.parametrize("attempt", [1_025, 1_100, 10**6])
def test_backoff_never_overflows_after_long_outages(attempt: int) -> None:
    # Antes: inicial * 2 ** (n - 1) com n > ~1024 estourava (OverflowError) e o processo caía
    # depois de ~17 h de broker ou banco fora, uma falha por minuto.
    assert exponential_backoff(attempt, 1.0, 60.0) == 60.0
    policy = c.ReconnectPolicy()
    assert c.decide_on_close(None, attempt, policy, lambda: 0.0).wait_s == policy.backoff_max_s
    assert publish_retry_delay_s(attempt) == 300


# ------------------------------------------------------------------ sleep_or_stop


async def test_sleeps_with_the_given_clock() -> None:
    sleep = Recorder()
    assert await sleep_or_stop(3, asyncio.Event(), sleep=sleep) is False
    assert sleep.calls == [3]
    assert await sleep_or_stop(2, None, sleep=sleep) is False
    assert sleep.calls == [3, 2]


async def test_stop_already_set_returns_without_sleeping() -> None:
    stop = asyncio.Event()
    stop.set()
    sleep = Recorder()
    assert await sleep_or_stop(30, stop, sleep=sleep) is True
    assert sleep.calls == []  # nada de laço girando nem relógio avançando


async def test_zero_only_yields() -> None:
    sleep = Recorder()
    assert await sleep_or_stop(0, asyncio.Event(), sleep=sleep) is False
    assert await sleep_or_stop(-1, None, sleep=sleep) is False
    assert sleep.calls == []


async def test_stop_interrupts_and_leaves_no_pending_task() -> None:
    stop = asyncio.Event()
    before = len(asyncio.all_tasks())
    task = asyncio.create_task(sleep_or_stop(30, stop, sleep=Recorder(real_s=30)))
    await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(task, 0.1) is True
    await asyncio.sleep(0)
    assert len(asyncio.all_tasks()) == before  # sleeper e stopper cancelados e aguardados


# ------------------------------------------------------------------ run_or_stop


async def test_returns_the_work_result() -> None:
    async def work() -> int:
        return 7

    assert await run_or_stop(work(), asyncio.Event()) == 7


async def test_stop_cancels_the_work_and_swallows_listed_errors() -> None:
    cancelled = asyncio.Event()

    async def work() -> int:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise ConnectionResetError("canal fechou no cancelamento") from None
        return 1

    stop = asyncio.Event()
    task = asyncio.create_task(run_or_stop(work(), stop, ignore=(ConnectionResetError,)))
    await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(task, 0.1) is None
    assert cancelled.is_set()


async def test_cancelling_the_caller_cancels_the_work() -> None:
    finished = asyncio.Event()

    async def work() -> None:
        try:
            await asyncio.sleep(30)
        finally:
            finished.set()

    task = asyncio.create_task(run_or_stop(work(), asyncio.Event()))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(finished.wait(), 0.1)  # o trabalho não ficou órfão


async def test_caller_cancelled_during_cleanup_is_not_swallowed() -> None:
    """A parada chega, e durante a limpeza das tarefas auxiliares quem chamou é cancelado: o
    cancelamento precisa subir (antes, o ``suppress`` o engolia e a função voltava True)."""
    cleaning = asyncio.Event()

    async def slow_cleanup_sleep(seconds: float) -> None:
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            cleaning.set()
            await asyncio.sleep(0.05)  # limpeza que demora (ex.: fechar algo)
            raise

    stop = asyncio.Event()
    task = asyncio.create_task(sleep_or_stop(30, stop, sleep=slow_cleanup_sleep))
    await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(cleaning.wait(), 0.1)
    task.cancel()  # chega no meio da limpeza
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 0.5)


async def test_work_error_after_caller_cancel_is_consumed_and_logged(
    capsys: pytest.CaptureFixture[str],
) -> None:
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    try:

        async def work() -> None:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                raise ConnectionResetError("erro ao fechar o canal") from None

        task = asyncio.create_task(run_or_stop(work(), asyncio.Event()))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.01)  # o trabalho termina e o callback consome o erro
        import gc

        gc.collect()  # "Task exception was never retrieved" sairia aqui
        assert unhandled == []
        assert "tarefa_cancelada_com_erro" in capsys.readouterr().out
    finally:
        loop.set_exception_handler(None)


async def test_unexpected_work_error_on_stop_still_propagates() -> None:
    async def work() -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise RuntimeError("bug no cancelamento") from None

    stop = asyncio.Event()
    task = asyncio.create_task(run_or_stop(work(), stop, ignore=(ConnectionResetError,)))
    await asyncio.sleep(0.01)
    stop.set()
    with pytest.raises(RuntimeError, match="bug no cancelamento"):
        await asyncio.wait_for(task, 0.5)


async def test_broken_sleep_raises_instead_of_pretending_it_slept() -> None:
    async def broken(seconds: float) -> None:
        raise RuntimeError("relógio quebrado")

    with pytest.raises(RuntimeError, match="relógio quebrado"):
        await sleep_or_stop(5, asyncio.Event(), sleep=broken)
    with pytest.raises(RuntimeError, match="relógio quebrado"):
        await sleep_or_stop(5, None, sleep=broken)  # mesmo comportamento sem stop


async def test_stop_then_caller_cancel_still_logs_the_work_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    cancelling = asyncio.Event()

    async def work() -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelling.set()
            # Fecha devagar e insiste mesmo se for cancelado de novo; termina com erro.
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(0.05)
            raise RuntimeError("erro ao fechar") from None

    stop = asyncio.Event()
    task = asyncio.create_task(run_or_stop(work(), stop))
    await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(cancelling.wait(), 0.1)
    task.cancel()  # parada e cancelamento de quem chamou sobrepostos
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.1)
    assert "tarefa_cancelada_com_erro" in capsys.readouterr().out
