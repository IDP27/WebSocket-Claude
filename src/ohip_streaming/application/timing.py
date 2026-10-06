"""Esperas interrompíveis pelo pedido de parada (refatoração 1 do /entender).

Uma implementação só para o que antes se repetia em sete lugares (lease, consumer, publisher,
enricher, expurgo e o consumo da fila), cada um com uma variação. Regras comuns:

- parada já pedida: volta na hora, sem dormir (quem está num laço deve sair dele; o laço de
  controle do consumer, que segue rodando na drenagem, dorme o intervalo inteiro à parte);
- ``seconds`` <= 0: só cede a vez ao event loop;
- as tarefas auxiliares são canceladas **e aguardadas** com ``gather(return_exceptions=True)``,
  que não engole um cancelamento de quem chamou chegando durante a limpeza (``suppress`` engolia);
- trabalho cancelado porque quem chamou foi cancelado tem a exceção consumida e registrada no
  log estruturado (sem "Task exception was never retrieved" do asyncio).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from ohip_streaming.logging import get_logger

log = get_logger(__name__)
T = TypeVar("T")
Sleep = Callable[[float], Awaitable[None]]


async def sleep_or_stop(
    seconds: float, stop: asyncio.Event | None, *, sleep: Sleep = asyncio.sleep
) -> bool:
    """Dorme ``seconds`` (com ``sleep``, ex.: o ``Clock`` do caso de uso) ou até ``stop``.
    Devolve True se a parada foi pedida."""
    if stop is not None and stop.is_set():
        return True
    if seconds <= 0:
        await asyncio.sleep(0)
        return stop is not None and stop.is_set()
    if stop is None:
        await sleep(seconds)
        return False
    sleeper = asyncio.ensure_future(sleep(seconds))
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await _cancel_and_wait(sleeper, stopper)
    if not sleeper.cancelled() and (error := sleeper.exception()) is not None:
        raise error  # relógio quebrado: sobe (como com stop=None), nunca "dormiu" em silêncio
    return stop.is_set()


async def run_or_stop(
    work: Awaitable[T],
    stop: asyncio.Event,
    *,
    ignore: tuple[type[BaseException], ...] = (),
) -> T | None:
    """Espera ``work`` ou a parada. Na parada, cancela ``work`` (engolindo ``ignore``, ex.:
    erros de rede do cancelamento) e devolve None."""
    task = asyncio.ensure_future(work)
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        # Quem chamou foi cancelado: o trabalho não fica órfão, e o erro que ele levantar ao
        # ser cancelado é consumido e logado quando terminar.
        task.add_done_callback(_consume_outcome)
        task.cancel()
        raise
    finally:
        await _cancel_and_wait(stopper)
    if task.done():
        return task.result()
    task.cancel()
    try:
        (outcome,) = await asyncio.gather(task, return_exceptions=True)
    except asyncio.CancelledError:
        task.add_done_callback(_consume_outcome)  # quem chamou foi cancelado no meio da espera
        raise
    if isinstance(outcome, BaseException) and not isinstance(
        outcome, (asyncio.CancelledError, *ignore)
    ):
        raise outcome
    return None


async def _cancel_and_wait(*tasks: asyncio.Future[Any]) -> None:
    """Cancela e aguarda. Um cancelamento de quem chamou durante a espera sobe normalmente."""
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _consume_outcome(task: asyncio.Future[Any]) -> None:
    if not task.cancelled() and (error := task.exception()) is not None:
        log.warning("tarefa_cancelada_com_erro", error=type(error).__name__)
