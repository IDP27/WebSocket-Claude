"""Partes comuns dos processos assíncronos (refatoração 6 do /entender, caracterização).

Escrito antes de extrair ``entrypoints/runtime.py``: em consumer, publisher e enricher, o
``serve`` liga SIGTERM/SIGINT ao pedido de parada e, ao sair, grava o último retrato das
métricas no Redis (chave por processo e instância, TTL, índice e contadores de alerta em 0).
"""

from __future__ import annotations

import asyncio
import json
import signal
from collections.abc import Callable
from typing import Any

import pytest
from tests.fakes.oracle_driver import FakeOracle
from tests.unit.adapters.test_redis_adapters import FakeRedis
from tests.unit.entrypoints import (
    test_consumer_entrypoint as consumer_t,
)
from tests.unit.entrypoints import (
    test_enricher_entrypoint as enricher_t,
)
from tests.unit.entrypoints import (
    test_publisher_entrypoint as publisher_t,
)
from tests.unit.test_config import set_env

from ohip_streaming.adapters.rabbitmq.consumer import AioPikaQueueConsumer
from ohip_streaming.adapters.redis.metrics import METRICS_INDEX_KEY
from ohip_streaming.application.use_cases.consume_chain import ChainConsumer
from ohip_streaming.application.use_cases.publisher_service import PublisherService
from ohip_streaming.config import load_settings
from ohip_streaming.entrypoints import consumer, enricher, publisher

CASES = [
    ("consumer-CHAIN1", consumer, consumer_t.ENV, ChainConsumer, "run"),
    ("publisher", publisher, publisher_t.ENV, PublisherService, "run"),
    ("enricher", enricher, enricher_t.ENV, AioPikaQueueConsumer, "run"),
]


class Closable(FakeRedis):
    """Depois de fechado, recusa gravações: o último retrato precisa sair antes do
    ``redis.aclose()`` do ``compose`` (senão some, só com aviso)."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True

    def pipeline(self, transaction: bool = True) -> Any:
        if self.closed:
            raise ConnectionError("cliente Redis já fechado")
        return super().pipeline(transaction=transaction)


@pytest.mark.parametrize(("process", "module", "env", "runner", "method"), CASES)
async def test_serve_wires_signals_and_writes_the_last_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    process: str,
    module: Any,
    env: dict[str, str],
    runner: type,
    method: str,
) -> None:
    set_env(monkeypatch, **{**env, "OHIP_CHAIN_CODE": "CHAIN1"} if module is consumer else env)
    load_settings.cache_clear()
    config_cls: Any = {
        consumer: consumer.ConsumerConfig,
        publisher: publisher.PublisherConfig,
        enricher: enricher.EnricherConfig,
    }[module]
    config = config_cls.load()
    redis = Closable()
    monkeypatch.setattr(module, "open_pool", lambda settings: FakeOracle())
    monkeypatch.setattr(module, "open_redis", lambda settings: redis)

    async def run(self: Any, *args: Any) -> None:
        return None

    monkeypatch.setattr(runner, method, run)
    handlers: dict[int, Callable[[], object]] = {}
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda sig, cb: handlers.__setitem__(sig, cb))

    await module.serve(config)

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    assert all(getattr(cb, "__name__", "") == "request_stop" for cb in handlers.values())
    (key,) = [k for k in redis.data if k.startswith(f"ohip:metrics:{process}:")]
    assert redis.ttls[key] == config.redis.metrics_ttl_s
    assert key in redis.sets[METRICS_INDEX_KEY]
    counters = json.loads(redis.data[key])
    expected: Any = {
        consumer: consumer.alert_counters,
        publisher: publisher.alert_counters,
        enricher: enricher.alert_counters,
    }[module]
    names = {c["name"] for c in counters if c["value"] == 0}
    args = ("CHAIN1",) if module is consumer else ()
    assert {name for name, _ in expected(*args)} <= names  # registrados com 0 na partida
