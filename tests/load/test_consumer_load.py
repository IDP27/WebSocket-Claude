"""Teste de carga do consumer (RNF-13, ADR-0020 §3): fora do ``make check``; ``make test-load``.

Consumer real (``ChainConsumer`` + ``WebsocketsConnector``) contra o ``FakeOhipServer`` em
localhost. O banco é o fake em memória com **latência simulada do Oracle** numa thread única
(como o ``DedicatedSession`` do adapter real): ``LOAD_DB_BATCH_MS`` por lote +
``LOAD_DB_EVENT_MS`` por evento. A medição com Oracle 11g real entra no checklist da Q-17.

Latência = envio pelo servidor → commit do lote. Orçamentos: p95 ≤ 5 s no sustentado a 10x o
pico da Q-8; nenhum evento perdido nem duplicado; memória estável (pico de RSS).

Variáveis (padrões entre parênteses): ``LOAD_RATE_PER_S`` (50 = 10 x 300/min), ``LOAD_DURATION_S``
(60), ``LOAD_BURST_EVENTS`` (10000), ``LOAD_DB_BATCH_MS`` (100), ``LOAD_DB_EVENT_MS`` (1),
``LOAD_MEMORY_GROWTH_MB`` (20), ``LOAD_REPORT`` (arquivo JSON opcional com os números).
"""

from __future__ import annotations

import asyncio
import json
import os
import resource
import statistics
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC
from pathlib import Path

import pytest
from tests.fakes.fake_ohip_server import Behavior, FakeOhipServer
from tests.fakes.frames import new_event
from tests.fakes.memory import (
    FakeMetrics,
    FakeSeenCache,
    FakeTokenCache,
    FakeTokenIssuer,
    InMemoryDatabase,
    MemoryLeaseStore,
)

from ohip_streaming.adapters.ohip_ws.client import WebsocketsConnector
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.ports import BatchResult, BatchToPersist
from ohip_streaming.application.use_cases.consume_chain import (
    ChainConsumer,
    ConsumerDeps,
    ConsumerOptions,
)
from ohip_streaming.application.use_cases.lease import LeaseKeeper, LeaseOptions
from ohip_streaming.application.use_cases.process_event_batch import (
    BatchOptions,
    ProcessEventBatch,
    RetryConsumeDlq,
)
from ohip_streaming.application.use_cases.replay import ApplyReplay
from ohip_streaming.application.use_cases.token_provider import TokenProvider
from ohip_streaming.domain.connection import ReconnectPolicy

pytestmark = pytest.mark.load

CHAIN = "CHAIN1"
APP_KEY = "41ecd082-8997-4c69-af34-2f72b83645ff"
P95_BUDGET_S = 5.0


def _env(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


RATE_PER_S = _env("LOAD_RATE_PER_S", 50)
DURATION_S = _env("LOAD_DURATION_S", 60)
BURST_EVENTS = int(_env("LOAD_BURST_EVENTS", 10_000))
DB_BATCH_S = _env("LOAD_DB_BATCH_MS", 100) / 1000
DB_EVENT_S = _env("LOAD_DB_EVENT_MS", 1) / 1000
MEMORY_GROWTH_MB = _env("LOAD_MEMORY_GROWTH_MB", 20)

# ~2,5 KB por evento, como uma reserva com vários elementos alterados.
DETAIL = [
    {"elementName": f"ELEMENTO {n}", "oldValue": "x" * 40, "newValue": "y" * 40} for n in range(20)
]


def rss_mb() -> float:
    """Pico de RSS do processo (``ru_maxrss``: bytes no macOS, KiB no Linux)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


class LoadDatabase(InMemoryDatabase):
    """Grava com a latência simulada do Oracle e mede envio → commit. Não guarda as linhas
    (a memória medida é a do consumer, não a do fake)."""

    def __init__(self, server: FakeOhipServer) -> None:
        super().__init__(clock=SystemClock())  # type: ignore[arg-type]
        self.server = server
        self.executor = ThreadPoolExecutor(1, thread_name_prefix="oracle-simulado")
        self.latencies: list[float] = []
        self.committed = 0
        self.duplicates = 0
        self.batches = 0

    async def persist_batch(self, batch: BatchToPersist) -> BatchResult:
        cost = DB_BATCH_S + DB_EVENT_S * len(batch.events)
        await asyncio.get_running_loop().run_in_executor(self.executor, time.sleep, cost)
        result = await super().persist_batch(batch)
        committed_at = time.perf_counter()
        for uid in result.inserted:
            sent = self.server.sent_at.pop(uid, None)
            if sent is not None:
                self.latencies.append(committed_at - sent)
        self.committed += len(result.inserted)
        self.duplicates += len(result.duplicates)
        self.batches += 1
        self.raw.clear()  # sem histórico: dedup pelo Oracle não é o que se mede aqui
        self.outbox.clear()
        self.persisted_batches.clear()
        return result


@dataclass
class Report:
    scenario: str
    events: int
    committed: int
    duplicates: int
    batches: int
    elapsed_s: float
    events_per_s: float
    p50_s: float
    p95_s: float
    p99_s: float
    max_s: float
    rss_start_mb: float
    rss_end_mb: float
    reconnections: int
    poisoned: int
    params: dict[str, float] = field(default_factory=dict)


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    return statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1]


def build(server: FakeOhipServer) -> tuple[ChainConsumer, LoadDatabase, FakeMetrics]:
    clock = SystemClock()
    db = LoadDatabase(server)
    db.provision_chain(CHAIN)
    metrics = FakeMetrics()
    tokens = TokenProvider(
        issuer=FakeTokenIssuer(clock),
        cache=FakeTokenCache(),
        clock=clock,
        metrics=metrics,
        cache_key="ohip:token:load:CHAIN1",
        margin_s=300,
    )
    seen = FakeSeenCache()
    deps = ConsumerDeps(
        connector=WebsocketsConnector(url=server.url, max_message_bytes=10_000_000),
        tokens=tokens,
        lease=LeaseKeeper(
            store=MemoryLeaseStore(db),
            clock=clock,
            metrics=metrics,
            lease_name=f"consumer:{CHAIN}",
            owner="load:1",
            options=LeaseOptions(ttl_s=30, renew_interval_s=10),
        ),
        batch=ProcessEventBatch(
            store=db, seen=seen, clock=clock, metrics=metrics, options=BatchOptions()
        ),
        retry_dlq=RetryConsumeDlq(store=db, seen=seen, clock=clock, options=BatchOptions()),
        replay_store=db,
        apply_replay=ApplyReplay(store=db),
        status=db,
        clock=clock,
        metrics=metrics,
        app_key=APP_KEY,
    )
    # Valores de produção (ConsumerSettings), exceto a espera inicial da regra dos 10 s.
    options = ConsumerOptions(
        chain_code=CHAIN,
        instance_id="load:1",
        event_tz=UTC,
        policy=ReconnectPolicy(min_gap_s=0.1),
    )
    return ChainConsumer(deps, options), db, metrics


async def run_scenario(
    name: str, events: int, behavior: Behavior, *, limit_s: float
) -> tuple[Report, list[float]]:
    async with FakeOhipServer(
        app_key=APP_KEY, valid_tokens={"token-1", "token-2"}, lockout_window_s=0.0
    ) as server:
        server.events = [new_event(str(n), f"uid-{n}", detail=DETAIL) for n in range(1, events + 1)]
        server.track_send_times = True
        server.behaviors = [behavior]
        consumer, db, metrics = build(server)
        rss_start = rss_mb()
        started = time.perf_counter()
        task = asyncio.create_task(consumer.run())
        samples: list[float] = []
        try:
            while db.committed + db.duplicates < events:
                if task.done():
                    task.result()
                    raise AssertionError("consumer terminou antes do fim da carga")
                if time.perf_counter() - started > limit_s:
                    raise AssertionError(f"{name}: {db.committed}/{events} em {limit_s:.0f} s")
                samples.append(rss_mb())
                await asyncio.sleep(0.25)
            elapsed = time.perf_counter() - started
        finally:
            consumer.request_stop()
            await asyncio.wait_for(task, 30)
            db.executor.shutdown(wait=True)
        lat = db.latencies
        report = Report(
            scenario=name,
            events=events,
            committed=db.committed,
            duplicates=db.duplicates,
            batches=db.batches,
            elapsed_s=round(elapsed, 2),
            events_per_s=round(db.committed / elapsed, 1),
            p50_s=round(percentile(lat, 50), 3),
            p95_s=round(percentile(lat, 95), 3),
            p99_s=round(percentile(lat, 99), 3),
            max_s=round(max(lat, default=float("nan")), 3),
            rss_start_mb=round(rss_start, 1),
            rss_end_mb=round(rss_mb(), 1),
            reconnections=len(server.connections) - 1,
            poisoned=metrics.total("ohip_connection_poisoned_total"),
            params={
                "rate_per_s": behavior.rate_per_s or 0.0,
                "db_batch_ms": DB_BATCH_S * 1000,
                "db_event_ms": DB_EVENT_S * 1000,
            },
        )
        return report, samples


def publish(report: Report, emit: Callable[[str], None]) -> None:
    line = json.dumps(asdict(report), ensure_ascii=False)
    emit(line)
    target = os.environ.get("LOAD_REPORT")
    if target:
        with Path(target).open("a", encoding="utf-8") as out:
            out.write(line + "\n")


async def test_sustained_ten_times_peak(capsys: pytest.CaptureFixture[str]) -> None:
    events = int(RATE_PER_S * DURATION_S)
    report, samples = await run_scenario(
        "sustentado_10x",
        events,
        Behavior(rate_per_s=RATE_PER_S),
        limit_s=DURATION_S * 2 + 30,
    )
    with capsys.disabled():
        publish(report, print)
    assert report.committed == events  # nada perdido
    assert report.duplicates == 0
    assert report.reconnections == 0
    assert report.poisoned == 0
    assert report.p95_s <= P95_BUDGET_S
    # Pico de RSS: o primeiro quarto aquece (buffers, import); depois não pode crescer.
    quarter = samples[len(samples) // 4] if samples else report.rss_start_mb
    assert report.rss_end_mb - quarter <= MEMORY_GROWTH_MB


async def test_burst_end_of_day(capsys: pytest.CaptureFixture[str]) -> None:
    report, _ = await run_scenario(
        "rajada", BURST_EVENTS, Behavior(), limit_s=max(120.0, BURST_EVENTS / 20)
    )
    with capsys.disabled():
        publish(report, print)
    # Fila interna cheia: o leitor espera (nunca descarta) e a conexão segue de pé.
    assert report.committed == BURST_EVENTS
    assert report.duplicates == 0
    assert report.reconnections == 0
    assert report.poisoned == 0
