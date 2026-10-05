"""Consumer ponta a ponta contra o servidor OHIP simulado (WebSocket real em localhost).

Tempos em milissegundos (a política padrão exige 10 s; aqui o mínimo é 0,1 s). Persistência
no fake em memória: o que importa aqui é o protocolo e o ciclo de vida (ADR-0007).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from datetime import UTC, timedelta
from typing import Any

import pytest
from tests.fakes.fake_ohip_server import Behavior, FakeOhipServer
from tests.fakes.memory import (
    FakeMetrics,
    FakeSeenCache,
    FakeTokenCache,
    FakeTokenIssuer,
    InMemoryDatabase,
    MemoryLeaseStore,
    StatusRow,
)

from ohip_streaming.adapters.ohip_ws.client import WebsocketsConnector
from ohip_streaming.adapters.system import SystemClock
from ohip_streaming.application.errors import (
    AuthRejectedError,
    LeaseLostError,
    StoreOperationError,
    StoreUnavailableError,
)
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
from ohip_streaming.domain.connection import ConsumerState, ReconnectPolicy
from ohip_streaming.domain.offset import Offset

CHAIN = "CHAIN1"
APP_KEY = "41ecd082-8997-4c69-af34-2f72b83645ff"
FAST = ReconnectPolicy(
    min_gap_s=0.1,
    lockout_4409_s=0.2,
    lockout_jitter_max_s=0.0,
    backoff_4504_s=0.1,
    backoff_initial_s=0.1,
    backoff_max_s=0.2,
    oversize_max_repeats=2,
)
OPTIONS = ConsumerOptions(
    chain_code=CHAIN,
    instance_id="vm1:1",
    event_tz=UTC,
    ping_interval_s=0.05,
    ack_timeout_s=0.5,
    pong_timeout_min_s=0.4,
    drain_timeout_s=0.5,
    control_poll_interval_s=0.05,
    batch_max_wait_s=0.01,
    healthy_after_s=0.3,
    policy=FAST,
)


class Harness:
    def __init__(self, server: FakeOhipServer, **option_overrides: Any) -> None:
        self.server = server
        self.clock = SystemClock()
        self.db = InMemoryDatabase()
        self.db.provision_chain(CHAIN)
        self.metrics = FakeMetrics()
        self.issuer = FakeTokenIssuer(self.clock)
        self.tokens = TokenProvider(
            issuer=self.issuer,
            cache=FakeTokenCache(),
            clock=self.clock,
            metrics=self.metrics,
            cache_key="ohip:token:t:CHAIN1",
            margin_s=300,
        )
        seen = FakeSeenCache()
        self.batch = ProcessEventBatch(
            store=self.db, seen=seen, clock=self.clock, metrics=self.metrics, options=BatchOptions()
        )
        self.lease = LeaseKeeper(
            store=MemoryLeaseStore(self.db),
            clock=self.clock,
            metrics=self.metrics,
            lease_name=f"consumer:{CHAIN}",
            owner="vm1:1",
            options=LeaseOptions(ttl_s=30, renew_interval_s=10),
        )
        self.deps = ConsumerDeps(
            connector=WebsocketsConnector(url=server.url, max_message_bytes=1_000_000),
            tokens=self.tokens,
            lease=self.lease,
            batch=self.batch,
            retry_dlq=RetryConsumeDlq(
                store=self.db, seen=seen, clock=self.clock, options=BatchOptions()
            ),
            replay_store=self.db,
            apply_replay=ApplyReplay(store=self.db),
            status=self.db,
            clock=self.clock,
            metrics=self.metrics,
            app_key=APP_KEY,
            rng=lambda: 0.0,
        )
        self.consumer = ChainConsumer(self.deps, replace(OPTIONS, **option_overrides))
        self.task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self.consumer.run())

    async def until(self, condition: Callable[[], bool], limit_s: float = 5.0) -> None:
        async def poll() -> None:
            while not condition():
                if self.task is not None and self.task.done():
                    self.task.result()  # propaga erro do consumer
                    raise AssertionError("consumer terminou antes da condição")
                await asyncio.sleep(0.01)

        await asyncio.wait_for(poll(), limit_s)

    async def stop(self) -> None:
        self.consumer.request_stop()
        assert self.task is not None
        await asyncio.wait_for(self.task, 5)

    @property
    def offset(self) -> Offset | None:
        return self.db.offsets[CHAIN].last_offset

    def state(self) -> ConsumerState:
        return self.db.statuses[CHAIN].state


@pytest.fixture
async def server() -> AsyncIterator[FakeOhipServer]:
    async with FakeOhipServer(app_key=APP_KEY, valid_tokens={"token-1", "token-2"}, events=5) as s:
        yield s


async def test_consumes_events_and_stops_gracefully(server: FakeOhipServer) -> None:
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    assert len(h.db.raw) == 5
    assert len(h.db.outbox) == 5
    assert h.state() is ConsumerState.SUBSCRIBED

    await h.stop()
    (conn,) = server.connections
    assert [f["type"] for f in conn.frames][:2] == ["connection_init", "subscribe"]
    assert conn.frames[-1]["type"] == "complete"  # unsubscribe antes de sair
    assert conn.close_code == 1000  # quem fechou foi o servidor
    assert conn.subscribe_offsets == [None]  # primeira assinatura: sem offset
    assert conn.pongs_received == 0
    assert h.state() is ConsumerState.WAITING  # desconexão gravada com o relógio do banco
    assert h.db.statuses[CHAIN].last_disconnect_at is not None
    assert not h.lease.lost
    assert h.lease.epoch is None  # lease liberado


async def test_subscribe_matches_the_oracle_guide_format(server: FakeOhipServer) -> None:
    h = Harness(server, hotel_codes=("H1", "H2"), delta=True)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    init, subscribe = server.connections[0].frames[:2]
    assert init == {
        "type": "connection_init",
        "payload": {"Authorization": "Bearer token-1", "x-app-key": APP_KEY},
    }
    assert subscribe["payload"]["variables"] == {"input": {"chainCode": CHAIN}}
    assert subscribe["payload"]["operationName"] is None
    query = subscribe["payload"]["query"]
    assert 'newEvent(input: { chainCode: "CHAIN1" hotelCode: "H1,H2" delta: true })' in query
    assert len(subscribe["id"]) == 36  # GUID


async def test_reconnects_from_committed_offset_after_4504(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(close_after_events=3, close_code=4504)]
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    first, second = server.connections[:2]
    assert first.close_code == 4504
    assert second.subscribe_offsets == ["3"]  # sempre o último offset confirmado
    assert len(h.db.raw) == 5  # nada perdido nem duplicado
    gap = second.opened_at - (first.closed_at or 0)
    assert gap >= FAST.backoff_4504_s * 0.9


async def test_4401_invalidates_token_and_reconnects(server: FakeOhipServer) -> None:
    server.valid_tokens = {"token-2"}  # o primeiro token é recusado
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    assert [c.close_code for c in server.connections][:1] == [4401]
    assert server.connections[1].token == "token-2"
    assert h.issuer.issued == 2


async def test_4403_stops_without_reconnecting(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(close_after_subscribe=4403)]
    h = Harness(server)
    h.start()
    await h.until(lambda: bool(server.connections) and h.state() is ConsumerState.STOPPED)
    await asyncio.sleep(0.4)
    assert len(server.connections) == 1  # exige ação humana
    await h.stop()


async def test_min_gap_avoids_4409(server: FakeOhipServer) -> None:
    server.lockout_window_s = 0.08  # menor que o intervalo mínimo do consumer (0,1 s)
    server.behaviors = [Behavior(close_after_events=2, close_code=1000)]
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    assert 4409 not in [c.close_code for c in server.connections]


async def test_4409_waits_the_lockout(server: FakeOhipServer) -> None:
    server.lockout_window_s = 0.15  # maior que o intervalo mínimo: a 2ª conexão leva 4409
    server.behaviors = [Behavior(close_after_events=2, close_code=1000)]
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    codes = [c.close_code for c in server.connections]
    assert codes[:2] == [1000, 4409]
    third = server.connections[2]
    assert third.opened_at - (server.connections[1].closed_at or 0) >= FAST.lockout_4409_s * 0.9


async def test_replay_request_drains_and_resubscribes_from_offset(
    server: FakeOhipServer,
) -> None:
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.db.create_request(CHAIN, Offset("2"), "lacuna", "ana")
    await h.until(
        lambda: len(server.connections) == 2 and bool(server.connections[1].subscribe_offsets)
    )
    await h.stop()
    first, second = server.connections
    assert first.frames[-1]["type"] == "complete"
    assert first.close_code == 1000  # drenou e o servidor fechou
    assert second.subscribe_offsets == ["2"]
    assert len(h.db.raw) == 5  # 3..5 reentregues e descartados como duplicados


async def test_database_outage_poisons_and_resumes_from_committed(
    server: FakeOhipServer,
) -> None:
    h = Harness(server)
    calls = {"n": 0}

    def fail_second_batch(_batch: Any) -> Exception | None:
        calls["n"] += 1
        return StoreUnavailableError("oracle fora") if calls["n"] == 2 else None

    h.db.fail_persist_when = fail_second_batch
    h.consumer = ChainConsumer(h.deps, replace(OPTIONS, batch_max_events=2))
    h.start()
    await h.until(lambda: h.offset == Offset("5") and len(server.connections) >= 2)
    await h.stop()
    assert h.metrics.total("ohip_connection_poisoned_total") == 1
    assert server.connections[0].frames[-1]["type"] == "complete"
    second_offset = server.connections[1].subscribe_offsets[0]
    assert second_offset == "2"  # o lote envenenado não avançou o offset
    assert len(h.db.raw) == 5


async def test_oversize_twice_at_same_offset_stops(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(oversize=True), Behavior(oversize=True)]
    h = Harness(server)
    h.start()  # a chain nasce STOPPED (sql/003): espere a conexão antes de conferir o estado
    await h.until(lambda: len(server.connections) == 2 and h.state() is ConsumerState.STOPPED)
    await h.stop()
    assert len(server.connections) == 2


async def test_silent_server_triggers_liveness_reconnect(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(silent=True)]
    h = Harness(server)
    h.start()
    await h.until(lambda: len(server.connections) >= 2 and h.offset == Offset("5"))
    await h.stop()


async def test_server_pings_are_answered(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(server_ping=True)]
    h = Harness(server)
    h.start()
    await h.until(lambda: bool(server.connections) and server.connections[0].pongs_received >= 2)
    await h.stop()


async def test_token_near_expiry_drains_and_reconnects(server: FakeOhipServer) -> None:
    h = Harness(server)
    h.issuer.lifetime_s = 1.0  # vida curta: margem efetiva de 0,5 s (meia-vida)
    h.start()
    await h.until(lambda: len(server.connections) >= 2)
    await h.stop()
    assert server.connections[0].frames[-1]["type"] == "complete"
    assert server.connections[1].token != server.connections[0].token


async def test_subscription_error_frame_reconnects(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(error_after_events=2)]
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    assert server.connections[0].frames[-1]["type"] == "complete"
    assert server.connections[1].subscribe_offsets == ["2"]


async def test_wrong_app_key_hash_is_a_handshake_rejection(server: FakeOhipServer) -> None:
    h = Harness(server)
    h.deps.connector = WebsocketsConnector(
        url=server.url.replace("key=", "key=0"), max_message_bytes=1000
    )
    h.start()
    await asyncio.sleep(0.3)
    assert server.connections == []  # HTTP 400 antes do WebSocket abrir
    assert h.task is not None
    assert not h.task.done()  # espera e tenta de novo, sem cair
    await h.stop()


async def test_status_check_before_subscribe(server: FakeOhipServer) -> None:
    h = Harness(server, status_check_enabled=True)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    queries = server.connections[0].queries
    assert queries[0] == "query { connection { id status } }"
    assert queries[1].startswith("subscription")


async def test_restart_after_crash_waits_the_full_gap(server: FakeOhipServer) -> None:
    h = Harness(server)
    h.db.statuses[CHAIN] = StatusRow(ConsumerState.SUBSCRIBED, "vm-antiga")  # caiu assinado
    loop = asyncio.get_running_loop()
    started = loop.time()
    h.start()
    await h.until(lambda: bool(server.connections))
    assert server.connections[0].opened_at - started >= FAST.min_gap_s * 0.9
    await h.stop()


async def test_lease_is_released_only_by_its_owner(server: FakeOhipServer) -> None:
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    h.db.leases[f"consumer:{CHAIN}"] += 1  # outro processo assumiu (failover)
    h.lease._lose("teste")
    assert h.task is not None
    await asyncio.wait_for(h.task, 5)  # sai sozinho, sem liberar o lease do outro
    assert h.lease.lost


async def test_stalled_intake_poisons_and_resumes(
    server: FakeOhipServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness(server, intake_max_items=1, intake_stall_timeout_s=0.1, batch_max_events=1)
    real_execute = h.batch.execute
    calls = {"n": 0}

    async def slow_first(context: Any, messages: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(0.5)  # banco lento: a fila enche e o put espera demais
        return await real_execute(context, messages)

    monkeypatch.setattr(h.batch, "execute", slow_first)
    h.start()
    await h.until(lambda: h.offset == Offset("5") and len(server.connections) >= 2)
    await h.stop()
    assert h.metrics.total("ohip_connection_poisoned_total") >= 1
    assert server.connections[0].frames[-1]["type"] == "complete"
    assert len(h.db.raw) == 5


async def test_rejected_credentials_back_off_and_retry(server: FakeOhipServer) -> None:
    h = Harness(server)
    h.issuer.fail = [AuthRejectedError("401")]
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    assert len(server.connections) == 1  # nem chegou a abrir o WebSocket na 1ª tentativa


async def test_status_unavailable_at_start_waits_the_full_gap(
    server: FakeOhipServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness(server)

    async def unavailable(chain_code: str) -> Any:
        raise StoreUnavailableError("fora")

    monkeypatch.setattr(h.db, "disconnect_snapshot", unavailable)
    started = asyncio.get_running_loop().time()
    h.start()
    await h.until(lambda: bool(server.connections))
    assert server.connections[0].opened_at - started >= FAST.min_gap_s * 0.9
    await h.stop()


async def test_unexpected_write_error_poisons_quickly(
    server: FakeOhipServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = Harness(server, batch_max_events=1)
    real_execute = h.batch.execute
    calls = {"n": 0}

    async def fail_second(context: Any, messages: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 2:
            raise StoreOperationError("ORA-01031: insufficient privileges")
        return await real_execute(context, messages)

    monkeypatch.setattr(h.batch, "execute", fail_second)
    started = asyncio.get_running_loop().time()
    h.start()
    await h.until(lambda: len(server.connections) >= 2 and h.offset == Offset("5"))
    assert asyncio.get_running_loop().time() - started < 3  # não esperou a fila encher
    await h.stop()
    assert h.metrics.total("ohip_connection_poisoned_total") == 1
    assert server.connections[1].subscribe_offsets == ["1"]
    assert len(h.db.raw) == 5


async def test_unexpected_read_error_ends_the_session(
    server: FakeOhipServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ohip_streaming.domain import protocol

    real_classify = protocol.classify
    calls = {"n": 0}

    def flaky(raw: str) -> Any:
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("bug no adapter")
        return real_classify(raw)

    monkeypatch.setattr(protocol, "classify", flaky)
    h = Harness(server)
    h.start()
    await h.until(lambda: len(server.connections) >= 2 and h.offset == Offset("5"))
    await h.stop()
    assert len(h.db.raw) == 5


async def test_stop_while_waiting_for_the_lease(server: FakeOhipServer) -> None:
    h = Harness(server)
    store = h.lease._store
    assert isinstance(store, MemoryLeaseStore)
    store.holders[f"consumer:{CHAIN}"] = ("outra-vm", h.db.clock.now() + timedelta(hours=1))
    active = StatusRow(ConsumerState.SUBSCRIBED, "outra-vm")  # a ativa está assinada
    h.db.statuses[CHAIN] = active
    h.start()
    await asyncio.sleep(0.1)
    await h.stop()  # instância passiva sai na hora, sem SIGKILL
    assert server.connections == []
    assert h.db.statuses[CHAIN] == active  # sem lease, não toca no estado da chain


async def test_repeated_oauth_rejection_stops(server: FakeOhipServer) -> None:
    h = Harness(server, auth_rejected_max=3)
    h.issuer.fail = [AuthRejectedError("401")] * 3
    h.start()
    await h.until(lambda: h.metrics.total("ohip_token_issued_total") == 0 and h.issuer.fail == [])
    await h.until(lambda: h.state() is ConsumerState.STOPPED)
    await asyncio.sleep(0.3)
    assert server.connections == []  # credencial errada: ação humana, sem insistir
    await h.stop()


@pytest.fixture
async def inclusive_server() -> AsyncIterator[FakeOhipServer]:
    async with FakeOhipServer(
        app_key=APP_KEY, valid_tokens={"token-1"}, events=5, inclusive_offset=True
    ) as s:
        yield s


async def test_inclusive_offset_redelivery_is_deduplicated(
    inclusive_server: FakeOhipServer,
) -> None:
    """D-1: se o OHIP reenviar o evento do offset pedido, a dedup descarta."""
    inclusive_server.behaviors = [Behavior(close_after_events=3, close_code=4504)]
    h = Harness(inclusive_server)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    await h.stop()
    assert inclusive_server.connections[1].subscribe_offsets == ["3"]
    assert len(h.db.raw) == 5
    assert h.metrics.total("ohip_duplicates_total") >= 1  # o evento 3 voltou


async def test_drain_writes_events_arriving_after_complete(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(hold_after_events=2)]
    h = Harness(server)
    h.start()
    await h.until(lambda: h.offset == Offset("2"))
    await h.stop()  # complete → o servidor manda 3..5 → gravados → servidor fecha
    assert server.connections[0].frames[-1]["type"] == "complete"
    assert h.offset == Offset("5")
    assert len(h.db.raw) == 5


async def test_poisoned_connection_writes_nothing_after_complete(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(hold_after_events=3)]
    h = Harness(server, batch_max_events=1)
    seen: list[tuple[str | None, int]] = []

    def fail_second(batch: Any) -> Exception | None:
        seen.append((batch.offset.value if batch.offset else None, len(server.connections)))
        return StoreUnavailableError("fora") if len(seen) == 2 else None

    h.db.fail_persist_when = fail_second
    h.start()
    await h.until(lambda: len(server.connections) >= 2 and h.offset == Offset("5"))
    await h.stop()
    first_connection = [offset for offset, conn in seen if conn == 1]
    assert first_connection[0] == "1"
    assert all(o in ("1", "2") for o in first_connection)  # 4 e 5 chegaram depois: descartados
    assert server.connections[1].subscribe_offsets == ["1"]  # nenhum salto de offset
    assert len(h.db.raw) == 5


# ---------------------------------------------------------------- OHIP_CONSUMER_STATUS (ADR-0020)


async def test_status_row_tracks_the_connection(server: FakeOhipServer) -> None:
    h = Harness(server, status_interval_s=0.05)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))

    def healthy() -> bool:
        row = h.db.statuses[CHAIN]
        return None not in (row.last_message_at, row.last_ping_at, row.last_pong_at, row.rtt_ms)

    await h.until(healthy)
    row = h.db.statuses[CHAIN]
    assert row.state is ConsumerState.SUBSCRIBED
    assert row.subscription_id == server.connections[0].frames[1]["id"]  # GUID do subscribe
    assert row.connected_at is not None
    assert row.token_expires_at is not None
    assert row.token_expires_at > h.clock.now()
    assert row.instance_id == "vm1:1"
    assert row.rtt_ms is not None
    assert row.rtt_ms >= 0

    await h.stop()
    final = h.db.statuses[CHAIN]
    assert final.state is ConsumerState.WAITING
    assert (final.reconnects, final.consecutive_failures, final.next_attempt_at) == (0, 0, None)


async def test_reconnect_records_failures_and_next_attempt(server: FakeOhipServer) -> None:
    server.behaviors = [Behavior(close_after_subscribe=4504)]  # sessão sem evento: falha
    h = Harness(server)
    disconnects: list[dict[str, Any]] = []
    original = h.db.record_disconnect

    async def spy(*args: Any, **kwargs: Any) -> None:
        disconnects.append(kwargs)
        await original(*args, **kwargs)

    h.db.record_disconnect = spy  # type: ignore[method-assign]
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    row = h.db.statuses[CHAIN]
    assert (row.reconnects, row.consecutive_failures) == (1, 1)
    assert row.next_attempt_at is None  # limpo ao assinar de novo
    await h.stop()

    first, last = disconnects[0], disconnects[-1]
    assert first == {
        "consecutive_failures": 1,
        "reconnect": True,
        "next_attempt_in_s": pytest.approx(FAST.backoff_4504_s),
    }
    # A 2ª sessão recebeu eventos (saudável): na parada, falhas zeradas e sem próxima tentativa.
    assert last == {"consecutive_failures": 0, "reconnect": False, "next_attempt_in_s": None}
    assert h.db.statuses[CHAIN].reconnects == 1


async def test_status_write_failure_does_not_break_the_session(server: FakeOhipServer) -> None:
    h = Harness(server, status_interval_s=0.05)
    calls = 0

    async def broken(chain_code: str, health: Any) -> None:
        nonlocal calls
        calls += 1
        raise StoreOperationError("ORA-01031: privilégios insuficientes")

    h.db.record_health = broken  # type: ignore[method-assign]
    h.start()
    await h.until(lambda: h.offset == Offset("5") and calls >= 2)
    assert len(server.connections) == 1  # o status é informativo: a conexão segue
    assert h.state() is ConsumerState.SUBSCRIBED
    await h.stop()


async def test_lost_lease_never_touches_the_new_owners_row(server: FakeOhipServer) -> None:
    h = Harness(server, status_interval_s=0.05)
    h.start()
    await h.until(lambda: h.offset == Offset("5"))
    # Failover: a nova dona assinou e gravou a linha; a antiga ainda vai drenar e sair.
    h.db.leases[f"consumer:{CHAIN}"] += 1
    new_owner = StatusRow(ConsumerState.SUBSCRIBED, "vm-nova", subscription_id="sub-nova")
    h.db.statuses[CHAIN] = new_owner
    h.lease._lose("teste")
    assert h.task is not None
    await asyncio.wait_for(h.task, 5)
    assert h.db.statuses[CHAIN] == new_owner  # nem DRAINING, nem saúde, nem WAITING


async def test_epoch_barrier_failure_stops_status_writes(server: FakeOhipServer) -> None:
    h = Harness(server, status_interval_s=0.05)
    log: list[str] = []
    for name in ("record_state", "record_subscribed", "record_health", "record_disconnect"):
        original = getattr(h.db, name)

        def spy(*args: Any, _name: str = name, _original: Any = original, **kwargs: Any) -> Any:
            log.append(_name)
            return _original(*args, **kwargs)

        setattr(h.db, name, spy)

    def barrier(batch: Any) -> Exception | None:
        log.append("barreira")  # outra instância assumiu: o UPDATE do epoch não casou
        return LeaseLostError("epoch mudou")

    h.db.fail_persist_when = barrier
    h.start()
    await h.until(lambda: "barreira" in log)
    assert h.task is not None
    await asyncio.wait_for(h.task, 5)  # sai sem reconectar
    assert "record_subscribed" in log
    assert log[log.index("barreira") + 1 :] == []  # nada gravado depois da barreira
    assert len(server.connections) == 1
