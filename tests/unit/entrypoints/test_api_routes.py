"""API de controle com TestClient e os fakes em memória (docs/API.md, ADR-0017)."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from typing import Any
from zoneinfo import ZoneInfo

import pytest
import structlog
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from tests.fakes.frames import SUBSCRIPTION_ID, frame
from tests.fakes.memory import (
    EXCHANGES,
    T0,
    FakeClock,
    FakeMetrics,
    FakeSeenCache,
    InMemoryDatabase,
    MemoryMonitoringStore,
)

from ohip_streaming.adapters.redis.api_cache import ProcessSnapshot
from ohip_streaming.application.errors import StoreUnavailableError
from ohip_streaming.application.ports import ProcessingStatus
from ohip_streaming.application.use_cases.monitoring import Monitoring
from ohip_streaming.application.use_cases.operations import (
    MessagingOptions,
    ReprocessEvent,
    RetryDlqItem,
)
from ohip_streaming.application.use_cases.process_event_batch import (
    BatchOptions,
    ConsumerContext,
    IncomingMessage,
    ProcessEventBatch,
)
from ohip_streaming.application.use_cases.replay import CancelReplay, RequestReplay
from ohip_streaming.domain.errors import InvalidIdentifierError
from ohip_streaming.domain.masking import MaskingPolicy
from ohip_streaming.entrypoints.api.app import build_app
from ohip_streaming.entrypoints.api.routes import api, internal
from ohip_streaming.entrypoints.api.security import Role, token_hash
from ohip_streaming.entrypoints.api.services import (
    ApiServices,
    MetricsService,
    Readiness,
    StatusService,
    TtlCache,
    status_payload,
)
from ohip_streaming.logging import configure_logging

CHAIN = "CHAIN1"
READ_TOKEN = "token-de-teste-leitura"
ADMIN_TOKEN = "token-de-teste-admin"
READ = {"Authorization": f"Bearer {READ_TOKEN}"}
ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}", "X-Actor": "ana"}
PII = [
    {"elementName": "FIRST NAME", "oldValue": "Ana", "newValue": "Maria"},
    {"elementName": "ARRIVAL DATE", "oldValue": "2026-10-10", "newValue": "2026-10-11"},
]


@dataclass
class FakeStatusCache:
    stored: bytes | None = None
    puts: int = 0

    def get_status(self) -> bytes | None:
        return self.stored

    def put_status(self, payload: bytes) -> None:
        self.stored = payload
        self.puts += 1


@dataclass
class Env:
    db: InMemoryDatabase
    store: MemoryMonitoringStore
    cache: FakeStatusCache
    client: TestClient
    checks: dict[str, Callable[[], object]] = field(default_factory=dict)
    snapshots: list[ProcessSnapshot] = field(default_factory=list)


async def _seed(db: InMemoryDatabase, *frames: str) -> None:
    epoch = db.acquire(f"consumer:{CHAIN}")
    use_case = ProcessEventBatch(
        store=db,
        seen=FakeSeenCache(),
        clock=db.clock,
        metrics=FakeMetrics(),
        options=BatchOptions(),
    )
    await use_case.execute(
        ConsumerContext(CHAIN, SUBSCRIPTION_ID, epoch, ZoneInfo("UTC")),
        [IncomingMessage(f, db.clock.now()) for f in frames],
    )


def seeded_db() -> InMemoryDatabase:
    db = InMemoryDatabase(clock=FakeClock())
    db.provision_chain(CHAIN)
    asyncio.run(
        _seed(
            db,
            frame("90", detail=PII),
            frame("100", event_name="CHECKIN RESERVATION", module_name="Reservation"),
            frame("110", hotel_id="HOTEL2"),
        )
    )
    return db


@pytest.fixture(autouse=True)
def json_logs() -> Iterator[None]:
    configure_logging(service="ohip-api", environment="homologacao", code_version="t", level="INFO")
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)


def make_env(db: InMemoryDatabase | None = None, *, raise_errors: bool = True) -> Env:
    db = db or seeded_db()
    store = MemoryMonitoringStore(db)
    cache = FakeStatusCache()
    monitoring = Monitoring(store=store, masking=MaskingPolicy(), module_codes={})
    messaging = MessagingOptions(exchange_names=EXCHANGES)
    env = Env(db, store, cache, client=None)  # type: ignore[arg-type]
    env.checks = {"oracle": lambda: asyncio.run(monitoring.ping()), "redis": lambda: None}
    services = ApiServices(
        tokens={token_hash(READ_TOKEN): Role.READ, token_hash(ADMIN_TOKEN): Role.ADMIN},
        monitoring=monitoring,
        request_replay=RequestReplay(store=db, clock=db.clock),
        cancel_replay=CancelReplay(store=db),
        reprocess=ReprocessEvent(store=db, options=messaging),
        retry_dlq=RetryDlqItem(store=db, options=messaging),
        status=StatusService(cache=cache, compute=status_payload(monitoring), ttl_s=0),
        ready=TtlCache(lambda: Readiness(env.checks).check(), 0),
        metrics=TtlCache(
            MetricsService(monitoring=monitoring, snapshots=lambda: env.snapshots).render, 0
        ),
    )
    env.client = TestClient(build_app(services), raise_server_exceptions=raise_errors)
    return env


@pytest.fixture
def env() -> Env:
    return make_env()


def logs(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    """Eventos do structlog (o ``msg`` de cada registro é o dicionário do evento)."""
    return [r.msg for r in caplog.records if isinstance(r.msg, dict)]


# ------------------------------------------------------------------ autenticação e erros


def test_health_needs_no_token(env: Env) -> None:
    response = env.client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer outro"}, {"Authorization": "x"}])
def test_api_requires_a_known_token(env: Env, headers: dict[str, str]) -> None:
    response = env.client.get("/api/v1/status", headers=headers)
    assert response.status_code == 401
    body = response.json()["error"]
    assert body["code"] == "UNAUTHENTICATED"
    assert body["request_id"] == response.headers["X-Request-ID"]


def test_read_token_cannot_run_admin_actions(env: Env) -> None:
    response = env.client.post(
        "/api/v1/events/uid-90/reprocess", headers={**READ, "X-Actor": "ana"}
    )
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"


def test_admin_action_requires_actor(env: Env) -> None:
    headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    response = env.client.post("/api/v1/events/uid-90/reprocess", headers=headers)
    assert response.status_code == 403
    assert "X-Actor" in response.json()["error"]["message"]


def test_actor_too_long_is_rejected(env: Env) -> None:
    response = env.client.get("/api/v1/status", headers={**READ, "X-Actor": "a" * 101})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_request_id_is_echoed_only_when_safe(env: Env) -> None:
    ok = env.client.get("/health", headers={"X-Request-ID": "abc-123"})
    assert ok.headers["X-Request-ID"] == "abc-123"
    bad = env.client.get("/health", headers={"X-Request-ID": "a b"})
    assert bad.headers["X-Request-ID"] != "a b"


def test_validation_error_format(env: Env) -> None:
    response = env.client.get("/api/v1/events?limit=500", headers=READ)
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert "500" not in error["message"]  # não ecoa o valor recebido


def test_database_down_is_503(env: Env) -> None:
    env.store.unavailable = StoreUnavailableError("ORA-03113")
    response = env.client.get("/api/v1/events", headers=READ)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"
    assert "ORA" not in response.json()["error"]["message"]


def test_unexpected_error_is_500_without_detail() -> None:
    env = make_env(raise_errors=False)
    env.store.unavailable = RuntimeError("detalhe-interno")
    response = env.client.get("/api/v1/events", headers=READ)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"
    assert "detalhe-interno" not in response.text
    assert response.headers["X-Request-ID"] == response.json()["error"]["request_id"]


def test_every_route_is_sync() -> None:
    """ADR-0003: rota que toca o Oracle é ``def`` (threadpool), nunca ``async def``."""
    routes = [r for router in (api, internal) for r in router.routes if isinstance(r, APIRoute)]
    assert len(routes) == 14  # as 14 rotas da tabela do API.md
    assert not [r.path for r in routes if inspect.iscoroutinefunction(r.endpoint)]


def test_request_log_has_no_query_string(env: Env, caplog: pytest.LogCaptureFixture) -> None:
    caplog.clear()
    env.client.get("/api/v1/events?primary_key=VALOR-SENSIVEL", headers=READ)
    records = logs(caplog)  # só structlog (o httpx do cliente de teste loga a URL)
    (line,) = [r for r in records if r["event"] == "api_requisicao"]
    assert line["path"] == "/api/v1/events"
    assert line["status"] == 200
    assert line["role"] == "read"
    assert line["request_id"]
    assert "VALOR-SENSIVEL" not in json.dumps(records)
    assert READ_TOKEN not in json.dumps(records)


# ------------------------------------------------------------------ status


def test_status_contract_and_cache(env: Env) -> None:
    response = env.client.get("/api/v1/status", headers=READ)
    assert response.status_code == 200
    (chain,) = response.json()["chains"]
    assert chain["chain_code"] == CHAIN
    assert chain["state"] == "STOPPED"
    assert chain["last_offset"] == "110"
    assert chain["events_per_minute"] == 0.6  # 3 eventos em 5 min
    assert chain["outbox"] == {"pending": 3, "failed": 0, "oldest_pending_seconds": 0.0}
    assert chain["dlq_open"] == {"CONSUME": 0, "PUBLISH": 0, "NORMALIZE": 0, "ENRICH": 0}
    assert chain["last_close"] is None
    assert env.cache.puts == 1

    env.db.provision_chain("CHAIN2")  # muda o banco: a resposta ainda vem do cache
    cached = env.client.get("/api/v1/status", headers=READ)
    assert cached.json() == response.json()
    assert env.cache.puts == 1


# ------------------------------------------------------------------ eventos


def test_events_are_paginated_by_key(env: Env) -> None:
    first = env.client.get("/api/v1/events?limit=2", headers=READ).json()
    assert [e["offset"] for e in first["items"]] == ["110", "100"]
    assert first["next_cursor"] == first["items"][-1]["raw_event_id"]
    second = env.client.get(
        f"/api/v1/events?limit=2&cursor={first['next_cursor']}", headers=READ
    ).json()
    assert [e["offset"] for e in second["items"]] == ["90"]
    assert second["next_cursor"] is None


def test_events_filters(env: Env) -> None:
    def offsets(query: str | dict[str, str]) -> list[str]:
        if isinstance(query, dict):
            response = env.client.get("/api/v1/events", params=query, headers=READ)
        else:
            response = env.client.get(f"/api/v1/events?{query}", headers=READ)
        return [e["offset"] for e in response.json()["items"]]

    assert offsets("event_name=checkin%20reservation") == ["100"]  # normalizado
    assert offsets("module_name=RESERVATION") == ["110", "100", "90"]  # sem caixa
    assert offsets("hotel_id=HOTEL2") == ["110"]
    assert offsets("processing_status=IGNORED") == []
    window = {"from": T0.isoformat(), "to": "2026-10-01T12:00:00.001"}  # sem fuso = UTC
    assert offsets(window) == ["110", "100", "90"]
    assert offsets({"from": "2026-10-01T12:00:00.001Z"}) == []


def test_event_detail_is_masked(env: Env) -> None:
    body = env.client.get("/api/v1/events/uid-90", headers=READ).json()
    assert body["masked"] is True
    detail = {d["element_name"]: d for d in body["detail"]}
    assert (detail["FIRST NAME"]["old_value"], detail["FIRST NAME"]["new_value"]) == ("***", "***")
    assert detail["ARRIVAL DATE"]["new_value"] == "2026-10-11"
    assert body["routing_key"] == "ohip.reservation.UPDATE_RESERVATION"
    (outbox,) = body["outbox"]
    assert outbox["status"] == "PENDING"
    assert body["dlq"] == []
    assert "Maria" not in json.dumps(body)


def test_unmasked_detail_is_admin_only_and_audited(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    denied = env.client.get("/api/v1/events/uid-90?unmasked=true", headers=READ)
    assert denied.status_code == 403

    caplog.clear()
    body = env.client.get("/api/v1/events/uid-90?unmasked=true", headers=ADMIN).json()
    assert body["masked"] is False
    assert {d["new_value"] for d in body["detail"]} == {"Maria", "2026-10-11"}
    (audit,) = [r for r in logs(caplog) if r["event"] == "auditoria_dado_sem_mascara"]
    assert (audit["actor"], audit["unique_event_id"]) == ("ana", "uid-90")


def test_event_not_found(env: Env) -> None:
    response = env.client.get("/api/v1/events/nao-existe", headers=READ)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


def test_export_is_masked_by_default(env: Env) -> None:
    response = env.client.get("/api/v1/events/uid-90/export", headers=ADMIN)
    assert response.status_code == 200
    assert "attachment" in response.headers["Content-Disposition"]
    body = response.json()
    assert body["masked"] is True
    assert body["new_event"]["metadata"]["uniqueEventId"] == "uid-90"
    names = {d["elementName"]: d["newValue"] for d in body["new_event"]["detail"]}
    assert names == {"FIRST NAME": "***", "ARRIVAL DATE": "2026-10-11"}
    assert {d["element_name"]: d["new_value"] for d in body["message"]["detail"]} == {
        "FIRST NAME": "***",
        "ARRIVAL DATE": "2026-10-11",
    }
    assert "Maria" not in response.text
    assert env.client.get("/api/v1/events/uid-90/export", headers=READ).status_code == 403


def test_export_unmasked(env: Env) -> None:
    body = env.client.get("/api/v1/events/uid-90/export?unmasked=true", headers=ADMIN).json()
    assert body["masked"] is False
    assert "Maria" in json.dumps(body["new_event"])


# ------------------------------------------------------------------ comandos


def test_reprocess_goes_through_the_outbox(env: Env) -> None:
    before = len(env.db.outbox)
    response = env.client.post("/api/v1/events/uid-90/reprocess", headers=ADMIN)
    assert response.status_code == 202
    outbox_id = response.json()["outbox_id"]
    assert len(env.db.outbox) == before + 1
    assert env.db.outbox[outbox_id].exchange_name == "ohip.reprocess"


def test_reprocess_of_ignored_event_is_409(env: Env) -> None:
    raw = next(r for r in env.db.raw.values() if r.event.unique_event_id == "uid-90")
    env.db.raw[raw.id] = replace(raw, status=ProcessingStatus.IGNORED)
    response = env.client.post("/api/v1/events/uid-90/reprocess", headers=ADMIN)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "INVALID_STATE"


def replay_body(**overrides: str) -> dict[str, str]:
    body = {"chain_code": CHAIN, "from_offset": "90", "reason": "lacuna INC-1", "confirm": CHAIN}
    body.update(overrides)
    return body


def test_replay_request_list_and_cancel(env: Env) -> None:
    created = env.client.post("/api/v1/replay", headers=ADMIN, json=replay_body())
    assert created.status_code == 202
    request = created.json()
    assert (request["status"], request["from_offset"], request["warnings"]) == (
        "PENDING",
        "90",
        [],
    )

    again = env.client.post("/api/v1/replay", headers=ADMIN, json=replay_body())
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "REPLAY_ALREADY_PENDING"

    listed = env.client.get("/api/v1/replay?status=PENDING", headers=READ).json()
    assert [r["id"] for r in listed["items"]] == [request["id"]]
    assert listed["items"][0]["requested_by"] == "ana"

    cancelled = env.client.delete(f"/api/v1/replay/{request['id']}", headers=ADMIN)
    assert cancelled.status_code == 200
    assert (cancelled.json()["status"], cancelled.json()["cancelled_by"]) == ("CANCELLED", "ana")

    twice = env.client.delete(f"/api/v1/replay/{request['id']}", headers=ADMIN)
    assert twice.status_code == 409
    assert twice.json()["error"]["code"] == "REPLAY_NOT_PENDING"
    assert env.client.delete("/api/v1/replay/999", headers=ADMIN).status_code == 404


def test_replay_warns_when_offset_is_unknown_locally(env: Env) -> None:
    response = env.client.post("/api/v1/replay", headers=ADMIN, json=replay_body(from_offset="5"))
    assert response.status_code == 202
    assert response.json()["warnings"]


@pytest.mark.parametrize(
    ("overrides", "status", "code"),
    [
        ({"from_offset": "999"}, 422, "REPLAY_FORWARD_NOT_ALLOWED"),
        ({"from_offset": "abc"}, 400, "REPLAY_OFFSET_INVALID"),
        ({"confirm": "OUTRA"}, 400, "REPLAY_CONFIRMATION_MISMATCH"),
        ({"reason": "   "}, 400, "REPLAY_REASON_REQUIRED"),
        ({"chain_code": "NOPE", "confirm": "NOPE"}, 404, "NOT_FOUND"),
    ],
)
def test_replay_rules(env: Env, overrides: dict[str, str], status: int, code: str) -> None:
    response = env.client.post("/api/v1/replay", headers=ADMIN, json=replay_body(**overrides))
    assert response.status_code == status
    assert response.json()["error"]["code"] == code


def test_replay_missing_field_is_validation_error(env: Env) -> None:
    body = replay_body()
    del body["reason"]
    response = env.client.post("/api/v1/replay", headers=ADMIN, json=body)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_outbox_listing(env: Env) -> None:
    env.db.clock.advance(30)
    body = env.client.get("/api/v1/outbox?chain_code=CHAIN1&limit=2", headers=READ).json()
    assert len(body["items"]) == 2
    assert body["next_cursor"] is not None
    assert body["oldest_pending_seconds"] is not None
    failed = env.client.get("/api/v1/outbox?status=FAILED", headers=READ).json()
    assert failed["items"] == []
    assert env.client.get("/api/v1/outbox?status=SENT", headers=READ).status_code == 400


def test_dlq_listing_and_publish_retry(env: Env) -> None:
    epoch = env.db.acquire("publisher")
    asyncio.run(env.db.mark_failed(1, 10, "NACK", epoch))
    listed = env.client.get("/api/v1/dlq?stage=PUBLISH&resolved=false", headers=READ).json()
    (item,) = listed["items"]
    assert item["outbox_id"] == 1
    assert "raw_message" not in item

    retried = env.client.post(f"/api/v1/dlq/{item['id']}/retry", headers=ADMIN)
    assert retried.status_code == 202
    assert retried.json()["result"] == "REPUBLISHED"
    assert env.db.dlq[item["id"]].resolved_by == "ana"

    again = env.client.post(f"/api/v1/dlq/{item['id']}/retry", headers=ADMIN)
    assert again.status_code == 409
    assert env.client.get("/api/v1/dlq?resolved=true", headers=READ).json()["items"]


def test_dlq_consume_retry_is_only_a_request() -> None:
    db = seeded_db()
    asyncio.run(_seed(db, "isto não é json"))
    env = make_env(db)
    (item,) = env.client.get("/api/v1/dlq?stage=CONSUME", headers=READ).json()["items"]
    response = env.client.post(f"/api/v1/dlq/{item['id']}/retry", headers=ADMIN)
    assert response.json() == {"result": "CONSUME_REQUESTED", "outbox_id": None}
    assert db.dlq[item["id"]].retry_requested_by == "ana"


# ------------------------------------------------------------------ interno


def test_ready_reports_each_dependency(env: Env) -> None:
    ok = env.client.get("/ready")
    assert ok.status_code == 200
    assert set(ok.json()["checks"]) == {"oracle", "redis"}

    def broken() -> None:
        raise ConnectionError("redis://:senha@host")

    env.checks["redis"] = broken
    failed = env.client.get("/ready")
    assert failed.status_code == 503
    assert failed.json()["checks"]["redis"] == {
        "ok": False,
        "latency_ms": failed.json()["checks"]["redis"]["latency_ms"],
        "error": "ConnectionError",
    }
    assert "senha" not in failed.text


def test_metrics_endpoint(env: Env) -> None:
    env.snapshots.append(
        ProcessSnapshot(
            "publisher",
            "vm1:10:abc",
            ({"name": "ohip_broker_unavailable_total", "labels": {}, "value": 2},),
        )
    )
    response = env.client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    text = response.text
    assert 'ohip_consumer_state{chain_code="CHAIN1",state="STOPPED"} 1' in text
    assert 'ohip_outbox_pending{chain_code="CHAIN1"} 3' in text
    assert 'ohip_broker_unavailable_total{process="publisher",instance="vm1:10:abc"} 2' in text
    assert 'ohip_metrics_source_up{source="oracle"} 1' in text


def test_metrics_survive_oracle_down(env: Env) -> None:
    env.store.unavailable = StoreUnavailableError("fora")
    text = env.client.get("/metrics").text
    assert 'ohip_metrics_source_up{source="oracle"} 0' in text
    assert "ohip_consumer_state" not in text


# ------------------------------------------------------------------ garantias da ponte (ADR-0017)


def test_store_never_runs_on_the_server_event_loop() -> None:
    """O Oracle (store) roda no thread da requisição, fora do event loop do servidor."""
    env = make_env()
    server: dict[str, Any] = {}
    store_calls: list[tuple[int, asyncio.AbstractEventLoop]] = []
    original = env.store.events

    async def spy(*args: Any, **kwargs: Any) -> Any:
        store_calls.append((threading.get_ident(), asyncio.get_running_loop()))
        return await original(*args, **kwargs)

    env.store.events = spy  # type: ignore[method-assign]
    app = env.client.app

    async def recording_app(scope: Any, receive: Any, send: Any) -> None:
        server["thread"], server["loop"] = threading.get_ident(), asyncio.get_running_loop()
        await app(scope, receive, send)

    client = TestClient(recording_app)
    assert client.get("/api/v1/events", headers=READ).status_code == 200
    ((thread, loop),) = store_calls
    assert thread != server["thread"]
    assert loop is not server["loop"]


def test_request_id_reaches_logs_inside_the_use_case(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.clear()
    headers = {**ADMIN, "X-Request-ID": "req-123"}
    env.client.get("/api/v1/events/uid-90?unmasked=true", headers=headers)
    (audit,) = [r for r in logs(caplog) if r["event"] == "auditoria_dado_sem_mascara"]
    assert audit["request_id"] == "req-123"


def test_dlq_response_has_no_raw_message() -> None:
    db = seeded_db()
    asyncio.run(_seed(db, "isto não é json com dado pessoal"))
    env = make_env(db)
    body = env.client.get("/api/v1/dlq", headers=READ)
    (item,) = body.json()["items"]
    assert "raw_message" not in item
    assert "stack_trace" not in item
    assert "dado pessoal" not in body.text


def test_other_domain_errors_are_validation_errors(env: Env) -> None:
    env.store.unavailable = InvalidIdentifierError("chain inválida")
    response = env.client.get("/api/v1/events", headers=READ)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_outbox_age_uses_the_database_clock(env: Env) -> None:
    env.db.clock.advance(42)  # o "banco" andou; o relógio da VM não importa
    body = env.client.get("/api/v1/outbox", headers=READ).json()
    assert body["oldest_pending_seconds"] == 42.0


@pytest.mark.parametrize(
    ("overrides", "echoed"),
    [
        ({"from_offset": "12a"}, "12a"),
        ({"from_offset": "9876543"}, "9876543"),
        ({"reason": "x" * 501}, "x" * 50),
    ],
)
def test_domain_error_messages_do_not_echo_input(
    env: Env, overrides: dict[str, str], echoed: str
) -> None:
    response = env.client.post("/api/v1/replay", headers=ADMIN, json=replay_body(**overrides))
    assert response.status_code in (400, 422)
    message = response.json()["error"]["message"]
    assert echoed not in message
    assert "110" not in message  # nem o último offset confirmado


def test_other_domain_error_message_is_generic(env: Env) -> None:
    env.store.unavailable = InvalidIdentifierError("chain inválida: 'SEGREDO'")
    response = env.client.get("/api/v1/events", headers=READ)
    assert response.json()["error"]["message"] == "parâmetro com formato inválido"
