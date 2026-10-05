"""Composição de produção da API, fábrica do Uvicorn e processo ``ohip-api`` (ADR-0017)."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
import structlog
import uvicorn
from fastapi.testclient import TestClient
from tests.fakes.oracle_driver import FakeOracle
from tests.fakes.redis_sync import FakeSyncRedis

from ohip_streaming.adapters.oracle import monitoring_store as sql
from ohip_streaming.application.errors import BrokerUnavailableError
from ohip_streaming.entrypoints.api import app as app_module
from ohip_streaming.entrypoints.api import server
from ohip_streaming.entrypoints.api.app import ApiConfig, build_app, compose, create_app
from ohip_streaming.entrypoints.api.security import Role, token_hash
from ohip_streaming.entrypoints.api.services import StatusService

TOKEN = "token-de-teste"


@pytest.fixture(autouse=True)
def api_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("ORACLE_DSN", "localhost/teste")
    monkeypatch.setenv("ORACLE_USER", "teste")
    monkeypatch.setenv("ORACLE_PASSWORD", "senha-teste")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://u:senha@localhost/v")
    monkeypatch.setenv("API_SERVICE_TOKENS", f'{{"{token_hash(TOKEN)}": "admin"}}')
    monkeypatch.setenv("API_READY_CACHE_S", "0")
    monkeypatch.setenv("OHIP_MODULE_CODES", '{"Reservation": "rsv"}')
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)


def test_config_does_not_need_ohip_credentials() -> None:
    config = ApiConfig.load()
    assert config.routing.module_codes == {"reservation": "rsv"}
    assert config.api.host == "127.0.0.1"


def test_compose_wires_oracle_redis_and_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    probes: list[tuple[str, float]] = []

    async def probe(url: str, *, timeout_s: float) -> None:
        probes.append((url, timeout_s))

    monkeypatch.setattr(app_module, "probe_broker", probe)
    oracle = FakeOracle()
    services = compose(ApiConfig.load(), oracle, FakeSyncRedis())
    assert services.tokens == {token_hash(TOKEN): Role.ADMIN}

    client = TestClient(build_app(services))
    ready = client.get("/ready")
    assert ready.status_code == 200, ready.text
    assert set(ready.json()["checks"]) == {"oracle", "redis", "rabbitmq"}
    assert sql.PING_SQL in oracle.statements()
    assert probes == [("amqp://u:senha@localhost/v", 10.0)]

    async def down(url: str, *, timeout_s: float) -> None:
        raise BrokerUnavailableError("fora")

    monkeypatch.setattr(app_module, "probe_broker", down)
    failed = client.get("/ready")
    assert failed.status_code == 503
    assert failed.json()["checks"]["rabbitmq"]["error"] == "BrokerUnavailableError"
    assert "senha" not in failed.text


def test_compose_status_goes_to_oracle_then_cache() -> None:
    oracle = FakeOracle()
    oracle.rows(sql.DB_NOW_SQL, [(datetime(2026, 10, 1, 12, tzinfo=UTC).replace(tzinfo=None),)])
    redis = FakeSyncRedis()
    client = TestClient(build_app(compose(ApiConfig.load(), oracle, redis)))
    headers = {"Authorization": f"Bearer {TOKEN}"}
    first = client.get("/api/v1/status", headers=headers)
    assert first.status_code == 200
    assert redis.data[b"ohip:api:status"] == first.content
    calls = len(oracle.calls)
    assert client.get("/api/v1/status", headers=headers).content == first.content
    assert len(oracle.calls) == calls  # veio do Redis


def test_create_app_opens_and_closes_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    oracle = FakeOracle()
    closed: list[str] = []

    class Redis(FakeSyncRedis):
        def close(self) -> None:
            closed.append("redis")

    class Pool(FakeOracle):
        def close(self, force: bool = False) -> None:
            closed.append(f"pool:{force}")

    pool = Pool()
    monkeypatch.setattr(app_module, "open_pool", lambda settings: pool)
    monkeypatch.setattr(app_module, "open_sync_redis", lambda settings: Redis())
    with TestClient(create_app()) as client:
        assert client.get("/health").status_code == 200
        assert client.app.state.services is not None  # type: ignore[attr-defined]
    assert closed == ["redis", "pool:True"]
    assert oracle.calls == []


def test_create_app_warns_without_tokens(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("API_SERVICE_TOKENS", "{}")
    create_app()  # configura o log JSON na saída padrão (capturada pelo capsys)
    events = [json.loads(line)["event"] for line in capsys.readouterr().out.splitlines() if line]
    assert "api_sem_tokens_de_servico" in events


def test_server_main_runs_uvicorn_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, dict[str, Any]]] = []
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: calls.append((app, kw)))
    monkeypatch.setenv("API_PORT", "9090")
    monkeypatch.setenv("API_WORKERS", "3")
    assert server.main() == 0
    ((target, options),) = calls
    assert target == "ohip_streaming.entrypoints.api.app:create_app"
    assert options["factory"] is True
    assert (options["host"], options["port"], options["workers"]) == ("127.0.0.1", 9090, 3)
    assert options["access_log"] is False


def test_api_uses_its_own_oracle_call_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ORACLE_CALL_TIMEOUT_MS", "12000")
    oracle = FakeOracle()
    connections: list[Any] = []
    acquire = oracle.acquire

    def recording_acquire() -> Any:
        connection = acquire()
        connections.append(connection)
        return connection

    monkeypatch.setattr(oracle, "acquire", recording_acquire)
    services = compose(ApiConfig.load(), oracle, FakeSyncRedis())
    client = TestClient(build_app(services))
    response = client.get("/api/v1/replay", headers={"Authorization": f"Bearer {TOKEN}"})
    assert response.status_code == 200
    assert connections
    assert {c.call_timeout for c in connections} == {12_000}  # não o ORACLE_CALL_TIMEOUT_MS


@pytest.mark.parametrize(("environment", "exposed"), [("producao", False), ("homologacao", True)])
def test_docs_are_off_in_production(
    monkeypatch: pytest.MonkeyPatch, environment: str, exposed: bool
) -> None:
    monkeypatch.setenv("APP_ENVIRONMENT", environment)
    monkeypatch.setattr(app_module, "open_pool", lambda settings: FakeOracle())
    monkeypatch.setattr(app_module, "open_sync_redis", lambda settings: FakeSyncRedis())
    with TestClient(create_app()) as client:
        for path in ("/docs", "/redoc", "/openapi.json"):
            assert (client.get(path).status_code == 200) is exposed, path
        assert client.get("/health").status_code == 200


# ------------------------------------------------------------------ status: cache e single-flight


@dataclass
class Cache:
    stored: bytes | None = None
    broken: bool = False

    def get_status(self) -> bytes | None:
        return None if self.broken else self.stored

    def put_status(self, payload: bytes) -> None:
        if not self.broken:
            self.stored = payload


def test_status_is_computed_once_per_worker_at_a_time() -> None:
    started, release = threading.Event(), threading.Event()
    calls: list[int] = []

    def slow() -> bytes:
        calls.append(1)
        started.set()
        release.wait(5)
        return b"{}"

    service = StatusService(cache=Cache(), compute=slow, ttl_s=5)
    with ThreadPoolExecutor(8) as pool:
        first = pool.submit(service.payload)
        started.wait(5)
        others = [pool.submit(service.payload) for _ in range(7)]
        release.set()
        results = [first.result(5), *(f.result(5) for f in others)]
    assert results == [b"{}"] * 8
    assert len(calls) == 1


def test_status_local_copy_covers_redis_down() -> None:
    now = [0.0]
    calls: list[int] = []

    def compute() -> bytes:
        calls.append(1)
        return b'{"n":%d}' % len(calls)

    service = StatusService(
        cache=Cache(broken=True), compute=compute, ttl_s=5, clock=lambda: now[0]
    )
    assert service.payload() == b'{"n":1}'
    now[0] = 4.9
    assert service.payload() == b'{"n":1}'  # cópia local, sem Redis
    now[0] = 5.0
    assert service.payload() == b'{"n":2}'  # venceu
