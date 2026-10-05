"""Cliente REST do enricher: headers, respostas, rate limit e cache (ADR-0019)."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

import httpx
import pytest
from tests.fakes.memory import T0, FakeClock, FakeMetrics

from ohip_streaming.adapters.ohip_rest.resources import (
    OhipResourceFetcher,
    RateLimiter,
    _retry_after_s,
    resource_cache_key,
)
from ohip_streaming.application.errors import (
    AuthRejectedError,
    AuthUnavailableError,
    ResourceRejectedError,
    ResourceUnavailableError,
)
from ohip_streaming.application.ports import AccessToken

PATHS = {
    "Reservation": "/rsv/v1/hotels/{hotelId}/reservations/{primaryKey}",
    "PROFILE": "/crm/v1/profiles/{primaryKey}",
}


class Tokens:
    def __init__(self, error: Exception | None = None) -> None:
        self.issued = 0
        self.invalidated = 0
        self.error = error

    async def get(self) -> AccessToken:
        if self.error is not None:
            raise self.error
        self.issued += 1
        return AccessToken(f"tok-{self.issued}", T0 + timedelta(hours=1))

    async def invalidate(self) -> None:
        self.invalidated += 1


class Cache:
    def __init__(self) -> None:
        self.data: dict[str, Mapping[str, Any] | None] = {}

    async def get(self, key: str) -> tuple[bool, Mapping[str, Any] | None]:
        return (key in self.data), self.data.get(key)

    async def put(self, key: str, value: Mapping[str, Any] | None) -> None:
        self.data[key] = value


Responder = Callable[[httpx.Request], httpx.Response]


def fetcher(
    respond: Responder | list[httpx.Response], *, tokens: Tokens | None = None
) -> tuple[OhipResourceFetcher, list[httpx.Request], Tokens, Cache, FakeClock]:
    seen: list[httpx.Request] = []
    queue = list(respond) if isinstance(respond, list) else None

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return queue.pop(0) if queue is not None else respond(request)  # type: ignore[operator]

    clock = FakeClock()
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://gw")
    tokens = tokens or Tokens()
    cache = Cache()
    f = OhipResourceFetcher(
        http,
        tokens={"C1": tokens},
        app_key="app-key-secreta",
        paths=PATHS,
        limiter=RateLimiter(rate_per_s=100, burst=10, clock=clock),
        cache=cache,
        clock=clock,
        metrics=FakeMetrics(),
        auth_rejected_alert=3,
    )
    return f, seen, tokens, cache, clock


async def test_headers_path_and_cache() -> None:
    f, seen, _, cache, _ = fetcher([httpx.Response(200, json={"reservation": {"id": 1}})])
    resource = await f.fetch("C1", "RESERVATION", "HOTEL 1", "12/3")
    assert resource == {"reservation": {"id": 1}}
    (request,) = seen
    assert request.url.raw_path == b"/rsv/v1/hotels/HOTEL%201/reservations/12%2F3"
    assert request.headers["Authorization"] == "Bearer tok-1"
    assert request.headers["x-app-key"] == "app-key-secreta"
    assert request.headers["x-hotelid"] == "HOTEL 1"
    assert len(request.headers["x-request-id"]) == 36
    # segunda vez: do cache, sem chamada
    assert await f.fetch("C1", "reservation", "HOTEL 1", "12/3") == resource
    assert len(seen) == 1
    assert resource_cache_key("C1", "reservation", "HOTEL 1", "12/3") in cache.data


@pytest.mark.parametrize("status", [204, 404])
async def test_missing_resource_is_none_and_cached(status: int) -> None:
    f, seen, _, cache, _ = fetcher([httpx.Response(status)])
    assert await f.fetch("C1", "profile", "H1", "9") is None
    assert await f.fetch("C1", "profile", "H1", "9") is None
    assert len(seen) == 1
    assert list(cache.data.values()) == [None]


async def test_401_gets_a_new_token_once() -> None:
    f, seen, tokens, _, _ = fetcher([httpx.Response(401), httpx.Response(200, json={})])
    assert await f.fetch("C1", "profile", "H1", "9") == {}
    assert tokens.invalidated == 1
    assert [r.headers["Authorization"] for r in seen] == ["Bearer tok-1", "Bearer tok-2"]

    f, _, _, _, _ = fetcher([httpx.Response(401), httpx.Response(401)])
    with pytest.raises(ResourceUnavailableError, match="401"):
        await f.fetch("C1", "profile", "H1", "9")


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (httpx.Response(403), ResourceRejectedError),
        (httpx.Response(400), ResourceRejectedError),
        (httpx.Response(500), ResourceUnavailableError),
        (httpx.Response(503), ResourceUnavailableError),
        (httpx.Response(200, text="não é json"), ResourceUnavailableError),
        (httpx.Response(200, json=[1]), ResourceUnavailableError),
    ],
)
async def test_response_classification(response: httpx.Response, error: type[Exception]) -> None:
    f, _, _, cache, _ = fetcher([response])
    with pytest.raises(error):
        await f.fetch("C1", "profile", "H1", "9")
    assert cache.data == {}  # erro não vai para o cache


async def test_429_pauses_every_call() -> None:
    f, seen, _, _, clock = fetcher(
        [httpx.Response(429, headers={"Retry-After": "20"}), httpx.Response(200, json={})]
    )
    with pytest.raises(ResourceUnavailableError) as info:
        await f.fetch("C1", "profile", "H1", "9")
    assert info.value.retry_after_s == 20
    assert await f.fetch("C1", "profile", "H1", "9") == {}
    assert clock.sleeps == [20]  # a próxima chamada esperou o Retry-After
    assert len(seen) == 2


async def test_network_error_is_unavailable() -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("lento", request=request)

    f, _, _, _, _ = fetcher(fail)
    with pytest.raises(ResourceUnavailableError, match="ConnectTimeout"):
        await f.fetch("C1", "profile", "H1", "9")


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (("C1", "BLOCK", "H1", "9"), "módulo"),
        (("C1", "profile", None, "9"), "hotelId"),
        (("C2", "profile", "H1", "9"), "credenciais"),
    ],
)
async def test_message_level_refusals(args: tuple[Any, ...], message: str) -> None:
    f, seen, _, _, _ = fetcher([])
    with pytest.raises(ResourceRejectedError, match=message):
        await f.fetch(*args)
    assert seen == []


@pytest.mark.parametrize("error", [AuthUnavailableError("x"), AuthRejectedError("y")])
async def test_token_problems_are_infrastructure(error: Exception) -> None:
    f, seen, _, _, _ = fetcher([], tokens=Tokens(error))
    with pytest.raises(ResourceUnavailableError, match="sem token"):
        await f.fetch("C1", "profile", "H1", "9")
    assert seen == []


async def test_repeated_auth_rejections_raise_a_critical_alert(
    capsys: pytest.CaptureFixture[str],
) -> None:
    tokens = Tokens(AuthRejectedError("OAuth recusou as credenciais (HTTP 401)"))
    f, _, _, _, _ = fetcher([httpx.Response(200, json={})], tokens=tokens)
    for _ in range(2):
        with pytest.raises(ResourceUnavailableError):
            await f.fetch("C1", "profile", "H1", "9")
    assert "ohip_rest_credenciais_recusadas" not in capsys.readouterr().out
    with pytest.raises(ResourceUnavailableError):  # 3ª seguida: alerta (auth_rejected_alert)
        await f.fetch("C1", "profile", "H1", "9")
    out = capsys.readouterr().out
    assert "ohip_rest_credenciais_recusadas" in out
    assert "app-key-secreta" not in out
    with pytest.raises(ResourceUnavailableError):  # 4ª: só aviso e métrica, crítico não repete
        await f.fetch("C1", "profile", "H1", "9")
    assert "ohip_rest_credenciais_recusadas" not in capsys.readouterr().out
    assert f._metrics.total("ohip_rest_auth_rejected_total") == 4  # type: ignore[attr-defined]

    tokens.error = None  # assinatura corrigida no Developer Portal: a contagem zera
    assert await f.fetch("C1", "profile", "H1", "9") == {}
    assert f._auth_rejections == 0


async def test_token_refused_by_the_rest_counts_as_rejection() -> None:
    f, _, _, _, _ = fetcher([httpx.Response(401)] * 6)
    for _ in range(3):
        with pytest.raises(ResourceUnavailableError, match="401"):
            await f.fetch("C1", "profile", "H1", "9")
    assert f._auth_rejections == 3


def test_retry_after_parsing() -> None:
    assert _retry_after_s(None, T0) == 30
    assert _retry_after_s("5", T0) == 5
    assert _retry_after_s("0", T0) == 1
    assert _retry_after_s("99999", T0) == 600
    assert _retry_after_s("Thu, 01 Oct 2026 12:01:00 GMT", T0) == 60
    assert _retry_after_s("lixo", T0) == 30


async def test_rate_limiter_token_bucket() -> None:
    clock = FakeClock()
    limiter = RateLimiter(rate_per_s=2, burst=2, clock=clock)
    for _ in range(4):
        await limiter.acquire()
    assert clock.sleeps == [0.5, 0.5]  # duas de rajada, depois 2 por segundo
    clock.advance(10)
    await limiter.acquire()
    assert clock.sleeps == [0.5, 0.5]  # o balde encheu (até o limite) enquanto parado
    limiter.pause_for(5)
    limiter.pause_for(1)  # pausa menor não encurta a maior
    await limiter.acquire()
    assert clock.sleeps[-1] == 5
