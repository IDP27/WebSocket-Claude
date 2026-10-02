"""OhipTokenIssuer contra um gateway simulado (respx), conforme publishedoauth.json."""

from __future__ import annotations

import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
import respx
from tests.fakes.memory import FakeClock

from ohip_streaming.adapters.ohip_rest.oauth import OAuthCredentials, OhipTokenIssuer, jwt_expiry
from ohip_streaming.application.errors import AuthRejectedError, AuthUnavailableError

URL = "https://gw.example.com/oauth/v1/tokens"
APP_KEY = "41ecd082-8997-4c69-af34-2f72b83645ff"


def credentials(**overrides: Any) -> OAuthCredentials:
    values: dict[str, Any] = {
        "token_url": URL,
        "client_id": "cliente",
        "client_secret": "segredo",
        "app_key": APP_KEY,
        "grant_type": "client_credentials",
        "scope": "urn:opc:hgbu:ws:__myscopes__",
        "enterprise_id": "ENT123",
    }
    values.update(overrides)
    return OAuthCredentials(**values)


def jwt(exp: float) -> str:
    def part(data: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")

    return f"{part({'alg': 'RS256'})}.{part({'exp': exp})}.assinatura"


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as http:
        yield http


@respx.mock
async def test_client_credentials_request(client: httpx.AsyncClient) -> None:
    route = respx.post(URL).respond(200, json={"access_token": "tok", "expires_in": 3600})
    clock = FakeClock()

    token = await OhipTokenIssuer(client, credentials(), clock).issue()

    assert token.value == "tok"
    assert token.expires_at == clock.now() + timedelta(seconds=3600)
    request = route.calls.last.request
    expected_basic = "Basic " + base64.b64encode(b"cliente:segredo").decode()
    assert request.headers["authorization"] == expected_basic
    assert request.headers["x-app-key"] == APP_KEY
    assert request.headers["enterpriseid"] == "ENT123"
    assert len(request.headers["x-request-id"]) == 36  # GUID
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    assert parse_qs(request.content.decode()) == {
        "grant_type": ["client_credentials"],
        "scope": ["urn:opc:hgbu:ws:__myscopes__"],
    }


@respx.mock
async def test_resource_owner_request(client: httpx.AsyncClient) -> None:
    route = respx.post(URL).respond(200, json={"access_token": "tok"})
    creds = credentials(
        grant_type="password", scope=None, enterprise_id=None, username="int", password="pw"
    )
    token = await OhipTokenIssuer(client, creds, FakeClock()).issue()

    request = route.calls.last.request
    assert "enterpriseid" not in request.headers  # só em client_credentials
    assert parse_qs(request.content.decode()) == {
        "grant_type": ["password"],
        "username": ["int"],
        "password": ["pw"],
    }
    assert token.expires_at == FakeClock().now() + timedelta(seconds=3600)  # padrão


@respx.mock
async def test_jwt_exp_wins_when_earlier(client: httpx.AsyncClient) -> None:
    clock = FakeClock()
    exp = (clock.now() + timedelta(seconds=600)).timestamp()
    respx.post(URL).respond(200, json={"access_token": jwt(exp), "expires_in": 3600})
    token = await OhipTokenIssuer(client, credentials(), clock).issue()
    assert token.expires_at == clock.now() + timedelta(seconds=600)


@pytest.mark.parametrize("status", [400, 401, 403])
@respx.mock
async def test_rejected_credentials(client: httpx.AsyncClient, status: int) -> None:
    respx.post(URL).respond(status, json={"error": "invalid_client"})
    with pytest.raises(AuthRejectedError, match=str(status)) as info:
        await OhipTokenIssuer(client, credentials(), FakeClock()).issue()
    assert "segredo" not in str(info.value)


@pytest.mark.parametrize("status", [429, 500, 502, 503])
@respx.mock
async def test_gateway_unavailable(client: httpx.AsyncClient, status: int) -> None:
    respx.post(URL).respond(status)
    with pytest.raises(AuthUnavailableError):
        await OhipTokenIssuer(client, credentials(), FakeClock()).issue()


@respx.mock
async def test_network_error(client: httpx.AsyncClient) -> None:
    respx.post(URL).mock(side_effect=httpx.ConnectTimeout("lento"))
    with pytest.raises(AuthUnavailableError, match="ConnectTimeout"):
        await OhipTokenIssuer(client, credentials(), FakeClock()).issue()


@pytest.mark.parametrize(
    "body", [{"token": "x"}, {"access_token": ""}, {"access_token": 5}, ["lista"]]
)
@respx.mock
async def test_malformed_response(client: httpx.AsyncClient, body: Any) -> None:
    respx.post(URL).respond(200, json=body)
    with pytest.raises(AuthUnavailableError, match="access_token"):
        await OhipTokenIssuer(client, credentials(), FakeClock()).issue()


@respx.mock
async def test_non_json_response(client: httpx.AsyncClient) -> None:
    respx.post(URL).respond(200, text="<html>")
    with pytest.raises(AuthUnavailableError):
        await OhipTokenIssuer(client, credentials(), FakeClock()).issue()


def test_credentials_repr_hides_secrets() -> None:
    text = repr(credentials(grant_type="password", password="pw"))
    assert "segredo" not in text
    assert APP_KEY not in text
    assert "pw" not in text.replace("password", "")


@pytest.mark.parametrize(
    "token",
    ["opaco", "a.b", "a.!!!.c", jwt(1e20), "a." + base64.urlsafe_b64encode(b"[1]").decode() + ".c"],
)
def test_jwt_expiry_ignores_garbage(token: str) -> None:
    assert jwt_expiry(token) is None


def test_jwt_expiry_reads_exp() -> None:
    assert jwt_expiry(jwt(0)) == datetime(1970, 1, 1, tzinfo=UTC)


def test_credentials_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.test_config import ohip

    from ohip_streaming.adapters.ohip_rest.oauth import credentials_from_settings

    creds = credentials_from_settings(ohip(monkeypatch))
    assert creds.token_url == "https://gateway.example.com/oauth/v1/tokens"
    assert (creds.grant_type, creds.scope, creds.enterprise_id) == (
        "client_credentials",
        "urn:opc:hgbu:ws:__myscopes__",
        "ENT1",
    )
    assert creds.username is None

    owner = credentials_from_settings(
        ohip(
            monkeypatch,
            OHIP_AUTH_MODE="resource_owner",
            OHIP_INTEGRATION_USERNAME="integ",
            OHIP_INTEGRATION_PASSWORD="senha",
        )
    )
    assert (owner.grant_type, owner.username, owner.password) == ("password", "integ", "senha")
    assert (owner.scope, owner.enterprise_id) == (None, None)
