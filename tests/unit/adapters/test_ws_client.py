"""Helpers do adapter WebSocket: URL com o hash da app key e mapeamento de fechamentos."""

from __future__ import annotations

import hashlib

import pytest
from websockets.exceptions import ConnectionClosedError as WsClosed
from websockets.frames import Close

from ohip_streaming.adapters.ohip_ws.client import (
    WebsocketsConnector,
    _closed,
    app_key_hash,
    subscriptions_url,
)
from ohip_streaming.application.errors import MessageTooLargeError

APP_KEY = "41ecd082-8997-4c69-af34-2f72b83645ff"
HASH = hashlib.sha256(APP_KEY.encode()).hexdigest()


def test_hash_is_lowercase_sha256_hex() -> None:
    assert app_key_hash(APP_KEY) == HASH
    assert HASH.lower() == HASH


@pytest.mark.parametrize(
    "gateway",
    [
        "https://gw.example.com",
        "https://gw.example.com/",
        "https://gw.example.com/subscriptions",
        "wss://gw.example.com",
        "http://gw.example.com",
    ],
)
def test_subscriptions_url(gateway: str) -> None:
    assert subscriptions_url(gateway, APP_KEY) == f"wss://gw.example.com/subscriptions?key={HASH}"


def test_close_mapping() -> None:
    received = _closed(WsClosed(Close(4409, "lock"), None))
    assert (received.close_code, received.reason) == (4409, "lock")
    too_large = _closed(WsClosed(None, Close(1009, "too big")))
    assert isinstance(too_large, MessageTooLargeError)
    lost = _closed(WsClosed(None, None))
    assert lost.close_code is None


def test_repr_hides_the_url() -> None:
    connector = WebsocketsConnector(url=f"wss://h/subscriptions?key={HASH}", max_message_bytes=1)
    assert HASH not in repr(connector)
