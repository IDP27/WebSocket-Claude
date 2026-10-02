"""``WsConnector`` com a biblioteca ``websockets`` (graphql-transport-ws manual, ADR-0007).

- URL ``wss://<gateway>/subscriptions?key=<sha256 hex minúsculo da app key>``.
- Subprotocolo ``graphql-transport-ws``; o servidor precisa confirmá-lo (senão 4406).
- **Sem pings de controle do WebSocket** (``ping_interval=None``): o heartbeat é o
  ``{"type":"ping"}`` do protocolo, feito pelo consumer.
- ``max_size`` = ``OHIP_WS_MAX_MESSAGE_BYTES``: acima disso a biblioteca fecha com 1009 e o
  consumer conta a repetição no mesmo offset.
- Recusa no upgrade (HTTP 4xx, ex.: 400 por chave/URL errada) → ``HandshakeRejectedError``;
  rede/timeout → ``ConnectionClosedError(None)``. A URL com o hash nunca vai para log.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from ohip_streaming.application.errors import (
    ConnectionClosedError,
    HandshakeRejectedError,
    MessageTooLargeError,
)
from ohip_streaming.domain.protocol import SUBPROTOCOL

if TYPE_CHECKING:
    from ohip_streaming.config import OhipSettings

TOO_LARGE = 1009


def app_key_hash(app_key: str) -> str:
    return hashlib.sha256(app_key.encode("utf-8")).hexdigest()


def subscriptions_url(gateway_url: str, app_key: str) -> str:
    """``https://host`` → ``wss://host/subscriptions?key=<hash>`` (como o cliente oficial)."""
    base = gateway_url.rstrip("/")
    if base.endswith("/subscriptions"):
        base = base[: -len("/subscriptions")]
    for scheme in ("https://", "http://", "wss://", "ws://"):
        if base.startswith(scheme):
            base = base[len(scheme) :]
            break
    return f"wss://{base}/subscriptions?key={app_key_hash(app_key)}"


def _closed(exc: ConnectionClosed) -> ConnectionClosedError:
    if exc.sent is not None and exc.sent.code == TOO_LARGE:
        return MessageTooLargeError(TOO_LARGE, "mensagem acima do limite")
    if exc.rcvd is not None:
        return ConnectionClosedError(exc.rcvd.code, exc.rcvd.reason)
    return ConnectionClosedError(None, "conexão perdida sem fechamento")


class WebsocketsConnection:
    def __init__(self, connection: ClientConnection) -> None:
        self._connection = connection

    async def send(self, text: str) -> None:
        try:
            await self._connection.send(text)
        except ConnectionClosed as exc:
            raise _closed(exc) from None

    async def recv(self) -> str:
        try:
            message = await self._connection.recv()
        except ConnectionClosed as exc:
            raise _closed(exc) from None
        return message if isinstance(message, str) else message.decode("utf-8", "replace")

    async def close(self) -> None:
        await self._connection.close()


class WebsocketsConnector:
    def __init__(
        self,
        *,
        url: str,
        max_message_bytes: int,
        open_timeout_s: float = 10.0,
        close_timeout_s: float = 10.0,
    ) -> None:
        self._url = url
        self._max_message_bytes = max_message_bytes
        self._open_timeout_s = open_timeout_s
        self._close_timeout_s = close_timeout_s

    @classmethod
    def from_settings(cls, settings: OhipSettings) -> WebsocketsConnector:
        return cls(
            url=subscriptions_url(settings.gateway_url, settings.app_key.get_secret_value()),
            max_message_bytes=settings.ws_max_message_bytes,
        )

    def __repr__(self) -> str:  # a URL tem o hash da app key
        return "WebsocketsConnector(url=***)"

    async def connect(self) -> WebsocketsConnection:
        try:
            connection = await connect(
                self._url,
                subprotocols=[SUBPROTOCOL],  # type: ignore[list-item]
                ping_interval=None,
                max_size=self._max_message_bytes,
                open_timeout=self._open_timeout_s,
                close_timeout=self._close_timeout_s,
                compression=None,
            )
        except InvalidStatus as exc:
            status = exc.response.status_code
            if 400 <= status < 500:
                raise HandshakeRejectedError(f"upgrade recusado com HTTP {status}") from None
            raise ConnectionClosedError(None, f"upgrade com HTTP {status}") from None
        except (OSError, TimeoutError, WebSocketException) as exc:
            raise ConnectionClosedError(None, type(exc).__name__) from None
        if connection.subprotocol != SUBPROTOCOL:
            await connection.close()
            raise HandshakeRejectedError(
                "servidor não confirmou o subprotocolo graphql-transport-ws"
            )
        return WebsocketsConnection(connection)
