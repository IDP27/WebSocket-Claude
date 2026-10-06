"""Servidor OHIP Streaming simulado (graphql-transport-ws) para os testes da Fase 5.

Reproduz o que o ADR-0007 exige testar: subprotocolo obrigatório (4406), ``connection_init``
com prazo (4408), credenciais (4401), chave na URL (HTTP 400), um consumidor por vez e
lockout em reconexão rápida (4409), fechamentos injetados (4403/4504/...), ``complete`` →
fechamento **pelo servidor**, ``pong`` atrasado ou ausente, ``ping`` do servidor, frame
``error`` da assinatura e mensagem grande demais.

Eventos: lista de ``newEvent`` com offset numérico; o ``subscribe`` entrega os que têm offset
**maior** que o pedido (semântica exclusiva; a inclusiva é a D-1 e a dedup cobre as duas).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from tests.fakes.frames import new_event

SUBPROTOCOL = "graphql-transport-ws"
_OFFSET_RE = re.compile(r'offset:\s*"(\d+)"')


@dataclass
class Behavior:
    """Comportamento de uma conexão (por ordem de chegada). O padrão é o servidor saudável."""

    close_after_subscribe: int | None = None  # fecha com este código logo após assinar
    close_after_events: int | None = None  # ... depois de enviar N eventos
    close_code: int = 4504
    error_after_events: int | None = None  # envia frame error da assinatura
    no_pong: bool = False
    silent: bool = False  # não envia nada nem responde ping (rede morta)
    oversize: bool = False  # manda uma mensagem gigante logo após assinar
    server_ping: bool = False
    no_ack: bool = False
    hold_after_events: int | None = None  # retém o resto até o complete (drenagem)
    rate_per_s: float | None = None  # envio ritmado (teste de carga); None = o mais rápido possível


@dataclass
class Connection:
    number: int
    token: str | None = None
    subscribe_offsets: list[str | None] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)
    frames: list[dict[str, Any]] = field(default_factory=list)
    close_code: int | None = None
    pongs_received: int = 0
    opened_at: float = 0.0
    closed_at: float | None = None


class FakeOhipServer:
    def __init__(
        self,
        *,
        app_key: str,
        valid_tokens: set[str],
        events: int = 0,
        lockout_window_s: float = 0.05,
        init_timeout_s: float = 1.0,
        drain_close_delay_s: float = 0.02,
        inclusive_offset: bool = False,
    ) -> None:
        self.app_key = app_key
        self.valid_tokens = valid_tokens
        self.events = [new_event(str(n), f"uid-{n}") for n in range(1, events + 1)]
        self.lockout_window_s = lockout_window_s
        self.init_timeout_s = init_timeout_s
        self.drain_close_delay_s = drain_close_delay_s
        self.inclusive_offset = inclusive_offset  # D-1: o evento do offset pedido volta?
        # Teste de carga: uniqueEventId → time.perf_counter() do envio (só se ligado).
        self.track_send_times = False
        self.sent_at: dict[str, float] = {}
        self._release: asyncio.Event | None = None
        self.behaviors: list[Behavior] = []
        self.connections: list[Connection] = []
        self._active: Connection | None = None
        self._last_close_at: float | None = None
        self._server: Server | None = None
        self.port = 0

    # ----------------------------------------------------------------- ciclo

    async def __aenter__(self) -> FakeOhipServer:
        self._server = await serve(
            self._handle,
            "127.0.0.1",
            0,
            subprotocols=[SUBPROTOCOL],  # type: ignore[list-item]
            process_request=self._check_key,
            ping_interval=None,
            max_size=None,
        )
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    @property
    def url(self) -> str:
        key = hashlib.sha256(self.app_key.encode()).hexdigest()
        return f"ws://127.0.0.1:{self.port}/subscriptions?key={key}"

    def add_event(self, offset: int, **kwargs: Any) -> None:
        self.events.append(new_event(str(offset), f"uid-{offset}", **kwargs))

    # ----------------------------------------------------------------- handshake

    def _check_key(self, connection: ServerConnection, request: Request) -> Response | None:
        expected = hashlib.sha256(self.app_key.encode()).hexdigest()
        if not request.path.startswith("/subscriptions") or f"key={expected}" not in request.path:
            return Response(HTTPStatus.BAD_REQUEST, "Bad Request", Headers(), b"Incorrect key")
        return None

    async def _handle(self, ws: ServerConnection) -> None:
        loop = asyncio.get_running_loop()
        conn = Connection(len(self.connections) + 1, opened_at=loop.time())
        self.connections.append(conn)
        behavior = (
            self.behaviors[conn.number - 1] if conn.number <= len(self.behaviors) else Behavior()
        )
        try:
            await self._session(ws, conn, behavior)
        except ConnectionClosed:  # o cliente pode cair a qualquer momento
            conn.close_code = conn.close_code or ws.close_code
        finally:
            conn.closed_at = loop.time()
            if self._active is conn:
                self._active = None
            self._last_close_at = loop.time()

    async def _close(self, ws: ServerConnection, conn: Connection, code: int) -> None:
        conn.close_code = code
        await ws.close(code, f"fake {code}")

    async def _session(self, ws: ServerConnection, conn: Connection, behavior: Behavior) -> None:
        loop = asyncio.get_running_loop()
        if ws.subprotocol != SUBPROTOCOL:
            await self._close(ws, conn, 4406)
            return
        try:
            init = json.loads(await asyncio.wait_for(ws.recv(), self.init_timeout_s))
        except TimeoutError:
            await self._close(ws, conn, 4408)
            return
        conn.frames.append(init)
        payload = init.get("payload") or {}
        token = str(payload.get("Authorization", "")).removeprefix("Bearer ")
        conn.token = token
        if token not in self.valid_tokens or payload.get("x-app-key") != self.app_key:
            await self._close(ws, conn, 4401)
            return
        recent = (
            self._last_close_at is not None
            and loop.time() - self._last_close_at < self.lockout_window_s
        )
        if self._active is not None or recent:
            await self._close(ws, conn, 4409)  # um consumidor por vez; reconexão rápida demais
            return
        self._active = conn
        if behavior.no_ack:
            await asyncio.sleep(3600)
        await ws.send(json.dumps({"type": "connection_ack"}))
        await self._serve(ws, conn, behavior)

    async def _serve(self, ws: ServerConnection, conn: Connection, behavior: Behavior) -> None:
        sender: asyncio.Task[None] | None = None
        pinger = asyncio.create_task(self._server_pings(ws)) if behavior.server_ping else None
        try:
            async for raw in ws:
                frame = json.loads(raw)
                conn.frames.append(frame)
                kind = frame.get("type")
                if behavior.silent:
                    continue
                if kind == "ping" and not behavior.no_pong:
                    await ws.send(json.dumps({"type": "pong"}))
                elif kind == "pong":
                    conn.pongs_received += 1
                elif kind == "subscribe":
                    query = frame["payload"]["query"]
                    conn.queries.append(query)
                    if query.startswith("query"):
                        await ws.send(
                            json.dumps(
                                {
                                    "id": frame["id"],
                                    "type": "next",
                                    "payload": {
                                        "data": {"connection": {"id": "c1", "status": "Inactive"}}
                                    },
                                }
                            )
                        )
                        await ws.send(json.dumps({"id": frame["id"], "type": "complete"}))
                        continue
                    match = _OFFSET_RE.search(query)
                    offset = match.group(1) if match else None
                    conn.subscribe_offsets.append(offset)
                    sender = asyncio.create_task(
                        self._send_events(ws, conn, frame["id"], offset, behavior)
                    )
                elif kind == "complete":
                    if self._release is not None:
                        self._release.set()  # manda o que estava retido (chega após o complete)
                    if sender is not None:
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await sender  # termina de mandar o que já estava a caminho
                    await asyncio.sleep(self.drain_close_delay_s)
                    await self._close(ws, conn, 1000)  # quem fecha é o servidor
                    return
        finally:
            for task in (sender, pinger):
                if task is not None:
                    task.cancel()

    async def _send_events(
        self,
        ws: ServerConnection,
        conn: Connection,
        sub_id: str,
        offset: str | None,
        behavior: Behavior,
    ) -> None:
        if behavior.close_after_subscribe is not None:
            await self._close(ws, conn, behavior.close_after_subscribe)
            return
        if behavior.oversize:
            await ws.send("x" * 10_000_000)
            return
        start = int(offset) if offset is not None else 0
        if self.inclusive_offset and offset is not None:
            start -= 1
        sent = 0
        self._release = asyncio.Event()
        started = time.perf_counter()
        for event in self.events:
            if int(event["metadata"]["offset"]) <= start:
                continue
            if behavior.hold_after_events is not None and sent == behavior.hold_after_events:
                await self._release.wait()
            if behavior.close_after_events is not None and sent >= behavior.close_after_events:
                await self._close(ws, conn, behavior.close_code)
                return
            if behavior.error_after_events is not None and sent >= behavior.error_after_events:
                await ws.send(
                    json.dumps(
                        {
                            "id": sub_id,
                            "type": "error",
                            "payload": [{"message": "offset expirado"}],
                        }
                    )
                )
                return
            if behavior.rate_per_s is not None:
                delay = started + sent / behavior.rate_per_s - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
            frame = {"id": sub_id, "type": "next", "payload": {"data": {"newEvent": event}}}
            if self.track_send_times:
                self.sent_at[event["metadata"]["uniqueEventId"]] = time.perf_counter()
            await ws.send(json.dumps(frame))
            sent += 1

    async def _server_pings(self, ws: ServerConnection) -> None:
        while True:
            await asyncio.sleep(0.02)
            await ws.send(json.dumps({"type": "ping"}))
