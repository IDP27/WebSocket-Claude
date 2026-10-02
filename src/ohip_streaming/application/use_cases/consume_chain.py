"""Consumer de uma chain: ciclo de vida da conexão OHIP (ADR-0007, ARCHITECTURE §3.1 e §4).

``ChainConsumer.run`` é o laço externo: lease → regra dos 10 s → [replay pendente →
sessão → decisão de fechamento → espera] até parar. ``_Session`` é uma conexão:

- ``connection_init`` logo após abrir; ``connection_ack`` com prazo;
- status opcional (D-4) num ``subscribe`` próprio;
- ``subscribe`` com GUID e o último offset **confirmado no banco**;
- tarefas em paralelo: leitura (frames → fila interna; ``ping`` → ``pong``), gravação
  (micro-lotes → ``ProcessEventBatch``), heartbeat (``ping`` a cada 15 s; prova de vida =
  ``pong`` ou ``next``) e controle (parar, lease, token perto do ``exp``, replay pendente,
  retry de DLQ);
- encerramento: ``complete`` → continua lendo e gravando → **espera o servidor fechar**
  (``DRAIN_TIMEOUT``; só então fecha do lado do cliente);
- **conexão envenenada** (fila travada, banco fora, lease perdido): o não gravado é descartado
  e nada mais desta conexão é gravado; o próximo ``subscribe`` parte do offset confirmado.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import tzinfo
from enum import StrEnum
from typing import Any

from ohip_streaming.application.errors import (
    AuthRejectedError,
    AuthUnavailableError,
    ConnectionClosedError,
    HandshakeRejectedError,
    LeaseLostError,
    MessageTooLargeError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.intake import IntakeItem, IntakeQueue
from ohip_streaming.application.ports import (
    AccessToken,
    Clock,
    ConsumerStatusStore,
    MetricsSink,
    ReplayStore,
    WsConnection,
    WsConnector,
)
from ohip_streaming.application.use_cases.lease import LeaseKeeper
from ohip_streaming.application.use_cases.process_event_batch import (
    ConsumerContext,
    IncomingMessage,
    ProcessEventBatch,
    RetryConsumeDlq,
)
from ohip_streaming.application.use_cases.replay import ApplyReplay
from ohip_streaming.application.use_cases.token_provider import TokenProvider
from ohip_streaming.domain import protocol
from ohip_streaming.domain.connection import (
    SINGLE_CONSUMER_LOCK,
    AlertLevel,
    CloseAction,
    CloseDecision,
    ConsumerState,
    ReconnectPolicy,
    RttEstimator,
    decide_on_close,
    decide_on_oversize,
    wait_before_connect,
)
from ohip_streaming.domain.offset import Offset
from ohip_streaming.logging import bind_context, get_logger

log = get_logger(__name__)

CLIENT_TOO_LARGE = 1009


@dataclass(frozen=True, slots=True)
class ConsumerOptions:
    chain_code: str
    instance_id: str
    event_tz: tzinfo
    hotel_codes: tuple[str, ...] = ()
    delta: bool = False
    ping_interval_s: float = 15.0
    ack_timeout_s: float = 10.0
    pong_timeout_min_s: float = 180.0
    drain_timeout_s: float = 30.0
    intake_max_items: int = 5_000
    intake_max_bytes: int = 64 * 1024 * 1024
    intake_stall_timeout_s: float = 120.0
    batch_max_events: int = 200
    batch_max_wait_s: float = 0.2
    control_poll_interval_s: float = 5.0
    status_check_enabled: bool = False
    dlq_retry_limit: int = 20
    # Sessão que recebeu eventos ou ficou assinada por este tempo conta como sucesso.
    healthy_after_s: float = 60.0
    # Recusas seguidas do OAuth antes de parar (credencial errada exige ação humana).
    auth_rejected_max: int = 5
    policy: ReconnectPolicy = field(default_factory=ReconnectPolicy)


class EndReason(StrEnum):
    SERVER_CLOSED = "SERVER_CLOSED"  # fechou sem a gente pedir (código em close_code)
    DRAINED = "DRAINED"  # complete enviado e o servidor fechou (ou o prazo venceu)
    LIVENESS_TIMEOUT = "LIVENESS_TIMEOUT"  # sem pong nem next além do prazo
    ACK_TIMEOUT = "ACK_TIMEOUT"
    OVERSIZE = "OVERSIZE"
    HANDSHAKE_REJECTED = "HANDSHAKE_REJECTED"  # HTTP 400 no upgrade: chave/URL (config)
    CONNECTION_ACTIVE = "CONNECTION_ACTIVE"  # status check: há outra conexão
    AUTH_REJECTED = "AUTH_REJECTED"  # OAuth recusou as credenciais
    UNAVAILABLE = "UNAVAILABLE"  # OAuth ou banco fora antes de assinar
    INTERNAL_ERROR = "INTERNAL_ERROR"  # tarefa da sessão falhou com erro inesperado


class DrainCause(StrEnum):
    STOP = "STOP"
    TOKEN = "TOKEN"  # noqa: S105 - motivo, não segredo
    REPLAY = "REPLAY"
    POISONED = "POISONED"
    LEASE_LOST = "LEASE_LOST"
    SUBSCRIPTION_ENDED = "SUBSCRIPTION_ENDED"  # error/complete do servidor (D-10)


@dataclass(slots=True)
class SessionEnd:
    reason: EndReason
    close_code: int | None = None
    detail: str = ""
    healthy: bool = False  # recebeu evento ou ficou assinada o bastante (zera as falhas)
    drain_cause: DrainCause | None = None
    offset: Offset | None = None  # offset confirmado ao assinar


@dataclass
class ConsumerDeps:
    connector: WsConnector
    tokens: TokenProvider
    lease: LeaseKeeper
    batch: ProcessEventBatch
    retry_dlq: RetryConsumeDlq
    replay_store: ReplayStore
    apply_replay: ApplyReplay
    status: ConsumerStatusStore
    clock: Clock
    metrics: MetricsSink
    app_key: str = field(default="", repr=False)  # vai no connection_init; nunca em log
    rng: Callable[[], float] = field(default=random.random)


class ChainConsumer:
    def __init__(self, deps: ConsumerDeps, options: ConsumerOptions) -> None:
        self._deps = deps
        self._options = options
        self._stop = asyncio.Event()
        self._failures = 0
        self._auth_rejections = 0
        self._oversize: tuple[Offset | None, int] = (None, 0)

    def request_stop(self) -> None:
        """SIGTERM: drena, grava a desconexão, libera o lease e sai."""
        self._stop.set()

    async def run(self) -> None:
        d, o = self._deps, self._options
        with bind_context(chain_code=o.chain_code):
            # Sem lease, nada é gravado em OHIP_CONSUMER_STATUS: a linha é da chain e uma
            # instância passiva sobrescreveria o estado da ativa (e a regra dos 10 s, ADR-0007).
            epoch = await d.lease.acquire(self._stop)
            if epoch is None:  # parada pedida antes de virar dono
                log.info("consumer_encerrado_sem_lease")
                return
            keeper = asyncio.create_task(d.lease.keep(), name="lease-renovacao")
            try:
                await self._first_wait()
                while not self._stop.is_set() and not d.lease.lost:
                    try:
                        await self._apply_pending_replay(epoch)
                    except StoreUnavailableError:
                        end = SessionEnd(EndReason.UNAVAILABLE, detail="banco fora (replay)")
                    else:
                        end = await _Session(d, o, epoch, self._stop).run()
                    if await self._after_session(end):
                        break
            finally:
                keeper.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await keeper
                if not d.lease.lost:
                    await d.lease.release()
                log.info("consumer_encerrado", lease_lost=d.lease.lost)

    async def _first_wait(self) -> None:
        """Regra dos 10 s também após crash/restart, com o relógio do banco (ADR-0007)."""
        min_gap = self._options.policy.min_gap_s
        try:
            snapshot = await self._deps.status.disconnect_snapshot(self._options.chain_code)
        except StoreUnavailableError:
            wait = min_gap  # horário desconhecido: espera integral
        else:
            wait = wait_before_connect(
                db_now=snapshot.db_now,
                last_disconnect_at=snapshot.last_disconnect_at,
                last_state=snapshot.last_state,
                min_gap_s=min_gap,
            )
        await self._state(ConsumerState.WAITING)
        await self._sleep(wait)

    async def _apply_pending_replay(self, epoch: int) -> None:
        """Com a conexão fechada: troca o offset antes do próximo subscribe (§4.6)."""
        request = await self._deps.apply_replay.pending(self._options.chain_code)
        if request is not None and await self._deps.apply_replay.execute(request, epoch):
            log.warning("replay_aplicado", request_id=request.id, offset=request.from_offset.value)

    async def _after_session(self, end: SessionEnd) -> bool:
        """Grava a desconexão e espera. True = encerrar o laço."""
        d, o = self._deps, self._options
        if end.healthy:
            self._failures = 0
        if self._stop.is_set() or d.lease.lost or end.drain_cause is DrainCause.LEASE_LOST:
            await self._disconnected(ConsumerState.WAITING, end)
            return True

        decision = self._decide(end)
        if decision.alert is not None:
            log.error("consumer_alerta", level=decision.alert.value, reason=decision.reason)
        if decision.action is CloseAction.STOP:
            await self._disconnected(ConsumerState.STOPPED, end)
            log.critical("consumer_parado", reason=decision.reason)
            await self._stop.wait()  # exige ação humana; não reconecta
            return True
        await self._disconnected(ConsumerState.WAITING, end)
        if decision.action is CloseAction.REFRESH_TOKEN_AND_RECONNECT:
            await d.tokens.invalidate()
        d.metrics.increment(
            "ohip_ws_reconnects_total", code=str(end.close_code), chain_code=o.chain_code
        )
        log.info("reconexao_agendada", wait_s=round(decision.wait_s, 1), reason=decision.reason)
        await self._sleep(decision.wait_s)
        return self._stop.is_set()

    def _decide(self, end: SessionEnd) -> CloseDecision:
        policy, rng = self._options.policy, self._deps.rng
        if end.reason is EndReason.OVERSIZE:
            offset, repeats = self._oversize
            repeats = repeats + 1 if offset == end.offset else 1
            self._oversize = (end.offset, repeats)
            return decide_on_oversize(repeats, policy)
        self._oversize = (None, 0)
        if end.reason is EndReason.DRAINED and end.drain_cause in (
            DrainCause.TOKEN,
            DrainCause.REPLAY,
        ):
            return CloseDecision(CloseAction.RECONNECT, policy.min_gap_s, str(end.drain_cause))
        if end.reason is EndReason.CONNECTION_ACTIVE:
            self._failures += 1
            return decide_on_close(SINGLE_CONSUMER_LOCK, self._failures, policy, rng)
        if not end.healthy:
            self._failures += 1
        failures = max(1, self._failures)
        if end.reason is EndReason.AUTH_REJECTED:
            self._auth_rejections += 1
            if self._auth_rejections >= self._options.auth_rejected_max:
                return CloseDecision(
                    CloseAction.STOP,
                    0,
                    f"OAuth recusou {self._auth_rejections}x seguidas: {end.detail}",
                    AlertLevel.CRITICAL,
                )
        else:
            self._auth_rejections = 0
        if end.reason in (
            EndReason.HANDSHAKE_REJECTED,
            EndReason.AUTH_REJECTED,
            EndReason.UNAVAILABLE,
        ):
            decision = decide_on_close(None, failures, policy, rng)
            critical = end.reason is not EndReason.UNAVAILABLE  # configuração: alerta já
            alert = AlertLevel.CRITICAL if critical else decision.alert
            return CloseDecision(decision.action, decision.wait_s, end.detail, alert)
        return decide_on_close(end.close_code, failures, policy, rng)

    async def _disconnected(self, state: ConsumerState, end: SessionEnd) -> None:
        reason = f"{end.reason.value}: {end.detail}"[:500]
        try:
            await self._deps.status.record_disconnect(
                self._options.chain_code, state, end.close_code, reason
            )
        except (StoreUnavailableError, UnknownChainError):
            log.warning("status_indisponivel", exc_info=True)  # a espera mínima continua

    async def _state(self, state: ConsumerState) -> None:
        try:
            await self._deps.status.record_state(
                self._options.chain_code, state, self._options.instance_id
            )
        except StoreUnavailableError:
            log.warning("status_indisponivel", exc_info=True)

    async def _sleep(self, seconds: float) -> None:
        """Espera interrompível pelo pedido de parada."""
        if seconds <= 0:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), seconds)


class _Session:
    """Uma conexão WebSocket, do connect ao fechamento."""

    def __init__(
        self, deps: ConsumerDeps, options: ConsumerOptions, epoch: int, stop: asyncio.Event
    ) -> None:
        self.d = deps
        self.o = options
        self.stop = stop
        self.subscription_id = str(uuid.uuid4())
        self.context = ConsumerContext(
            options.chain_code, self.subscription_id, epoch, options.event_tz
        )
        self.intake = IntakeQueue(
            max_items=options.intake_max_items, max_bytes=options.intake_max_bytes
        )
        self.conn: WsConnection | None = None
        self.rtt = RttEstimator()
        self.loop = asyncio.get_running_loop()
        self.last_alive = self.loop.time()
        self.ping_sent_at: float | None = None
        self.subscribed_at: float | None = None
        self.events_received = 0
        self.poisoned = False
        self.drain_cause: DrainCause | None = None
        self.completed = False
        self.socket_closed = False
        self.closed = asyncio.Event()  # a sessão acabou (o socket pode ainda estar aberto)
        self.end: SessionEnd | None = None
        self.offset: Offset | None = None
        self.token: AccessToken | None = None  # token usado no connection_init
        self.background: set[asyncio.Task[None]] = set()

    @property
    def ws(self) -> WsConnection:
        if self.conn is None:
            raise RuntimeError("sessão sem conexão")
        return self.conn

    # ------------------------------------------------------------------ ciclo

    async def run(self) -> SessionEnd:
        with bind_context(subscription_id=self.subscription_id):
            try:
                if await self._handshake():
                    await self._serve()
            except ConnectionClosedError as exc:
                self._finish(self._closed_end(exc))
            except AuthRejectedError as exc:
                self._finish(SessionEnd(EndReason.AUTH_REJECTED, detail=str(exc)))
            except (AuthUnavailableError, StoreUnavailableError) as exc:
                self._finish(SessionEnd(EndReason.UNAVAILABLE, detail=str(exc)))
            finally:
                for task in self.background:
                    task.cancel()
                if self.conn is not None and not self.socket_closed:
                    # Só aqui o cliente fecha: prazo de drenagem vencido, sem prova de vida
                    # ou falha antes de assinar (ADR-0007).
                    with contextlib.suppress(Exception):
                        await self.conn.close()
            end = self.end or SessionEnd(EndReason.SERVER_CLOSED, detail="fim sem motivo")
            end.healthy = self._healthy()
            end.offset = self.offset
            end.drain_cause = end.drain_cause or self.drain_cause
            return end

    async def _handshake(self) -> bool:
        """True quando assinou; False quando a sessão já terminou (``self.end``)."""
        d, o = self.d, self.o
        token = self.token = await d.tokens.get()
        await self._record(ConsumerState.CONNECTING)
        try:
            self.conn = await d.connector.connect()
        except HandshakeRejectedError as exc:
            log.error("ws_handshake_recusado", detail=str(exc))
            self._finish(SessionEnd(EndReason.HANDSHAKE_REJECTED, detail=str(exc)))
            return False
        await self.ws.send(protocol.connection_init(token.value, d.app_key))
        await self._record(ConsumerState.INIT_SENT)
        if await self._wait_frame(protocol.FrameType.CONNECTION_ACK, None, o.ack_timeout_s) is None:
            self._finish(SessionEnd(EndReason.ACK_TIMEOUT, detail="sem connection_ack"))
            return False

        if o.status_check_enabled:
            await self._record(ConsumerState.STATUS_CHECK)
            status = await self._status_check()
            if status not in (None, "Inactive"):
                log.warning("ohip_conexao_ativa", status=status)
                self._finish(SessionEnd(EndReason.CONNECTION_ACTIVE, detail=f"status {status}"))
                return False

        self.offset = await d.replay_store.last_offset(o.chain_code)
        await self.ws.send(
            protocol.subscribe(
                self.subscription_id,
                chain_code=o.chain_code,
                offset=self.offset,
                hotel_codes=o.hotel_codes,
                delta=o.delta,
            )
        )
        self.subscribed_at = self.last_alive = self.loop.time()
        log.info(  # dados pedidos pelo suporte Oracle (ADR-0007)
            "ohip_assinado",
            offset=self.offset.value if self.offset else None,
            hotel_codes=list(o.hotel_codes),
            instance_id=o.instance_id,
        )
        await self._record(ConsumerState.SUBSCRIBED)
        return True

    async def _serve(self) -> None:
        reader = asyncio.create_task(self._reader(), name="ws-leitura")
        writer = asyncio.create_task(self._writer(), name="ws-gravacao")
        helpers = [
            asyncio.create_task(self._heartbeat(), name="ws-heartbeat"),
            asyncio.create_task(self._control(), name="ws-controle"),
        ]
        closed = asyncio.ensure_future(self.closed.wait())
        try:
            pending: set[asyncio.Future[Any]] = {closed, reader, writer}
            while not self.closed.is_set():
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done - {closed}:
                    if self._died(task):  # erro ou fim sem envenenamento/drenagem
                        await self._task_died(task)
            reader.cancel()
            # A gravação esvazia o que já está na fila (descartado se envenenada).
            await self.intake.close()
            if not writer.done():
                await writer
        finally:
            for task in (closed, reader, writer, *helpers):
                task.cancel()
            results = await asyncio.gather(reader, writer, *helpers, return_exceptions=True)
            for task, result in zip((reader, writer, *helpers), results, strict=True):
                if isinstance(result, Exception):
                    log.error("tarefa_da_sessao_falhou", task=task.get_name(), exc_info=result)

    def _died(self, task: asyncio.Future[Any]) -> bool:
        if task.cancelled() or task.exception() is not None:
            return True
        if self.closed.is_set():  # a leitura sai normalmente depois de encerrar a sessão
            return False
        # A gravação termina normalmente quando a fila foi descartada (envenenada) ou
        # fechada; isso não é falha: a sessão segue esperando o servidor fechar.
        return not (self.poisoned or self.completed)

    async def _task_died(self, task: asyncio.Future[Any]) -> None:
        error = task.exception() if not task.cancelled() else None
        name = task.get_name() if isinstance(task, asyncio.Task) else "tarefa"
        log.error("tarefa_da_sessao_terminou", task=name, exc_info=error)
        await self._poison(f"{name} terminou: {type(error).__name__}")
        self._finish(SessionEnd(EndReason.INTERNAL_ERROR, detail=f"{name}: {error!r}"))

    # ------------------------------------------------------------------ tarefas

    async def _reader(self) -> None:
        try:
            while True:
                raw = await self.ws.recv()
                await self._on_frame(protocol.classify(raw), raw)
        except ConnectionClosedError as exc:
            self._finish(self._closed_end(exc))
        except TimeoutError:  # put esperou mais que INTAKE_STALL_TIMEOUT
            await self._poison("fila interna travada além do prazo")
            await self._drain(DrainCause.POISONED)
            await self._read_until_closed()
        except Exception as exc:
            log.error("leitura_falhou", exc_info=True)
            await self._poison(f"leitura: {type(exc).__name__}")
            self._finish(SessionEnd(EndReason.INTERNAL_ERROR, detail=f"leitura: {exc!r}"))

    async def _read_until_closed(self) -> None:
        """Conexão envenenada: só lê (e responde ping) até o servidor fechar."""
        try:
            while True:
                if protocol.classify(await self.ws.recv()).type is protocol.FrameType.PING:
                    await self.ws.send(protocol.PONG)
        except ConnectionClosedError as exc:
            self._finish(self._closed_end(exc))

    async def _on_frame(self, frame: protocol.Frame, raw: str) -> None:
        kind = frame.type
        if kind is protocol.FrameType.NEXT and frame.id == self.subscription_id:
            self.last_alive = self.loop.time()  # next conta como prova de vida
            self.events_received += 1
            if self.poisoned:
                return  # nada desta conexão é gravado depois de envenenada
            item = IntakeItem(raw, self.d.clock.now(), len(raw))
            await asyncio.wait_for(self.intake.put(item), self.o.intake_stall_timeout_s)
        elif kind is protocol.FrameType.PING:
            await self.ws.send(protocol.PONG)
        elif kind is protocol.FrameType.PONG:
            self.last_alive = self.loop.time()
            if self.ping_sent_at is not None:
                self.rtt.update(self.loop.time() - self.ping_sent_at)
                self.ping_sent_at = None
        elif (
            kind in (protocol.FrameType.ERROR, protocol.FrameType.COMPLETE)
            and frame.id == self.subscription_id
        ):
            if kind is protocol.FrameType.COMPLETE and self.completed:
                return  # resposta ao nosso complete: continua esperando o fechamento
            log.error(
                "ohip_assinatura_encerrada",
                type=kind.value,
                errors=protocol.error_messages(frame.payload),
            )
            await self._drain(DrainCause.SUBSCRIPTION_ENDED)
        else:
            self.d.metrics.increment("ohip_ws_frames_ignored_total", type=kind.value)
            log.warning("ws_frame_ignorado", type=kind.value)

    async def _writer(self) -> None:
        while True:
            batch = await self.intake.get_batch(self.o.batch_max_events, self.o.batch_max_wait_s)
            if not batch:
                return
            if self.poisoned:
                continue
            messages = [IncomingMessage(i.raw, i.received_at) for i in batch]
            try:
                await self.d.batch.execute(self.context, messages)
            except StoreUnavailableError:
                await self._poison("banco indisponível")
                await self._drain(DrainCause.POISONED)
            except (LeaseLostError, UnknownChainError) as exc:
                await self._poison(f"{type(exc).__name__}: {exc}")
                await self._drain(DrainCause.LEASE_LOST)
            except Exception as exc:  # nunca morrer em silêncio (a fila encheria por horas)
                log.error("gravacao_falhou", exc_info=True)
                await self._poison(f"gravação: {type(exc).__name__}")
                await self._drain(DrainCause.POISONED)

    async def _heartbeat(self) -> None:
        try:
            await self._heartbeat_loop()
        except Exception as exc:
            log.error("heartbeat_falhou", exc_info=True)
            self._finish(SessionEnd(EndReason.INTERNAL_ERROR, detail=f"heartbeat: {exc!r}"))

    async def _heartbeat_loop(self) -> None:
        while not self.closed.is_set():
            await asyncio.sleep(self.o.ping_interval_s)
            silence = self.loop.time() - self.last_alive
            limit = self.rtt.liveness_timeout_s(
                min_timeout_s=self.o.pong_timeout_min_s, jitter_s=self.d.rng() * 5
            )
            if silence > limit:
                log.error("ohip_sem_prova_de_vida", silence_s=round(silence, 1))
                self._finish(SessionEnd(EndReason.LIVENESS_TIMEOUT, detail="sem pong nem next"))
                return
            if self.ping_sent_at is None:
                self.ping_sent_at = self.loop.time()
            with contextlib.suppress(ConnectionClosedError):
                await self.ws.send(protocol.PING)

    async def _control(self) -> None:
        while not self.closed.is_set():
            await self._wait_stop(self.o.control_poll_interval_s)
            if self.completed or self.closed.is_set():
                continue
            try:
                await self._control_step()
            except Exception:  # o controle nunca morre em silêncio
                log.error("controle_falhou", exc_info=True)

    async def _control_step(self) -> None:
        if self.stop.is_set():
            await self._drain(DrainCause.STOP)
        elif self.d.lease.lost:
            await self._poison("lease perdido")
            await self._drain(DrainCause.LEASE_LOST)
        elif await self._token_due():
            await self._drain(DrainCause.TOKEN)
        elif await self._replay_pending():
            await self._drain(DrainCause.REPLAY)
        elif not self.poisoned:
            await self._retry_dlq()

    # ------------------------------------------------------------------ apoio

    async def _token_due(self) -> bool:
        """O token desta conexão entrou na margem: renovar exige reconectar (ADR-0007)."""
        return self.token is not None and self.d.tokens.refresh_due(self.token)

    async def _replay_pending(self) -> bool:
        try:
            return await self.d.replay_store.pending_request(self.o.chain_code) is not None
        except StoreUnavailableError:
            return False

    async def _retry_dlq(self) -> None:
        try:
            await self.d.retry_dlq.execute(self.context, limit=self.o.dlq_retry_limit)
        except StoreUnavailableError:
            log.warning("retry_dlq_sem_banco")
        except LeaseLostError:
            await self._poison("lease perdido")
            await self._drain(DrainCause.LEASE_LOST)

    async def _drain(self, cause: DrainCause) -> None:
        """``complete`` e espera o servidor fechar (só fecha do lado do cliente no prazo)."""
        if self.completed or self.conn is None:
            return
        self.completed = True
        self.drain_cause = cause
        log.info("ohip_complete", cause=cause.value)
        await self._record(ConsumerState.DRAINING)
        with contextlib.suppress(ConnectionClosedError):
            await self.ws.send(protocol.complete(self.subscription_id))
        task = asyncio.create_task(self._drain_deadline(), name="ws-prazo-drenagem")
        self.background.add(task)
        task.add_done_callback(self.background.discard)

    async def _drain_deadline(self) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.closed.wait(), self.o.drain_timeout_s)
            return
        log.warning("ohip_nao_fechou_no_prazo", timeout_s=self.o.drain_timeout_s)
        self._finish(SessionEnd(EndReason.DRAINED, detail="prazo de drenagem vencido"))

    async def _poison(self, why: str) -> None:
        if self.poisoned:
            return
        self.poisoned = True
        dropped = await self.intake.discard()
        self.d.metrics.increment("ohip_connection_poisoned_total", chain_code=self.o.chain_code)
        log.error("conexao_envenenada", reason=why, descartadas=dropped)

    async def _status_check(self) -> str | None:
        query_id = str(uuid.uuid4())
        await self.ws.send(protocol.status_query(query_id))
        frame = await self._wait_frame(protocol.FrameType.NEXT, query_id, self.o.ack_timeout_s)
        return protocol.connection_status(frame.payload if frame else None)

    async def _wait_frame(
        self, kind: protocol.FrameType, frame_id: str | None, timeout_s: float
    ) -> protocol.Frame | None:
        """Lê até achar o frame (respondendo ping) ou o prazo vencer."""
        deadline = self.loop.time() + timeout_s
        while (remaining := deadline - self.loop.time()) > 0:
            try:
                raw = await asyncio.wait_for(self.ws.recv(), remaining)
            except TimeoutError:
                return None
            frame = protocol.classify(raw)
            if frame.type is protocol.FrameType.PING:
                await self.ws.send(protocol.PONG)
            elif frame.type is kind and frame.id == frame_id:
                if kind is protocol.FrameType.NEXT:  # o classify não guarda o payload do next
                    return protocol.Frame(kind, frame_id, raw, _payload(raw))
                return frame
        return None

    async def _wait_stop(self, seconds: float) -> None:
        """Espera o intervalo ou o pedido de parada. Já parado: espera o intervalo inteiro
        (``Event.wait`` de um evento marcado não suspende e travaria o laço)."""
        if self.stop.is_set():
            await asyncio.sleep(seconds)
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.stop.wait(), seconds)

    def _closed_end(self, exc: ConnectionClosedError) -> SessionEnd:
        self.socket_closed = True
        if isinstance(exc, MessageTooLargeError) or exc.close_code == CLIENT_TOO_LARGE:
            return SessionEnd(EndReason.OVERSIZE, CLIENT_TOO_LARGE, "mensagem grande demais")
        if self.completed:
            return SessionEnd(EndReason.DRAINED, exc.close_code, exc.reason)
        return SessionEnd(EndReason.SERVER_CLOSED, exc.close_code, exc.reason)

    def _finish(self, end: SessionEnd) -> None:
        if self.end is None:
            self.end = end
        self.closed.set()

    def _healthy(self) -> bool:
        if self.events_received:
            return True
        if self.subscribed_at is None:
            return False
        return self.loop.time() - self.subscribed_at >= self.o.healthy_after_s

    async def _record(self, state: ConsumerState) -> None:
        try:
            await self.d.status.record_state(self.o.chain_code, state, self.o.instance_id)
        except StoreUnavailableError:
            log.warning("status_indisponivel")


def _payload(raw: str) -> object:
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    return data.get("payload") if isinstance(data, dict) else None
