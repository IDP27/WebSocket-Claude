"""Replay por offset (ARCHITECTURE §4.6, API.md).

- ``RequestReplay`` (API): valida e grava o pedido PENDING.
- ``ApplyReplay`` (consumer): chamado com a conexão fechada, a fila vazia e o escritor ocioso;
  troca o offset numa transação com barreira de epoch.
- ``CancelReplay`` (API).
"""

from __future__ import annotations

from dataclasses import dataclass

from ohip_streaming.application.errors import ReplayNotPendingError
from ohip_streaming.application.ports import Clock, ReplayRequest, ReplayStore
from ohip_streaming.domain.rules import replay_retention_warning, validate_replay_request
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ReplayCommand:
    chain_code: str
    from_offset: str
    reason: str
    confirm: str
    requested_by: str


@dataclass(frozen=True, slots=True)
class ReplayRequested:
    request: ReplayRequest
    warnings: tuple[str, ...]


class RequestReplay:
    def __init__(self, *, store: ReplayStore, clock: Clock) -> None:
        self._store = store
        self._clock = clock

    async def execute(self, command: ReplayCommand) -> ReplayRequested:
        last_offset = await self._store.last_offset(command.chain_code)
        from_offset = validate_replay_request(
            chain_code=command.chain_code,
            from_offset_raw=command.from_offset,
            confirm=command.confirm,
            reason=command.reason,
            last_offset=last_offset,
        )
        received_at = await self._store.offset_received_at(command.chain_code, from_offset)
        warning = replay_retention_warning(received_at, self._clock.now())
        request = await self._store.create_request(
            command.chain_code, from_offset, command.reason.strip(), command.requested_by
        )
        log.info(
            "replay_solicitado",
            replay_id=request.id,
            chain_code=command.chain_code,
            from_offset=str(from_offset),
            requested_by=command.requested_by,
        )
        return ReplayRequested(request, (warning,) if warning else ())


class ApplyReplay:
    def __init__(self, *, store: ReplayStore) -> None:
        self._store = store

    async def pending(self, chain_code: str) -> ReplayRequest | None:
        return await self._store.pending_request(chain_code)

    async def execute(self, request: ReplayRequest, lease_epoch: int) -> bool:
        """Aplica o pedido. False se ele foi cancelado entre a leitura e a aplicação."""
        applied = await self._store.apply_request(request, lease_epoch)
        log.info(
            "replay_aplicado" if applied else "replay_nao_aplicado",
            replay_id=request.id,
            chain_code=request.chain_code,
            from_offset=str(request.from_offset),
        )
        return applied


class CancelReplay:
    def __init__(self, *, store: ReplayStore) -> None:
        self._store = store

    async def execute(self, request_id: int, cancelled_by: str) -> None:
        if not await self._store.cancel_request(request_id, cancelled_by):
            raise ReplayNotPendingError(f"pedido {request_id} não está PENDING")
        log.info("replay_cancelado", replay_id=request_id, cancelled_by=cancelled_by)
