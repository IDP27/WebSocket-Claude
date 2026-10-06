"""``ReplayStore``: pedidos de replay e offset confirmado."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from ohip_streaming.application.errors import (
    ReplayAlreadyPendingError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    ReplayRequest,
    ReplayStatus,
)
from ohip_streaming.domain.offset import Offset
from tests.fakes.memory.rows import OffsetState
from tests.fakes.memory.state import MemoryState


class ReplayFake(MemoryState):
    """``ReplayStore``: pedidos de replay e offset confirmado."""

    async def last_offset(self, chain_code: str) -> Offset | None:
        if chain_code not in self.offsets:
            raise UnknownChainError(chain_code)
        return self.offsets[chain_code].last_offset

    async def offset_received_at(self, chain_code: str, offset: Offset) -> datetime | None:
        for row in self.raw.values():
            if row.event.chain_code == chain_code and row.event.offset == offset:
                return row.event.received_at
        return None

    async def create_request(
        self, chain_code: str, from_offset: Offset, reason: str, requested_by: str
    ) -> ReplayRequest:
        if await self.pending_request(chain_code) is not None:
            raise ReplayAlreadyPendingError(chain_code)
        request = ReplayRequest(
            id=self._next_id("replay"),
            chain_code=chain_code,
            from_offset=from_offset,
            reason=reason,
            requested_by=requested_by,
            status=ReplayStatus.PENDING,
            created_at=self.clock.now(),
        )
        self.replays[request.id] = request
        return request

    async def pending_request(self, chain_code: str) -> ReplayRequest | None:
        return next(
            (
                r
                for r in self.replays.values()
                if r.chain_code == chain_code and r.status is ReplayStatus.PENDING
            ),
            None,
        )

    async def apply_request(self, request: ReplayRequest, lease_epoch: int) -> bool:
        self._fence(f"consumer:{request.chain_code}", lease_epoch)
        current = self.replays.get(request.id)
        if current is None or current.status is not ReplayStatus.PENDING:
            return False
        self.replays[request.id] = replace(current, status=ReplayStatus.APPLIED)
        self.offsets[request.chain_code] = OffsetState(request.from_offset, None)
        return True

    async def cancel_request(self, request_id: int, cancelled_by: str) -> bool:
        current = self.replays.get(request_id)
        if current is None or current.status is not ReplayStatus.PENDING:
            return False
        self.replays[request_id] = replace(current, status=ReplayStatus.CANCELLED)
        self.cancelled_by[request_id] = cancelled_by
        return True
