"""Replay por offset (ARCHITECTURE §4.6)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from ohip_streaming.domain.offset import Offset


class ReplayStatus(StrEnum):
    PENDING = "PENDING"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    id: int
    chain_code: str
    from_offset: Offset
    reason: str
    requested_by: str
    status: ReplayStatus
    created_at: datetime


class ReplayStore(Protocol):
    async def last_offset(self, chain_code: str) -> Offset | None:
        """Levanta ``UnknownChainError`` se a chain não estiver provisionada."""
        ...

    async def offset_received_at(self, chain_code: str, offset: Offset) -> datetime | None: ...

    async def create_request(
        self, chain_code: str, from_offset: Offset, reason: str, requested_by: str
    ) -> ReplayRequest:
        """Levanta ``ReplayAlreadyPendingError`` se já houver PENDING na chain (índice único)."""
        ...

    async def pending_request(self, chain_code: str) -> ReplayRequest | None: ...

    async def apply_request(self, request: ReplayRequest, lease_epoch: int) -> bool:
        """Numa transação com barreira de epoch: ``APPLIED WHERE status='PENDING'`` e
        ``OHIP_OFFSET.last_offset = from_offset``. False se o pedido não estava mais PENDING."""
        ...

    async def cancel_request(self, request_id: int, cancelled_by: str) -> bool:
        """False se o pedido não existir ou não estiver PENDING."""
        ...
