"""``ConsumerStatusStore``: OHIP_CONSUMER_STATUS."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

from ohip_streaming.application.errors import (
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    ConnectionHealth,
    DisconnectSnapshot,
)
from ohip_streaming.domain.connection import ConsumerState
from tests.fakes.memory.rows import StatusRow
from tests.fakes.memory.state import MemoryState


class StatusFake(MemoryState):
    """``ConsumerStatusStore``: OHIP_CONSUMER_STATUS."""

    def _status(self, chain_code: str) -> StatusRow:
        if chain_code not in self.statuses:
            raise UnknownChainError(chain_code)
        if self.fail_status:
            raise self.fail_status.pop(0)
        return self.statuses[chain_code]

    async def record_state(self, chain_code: str, state: ConsumerState, instance_id: str) -> None:
        row = self._status(chain_code)
        self.statuses[chain_code] = replace(row, state=state, instance_id=instance_id)

    async def record_subscribed(
        self,
        chain_code: str,
        instance_id: str,
        subscription_id: str,
        token_expires_at: datetime,
    ) -> None:
        row = self._status(chain_code)
        self.statuses[chain_code] = replace(
            row,
            state=ConsumerState.SUBSCRIBED,
            instance_id=instance_id,
            subscription_id=subscription_id,
            connected_at=self.clock.now(),
            token_expires_at=token_expires_at,
            next_attempt_at=None,
        )

    async def record_health(self, chain_code: str, health: ConnectionHealth) -> None:
        row = self._status(chain_code)
        self.statuses[chain_code] = replace(
            row,
            last_message_at=health.last_message_at or row.last_message_at,
            last_ping_at=health.last_ping_at or row.last_ping_at,
            last_pong_at=health.last_pong_at or row.last_pong_at,
            rtt_ms=row.rtt_ms if health.rtt_ms is None else health.rtt_ms,
        )

    async def record_disconnect(
        self,
        chain_code: str,
        state: ConsumerState,
        close_code: int | None,
        close_reason: str | None,
        *,
        consecutive_failures: int = 0,
        reconnect: bool = False,
        next_attempt_in_s: float | None = None,
    ) -> None:
        row = self._status(chain_code)
        now = self.clock.now()
        self.statuses[chain_code] = replace(
            row,
            state=state,
            last_disconnect_at=now,
            last_close_code=close_code,
            last_close_reason=close_reason,
            consecutive_failures=consecutive_failures,
            reconnects=row.reconnects + (1 if reconnect else 0),
            next_attempt_at=(
                None if next_attempt_in_s is None else now + timedelta(seconds=next_attempt_in_s)
            ),
        )

    async def disconnect_snapshot(self, chain_code: str) -> DisconnectSnapshot:
        row = self._status(chain_code)
        return DisconnectSnapshot(self.clock.now(), row.last_disconnect_at, row.state)
