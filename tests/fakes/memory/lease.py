"""Lease em memória (ADR-0008)."""

from __future__ import annotations

from datetime import datetime, timedelta

from ohip_streaming.application.errors import (
    LeaseNotProvisionedError,
)
from tests.fakes.memory.database import InMemoryDatabase


class MemoryLeaseStore:
    """``LeaseStore`` sobre os epochs do ``InMemoryDatabase`` (a barreira dos stores vê o mesmo
    epoch). O relógio do fake faz o papel do relógio do banco."""

    def __init__(self, db: InMemoryDatabase) -> None:
        self.db = db
        self.holders: dict[str, tuple[str | None, datetime]] = {}
        self.fail: list[Exception] = []

    def _holder(self, lease_name: str) -> tuple[str | None, datetime]:
        if self.fail:
            raise self.fail.pop(0)
        if lease_name not in self.db.leases:
            raise LeaseNotProvisionedError(lease_name)
        # Semeado com prazo já vencido (sql/003): o primeiro dono entra.
        return self.holders.get(lease_name, (None, self.db.clock.now() - timedelta(seconds=1)))

    async def acquire(self, lease_name: str, owner: str, ttl_s: float) -> int | None:
        holder, expires_at = self._holder(lease_name)
        now = self.db.clock.now()
        if not (expires_at < now or holder == owner):
            return None
        self.db.leases[lease_name] += 1
        self.holders[lease_name] = (owner, now + timedelta(seconds=ttl_s))
        return self.db.leases[lease_name]

    async def renew(self, lease_name: str, owner: str, epoch: int, ttl_s: float) -> bool:
        holder, _ = self._holder(lease_name)
        if holder != owner or self.db.leases[lease_name] != epoch:
            return False
        self.holders[lease_name] = (owner, self.db.clock.now() + timedelta(seconds=ttl_s))
        return True

    async def release(self, lease_name: str, owner: str, epoch: int) -> None:
        holder, _ = self._holder(lease_name)
        if holder == owner and self.db.leases[lease_name] == epoch:
            # No banco, o próximo acquire já vê o relógio alguns µs adiante.
            self.holders[lease_name] = (owner, self.db.clock.now() - timedelta(microseconds=1))
