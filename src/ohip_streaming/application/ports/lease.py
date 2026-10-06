"""Lease com prazo e epoch no Oracle (ADR-0008)."""

from __future__ import annotations

from typing import Protocol


class LeaseStore(Protocol):
    """OHIP_LEASE no Oracle, sempre com o relógio do banco (ADR-0008)."""

    async def acquire(self, lease_name: str, owner: str, ttl_s: float) -> int | None:
        """Novo epoch se o lease estava vencido ou já era nosso; None se tem outro dono.
        Levanta ``LeaseNotProvisionedError`` se a linha não existe."""
        ...

    async def renew(self, lease_name: str, owner: str, epoch: int, ttl_s: float) -> bool:
        """False se perdemos o lease (outro epoch ou outro dono)."""
        ...

    async def release(self, lease_name: str, owner: str, epoch: int) -> None:
        """Vence o prazo agora, só se o lease ainda for nosso nesse epoch."""
        ...
