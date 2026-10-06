"""Token OAuth em memória."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from ohip_streaming.application.ports import (
    AccessToken,
    Clock,
)


class FakeTokenIssuer:
    def __init__(self, clock: Clock, *, lifetime_s: float = 3600) -> None:
        self.clock = clock
        self.lifetime_s = lifetime_s
        self.issued = 0
        self.fail: list[Exception] = []

    async def issue(self) -> AccessToken:
        await asyncio.sleep(0)  # deixa chamadas concorrentes se encontrarem
        if self.fail:
            raise self.fail.pop(0)
        self.issued += 1
        return AccessToken(
            f"token-{self.issued}", self.clock.now() + timedelta(seconds=self.lifetime_s)
        )


class FakeTokenCache:
    def __init__(self) -> None:
        self.values: dict[str, tuple[AccessToken, float]] = {}
        self.broken = False

    async def get(self, key: str) -> AccessToken | None:
        if self.broken:
            raise ConnectionError("redis fora")
        entry = self.values.get(key)
        return entry[0] if entry else None

    async def put(self, key: str, token: AccessToken, ttl_s: float) -> None:
        if self.broken:
            raise ConnectionError("redis fora")
        self.values[key] = (token, ttl_s)

    async def delete(self, key: str) -> None:
        if self.broken:
            raise ConnectionError("redis fora")
        self.values.pop(key, None)
