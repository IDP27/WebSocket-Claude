"""Relógio real (UTC) para os processos. Os testes usam ``FakeClock``."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
