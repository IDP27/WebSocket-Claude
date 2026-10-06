"""Infraestrutura simples: relógio, métricas, caches, fetcher e publisher falsos."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from ohip_streaming.application.ports import (
    OutgoingMessage,
    PublishOutcome,
)
from ohip_streaming.domain.messages import ExchangeKind

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


EXCHANGES = {ExchangeKind.EVENTS: "ohip.events", ExchangeKind.REPROCESS: "ohip.reprocess"}


class FakeClock:
    def __init__(self, start: datetime = T0) -> None:
        self.current = start
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.current

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)
        await asyncio.sleep(0)

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class FakeMetrics:
    def __init__(self) -> None:
        self.counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()

    def increment(self, name: str, value: int = 1, **labels: str) -> None:
        self.counters[(name, tuple(sorted(labels.items())))] += value

    def total(self, name: str) -> int:
        return sum(v for (n, _), v in self.counters.items() if n == name)


class FakeSeenCache:
    def __init__(self) -> None:
        self.seen: set[str] = set()
        self.fail_filter = False
        self.fail_mark = False
        self.mark_calls: list[list[str]] = []

    async def filter_seen(self, unique_event_ids: Sequence[str]) -> set[str]:
        if self.fail_filter:
            raise ConnectionError("redis fora")
        return self.seen.intersection(unique_event_ids)

    async def mark_seen(self, unique_event_ids: Sequence[str]) -> None:
        if self.fail_mark:
            raise ConnectionError("redis fora")
        self.mark_calls.append(list(unique_event_ids))
        self.seen.update(unique_event_ids)


class FakeQueueDedup:
    def __init__(self) -> None:
        self.processed: set[str] = set()

    async def is_processed(self, message_id: str) -> bool:
        return message_id in self.processed

    async def mark_processed(self, message_id: str) -> None:
        self.processed.add(message_id)


class FakeFetcher:
    def __init__(self, resource: Mapping[str, Any] | None = None) -> None:
        self.resource = resource
        self.calls: list[tuple[str, str, str | None, str]] = []
        self.errors: list[Exception] = []  # levantadas em ordem, uma por chamada

    async def fetch(
        self, chain_code: str, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        self.calls.append((chain_code, module_name, hotel_id, primary_key))
        if self.errors:
            raise self.errors.pop(0)
        return self.resource


class FakePublisher:
    """Responde com um roteiro: cada item é um ``PublishOutcome`` ou uma exceção a levantar."""

    def __init__(self, script: Sequence[PublishOutcome | Exception] = ()) -> None:
        self.script = list(script)
        self.published: list[OutgoingMessage] = []
        self.attempts: list[OutgoingMessage] = []

    async def publish(self, message: OutgoingMessage) -> PublishOutcome:
        self.attempts.append(message)
        outcome = self.script.pop(0) if self.script else PublishOutcome.ACKED
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is PublishOutcome.ACKED:
            self.published.append(message)
        return outcome
