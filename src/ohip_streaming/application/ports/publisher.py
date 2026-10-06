"""Publisher: outbox e broker (ADR-0002, ADR-0016)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class OutboxRow:
    id: int
    chain_code: str
    exchange_name: str
    routing_key: str
    message_id: str
    body: str
    schema_version: int
    attempts: int
    next_attempt_at: datetime


class OutboxStore(Protocol):
    async def chains_with_pending(self) -> list[str]: ...

    async def fetch_head(self, chain_code: str, limit: int) -> list[OutboxRow]:
        """Linhas PENDING da chain em ordem de id, a partir da cabeça (sem filtrar por horário)."""
        ...

    async def mark_sent(self, row_id: int, lease_epoch: int) -> None: ...

    async def record_failure(
        self, row_id: int, attempts: int, next_attempt_at: datetime, error: str, lease_epoch: int
    ) -> None: ...

    async def mark_failed(self, row_id: int, attempts: int, error: str, lease_epoch: int) -> None:
        """``FAILED`` + item de DLQ stage PUBLISH, na mesma transação."""
        ...


class PublishOutcome(StrEnum):
    ACKED = "ACKED"
    NACKED = "NACKED"


@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    exchange: str
    routing_key: str
    message_id: str
    body: bytes
    headers: Mapping[str, Any] = field(default_factory=dict)
    # Faixa de publicação (a chain): o adapter usa um canal por faixa, para a queda de canal
    # causada pela mensagem de uma chain não ser atribuída às outras (ADR-0011 nº 7).
    lane: str = ""


class MessagePublisher(Protocol):
    async def publish(self, message: OutgoingMessage) -> PublishOutcome:
        """Publica com confirmação do broker. Levanta ``BrokerUnavailableError`` se a conexão
        ou o canal cair."""
        ...
