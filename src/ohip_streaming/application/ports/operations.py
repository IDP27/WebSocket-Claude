"""Operações da API: reprocessamento e retry de DLQ."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ohip_streaming.application.ports.common import DlqStage, StoredEvent
from ohip_streaming.domain.messages import QueueMessage


@dataclass(frozen=True, slots=True)
class DlqItem:
    id: int
    stage: DlqStage
    chain_code: str | None
    event_raw_id: int | None
    outbox_id: int | None
    unique_event_id: str | None
    resolved: bool


class OperationsStore(Protocol):
    async def find_event(self, unique_event_id: str) -> StoredEvent | None: ...

    async def get_event(self, raw_event_id: int) -> StoredEvent | None: ...

    async def enqueue(
        self, *, chain_code: str, raw_event_id: int, message: QueueMessage, exchange_name: str
    ) -> int:
        """Insere uma linha PENDING na outbox (reprocessamento). Devolve o id."""
        ...

    async def get_dlq_item(self, item_id: int) -> DlqItem | None: ...

    # Operações de retry: cada uma é **uma transação** condicionada a ``resolution IS NULL``
    # (dois cliques ou duas réplicas da API não duplicam nada). Devolvem None/False se o item
    # não estava mais aberto. Ordem obrigatória no adapter (Oracle READ COMMITTED):
    #   1. ``UPDATE ohip_dlq ... WHERE id = :id AND resolution IS NULL [AND ...]`` primeiro;
    #      o lock da linha serializa pedidos concorrentes;
    #   2. rowcount = 0 → ROLLBACK e devolve None/False, sem inserir nada;
    #   3. só então o ``INSERT`` na outbox; COMMIT.
    # Conferir antes com SELECT e inserir depois não é atômico (duas sessões passam no SELECT).

    async def request_consume_retry(self, item_id: int, requested_by: str) -> bool:
        """``SET retry_requested_at/by WHERE resolution IS NULL AND retry_requested_at IS NULL``.
        O consumer da chain executa (``RetryConsumeDlq``)."""
        ...

    async def retry_publish(self, item_id: int, outbox_id: int, requested_by: str) -> int | None:
        """Copia a linha da outbox para o fim da fila (PENDING) e resolve o item."""
        ...

    async def enqueue_and_resolve(
        self,
        item_id: int,
        *,
        chain_code: str,
        raw_event_id: int,
        message: QueueMessage,
        exchange_name: str,
        requested_by: str,
    ) -> int | None:
        """Insere a mensagem de reprocessamento na outbox e resolve o item."""
        ...
