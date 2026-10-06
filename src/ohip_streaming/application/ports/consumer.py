"""Consumer: gravação do lote, estado da conexão e WebSocket do OHIP."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from ohip_streaming.application.ports.common import ProcessingStatus
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.messages import QueueMessage
from ohip_streaming.domain.offset import Offset


class SeenCache(Protocol):
    """Atalho de deduplicação no Redis (ADR-0009). Falhas aqui nunca afetam a correção."""

    async def filter_seen(self, unique_event_ids: Sequence[str]) -> set[str]: ...

    async def mark_seen(self, unique_event_ids: Sequence[str]) -> None: ...


@dataclass(frozen=True, slots=True)
class NewEventRecord:
    raw_event_id: int
    event: Event
    status: ProcessingStatus
    outbox: QueueMessage | None  # None quando IGNORED (DV-13)


@dataclass(frozen=True, slots=True)
class ConsumeDlqRecord:
    """Mensagem que não virou evento (rejeitada pelo domínio). Vai para OHIP_DLQ stage CONSUME."""

    raw_message: str
    reason: str
    offset: Offset | None
    unique_event_id: str | None


@dataclass(frozen=True, slots=True)
class BatchToPersist:
    chain_code: str
    lease_epoch: int
    events: tuple[NewEventRecord, ...]  # em ordem de chegada (define a ordem da outbox)
    rejected: tuple[ConsumeDlqRecord, ...]
    offset: Offset | None  # None = não alterar OHIP_OFFSET
    last_unique_event_id: str | None
    # Retry de DLQ CONSUME: ver ``EventStore.persist_batch``.
    retry_of_dlq_id: int | None = None


@dataclass(frozen=True, slots=True)
class RowFailure:
    unique_event_id: str
    error_class: str
    error_message: str


@dataclass(frozen=True, slots=True)
class BatchResult:
    inserted: tuple[str, ...] = ()  # uniqueEventIds gravados
    duplicates: tuple[str, ...] = ()  # ORA-00001 em uq_evt ou uq_off
    row_failures: tuple[RowFailure, ...] = ()  # outros erros por linha → DLQ CONSUME


@dataclass(frozen=True, slots=True)
class ConsumeRetryItem:
    dlq_id: int
    raw_message: str  # frame ``next`` original ou só o ``newEvent`` (parse_stored_message)
    created_at: datetime


class EventStore(Protocol):
    async def reserve_event_ids(self, count: int) -> list[int]:
        """Reserva ids da sequence de OHIP_EVENT_RAW, em ordem crescente (fora da transação)."""
        ...

    async def persist_batch(self, batch: BatchToPersist) -> BatchResult:
        """Grava o lote numa única transação (ARCHITECTURE §4.1):

        1. insere os eventos; duplicado (qualquer das duas UNIQUE) é ignorado e não gera outbox;
           qualquer outro erro por linha vira DLQ CONSUME e não gera outbox;
        2. insere a outbox ``PENDING`` dos eventos novos com ``outbox`` não nulo, na ordem;
        3. insere a DLQ CONSUME dos ``rejected``;
        4. atualiza OHIP_OFFSET se ``offset`` não for None;
        5. com ``retry_of_dlq_id`` (lote de um evento, ``offset`` None, vindo de
           ``RetryConsumeDlq``): evento gravado **ou duplicado** → ``UPDATE ohip_dlq SET
           resolution='RETRIED', resolved_at, resolved_by, retry_requested_at=NULL WHERE id=:id
           AND resolution IS NULL``; erro por linha → **não** cria item novo de DLQ: ``UPDATE``
           do mesmo item com o erro novo, ``attempts = attempts + 1`` e ``retry_requested_at =
           NULL``;
        6. barreira de epoch como último comando; falhou → rollback e ``LeaseLostError``.

        Levanta ``StoreUnavailableError`` (banco fora **ou sem recursos**: ORA-01653/01654/
        01688/01691/30036 etc. — falha que atingiria qualquer lote) ou ``BatchFailedError``
        (lote recusado por outro motivo); nos dois casos nada foi gravado.
        """
        ...

    async def pending_consume_retries(self, chain_code: str, limit: int) -> list[ConsumeRetryItem]:
        """Itens de DLQ CONSUME da chain com retry pedido pela API e ainda abertos."""
        ...

    async def mark_consume_retry_failed(self, dlq_id: int, reason: str, lease_epoch: int) -> None:
        """Mensagem continua inválida ou recusada: numa transação com barreira de epoch, mantém
        o item aberto, grava o motivo, ``attempts = attempts + 1`` e limpa o pedido."""
        ...


@dataclass(frozen=True, slots=True)
class DisconnectSnapshot:
    """Última desconexão registrada, com o relógio do banco (regra dos 10 s, ADR-0007)."""

    db_now: datetime
    last_disconnect_at: datetime | None
    last_state: ConsumerState | None


@dataclass(frozen=True, slots=True)
class ConnectionHealth:
    """Saúde da conexão gravada periodicamente (ADR-0020 §1). Relógio do processo (UTC):
    informativo, fora da regra dos 10 s. ``None`` = sem novidade (a coluna fica como está)."""

    last_message_at: datetime | None = None
    last_ping_at: datetime | None = None
    last_pong_at: datetime | None = None
    rtt_ms: int | None = None


class ConsumerStatusStore(Protocol):
    """OHIP_CONSUMER_STATUS (ADR-0020 §1). Todos levantam ``UnknownChainError`` se a chain
    não estiver provisionada."""

    async def record_state(
        self, chain_code: str, state: ConsumerState, instance_id: str
    ) -> None: ...

    async def record_subscribed(
        self,
        chain_code: str,
        instance_id: str,
        subscription_id: str,
        token_expires_at: datetime,
    ) -> None:
        """``SUBSCRIBED`` com ``subscription_id``, ``connected_at`` (relógio do banco),
        ``token_expires_at`` e ``next_attempt_at`` limpo."""
        ...

    async def record_health(self, chain_code: str, health: ConnectionHealth) -> None:
        """Grava só os campos com valor."""
        ...

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
        """Grava ``last_disconnect_at`` com o horário **do banco**, junto com o estado, as
        falhas seguidas, ``reconnects + 1`` se ``reconnect`` e ``next_attempt_at`` = horário
        do banco + ``next_attempt_in_s`` (``None`` limpa)."""
        ...

    async def disconnect_snapshot(self, chain_code: str) -> DisconnectSnapshot: ...


class WsConnection(Protocol):
    async def send(self, text: str) -> None:
        """Levanta ``ConnectionClosedError`` se a conexão caiu."""
        ...

    async def recv(self) -> str:
        """Próximo frame de texto. Levanta ``ConnectionClosedError`` (ou
        ``MessageTooLargeError``) quando a conexão fecha."""
        ...

    async def close(self) -> None:
        """Fecha do lado do cliente. Só depois de esgotar a espera pelo servidor (ADR-0007)."""
        ...


class WsConnector(Protocol):
    async def connect(self) -> WsConnection:
        """Abre ``wss://.../subscriptions?key=...`` com o subprotocolo. Levanta
        ``ConnectionClosedError`` (rede) ou ``HandshakeRejectedError``."""
        ...
