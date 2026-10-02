"""Ports (interfaces) da aplicação. Os adapters (Fases 3 a 9) implementam; os testes usam fakes.

Todos assíncronos: o adapter Oracle roda o driver síncrono em executor próprio
(ADR-0003, ``ohip_streaming.logging.run_in_executor``), sem que os casos de uso saibam disso.

Contratos transacionais importantes estão descritos em cada método: o fake em memória
(tests/fakes) e o adapter Oracle precisam se comportar igual.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.messages import QueueMessage
from ohip_streaming.domain.offset import Offset

# =============================================================== infraestrutura comum


class Clock(Protocol):
    def now(self) -> datetime:
        """Agora, com fuso (UTC)."""
        ...

    async def sleep(self, seconds: float) -> None: ...


class MetricsSink(Protocol):
    def increment(self, name: str, value: int = 1, **labels: str) -> None: ...


class SeenCache(Protocol):
    """Atalho de deduplicação no Redis (ADR-0009). Falhas aqui nunca afetam a correção."""

    async def filter_seen(self, unique_event_ids: Sequence[str]) -> set[str]: ...

    async def mark_seen(self, unique_event_ids: Sequence[str]) -> None: ...


# =============================================================== consumer: gravação do lote


class ProcessingStatus(StrEnum):
    """Valores de OHIP_EVENT_RAW.processing_status (CHECK na DDL)."""

    RECEIVED = "RECEIVED"
    IGNORED = "IGNORED"
    NORMALIZED = "NORMALIZED"
    UNMAPPED = "UNMAPPED"
    ENRICHED = "ENRICHED"
    FAILED = "FAILED"


class DlqStage(StrEnum):
    CONSUME = "CONSUME"
    PUBLISH = "PUBLISH"
    NORMALIZE = "NORMALIZE"
    ENRICH = "ENRICH"


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


# =============================================================== consumer: estado da conexão


@dataclass(frozen=True, slots=True)
class DisconnectSnapshot:
    """Última desconexão registrada, com o relógio do banco (regra dos 10 s, ADR-0007)."""

    db_now: datetime
    last_disconnect_at: datetime | None
    last_state: ConsumerState | None


class ConsumerStatusStore(Protocol):
    """OHIP_CONSUMER_STATUS. Os campos de heartbeat e RTT entram na Fase 5."""

    async def record_state(self, chain_code: str, state: ConsumerState, instance_id: str) -> None:
        """Levanta ``UnknownChainError`` se a chain não estiver provisionada."""
        ...

    async def record_disconnect(
        self,
        chain_code: str,
        state: ConsumerState,
        close_code: int | None,
        close_reason: str | None,
    ) -> None:
        """Grava ``last_disconnect_at`` com o horário **do banco**, junto com o estado."""
        ...

    async def disconnect_snapshot(self, chain_code: str) -> DisconnectSnapshot: ...


# =============================================================== token OAuth e lease


@dataclass(frozen=True, slots=True)
class AccessToken:
    value: str = field(repr=False)  # nunca em repr/log
    expires_at: datetime  # UTC
    # Vida total na emissão; viaja com o token pelo cache para todo processo calcular a
    # mesma margem efetiva (ADR-0014). None = desconhecida (margem cheia).
    lifetime_s: float | None = None


class TokenIssuer(Protocol):
    async def issue(self) -> AccessToken:
        """POST /oauth/v1/tokens. Levanta ``AuthRejectedError`` ou ``AuthUnavailableError``."""
        ...


class TokenCache(Protocol):
    """Cache compartilhado entre processos (Redis). Falhas aqui nunca impedem a emissão."""

    async def get(self, key: str) -> AccessToken | None: ...

    async def put(self, key: str, token: AccessToken, ttl_s: float) -> None: ...

    async def delete(self, key: str) -> None: ...


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


# =============================================================== publisher: outbox


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


class MessagePublisher(Protocol):
    async def publish(self, message: OutgoingMessage) -> PublishOutcome:
        """Publica com confirmação do broker. Levanta ``BrokerUnavailableError`` se a conexão
        ou o canal cair."""
        ...


# =============================================================== replay


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


# =============================================================== operações da API


@dataclass(frozen=True, slots=True)
class StoredEvent:
    raw_event_id: int
    event: Event
    status: ProcessingStatus


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


# =============================================================== enricher


class QueueDedup(Protocol):
    """Deduplicação das mensagens da fila pelo ``message_id`` (Redis)."""

    async def is_processed(self, message_id: str) -> bool: ...

    async def mark_processed(self, message_id: str) -> None: ...


class EnrichmentStore(Protocol):
    async def get_event(self, raw_event_id: int) -> StoredEvent | None: ...

    async def apply(
        self, raw_event_id: int, writes: Sequence[DomainWrite], status: ProcessingStatus
    ) -> int:
        """Numa transação: ``MERGE`` condicional de cada escrita (só atualiza se o
        ``source_offset_num`` gravado for <= o novo) e ``processing_status``. Devolve quantas
        escritas foram ignoradas pela condição."""
        ...

    async def add_dlq(self, raw_event_id: int, stage: DlqStage, error: str) -> None: ...


class ResourceFetcher(Protocol):
    async def fetch(
        self, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        """GET do recurso completo na REST do OHIP (com cache e rate limit)."""
        ...


@dataclass(frozen=True, slots=True)
class DomainWrite:
    table: str
    key: Mapping[str, Any]  # chave natural: chain_code, NVL(hotel_id,'#CHAIN'), primary_key
    values: Mapping[str, Any]
    source_event_raw_id: int
    source_offset_num: int


class NormalizationRule(Protocol):
    """Regra de um ou mais ``eventName`` (RF-08). As regras reais vêm após a Q-1 (Fase 9)."""

    @property
    def event_names(self) -> frozenset[str]: ...

    @property
    def needs_resource(self) -> bool: ...

    def build(
        self, event: Event, raw_event_id: int, resource: Mapping[str, Any] | None
    ) -> Sequence[DomainWrite]: ...
