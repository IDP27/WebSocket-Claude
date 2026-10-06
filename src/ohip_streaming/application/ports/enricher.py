"""Enricher: deduplicação, gravação, REST e regras (ADR-0019)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from ohip_streaming.application.ports.common import DlqStage, ProcessingStatus, StoredEvent
from ohip_streaming.domain.events import Event


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

    async def add_dlq(
        self, raw_event_id: int, stage: DlqStage, error_class: str, error: str
    ) -> None:
        """Numa transação: item de DLQ (``NORMALIZE``/``ENRICH``) com chain, uid e offset do
        bruto, e ``processing_status = FAILED`` no bruto (ADR-0019)."""
        ...


class ResourceFetcher(Protocol):
    async def fetch(
        self, chain_code: str, module_name: str, hotel_id: str | None, primary_key: str
    ) -> Mapping[str, Any] | None:
        """GET do recurso completo na REST do OHIP (com cache e rate limit). ``None`` se o
        recurso não existe mais. Levanta ``ResourceUnavailableError`` (transitória) ou
        ``ResourceRejectedError`` (falha da mensagem)."""
        ...


@dataclass(frozen=True, slots=True)
class DomainWrite:
    """Escrita numa tabela de domínio. A tabela precisa de ``UNIQUE`` nas colunas de ``key``
    (com N enrichers em paralelo, é o que impede duas linhas da mesma entidade, ADR-0019 §4).
    Nenhum valor de ``key`` pode ser ``None``: hotel ausente vira ``'#CHAIN'``."""

    table: str
    key: Mapping[str, Any]  # chave natural: chain_code, hotel_id (ou '#CHAIN'), primary_key
    values: Mapping[str, Any]
    source_event_raw_id: int
    source_offset_num: int


class NormalizationRule(Protocol):
    """Regra de um ou mais ``eventName`` (RF-08). As regras reais vêm após a Q-1 (Fase 9).

    Para recusar um evento, ``build`` levanta ``RuleError`` com texto sem dados pessoais (vai
    para log e DLQ). De qualquer outra exceção só o nome da classe é registrado (ADR-0019)."""

    @property
    def event_names(self) -> frozenset[str]: ...

    @property
    def needs_resource(self) -> bool: ...

    def build(
        self, event: Event, raw_event_id: int, resource: Mapping[str, Any] | None
    ) -> Sequence[DomainWrite]: ...
