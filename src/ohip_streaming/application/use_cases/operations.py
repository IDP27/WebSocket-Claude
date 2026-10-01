"""Ações administrativas da API: reprocessar evento e reprocessar item de DLQ (ARCHITECTURE §4.4).

Nada é publicado fora da outbox. A API nunca grava evento bruto: o retry de DLQ CONSUME é só
um pedido, executado pelo consumer da chain com barreira de epoch.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from ohip_streaming.application.errors import InvalidOperationError, NotFoundError
from ohip_streaming.application.ports import (
    DlqStage,
    OperationsStore,
    ProcessingStatus,
    StoredEvent,
)
from ohip_streaming.domain.masking import MaskingPolicy
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage, build_queue_message
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class MessagingOptions:
    exchange_names: Mapping[ExchangeKind, str]
    module_codes: Mapping[str, str] = field(default_factory=dict)
    masking: MaskingPolicy = field(default_factory=MaskingPolicy)


def _reprocess_message(options: MessagingOptions, stored: StoredEvent) -> QueueMessage:
    if stored.status is ProcessingStatus.IGNORED:
        # Fora da allowlist (DV-13): não vai ao enricher sem mudar a configuração.
        raise InvalidOperationError(
            f"evento {stored.event.unique_event_id} está IGNORED (fora da allowlist)"
        )
    message, _ = build_queue_message(
        stored.event,
        raw_event_id=stored.raw_event_id,
        masking=options.masking,
        module_codes=options.module_codes,
        kind=ExchangeKind.REPROCESS,
    )
    return message


class ReprocessEvent:
    def __init__(self, *, store: OperationsStore, options: MessagingOptions) -> None:
        self._store = store
        self._options = options

    async def execute(self, unique_event_id: str, requested_by: str) -> int:
        stored = await self._store.find_event(unique_event_id)
        if stored is None:
            raise NotFoundError(f"evento {unique_event_id} não encontrado")
        outbox_id = await self._store.enqueue(
            chain_code=stored.event.chain_code,
            raw_event_id=stored.raw_event_id,
            message=_reprocess_message(self._options, stored),
            exchange_name=self._options.exchange_names[ExchangeKind.REPROCESS],
        )
        log.info(
            "reprocessamento_solicitado",
            unique_event_id=unique_event_id,
            outbox_id=outbox_id,
            requested_by=requested_by,
        )
        return outbox_id


class DlqRetryKind(StrEnum):
    CONSUME_REQUESTED = "CONSUME_REQUESTED"  # o consumer executa
    REPUBLISHED = "REPUBLISHED"  # linha nova na outbox
    REPROCESS_ENQUEUED = "REPROCESS_ENQUEUED"  # mensagem para o enricher


@dataclass(frozen=True, slots=True)
class DlqRetryResult:
    kind: DlqRetryKind
    outbox_id: int | None = None


class RetryDlqItem:
    def __init__(self, *, store: OperationsStore, options: MessagingOptions) -> None:
        self._store = store
        self._options = options

    async def execute(self, item_id: int, requested_by: str) -> DlqRetryResult:
        item = await self._store.get_dlq_item(item_id)
        if item is None:
            raise NotFoundError(f"item de DLQ {item_id} não encontrado")
        if item.resolved:
            raise InvalidOperationError(f"item de DLQ {item_id} já foi resolvido")

        if item.stage is DlqStage.CONSUME:
            if not await self._store.request_consume_retry(item_id, requested_by):
                raise InvalidOperationError(f"item de DLQ {item_id} já tem retry pedido")
            result = DlqRetryResult(DlqRetryKind.CONSUME_REQUESTED)
        elif item.stage is DlqStage.PUBLISH:
            if item.outbox_id is None:
                raise InvalidOperationError(f"item de DLQ {item_id} sem linha de outbox")
            outbox_id = await self._store.retry_publish(item_id, item.outbox_id, requested_by)
            result = DlqRetryResult(DlqRetryKind.REPUBLISHED, self._opened(item_id, outbox_id))
        else:
            if item.event_raw_id is None:
                raise InvalidOperationError(f"item de DLQ {item_id} sem evento bruto")
            stored = await self._store.get_event(item.event_raw_id)
            if stored is None:
                raise NotFoundError(f"evento bruto {item.event_raw_id} não encontrado")
            outbox_id = await self._store.enqueue_and_resolve(
                item_id,
                chain_code=stored.event.chain_code,
                raw_event_id=stored.raw_event_id,
                message=_reprocess_message(self._options, stored),
                exchange_name=self._options.exchange_names[ExchangeKind.REPROCESS],
                requested_by=requested_by,
            )
            result = DlqRetryResult(
                DlqRetryKind.REPROCESS_ENQUEUED, self._opened(item_id, outbox_id)
            )

        log.info("dlq_retry", dlq_id=item_id, stage=item.stage, result=result.kind)
        return result

    @staticmethod
    def _opened(item_id: int, outbox_id: int | None) -> int:
        if outbox_id is None:  # outro pedido resolveu o item entre a leitura e a escrita
            raise InvalidOperationError(f"item de DLQ {item_id} já foi resolvido")
        return outbox_id
