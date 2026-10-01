"""Caso de uso central do consumer: grava um micro-lote de mensagens ``next`` (ARCHITECTURE §4.1).

Garantias (ADR-0002, ADR-0008, ADR-0009):
- nenhuma mensagem do lote é perdida: vira evento, duplicado ou item de DLQ CONSUME;
- o offset salvo é o da última mensagem com offset válido, e só depois do commit;
- falha do lote inteiro com o banco respondendo → novas tentativas e depois bisseção em ordem
  (prefixo commitado antes do sufixo), isolando a mensagem culpada na DLQ;
- disjuntor: se uma segunda culpada aparecer sem nenhum progresso desde a anterior (evento
  gravado ou reconhecido como duplicado num commit), a falha é sistêmica e sobe
  ``StoreUnavailableError``, em vez de mandar o fluxo inteiro para a DLQ. O contador vive
  enquanto a instância viver (uma por processo consumer) e **não** zera ao disparar: numa
  falha sistêmica longa, cada reconexão dispara de novo sem mandar mais nada para a DLQ.
  Reiniciar o processo zera;
- números de cartão são removidos antes de gravar (payload e DLQ), nunca chegam ao Oracle;
- banco fora ou lease perdido → a exceção sobe (o consumer envenena a conexão ou encerra).

``RetryConsumeDlq`` regrava itens de DLQ CONSUME cujo retry foi pedido pela API.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, tzinfo

from ohip_streaming.application.errors import BatchFailedError, StoreUnavailableError
from ohip_streaming.application.ports import (
    BatchResult,
    BatchToPersist,
    Clock,
    ConsumeDlqRecord,
    ConsumeRetryItem,
    EventStore,
    MetricsSink,
    NewEventRecord,
    ProcessingStatus,
    SeenCache,
)
from ohip_streaming.domain.events import (
    Event,
    RejectedMessage,
    parse_next_frame,
    parse_stored_message,
)
from ohip_streaming.domain.masking import (
    MaskingPolicy,
    scrub_card_numbers_in_event,
    scrub_card_numbers_in_message,
)
from ohip_streaming.domain.messages import build_queue_message
from ohip_streaming.domain.offset import Offset
from ohip_streaming.domain.rules import is_allowed, offset_to_persist
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    raw: str
    received_at: datetime


@dataclass(frozen=True, slots=True)
class ConsumerContext:
    chain_code: str
    subscription_id: str
    lease_epoch: int
    event_tz: tzinfo


@dataclass(frozen=True, slots=True)
class BatchOptions:
    allowlist: frozenset[str] = frozenset()
    module_codes: Mapping[str, str] = field(default_factory=dict)
    masking: MaskingPolicy = field(default_factory=MaskingPolicy)
    max_retries: int = 5
    retry_base_s: float = 0.5
    retry_cap_s: float = 10.0
    # Culpadas isoladas sem progresso entre elas antes de declarar falha sistêmica.
    max_isolations_without_success: int = 1


@dataclass(slots=True)
class BatchSummary:
    received: int = 0
    inserted: int = 0
    duplicates: int = 0
    rejected: int = 0
    ignored: int = 0
    row_failures: int = 0
    card_data_detected: int = 0
    offset: Offset | None = None

    def add(self, other: BatchSummary) -> None:
        self.inserted += other.inserted
        self.duplicates += other.duplicates
        self.rejected += other.rejected
        self.ignored += other.ignored
        self.row_failures += other.row_failures
        self.card_data_detected += other.card_data_detected
        self.offset = other.offset or self.offset


@dataclass(frozen=True, slots=True)
class _Item:
    raw: str  # texto guardado na DLQ se a mensagem for isolada (sem número de cartão)
    parsed: Event | RejectedMessage
    already_seen: bool = False  # duplicado pelo Redis ou repetido no próprio lote


def _scrubbed_item(parsed: Event | RejectedMessage, raw: str) -> tuple[_Item, bool]:
    """Remove números de cartão antes de qualquer gravação (ADR-0011).

    Para eventos válidos, a DLQ guarda o ``newEvent`` limpo (não o frame), o que o
    ``parse_stored_message`` sabe reler.
    """
    if isinstance(parsed, Event):
        event, found = scrub_card_numbers_in_event(parsed)
        return _Item(event.payload_json, event), found
    text, found = scrub_card_numbers_in_message(raw)
    return _Item(
        text,
        RejectedMessage(
            text, parsed.reason, parsed.received_at, parsed.offset, parsed.unique_event_id
        ),
    ), found


def _event_record(options: BatchOptions, raw_event_id: int, event: Event) -> NewEventRecord:
    if not is_allowed(event.event_name, options.allowlist):
        return NewEventRecord(raw_event_id, event, ProcessingStatus.IGNORED, None)
    message, _ = build_queue_message(
        event, raw_event_id=raw_event_id, masking=options.masking, module_codes=options.module_codes
    )
    return NewEventRecord(raw_event_id, event, ProcessingStatus.RECEIVED, message)


class ProcessEventBatch:
    def __init__(
        self,
        *,
        store: EventStore,
        seen: SeenCache,
        clock: Clock,
        metrics: MetricsSink,
        options: BatchOptions,
    ) -> None:
        self._store = store
        self._seen = seen
        self._clock = clock
        self._metrics = metrics
        self._options = options
        self._isolations_without_success = 0

    async def execute(
        self, context: ConsumerContext, messages: Sequence[IncomingMessage]
    ) -> BatchSummary:
        items: list[_Item] = []
        cards = 0
        for message in messages:
            parsed = parse_next_frame(
                message.raw,
                chain_code=context.chain_code,
                subscription_id=context.subscription_id,
                received_at=message.received_at,
                event_tz=context.event_tz,
            )
            item, found = _scrubbed_item(parsed, message.raw)
            if found:
                cards += 1
                log.error(
                    "dado_de_cartao_removido",
                    unique_event_id=item.parsed.unique_event_id,
                    offset=str(item.parsed.offset),
                )
            items.append(item)
        items = await self._mark_already_seen(items)
        summary = await self._persist_with_recovery(context, items, self._options.max_retries)
        summary.received = len(messages)
        summary.card_data_detected = cards
        self._record_metrics(context.chain_code, summary)
        return summary

    # ----------------------------------------------------------------- deduplicação rápida

    async def _mark_already_seen(self, items: list[_Item]) -> list[_Item]:
        unique_ids = [i.parsed.unique_event_id for i in items if isinstance(i.parsed, Event)]
        try:
            seen = await self._seen.filter_seen(unique_ids) if unique_ids else set()
        except Exception:  # Redis é só atalho (ADR-0009)
            log.warning("seen_cache_indisponivel", exc_info=True)
            seen = set()
        marked: list[_Item] = []
        in_batch: set[str] = set()
        for item in items:
            if isinstance(item.parsed, Event):
                uid = item.parsed.unique_event_id
                duplicated = uid in seen or uid in in_batch
                in_batch.add(uid)
                marked.append(_Item(item.raw, item.parsed, already_seen=duplicated))
            else:
                marked.append(item)
        return marked

    # ----------------------------------------------------------------- gravação com recuperação

    async def _persist_with_recovery(
        self, context: ConsumerContext, items: list[_Item], retries: int
    ) -> BatchSummary:
        if not items:
            return BatchSummary()
        attempt = 0
        while True:
            try:
                return await self._persist_once(context, items)
            except BatchFailedError as exc:
                if attempt >= retries:
                    last_error = exc
                    break
                delay = min(self._options.retry_cap_s, self._options.retry_base_s * 2.0**attempt)
                attempt += 1
                log.warning("lote_falhou_nova_tentativa", attempt=attempt, size=len(items))
                await self._clock.sleep(delay)

        if len(items) > 1:
            # Bisseção em ordem: o prefixo é commitado antes do sufixo (ADR-0009).
            middle = len(items) // 2
            log.warning("lote_em_bissecao", size=len(items))
            summary = await self._persist_with_recovery(context, items[:middle], retries=1)
            summary.add(await self._persist_with_recovery(context, items[middle:], retries=1))
            return summary

        # Uma única mensagem que o banco recusa sempre: vai para a DLQ CONSUME e o fluxo segue,
        # a não ser que já tenha havido outra culpada sem progresso no meio (sistêmico).
        if self._isolations_without_success >= self._options.max_isolations_without_success:
            log.error("falha_sistemica_no_lote", reason=str(last_error))
            raise StoreUnavailableError(f"falha sistêmica ao gravar eventos: {last_error}")
        self._isolations_without_success += 1
        (item,) = items
        log.error("mensagem_isolada_na_dlq", reason=str(last_error))
        culprit = RejectedMessage(
            raw=item.raw,
            reason=f"falha ao gravar: {last_error}",
            received_at=item.parsed.received_at,
            offset=item.parsed.offset,
            unique_event_id=item.parsed.unique_event_id,
        )
        return await self._persist_once(context, [_Item(item.raw, culprit)])

    async def _persist_once(self, context: ConsumerContext, items: list[_Item]) -> BatchSummary:
        options = self._options
        fresh = [i.parsed for i in items if isinstance(i.parsed, Event) and not i.already_seen]
        raw_ids = await self._store.reserve_event_ids(len(fresh)) if fresh else []

        summary = BatchSummary()
        records = [
            _event_record(options, raw_event_id, event)
            for raw_event_id, event in zip(raw_ids, fresh, strict=True)
        ]

        rejected = tuple(
            ConsumeDlqRecord(
                raw_message=i.raw,
                reason=i.parsed.reason,
                offset=i.parsed.offset,
                unique_event_id=i.parsed.unique_event_id,
            )
            for i in items
            if isinstance(i.parsed, RejectedMessage)
        )
        parsed = [i.parsed for i in items]
        offset = offset_to_persist(parsed)
        # uniqueEventId da mesma mensagem cujo offset é salvo (a última com offset válido).
        last_uid = next((p.unique_event_id for p in reversed(parsed) if p.offset is not None), None)
        result = await self._store.persist_batch(
            BatchToPersist(
                chain_code=context.chain_code,
                lease_epoch=context.lease_epoch,
                events=tuple(records),
                rejected=rejected,
                offset=offset,
                last_unique_event_id=last_uid,
            )
        )
        await self._mark_seen_after_commit(result)
        if result.inserted or result.duplicates or any(i.already_seen for i in items):
            # Progresso: o banco aceitou eventos neste commit (novos ou já gravados, como no
            # replay). Uma culpada isolada sozinha não conta.
            self._isolations_without_success = 0

        summary.inserted = len(result.inserted)
        summary.duplicates = len(result.duplicates) + sum(1 for i in items if i.already_seen)
        summary.rejected = len(rejected)
        summary.row_failures = len(result.row_failures)
        summary.ignored = sum(
            1
            for r in records
            if r.status is ProcessingStatus.IGNORED and r.event.unique_event_id in result.inserted
        )
        summary.offset = offset
        for failure in result.row_failures:
            log.error(
                "linha_recusada_pelo_banco",
                unique_event_id=failure.unique_event_id,
                error_class=failure.error_class,
            )
        return summary

    async def _mark_seen_after_commit(self, result: BatchResult) -> None:
        await _mark_seen(self._seen, result)

    def _record_metrics(self, chain_code: str, summary: BatchSummary) -> None:
        for name, value in (
            ("ohip_events_received_total", summary.received),
            ("ohip_events_inserted_total", summary.inserted),
            ("ohip_duplicates_total", summary.duplicates),
            ("ohip_events_ignored_total", summary.ignored),
            ("ohip_dlq_consume_total", summary.rejected + summary.row_failures),
            ("ohip_card_data_detected_total", summary.card_data_detected),
        ):
            if value:
                self._metrics.increment(name, value, chain_code=chain_code)


async def _mark_seen(seen: SeenCache, result: BatchResult) -> None:
    ids = [*result.inserted, *result.duplicates]
    if not ids:
        return
    try:
        await seen.mark_seen(ids)
    except Exception:  # só atalho; o UNIQUE do Oracle garante
        log.warning("seen_cache_indisponivel", exc_info=True)


class RetryConsumeDlq:
    """Regrava itens de DLQ CONSUME com retry pedido pela API (ARCHITECTURE §4.4).

    Roda no consumer da chain (dono do lease). Não mexe no offset. Cada item é uma transação:
    gravado ou já existente → item resolvido; ainda inválido ou recusado pelo banco → item
    aberto com o motivo novo (e ``attempts`` + 1), sem interromper os demais pedidos. Só
    ``StoreUnavailableError`` e ``LeaseLostError`` sobem.
    """

    def __init__(
        self,
        *,
        store: EventStore,
        seen: SeenCache,
        clock: Clock,
        options: BatchOptions,
    ) -> None:
        self._store = store
        self._seen = seen
        self._clock = clock
        self._options = options

    async def execute(self, context: ConsumerContext, limit: int = 20) -> int:
        """Processa até ``limit`` pedidos. Devolve quantos foram resolvidos."""
        resolved = 0
        for item in await self._store.pending_consume_retries(context.chain_code, limit):
            if await self._retry(context, item):
                resolved += 1
        return resolved

    async def _retry(self, context: ConsumerContext, item: ConsumeRetryItem) -> bool:
        parsed = parse_stored_message(
            item.raw_message,
            chain_code=context.chain_code,
            received_at=self._clock.now(),
            event_tz=context.event_tz,
        )
        if isinstance(parsed, RejectedMessage):
            await self._failed(context, item, parsed.reason)
            return False
        event, _ = scrub_card_numbers_in_event(parsed)
        (raw_event_id,) = await self._store.reserve_event_ids(1)
        try:
            result = await self._store.persist_batch(
                BatchToPersist(
                    chain_code=context.chain_code,
                    lease_epoch=context.lease_epoch,
                    events=(_event_record(self._options, raw_event_id, event),),
                    rejected=(),
                    offset=None,
                    last_unique_event_id=None,
                    retry_of_dlq_id=item.dlq_id,
                )
            )
        except BatchFailedError as exc:  # recusado de novo; os outros pedidos seguem
            await self._failed(context, item, f"falha ao gravar: {exc}")
            return False
        await _mark_seen(self._seen, result)
        ok = bool(result.inserted or result.duplicates)
        log.info("retry_dlq_consume", dlq_id=item.dlq_id, resolvido=ok)
        return ok

    async def _failed(self, context: ConsumerContext, item: ConsumeRetryItem, reason: str) -> None:
        await self._store.mark_consume_retry_failed(item.dlq_id, reason, context.lease_epoch)
        log.warning("retry_dlq_falhou", dlq_id=item.dlq_id, reason=reason)
