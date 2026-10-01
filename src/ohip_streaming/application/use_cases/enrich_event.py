"""Normalização e enriquecimento de um evento vindo da fila (ARCHITECTURE §4.3, RF-08, RF-09).

As regras por ``eventName`` só serão escritas após a Q-1 (Fase 9); aqui ficam o registro de
regras e o fluxo. O payload sempre vem do Oracle (a mensagem da fila tem ``detail`` mascarado).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ohip_streaming.application.ports import (
    EnrichmentStore,
    MetricsSink,
    NormalizationRule,
    ProcessingStatus,
    QueueDedup,
    ResourceFetcher,
)
from ohip_streaming.domain.identifiers import normalize_event_name
from ohip_streaming.logging import get_logger

log = get_logger(__name__)


class RuleRegistry:
    def __init__(self, rules: Iterable[NormalizationRule]) -> None:
        self._by_event: dict[str, NormalizationRule] = {}
        for rule in rules:
            for name in rule.event_names:
                key = normalize_event_name(name)
                if key in self._by_event:
                    raise ValueError(f"eventName com duas regras: {key}")
                self._by_event[key] = rule

    def get(self, event_name: str) -> NormalizationRule | None:
        return self._by_event.get(normalize_event_name(event_name))


@dataclass(frozen=True, slots=True)
class InboundMessage:
    message_id: str
    body: Mapping[str, Any]
    is_reprocess: bool  # veio do exchange ohip.reprocess: nunca deduplicar


class EnrichOutcome(StrEnum):
    DUPLICATE = "DUPLICATE"
    NOT_FOUND = "NOT_FOUND"
    UNMAPPED = "UNMAPPED"
    NORMALIZED = "NORMALIZED"
    ENRICHED = "ENRICHED"


class EnrichEvent:
    def __init__(
        self,
        *,
        store: EnrichmentStore,
        dedup: QueueDedup,
        fetcher: ResourceFetcher,
        rules: RuleRegistry,
        metrics: MetricsSink,
    ) -> None:
        self._store = store
        self._dedup = dedup
        self._fetcher = fetcher
        self._rules = rules
        self._metrics = metrics

    async def execute(self, message: InboundMessage) -> EnrichOutcome:
        if not message.is_reprocess and await self._dedup.is_processed(message.message_id):
            return EnrichOutcome.DUPLICATE

        raw_event_id = message.body.get("raw_event_id")
        if isinstance(raw_event_id, bool) or not isinstance(raw_event_id, int):
            log.error("mensagem_sem_raw_event_id", message_id=message.message_id)
            return EnrichOutcome.NOT_FOUND
        stored = await self._store.get_event(raw_event_id)
        if stored is None:
            log.error("evento_bruto_nao_encontrado", raw_event_id=raw_event_id)
            return EnrichOutcome.NOT_FOUND

        event = stored.event
        rule = self._rules.get(event.event_name)
        if rule is None:
            await self._store.apply(raw_event_id, (), ProcessingStatus.UNMAPPED)
            self._metrics.increment("ohip_events_unmapped_total", event_name=event.event_name)
            await self._dedup.mark_processed(message.message_id)
            return EnrichOutcome.UNMAPPED

        resource = (
            await self._fetcher.fetch(event.module_name, event.hotel_id, event.primary_key)
            if rule.needs_resource
            else None
        )
        writes = rule.build(event, raw_event_id, resource)
        outcome = EnrichOutcome.ENRICHED if rule.needs_resource else EnrichOutcome.NORMALIZED
        skipped = await self._store.apply(raw_event_id, writes, ProcessingStatus(outcome.value))
        if skipped:
            # Estado mais novo já gravado. Muitos seguidos: offsets reiniciados? (D-6)
            self._metrics.increment(
                "ohip_merge_skipped_total", skipped, event_name=event.event_name
            )
        await self._dedup.mark_processed(message.message_id)  # só depois do sucesso
        return outcome
