"""Tratamento das mensagens do enricher: tentativas, DLQ e falhas de infraestrutura (ADR-0019)."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, frame
from tests.fakes.memory import (
    FakeClock,
    FakeFetcher,
    FakeMetrics,
    FakeQueueDedup,
    FakeSeenCache,
    InMemoryDatabase,
)

from ohip_streaming.application.errors import (
    OMITTED_DETAIL,
    ResourceRejectedError,
    ResourceUnavailableError,
    RuleError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import DlqStage, DomainWrite, ProcessingStatus
from ohip_streaming.application.use_cases.enrich_event import (
    EnrichEvent,
    InboundMessage,
    RuleRegistry,
)
from ohip_streaming.application.use_cases.enricher_service import (
    Disposition,
    EnricherOptions,
    EnricherService,
)
from ohip_streaming.application.use_cases.process_event_batch import (
    BatchOptions,
    ConsumerContext,
    IncomingMessage,
    ProcessEventBatch,
)
from ohip_streaming.domain.events import Event

CHAIN = "CHAIN1"


class Rule:
    def __init__(self, *, needs_resource: bool = False, fail: Exception | None = None) -> None:
        self._needs_resource = needs_resource
        self.fail = fail

    @property
    def event_names(self) -> frozenset[str]:
        return frozenset({"UPDATE RESERVATION"})

    @property
    def needs_resource(self) -> bool:
        return self._needs_resource

    def build(
        self, event: Event, raw_event_id: int, resource: Mapping[str, Any] | None
    ) -> Sequence[DomainWrite]:
        if self.fail is not None:
            raise self.fail
        return [
            DomainWrite(
                table="T_RESERVA",
                key={"chain_code": event.chain_code, "primary_key": event.primary_key},
                values={"status": (resource or {}).get("status", "?")},
                source_event_raw_id=raw_event_id,
                source_offset_num=event.offset.numeric(),
            )
        ]


class BrokenDedup(FakeQueueDedup):
    async def is_processed(self, message_id: str) -> bool:
        raise ConnectionError("redis fora")

    async def mark_processed(self, message_id: str) -> None:
        raise ConnectionError("redis fora")


async def seeded() -> tuple[InMemoryDatabase, int]:
    db = InMemoryDatabase(clock=FakeClock())
    db.provision_chain(CHAIN)
    epoch = db.acquire(f"consumer:{CHAIN}")
    await ProcessEventBatch(
        store=db,
        seen=FakeSeenCache(),
        clock=db.clock,
        metrics=FakeMetrics(),
        options=BatchOptions(),
    ).execute(
        ConsumerContext(CHAIN, SUBSCRIPTION_ID, epoch, ZoneInfo("UTC")),
        [IncomingMessage(frame("100"), db.clock.now())],
    )
    return db, next(iter(db.raw))


def service(
    db: InMemoryDatabase,
    rules: Sequence[Any] = (),
    *,
    fetcher: FakeFetcher | None = None,
    dedup: FakeQueueDedup | None = None,
    **options: Any,
) -> tuple[EnricherService, FakeMetrics, FakeQueueDedup]:
    metrics, dedup = FakeMetrics(), dedup or FakeQueueDedup()
    enrich = EnrichEvent(
        store=db,
        dedup=dedup,
        fetcher=fetcher or FakeFetcher({"status": "RESERVED"}),
        rules=RuleRegistry(rules),
        metrics=metrics,
    )
    svc = EnricherService(
        enrich=enrich, store=db, clock=db.clock, metrics=metrics, options=EnricherOptions(**options)
    )
    return svc, metrics, dedup


def message(raw_id: int, uid: str = "uid-100") -> InboundMessage:
    return InboundMessage(uid, {"raw_event_id": raw_id}, is_reprocess=False)


async def test_without_rules_every_event_is_unmapped() -> None:
    db, raw_id = await seeded()
    svc, metrics, dedup = service(db)
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    assert db.raw[raw_id].status is ProcessingStatus.UNMAPPED
    assert metrics.total("ohip_events_unmapped_total") == 1
    assert dedup.processed == {"uid-100"}


async def test_message_failure_is_retried_then_goes_to_dlq() -> None:
    db, raw_id = await seeded()
    svc, metrics, dedup = service(db, [Rule(fail=RuleError("campo novo"))], retry_delay_s=2)
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    (item,) = db.dlq.values()
    assert (item.stage, item.event_raw_id, item.chain_code) == (DlqStage.NORMALIZE, raw_id, CHAIN)
    assert item.error == "RuleError: campo novo"
    assert db.raw[raw_id].status is ProcessingStatus.FAILED
    assert db.clock.sleeps == [2, 2]  # 3 tentativas
    assert dedup.processed == {"uid-100"}
    assert metrics.total("ohip_enricher_dlq_total") == 1


async def test_rest_rejection_goes_to_enrich_stage() -> None:
    db, raw_id = await seeded()
    fetcher = FakeFetcher()
    fetcher.errors = [ResourceRejectedError("HTTP 403")] * 3
    svc, _, _ = service(db, [Rule(needs_resource=True)], fetcher=fetcher, max_attempts=3)
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    (item,) = db.dlq.values()
    assert item.stage is DlqStage.ENRICH


async def test_success_on_second_attempt() -> None:
    db, raw_id = await seeded()
    db.fail_apply = [StoreOperationError("ORA-00001 corrida do MERGE")]
    svc, _, _ = service(db, [Rule()])
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    assert db.raw[raw_id].status is ProcessingStatus.NORMALIZED
    assert db.dlq == {}


@pytest.mark.parametrize(
    "error", [StoreUnavailableError("ORA-03113"), ResourceUnavailableError("REST fora")]
)
async def test_infrastructure_failure_requeues_with_backoff(error: Exception) -> None:
    db, raw_id = await seeded()
    db.fail_apply = [error, error, error]
    svc, metrics, dedup = service(
        db, [Rule()], transient_backoff_initial_s=5, transient_backoff_max_s=12
    )
    results = [await svc.handle(message(raw_id)) for _ in range(3)]
    assert results == [Disposition.REQUEUE] * 3
    assert db.clock.sleeps == [5, 10, 12]  # exponencial com teto; nenhuma tentativa gasta
    assert db.dlq == {}
    assert dedup.processed == set()
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    assert metrics.total("ohip_enricher_dependency_unavailable_total") == 3


async def test_retry_after_is_respected() -> None:
    db, raw_id = await seeded()
    fetcher = FakeFetcher()
    fetcher.errors = [ResourceUnavailableError("429", retry_after_s=90)]
    svc, _, _ = service(db, [Rule(needs_resource=True)], fetcher=fetcher)
    assert await svc.handle(message(raw_id)) is Disposition.REQUEUE
    assert db.clock.sleeps == [90]


async def test_dlq_write_failures_never_lose_the_message() -> None:
    db, raw_id = await seeded()
    svc, _, dedup = service(db, [Rule(fail=ValueError("x"))], max_attempts=1)
    db.fail_add_dlq = [StoreUnavailableError("fora"), RuntimeError("bug")]
    assert await svc.handle(message(raw_id)) is Disposition.REQUEUE
    assert await svc.handle(message(raw_id)) is Disposition.REQUEUE
    assert dedup.processed == set()
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    assert len(db.dlq) == 1


async def test_purged_raw_event_is_acked() -> None:
    db, raw_id = await seeded()
    svc, _, _ = service(db, [Rule(fail=ValueError("x"))], max_attempts=1)
    original = db.add_dlq

    async def gone(*args: Any) -> None:
        del db.raw[raw_id]
        await original(*args)

    db.add_dlq = gone  # type: ignore[method-assign,assignment]
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    assert db.dlq == {}


async def test_stop_during_retry_requeues() -> None:
    db, raw_id = await seeded()
    svc, _, _ = service(db, [Rule(fail=ValueError("x"))], retry_delay_s=30)
    svc.request_stop()
    assert await svc.handle(message(raw_id)) is Disposition.REQUEUE
    assert db.dlq == {}


async def test_redis_failure_does_not_fail_the_message() -> None:
    db, raw_id = await seeded()
    svc, _, _ = service(db, [Rule()], dedup=BrokenDedup())
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    assert db.raw[raw_id].status is ProcessingStatus.NORMALIZED


async def test_sleep_is_interrupted_by_stop() -> None:
    db, raw_id = await seeded()
    db.fail_apply = [StoreUnavailableError("fora")]

    class SlowClock(FakeClock):
        async def sleep(self, seconds: float) -> None:
            await asyncio.sleep(10)

    svc, _, _ = service(db, [Rule()])
    svc._clock = SlowClock()
    task = asyncio.create_task(svc.handle(message(raw_id)))
    await asyncio.sleep(0.01)
    svc.request_stop()
    assert await asyncio.wait_for(task, 1) is Disposition.REQUEUE


# ------------------------------------------------------------ dados pessoais fora de log e DLQ


async def test_foreign_exception_text_never_reaches_log_or_dlq(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db, raw_id = await seeded()
    leak = KeyError("maria.silva@exemplo.com")  # regra com bug, com o valor do evento no texto
    svc, _, _ = service(db, [Rule(fail=leak)], max_attempts=2)
    assert await svc.handle(message(raw_id)) is Disposition.ACK
    (item,) = db.dlq.values()
    assert item.error == f"KeyError: {OMITTED_DETAIL}"
    assert "maria" not in capsys.readouterr().out


# ------------------------------------------------------------ disjuntor (falha sistêmica)


async def test_repeated_rest_403_opens_the_breaker_instead_of_draining_to_dlq() -> None:
    db, raw_id = await seeded()
    fetcher = FakeFetcher({"status": "RESERVED"})
    rejected = ResourceRejectedError("REST do OHIP recusou o pedido: HTTP 403")
    fetcher.errors = [rejected] * 7  # app sem assinatura: toda mensagem falha igual
    svc, metrics, _ = service(
        db,
        [Rule(needs_resource=True)],
        fetcher=fetcher,
        max_attempts=1,
        systemic_failure_threshold=3,
        transient_backoff_initial_s=5,
        transient_backoff_max_s=60,
    )
    results = [await svc.handle(message(raw_id, f"uid-{n}")) for n in range(5)]

    assert results == [Disposition.ACK] * 2 + [Disposition.REQUEUE] * 3
    assert len(db.dlq) == 2  # só as anteriores ao limite (retry pela API)
    assert db.clock.sleeps == [5, 10, 20]  # backoff de infraestrutura, nada gira em laço
    assert metrics.total("ohip_enricher_systemic_failures_total") == 3


async def test_success_closes_the_breaker() -> None:
    db, raw_id = await seeded()
    rule = Rule(fail=StoreOperationError("ORA-00942: tabela ou view inexistente"))
    svc, _, _ = service(db, [rule], max_attempts=1, systemic_failure_threshold=2)
    assert await svc.handle(message(raw_id, "a")) is Disposition.ACK  # 1ª: DLQ
    assert await svc.handle(message(raw_id, "b")) is Disposition.REQUEUE  # aberto
    rule.fail = None  # o DBA deu o grant
    assert await svc.handle(message(raw_id, "b")) is Disposition.ACK
    rule.fail = StoreOperationError("ORA-00942")
    assert await svc.handle(message(raw_id, "c")) is Disposition.ACK  # contagem recomeçou
    assert len(db.dlq) == 2


async def test_different_failures_do_not_add_up() -> None:
    db, raw_id = await seeded()
    rule = Rule(fail=RuleError("campo ausente"))
    svc, _, _ = service(db, [rule], max_attempts=1, systemic_failure_threshold=2)
    assert await svc.handle(message(raw_id, "a")) is Disposition.ACK
    rule.fail = StoreOperationError("ORA-01400")
    assert await svc.handle(message(raw_id, "b")) is Disposition.ACK
    assert len(db.dlq) == 2


async def test_duplicates_do_not_close_the_breaker() -> None:
    db, raw_id = await seeded()
    dedup = FakeQueueDedup()
    dedup.processed.add("ja-feito")
    rule = Rule(fail=RuleError("bug"))
    svc, _, _ = service(db, [rule], dedup=dedup, max_attempts=1, systemic_failure_threshold=2)
    assert await svc.handle(message(raw_id, "a")) is Disposition.ACK
    assert await svc.handle(message(raw_id, "ja-feito")) is Disposition.ACK  # duplicado
    assert await svc.handle(message(raw_id, "b")) is Disposition.REQUEUE


# ------------------------------------------------------------ erro inesperado (bug nosso)


async def test_unexpected_error_requeues_with_backoff_instead_of_crashing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    db, raw_id = await seeded()
    svc, metrics, _ = service(
        db, [Rule()], transient_backoff_initial_s=5, transient_backoff_max_s=60
    )

    async def bug(_: InboundMessage) -> Any:
        raise RuntimeError("valor do evento: 4111-1111")

    svc._enrich.execute = bug  # type: ignore[method-assign,assignment]
    # Subir derrubaria o processo e a mesma mensagem voltaria ao reiniciar (laço de reinício).
    assert await svc.handle(message(raw_id)) is Disposition.REQUEUE
    assert await svc.handle(message(raw_id)) is Disposition.REQUEUE
    assert db.clock.sleeps == [5, 10]
    assert metrics.total("ohip_enricher_unexpected_errors_total") == 2
    out = capsys.readouterr().out
    assert "enricher_erro_inesperado" in out
    assert "4111" not in out  # nem o texto nem o traceback
