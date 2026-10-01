"""Replay, reprocessamento, retry de DLQ e enriquecimento."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, frame, new_event
from tests.fakes.memory import (
    EXCHANGES,
    T0,
    DlqRecord,
    FakeClock,
    FakeFetcher,
    FakeMetrics,
    FakeQueueDedup,
    FakeSeenCache,
    InMemoryDatabase,
)

from ohip_streaming.application.errors import (
    BatchFailedError,
    InvalidOperationError,
    LeaseLostError,
    NotFoundError,
    ReplayAlreadyPendingError,
    ReplayNotPendingError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    BatchToPersist,
    DlqItem,
    DlqStage,
    DomainWrite,
    ProcessingStatus,
    ReplayStatus,
)
from ohip_streaming.application.use_cases.enrich_event import (
    EnrichEvent,
    EnrichOutcome,
    InboundMessage,
    RuleRegistry,
)
from ohip_streaming.application.use_cases.operations import (
    DlqRetryKind,
    MessagingOptions,
    ReprocessEvent,
    RetryDlqItem,
)
from ohip_streaming.application.use_cases.process_event_batch import (
    BatchOptions,
    ConsumerContext,
    IncomingMessage,
    ProcessEventBatch,
    RetryConsumeDlq,
)
from ohip_streaming.application.use_cases.replay import (
    ApplyReplay,
    CancelReplay,
    ReplayCommand,
    RequestReplay,
)
from ohip_streaming.domain.errors import ReplayForwardNotAllowedError
from ohip_streaming.domain.events import Event
from ohip_streaming.domain.offset import Offset

CHAIN = "CHAIN1"


async def seeded_db(*offsets: int) -> InMemoryDatabase:
    """Banco com eventos já gravados pelo consumer (caminho real, não inserção manual)."""
    clock = FakeClock()
    db = InMemoryDatabase(clock=clock)
    db.provision_chain(CHAIN)
    epoch = db.acquire(f"consumer:{CHAIN}")
    use_case = ProcessEventBatch(
        store=db, seen=FakeSeenCache(), clock=clock, metrics=FakeMetrics(), options=BatchOptions()
    )
    messages = [IncomingMessage(frame(str(n)), T0) for n in offsets]
    await use_case.execute(
        ConsumerContext(CHAIN, SUBSCRIPTION_ID, epoch, ZoneInfo("UTC")), messages
    )
    return db


def command(from_offset: str = "90", **overrides: str) -> ReplayCommand:
    args = {
        "chain_code": CHAIN,
        "from_offset": from_offset,
        "reason": "lacuna INC-1",
        "confirm": CHAIN,
        "requested_by": "ana",
    }
    args.update(overrides)
    return ReplayCommand(**args)


# ------------------------------------------------------------------ replay


async def test_request_replay_creates_pending_request_without_warning() -> None:
    db = await seeded_db(90, 100)
    result = await RequestReplay(store=db, clock=db.clock).execute(command())

    assert result.request.status is ReplayStatus.PENDING
    assert result.request.from_offset == Offset("90")
    assert result.warnings == ()


async def test_request_replay_warns_when_offset_is_unknown_or_too_old() -> None:
    db = await seeded_db(90, 100)
    unknown = await RequestReplay(store=db, clock=db.clock).execute(command("50"))
    assert unknown.warnings

    db2 = await seeded_db(90, 100)
    db2.clock.advance(8 * 24 * 3600)
    old = await RequestReplay(store=db2, clock=db2.clock).execute(command())
    assert "7 dias" in old.warnings[0]


async def test_request_replay_rejections() -> None:
    db = await seeded_db(100)
    use_case = RequestReplay(store=db, clock=db.clock)

    with pytest.raises(ReplayForwardNotAllowedError):
        await use_case.execute(command("101"))
    with pytest.raises(UnknownChainError):
        await use_case.execute(command(chain_code="OUTRA", confirm="OUTRA"))
    await use_case.execute(command("100"))
    with pytest.raises(ReplayAlreadyPendingError):
        await use_case.execute(command("100"))


async def test_apply_replay_moves_offset_back_and_marks_applied() -> None:
    db = await seeded_db(90, 100)
    request = (await RequestReplay(store=db, clock=db.clock).execute(command())).request
    apply = ApplyReplay(store=db)

    pending = await apply.pending(CHAIN)
    assert pending == request
    assert await apply.execute(request, db.leases[f"consumer:{CHAIN}"])
    assert db.offsets[CHAIN].last_offset == Offset("90")
    assert db.replays[request.id].status is ReplayStatus.APPLIED
    assert await apply.pending(CHAIN) is None


async def test_apply_replay_after_cancel_does_nothing() -> None:
    db = await seeded_db(90, 100)
    request = (await RequestReplay(store=db, clock=db.clock).execute(command())).request
    await CancelReplay(store=db).execute(request.id, "ana")

    assert not await ApplyReplay(store=db).execute(request, db.leases[f"consumer:{CHAIN}"])
    assert db.offsets[CHAIN].last_offset == Offset("100")
    with pytest.raises(ReplayNotPendingError):
        await CancelReplay(store=db).execute(request.id, "ana")


async def test_apply_replay_requires_the_lease() -> None:
    db = await seeded_db(90, 100)
    request = (await RequestReplay(store=db, clock=db.clock).execute(command())).request
    stale_epoch = db.leases[f"consumer:{CHAIN}"]
    db.acquire(f"consumer:{CHAIN}")
    with pytest.raises(LeaseLostError):
        await ApplyReplay(store=db).execute(request, stale_epoch)


# ------------------------------------------------------------------ operações da API

OPTIONS = MessagingOptions(exchange_names=EXCHANGES, module_codes={"reservation": "rsv"})


async def test_reprocess_event_enqueues_message_for_reprocess_exchange() -> None:
    db = await seeded_db(1)
    outbox_id = await ReprocessEvent(store=db, options=OPTIONS).execute("uid-1", "ana")

    row = db.outbox[outbox_id]
    assert row.exchange_name == "ohip.reprocess"
    assert row.routing_key == "ohip.rsv.UPDATE_RESERVATION"
    assert row.status == "PENDING"
    with pytest.raises(NotFoundError):
        await ReprocessEvent(store=db, options=OPTIONS).execute("nao-existe", "ana")


def _dlq(db: InMemoryDatabase, stage: DlqStage, **fields: Any) -> int:
    item_id = db._next_id("dlq")
    db.dlq[item_id] = DlqRecord(id=item_id, stage=stage, chain_code=CHAIN, error="e", **fields)
    return item_id


async def test_retry_consume_only_requests_the_consumer() -> None:
    db = await seeded_db(1)
    item = _dlq(db, DlqStage.CONSUME, raw_message="{}")
    result = await RetryDlqItem(store=db, options=OPTIONS).execute(item, "ana")

    assert result.kind is DlqRetryKind.CONSUME_REQUESTED
    assert db.dlq[item].retry_requested_by == "ana"
    assert db.dlq[item].resolution is None  # o consumer resolve depois de gravar


async def test_retry_publish_creates_new_row_at_the_tail() -> None:
    db = await seeded_db(1, 2)
    first_outbox = min(db.outbox)
    db.outbox[first_outbox].status = "FAILED"
    item = _dlq(db, DlqStage.PUBLISH, outbox_id=first_outbox)

    result = await RetryDlqItem(store=db, options=OPTIONS).execute(item, "ana")

    assert result.kind is DlqRetryKind.REPUBLISHED
    assert result.outbox_id is not None
    assert result.outbox_id > max(i for i in db.outbox if i != result.outbox_id)
    assert db.outbox[first_outbox].status == "FAILED"
    assert db.outbox[result.outbox_id].status == "PENDING"
    assert db.dlq[item].resolution == "RETRIED"


@pytest.mark.parametrize("stage", [DlqStage.NORMALIZE, DlqStage.ENRICH])
async def test_retry_enrichment_stages_enqueue_reprocess(stage: DlqStage) -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    item = _dlq(db, stage, event_raw_id=raw_id)

    result = await RetryDlqItem(store=db, options=OPTIONS).execute(item, "ana")

    assert result.kind is DlqRetryKind.REPROCESS_ENQUEUED
    assert result.outbox_id is not None
    assert db.outbox[result.outbox_id].exchange_name == "ohip.reprocess"
    assert db.dlq[item].resolution == "RETRIED"


async def test_retry_dlq_errors() -> None:
    db = await seeded_db(1)
    use_case = RetryDlqItem(store=db, options=OPTIONS)
    with pytest.raises(NotFoundError):
        await use_case.execute(999, "ana")

    resolved = _dlq(db, DlqStage.CONSUME, resolution="DISCARDED")
    with pytest.raises(InvalidOperationError):
        await use_case.execute(resolved, "ana")

    without_outbox = _dlq(db, DlqStage.PUBLISH)
    with pytest.raises(InvalidOperationError):
        await use_case.execute(without_outbox, "ana")

    without_raw = _dlq(db, DlqStage.ENRICH)
    with pytest.raises(InvalidOperationError):
        await use_case.execute(without_raw, "ana")

    missing_raw = _dlq(db, DlqStage.ENRICH, event_raw_id=999)
    with pytest.raises(NotFoundError):
        await use_case.execute(missing_raw, "ana")


async def test_reprocess_of_ignored_event_is_refused() -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    db.raw[raw_id].status = ProcessingStatus.IGNORED
    with pytest.raises(InvalidOperationError, match="IGNORED"):
        await ReprocessEvent(store=db, options=OPTIONS).execute("uid-1", "ana")
    item = _dlq(db, DlqStage.ENRICH, event_raw_id=raw_id)
    with pytest.raises(InvalidOperationError, match="IGNORED"):
        await RetryDlqItem(store=db, options=OPTIONS).execute(item, "ana")
    assert db.dlq[item].resolution is None


async def test_retry_consume_twice_is_refused() -> None:
    db = await seeded_db(1)
    item = _dlq(db, DlqStage.CONSUME, raw_message="{}")
    use_case = RetryDlqItem(store=db, options=OPTIONS)
    await use_case.execute(item, "ana")
    with pytest.raises(InvalidOperationError, match="retry pedido"):
        await use_case.execute(item, "bia")
    assert db.dlq[item].retry_requested_by == "ana"


async def test_double_retry_publish_creates_a_single_outbox_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = await seeded_db(1)
    first_outbox = min(db.outbox)
    db.outbox[first_outbox].status = "FAILED"
    item = _dlq(db, DlqStage.PUBLISH, outbox_id=first_outbox)
    stale = await db.get_dlq_item(item)
    use_case = RetryDlqItem(store=db, options=OPTIONS)

    await use_case.execute(item, "ana")
    rows = len(db.outbox)
    with pytest.raises(InvalidOperationError, match="já foi resolvido"):
        await use_case.execute(item, "ana")

    # Corrida: dois cliques leram o item aberto antes de qualquer escrita.
    async def stale_read(_item_id: int) -> DlqItem | None:
        return stale

    monkeypatch.setattr(db, "get_dlq_item", stale_read)
    with pytest.raises(InvalidOperationError, match="já foi resolvido"):
        await use_case.execute(item, "bia")
    assert len(db.outbox) == rows
    assert db.dlq[item].resolved_by == "ana"


async def test_double_retry_of_enrichment_item_enqueues_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = await seeded_db(1)
    item = _dlq(db, DlqStage.ENRICH, event_raw_id=next(iter(db.raw)))
    stale = await db.get_dlq_item(item)
    use_case = RetryDlqItem(store=db, options=OPTIONS)
    await use_case.execute(item, "ana")
    rows = len(db.outbox)

    async def stale_read(_item_id: int) -> DlqItem | None:
        return stale

    monkeypatch.setattr(db, "get_dlq_item", stale_read)
    with pytest.raises(InvalidOperationError):
        await use_case.execute(item, "bia")
    assert len(db.outbox) == rows


# ------------------------------------------------------------- retry de DLQ CONSUME no consumer


class ConsumerHarness:
    """Consumer real (ProcessEventBatch) + API (RetryDlqItem) + RetryConsumeDlq."""

    def __init__(self) -> None:
        self.clock = FakeClock()
        self.db = InMemoryDatabase(clock=self.clock)
        self.db.provision_chain(CHAIN)
        self.epoch = self.db.acquire(f"consumer:{CHAIN}")
        self.seen = FakeSeenCache()
        self.batch = ProcessEventBatch(
            store=self.db,
            seen=self.seen,
            clock=self.clock,
            metrics=FakeMetrics(),
            options=BatchOptions(),
        )
        self.retry = RetryConsumeDlq(
            store=self.db, seen=self.seen, clock=self.clock, options=BatchOptions()
        )
        self.api = RetryDlqItem(store=self.db, options=OPTIONS)

    @property
    def context(self) -> ConsumerContext:
        return ConsumerContext(CHAIN, SUBSCRIPTION_ID, self.epoch, ZoneInfo("UTC"))

    async def consume(self, *raws: str) -> None:
        await self.batch.execute(self.context, [IncomingMessage(r, T0) for r in raws])

    def only_dlq_item(self) -> DlqRecord:
        (item,) = self.db.dlq.values()
        return item


async def test_consume_retry_of_row_failure_writes_event_and_resolves_item() -> None:
    h = ConsumerHarness()
    h.db.row_errors["uid-2"] = ("DatabaseError", "ORA-12899: valor muito grande")
    await h.consume(frame("1"), frame("2"), frame("3"))
    item = h.only_dlq_item()
    assert item.raw_message is not None
    assert h.db.offsets[CHAIN].last_offset == Offset("3")

    h.db.row_errors.clear()  # coluna ampliada pelo DBA
    await h.api.execute(item.id, "ana")
    assert await h.retry.execute(h.context) == 1

    assert item.resolution == "RETRIED"
    assert item.retry_requested_at is None
    assert await h.db.find_event("uid-2") is not None
    assert [o.unique_event_id for o in h.db.outbox.values()] == ["uid-1", "uid-3", "uid-2"]
    assert h.db.offsets[CHAIN].last_offset == Offset("3")  # retry não mexe no offset
    assert "uid-2" in h.seen.seen
    assert await h.retry.execute(h.context) == 0  # nada mais pendente


async def test_consume_retry_of_isolated_culprit_from_bisection() -> None:
    h = ConsumerHarness()
    poison = {"on": True}

    def fail(batch: BatchToPersist) -> Exception | None:
        has_uid2 = any(r.event.unique_event_id == "uid-2" for r in batch.events)
        return BatchFailedError("ORA-00600") if poison["on"] and has_uid2 else None

    h.db.fail_persist_when = fail
    await h.consume(frame("1"), frame("2"), frame("3"))
    item = h.only_dlq_item()
    assert item.unique_event_id == "uid-2"

    poison["on"] = False
    await h.api.execute(item.id, "ana")
    assert await h.retry.execute(h.context) == 1
    assert item.resolution == "RETRIED"
    assert await h.db.find_event("uid-2") is not None


async def test_consume_retry_of_event_that_already_exists_just_resolves() -> None:
    h = ConsumerHarness()
    await h.consume(frame("1"))
    raw = h.db.raw[next(iter(h.db.raw))].event.payload_json
    item_id = _dlq(h.db, DlqStage.CONSUME, raw_message=raw)
    await h.api.execute(item_id, "ana")

    assert await h.retry.execute(h.context) == 1
    assert h.db.dlq[item_id].resolution == "RETRIED"
    assert len(h.db.raw) == 1
    assert len(h.db.outbox) == 1


async def test_consume_retry_that_still_fails_keeps_item_open_without_new_dlq() -> None:
    h = ConsumerHarness()
    h.db.row_errors["uid-2"] = ("DatabaseError", "ORA-12899: primeira")
    await h.consume(frame("2"))
    item = h.only_dlq_item()

    h.db.row_errors["uid-2"] = ("DatabaseError", "ORA-12899: segunda")
    await h.api.execute(item.id, "ana")
    assert await h.retry.execute(h.context) == 0

    assert h.only_dlq_item() is item  # nenhum item novo
    assert item.resolution is None
    assert "segunda" in item.error
    assert item.attempts == 1
    assert item.retry_requested_at is None  # pode ser pedido de novo
    await h.api.execute(item.id, "ana")


async def test_consume_retry_of_message_still_invalid_records_reason() -> None:
    h = ConsumerHarness()
    await h.consume(frame("abc"))  # offset inválido: rejeitada pelo domínio
    item = h.only_dlq_item()
    assert item.offset is None

    await h.api.execute(item.id, "ana")
    assert await h.retry.execute(h.context) == 0
    assert item.resolution is None
    assert item.retry_requested_at is None
    assert "offset" in item.error.lower()


async def test_consume_retry_accepts_bare_new_event_and_scrubs_cards() -> None:
    h = ConsumerHarness()
    visa = "4111111111111111"
    bare = json.dumps(
        new_event(
            "7",
            "uid-7",
            detail=[{"elementName": "MARKET CODE", "oldValue": "", "newValue": visa}],
        )
    )
    item_id = _dlq(h.db, DlqStage.CONSUME, raw_message=bare)
    await h.api.execute(item_id, "ana")

    assert await h.retry.execute(h.context) == 1
    stored = await h.db.find_event("uid-7")
    assert stored is not None
    assert visa not in stored.event.payload_json
    assert all(visa not in o.body for o in h.db.outbox.values())


async def test_consume_retry_batch_failure_does_not_stop_other_requests() -> None:
    h = ConsumerHarness()
    h.db.row_errors.update({"uid-1": ("E", "x"), "uid-2": ("E", "y")})
    await h.consume(frame("1"), frame("2"))
    first, second = sorted(h.db.dlq.values(), key=lambda d: d.id)
    h.db.row_errors.clear()
    await h.api.execute(first.id, "ana")
    await h.api.execute(second.id, "ana")
    h.db.fail_persist_when = lambda b: (
        BatchFailedError("ORA-00600") if b.retry_of_dlq_id == first.id else None
    )

    assert await h.retry.execute(h.context) == 1

    assert first.resolution is None
    assert first.attempts == 1
    assert "ORA-00600" in first.error
    assert first.retry_requested_at is None
    assert second.resolution == "RETRIED"


async def test_consume_retry_failures_count_attempts() -> None:
    h = ConsumerHarness()
    await h.consume(frame("abc"))
    item = h.only_dlq_item()
    for expected in (1, 2):
        await h.api.execute(item.id, "ana")
        await h.retry.execute(h.context)
        assert item.attempts == expected


async def test_consume_retry_store_unavailable_propagates() -> None:
    h = ConsumerHarness()
    h.db.row_errors["uid-1"] = ("E", "x")
    await h.consume(frame("1"))
    item = h.only_dlq_item()
    h.db.row_errors.clear()
    await h.api.execute(item.id, "ana")
    h.db.fail_persist = [StoreUnavailableError("oracle fora")]

    with pytest.raises(StoreUnavailableError):
        await h.retry.execute(h.context)
    assert item.retry_requested_at is not None  # o pedido continua para a próxima vez
    assert item.attempts == 0


async def test_consume_retry_requires_the_lease() -> None:
    h = ConsumerHarness()
    await h.consume(frame("abc"))
    item = h.only_dlq_item()
    await h.api.execute(item.id, "ana")
    h.db.acquire(f"consumer:{CHAIN}")
    with pytest.raises(LeaseLostError):
        await h.retry.execute(h.context)
    assert item.retry_requested_at is not None


# ------------------------------------------------------------------ enriquecimento


class ReservationRule:
    def __init__(self, *, needs_resource: bool) -> None:
        self._needs_resource = needs_resource

    @property
    def event_names(self) -> frozenset[str]:
        return frozenset({"update  reservation"})

    @property
    def needs_resource(self) -> bool:
        return self._needs_resource

    def build(
        self, event: Event, raw_event_id: int, resource: Mapping[str, Any] | None
    ) -> Sequence[DomainWrite]:
        return [
            DomainWrite(
                table="OHIP_RESERVATION",
                key={"chain_code": event.chain_code, "primary_key": event.primary_key},
                values={"status": (resource or {}).get("status", "?")},
                source_event_raw_id=raw_event_id,
                source_offset_num=event.offset.numeric(),
            )
        ]


EnrichHarness = tuple[EnrichEvent, FakeQueueDedup, FakeMetrics, FakeFetcher]


def enrich(db: InMemoryDatabase, *, needs_resource: bool = False) -> EnrichHarness:
    dedup, metrics, fetcher = FakeQueueDedup(), FakeMetrics(), FakeFetcher({"status": "RESERVED"})
    use_case = EnrichEvent(
        store=db,
        dedup=dedup,
        fetcher=fetcher,
        rules=RuleRegistry([ReservationRule(needs_resource=needs_resource)]),
        metrics=metrics,
    )
    return use_case, dedup, metrics, fetcher


def message(raw_event_id: object, uid: str = "uid-1", *, reprocess: bool = False) -> InboundMessage:
    return InboundMessage(uid, {"raw_event_id": raw_event_id}, is_reprocess=reprocess)


async def test_enrich_normalizes_and_marks_processed_after_success() -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    use_case, dedup, _, fetcher = enrich(db)

    assert await use_case.execute(message(raw_id)) is EnrichOutcome.NORMALIZED
    assert db.raw[raw_id].status is ProcessingStatus.NORMALIZED
    assert fetcher.calls == []
    assert dedup.processed == {"uid-1"}
    assert await use_case.execute(message(raw_id)) is EnrichOutcome.DUPLICATE


async def test_enrich_with_rest_resource() -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    use_case, _, _, fetcher = enrich(db, needs_resource=True)

    assert await use_case.execute(message(raw_id)) is EnrichOutcome.ENRICHED
    assert fetcher.calls == [("RESERVATION", "HOTEL1", "123456")]
    ((values, _),) = db.domain_tables.values()
    assert values == {"status": "RESERVED"}


async def test_reprocess_messages_are_never_deduplicated() -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    use_case, dedup, _, _ = enrich(db)
    dedup.processed.add("uid-1")

    assert await use_case.execute(message(raw_id, reprocess=True)) is EnrichOutcome.NORMALIZED


async def test_older_event_does_not_overwrite_newer_state() -> None:
    db = await seeded_db(5, 6)
    newer, older = sorted(db.raw, key=lambda i: -db.raw[i].event.offset.numeric())
    use_case, _, metrics, _ = enrich(db)

    await use_case.execute(message(newer, "uid-6"))
    await use_case.execute(message(older, "uid-5"))

    ((_, offset_num),) = db.domain_tables.values()
    assert offset_num == 6
    assert metrics.total("ohip_merge_skipped_total") == 1


async def test_unmapped_event() -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    use_case = EnrichEvent(
        store=db,
        dedup=FakeQueueDedup(),
        fetcher=FakeFetcher(),
        rules=RuleRegistry([]),
        metrics=(metrics := FakeMetrics()),
    )
    assert await use_case.execute(message(raw_id)) is EnrichOutcome.UNMAPPED
    assert db.raw[raw_id].status is ProcessingStatus.UNMAPPED
    assert metrics.total("ohip_events_unmapped_total") == 1


@pytest.mark.parametrize("raw_event_id", [None, "1", True, 999])
async def test_enrich_not_found(raw_event_id: object) -> None:
    db = await seeded_db(1)
    use_case, dedup, _, _ = enrich(db)
    assert await use_case.execute(message(raw_event_id)) is EnrichOutcome.NOT_FOUND
    assert dedup.processed == set()


def test_registry_rejects_two_rules_for_same_event() -> None:
    rule = ReservationRule(needs_resource=False)
    with pytest.raises(ValueError, match="duas regras"):
        RuleRegistry([rule, rule])


async def test_enrich_apply_is_atomic_and_message_stays_unprocessed_on_failure() -> None:
    db = await seeded_db(1)
    raw_id = next(iter(db.raw))
    use_case, dedup, _, _ = enrich(db)
    db.fail_apply = [RuntimeError("ORA-03113")]

    with pytest.raises(RuntimeError):
        await use_case.execute(message(raw_id))

    assert db.domain_tables == {}
    assert db.raw[raw_id].status is ProcessingStatus.RECEIVED
    assert dedup.processed == set()
    assert await use_case.execute(message(raw_id)) is EnrichOutcome.NORMALIZED
