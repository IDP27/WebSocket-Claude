"""ProcessEventBatch: nenhuma mensagem se perde, offset correto, dedup, DLQ, retry e bisseção."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import timedelta
from zoneinfo import ZoneInfo

import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, frame
from tests.fakes.memory import T0, FakeClock, FakeMetrics, FakeSeenCache, InMemoryDatabase

from ohip_streaming.application.errors import (
    BatchFailedError,
    LeaseLostError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import BatchToPersist, DlqStage, ProcessingStatus
from ohip_streaming.application.use_cases.process_event_batch import (
    BatchOptions,
    BatchSummary,
    ConsumerContext,
    IncomingMessage,
    ProcessEventBatch,
)
from ohip_streaming.domain.offset import Offset

CHAIN = "CHAIN1"
VISA_TEST = "4111111111111111"


class Harness:
    def __init__(self, **options: object) -> None:
        self.clock = FakeClock()
        self.db = InMemoryDatabase(clock=self.clock)
        self.db.provision_chain(CHAIN)
        self.epoch = self.db.acquire(f"consumer:{CHAIN}")
        self.seen = FakeSeenCache()
        self.metrics = FakeMetrics()
        self.use_case = ProcessEventBatch(
            store=self.db,
            seen=self.seen,
            clock=self.clock,
            metrics=self.metrics,
            options=BatchOptions(**options),  # type: ignore[arg-type]
        )

    @property
    def context(self) -> ConsumerContext:
        return ConsumerContext(CHAIN, SUBSCRIPTION_ID, self.epoch, ZoneInfo("UTC"))

    async def run(self, *raws: str) -> BatchSummary:
        messages = [
            IncomingMessage(raw, T0 + timedelta(milliseconds=i)) for i, raw in enumerate(raws)
        ]
        return await self.use_case.execute(self.context, messages)

    def outbox_uids(self) -> list[str]:
        return [o.unique_event_id for o in sorted(self.db.outbox.values(), key=lambda o: o.id)]

    def dlq(self, stage: DlqStage = DlqStage.CONSUME) -> list[str | None]:
        return [d.offset for d in self.db.dlq.values() if d.stage is stage]

    @property
    def offset(self) -> Offset | None:
        return self.db.offsets[CHAIN].last_offset


@pytest.fixture
def h() -> Harness:
    return Harness(module_codes={"reservation": "rsv"})


async def test_happy_path_writes_events_outbox_in_order_and_offset(h: Harness) -> None:
    summary = await h.run(frame("1"), frame("2"), frame("3"))

    assert summary.inserted == 3
    assert summary.received == 3
    assert h.outbox_uids() == ["uid-1", "uid-2", "uid-3"]
    assert h.offset == Offset("3")
    assert h.db.offsets[CHAIN].last_unique_event_id == "uid-3"
    assert {r.status for r in h.db.raw.values()} == {ProcessingStatus.RECEIVED}
    assert h.seen.seen == {"uid-1", "uid-2", "uid-3"}
    outbox = sorted(h.db.outbox.values(), key=lambda o: o.id)[0]
    assert outbox.routing_key == "ohip.rsv.UPDATE_RESERVATION"
    assert json.loads(outbox.body)["raw_event_id"] == outbox.raw_event_id
    assert h.metrics.total("ohip_events_inserted_total") == 3


async def test_queue_message_detail_is_masked(h: Harness) -> None:
    await h.run(
        frame("1", detail=[{"elementName": "FIRST NAME", "oldValue": "", "newValue": "Ana"}])
    )

    (outbox,) = h.db.outbox.values()
    assert "Ana" not in outbox.body
    (raw,) = h.db.raw.values()
    assert "Ana" in raw.event.payload_json  # o bruto no Oracle fica íntegro


async def test_seen_cache_duplicates_are_skipped_but_offset_advances(h: Harness) -> None:
    h.seen.seen.add("uid-2")
    summary = await h.run(frame("1"), frame("2"))

    assert summary.duplicates == 1
    assert h.outbox_uids() == ["uid-1"]
    assert h.offset == Offset("2")


async def test_duplicates_inside_the_same_batch(h: Harness) -> None:
    summary = await h.run(frame("1"), frame("1"))
    assert summary.inserted == 1
    assert summary.duplicates == 1
    assert h.outbox_uids() == ["uid-1"]


async def test_database_duplicates_by_unique_event_id_and_by_offset_and_key(h: Harness) -> None:
    await h.run(frame("1"), frame("2"))
    # replay: mesmo uniqueEventId; e o caso do guia (mesmo primaryKey e offset, outro id)
    summary = await h.run(frame("1"), frame("2", uid="outro-id"), frame("3"))

    assert summary.duplicates == 2
    assert summary.inserted == 1
    assert h.outbox_uids() == ["uid-1", "uid-2", "uid-3"]
    assert h.offset == Offset("3")


async def test_rejected_messages_go_to_dlq_and_offset_uses_last_valid(h: Harness) -> None:
    bad_but_has_offset = frame("2", module_name="")
    no_offset = "{json quebrado"
    summary = await h.run(frame("1"), bad_but_has_offset, no_offset)

    assert summary.rejected == 2
    assert h.outbox_uids() == ["uid-1"]
    assert sorted(h.dlq(), key=str) == ["2", None]
    assert h.offset == Offset("2")  # a última mensagem não tem offset válido


async def test_batch_without_any_valid_offset_does_not_touch_offset(h: Harness) -> None:
    await h.run(frame("5"))
    await h.run("{json quebrado")
    assert h.offset == Offset("5")


async def test_allowlist_stores_raw_as_ignored_without_outbox() -> None:
    h = Harness(allowlist=frozenset({"NEW RESERVATION"}))
    summary = await h.run(frame("1", event_name="NEW RESERVATION"), frame("2"))

    assert summary.ignored == 1
    assert h.outbox_uids() == ["uid-1"]
    statuses = {r.event.unique_event_id: r.status for r in h.db.raw.values()}
    assert statuses == {"uid-1": ProcessingStatus.RECEIVED, "uid-2": ProcessingStatus.IGNORED}
    assert h.offset == Offset("2")


async def test_row_failures_go_to_dlq_without_outbox(h: Harness) -> None:
    h.db.row_errors["uid-2"] = ("DatabaseError", "ORA-12899: valor grande demais")
    summary = await h.run(frame("1"), frame("2"), frame("3"))

    assert summary.row_failures == 1
    assert h.outbox_uids() == ["uid-1", "uid-3"]
    assert h.dlq() == ["2"]
    assert h.offset == Offset("3")


async def test_card_numbers_never_reach_the_database(h: Harness) -> None:
    summary = await h.run(
        frame("1", detail=[{"elementName": "MARKET CODE", "oldValue": "", "newValue": VISA_TEST}])
    )
    assert summary.card_data_detected == 1
    assert h.metrics.total("ohip_card_data_detected_total") == 1
    (row,) = h.db.raw.values()
    assert VISA_TEST not in row.event.payload_json  # OHIP_EVENT_RAW.payload
    assert row.event.detail[0].new_value == "***"
    assert VISA_TEST not in next(iter(h.db.outbox.values())).body


async def test_card_numbers_are_removed_from_rejected_messages_in_dlq(h: Harness) -> None:
    rejected = frame("abc", detail=[{"elementName": "X", "oldValue": "", "newValue": VISA_TEST}])
    summary = await h.run(rejected)
    assert summary.card_data_detected == 1
    (item,) = h.db.dlq.values()
    assert item.raw_message is not None
    assert VISA_TEST not in item.raw_message


async def test_card_numbers_are_removed_from_isolated_culprit_in_dlq() -> None:
    h = Harness(max_retries=0)
    h.db.fail_persist_when = lambda b: BatchFailedError("ORA-00600") if b.events else None
    await h.run(
        frame("1", detail=[{"elementName": "MARKET CODE", "oldValue": "", "newValue": VISA_TEST}])
    )
    (item,) = h.db.dlq.values()
    assert item.raw_message is not None
    assert VISA_TEST not in item.raw_message
    assert "uid-1" in item.raw_message


async def test_redis_failures_never_affect_correctness(h: Harness) -> None:
    h.seen.fail_filter = True
    h.seen.fail_mark = True
    summary = await h.run(frame("1"), frame("2"))
    assert summary.inserted == 2
    assert h.offset == Offset("2")


async def test_seen_cache_is_only_marked_after_commit(h: Harness) -> None:
    h.db.fail_persist = [StoreUnavailableError("oracle fora")]
    with pytest.raises(StoreUnavailableError):
        await h.run(frame("1"))
    assert h.seen.seen == set()
    assert h.seen.mark_calls == []


async def test_store_unavailable_and_lease_lost_propagate_without_retry(h: Harness) -> None:
    h.db.fail_persist = [StoreUnavailableError("fora")]
    with pytest.raises(StoreUnavailableError):
        await h.run(frame("1"))

    h.db.acquire(f"consumer:{CHAIN}")  # outro processo assumiu
    with pytest.raises(LeaseLostError):
        await h.run(frame("1"))
    assert h.clock.sleeps == []
    assert h.db.raw == {}
    assert h.offset is None


async def test_transient_batch_failure_is_retried(h: Harness) -> None:
    h.db.fail_persist = [BatchFailedError("x"), BatchFailedError("y")]
    summary = await h.run(frame("1"), frame("2"))

    assert summary.inserted == 2
    assert h.clock.sleeps == [0.5, 1.0]
    assert h.offset == Offset("2")


async def test_persistent_failure_is_bisected_in_order_and_culprit_goes_to_dlq() -> None:
    h = Harness(max_retries=1)

    def fail_if_contains_poison(batch: BatchToPersist) -> Exception | None:
        if any(r.event.unique_event_id == "uid-3" for r in batch.events):
            return BatchFailedError("ORA-00600")
        return None

    h.db.fail_persist_when = fail_if_contains_poison
    summary = await h.run(*(frame(str(n)) for n in range(1, 6)))

    assert summary.inserted == 4
    assert h.outbox_uids() == ["uid-1", "uid-2", "uid-4", "uid-5"]  # ordem preservada
    (culprit,) = [d for d in h.db.dlq.values() if d.stage is DlqStage.CONSUME]
    assert culprit.offset == "3"
    assert culprit.unique_event_id == "uid-3"
    assert "ORA-00600" in culprit.error
    assert culprit.raw_message is not None
    assert "uid-3" in culprit.raw_message
    assert h.offset == Offset("5")
    # o offset nunca retrocede entre os commits da bisseção
    committed = [b.offset.numeric() for b in h.db.persisted_batches if b.offset]
    assert committed == sorted(committed)


async def test_failure_that_survives_bisection_propagates() -> None:
    h = Harness(max_retries=0)
    h.db.fail_persist_when = lambda _batch: BatchFailedError("sempre")
    with pytest.raises(BatchFailedError):
        await h.run(frame("1"))
    assert h.offset is None


async def test_empty_batch_is_a_no_op(h: Harness) -> None:
    summary = await h.run()
    assert summary.received == 0
    assert h.db.persisted_batches == []


def _fail_events(*poisoned: str) -> Callable[[BatchToPersist], Exception | None]:
    def check(batch: BatchToPersist) -> Exception | None:
        if any(r.event.unique_event_id in poisoned for r in batch.events):
            return BatchFailedError("ORA-01653: tablespace cheio")
        return None

    return check


async def test_systemic_failure_trips_the_breaker_instead_of_draining_to_dlq() -> None:
    h = Harness(max_retries=0)
    # Falha que atinge qualquer evento (ex.: tablespace do evento bruto cheio), mas a DLQ grava.
    h.db.fail_persist_when = lambda b: BatchFailedError("ORA-01653") if b.events else None

    with pytest.raises(StoreUnavailableError, match="sistêmica"):
        await h.run(*(frame(str(n)) for n in range(1, 9)))

    assert len(h.db.dlq) == 1  # só a primeira culpada; o resto não foi para a DLQ
    assert h.offset == Offset("1")  # reconecta a partir daqui; nada se perde


async def test_breaker_persists_across_batches_until_an_event_is_written() -> None:
    h = Harness(max_retries=0)
    h.db.fail_persist_when = _fail_events("uid-1", "uid-2", "uid-3")
    await h.run(frame("1"))
    with pytest.raises(StoreUnavailableError):
        await h.run(frame("2"))

    h.db.fail_persist_when = _fail_events("uid-3")
    await h.run(frame("4"))  # sucesso zera o disjuntor
    await h.run(frame("3"))  # nova culpada isolada vai para a DLQ normalmente
    assert sorted(d.unique_event_id or "" for d in h.db.dlq.values()) == ["uid-1", "uid-3"]


async def test_isolated_culprits_separated_by_successes_all_go_to_dlq() -> None:
    h = Harness(max_retries=0)
    h.db.fail_persist_when = _fail_events("uid-2", "uid-4")

    summary = await h.run(*(frame(str(n)) for n in range(1, 6)))

    assert summary.inserted == 3
    assert h.dlq() == ["2", "4"]
    assert h.offset == Offset("5")


async def test_adjacent_culprits_are_treated_as_systemic() -> None:
    h = Harness(max_retries=0)
    h.db.fail_persist_when = _fail_events("uid-2", "uid-3")
    with pytest.raises(StoreUnavailableError):
        await h.run(*(frame(str(n)) for n in range(1, 5)))
    assert h.dlq() == ["2"]
    assert h.offset == Offset("2")


@pytest.mark.parametrize("already_stored_in", ["oracle", "redis"])
async def test_replay_culprits_separated_only_by_duplicates_go_to_dlq(
    already_stored_in: str,
) -> None:
    h = Harness(max_retries=0)
    if already_stored_in == "oracle":
        await h.run(*(frame(str(n)) for n in range(13, 17)))
    else:
        h.seen.seen.update(f"uid-{n}" for n in range(13, 17))
    h.db.fail_persist_when = _fail_events("uid-12", "uid-17")

    summary = await h.run(*(frame(str(n)) for n in range(10, 20)))  # replay a partir de 10

    assert h.dlq() == ["12", "17"]
    assert summary.duplicates == 4
    assert h.offset == Offset("19")


async def test_breaker_does_not_reset_when_tripping_until_restart() -> None:
    h = Harness(max_retries=0)
    h.db.fail_persist_when = _fail_events("uid-1", "uid-2")
    with pytest.raises(StoreUnavailableError):
        await h.run(frame("1"), frame("2"))
    assert h.offset == Offset("1")

    # Reconexão com a mesma instância: dispara de novo sem mandar mais nada para a DLQ.
    for _ in range(3):
        with pytest.raises(StoreUnavailableError):
            await h.run(frame("2"))
    assert h.dlq() == ["1"]

    # Restart do processo (instância nova): isola a próxima culpada.
    restarted = ProcessEventBatch(
        store=h.db,
        seen=h.seen,
        clock=h.clock,
        metrics=h.metrics,
        options=BatchOptions(max_retries=0),
    )
    await restarted.execute(h.context, [IncomingMessage(frame("2"), T0)])
    assert h.dlq() == ["1", "2"]
    assert h.offset == Offset("2")


async def test_rejected_message_keeps_luhn_valid_identifiers_in_dlq(h: Harness) -> None:
    luhn_offset = "4111111111111103"
    rejected = frame(
        luhn_offset,
        primary_key=None,  # campo obrigatório ausente: rejeitada pelo domínio
        detail=[{"elementName": "X", "oldValue": "", "newValue": VISA_TEST}],
    )
    await h.run(rejected)

    (item,) = h.db.dlq.values()
    assert item.offset == luhn_offset
    assert item.raw_message is not None
    assert luhn_offset in item.raw_message
    assert VISA_TEST not in item.raw_message
    assert json.loads(item.raw_message)["payload"]["data"]["newEvent"]["metadata"] == {
        "offset": luhn_offset,
        "uniqueEventId": f"uid-{luhn_offset}",
    }
