"""Cenários de contrato dos stores, iguais para o fake em memória e para o Oracle (Fase 3).

Cada cenário fala só com os ports (e com as consultas de inspeção do backend). Se um cenário
passa no fake e falha no Oracle, o adapter (ou o fake) não cumpre o contrato.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from tests.fakes.frames import SUBSCRIPTION_ID, frame, new_event
from tests.stores.backends import Backend

from ohip_streaming.application.errors import (
    LeaseLostError,
    LeaseNotProvisionedError,
    ReplayAlreadyPendingError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    BatchToPersist,
    ConsumeDlqRecord,
    DlqQuery,
    DlqStage,
    EventFilter,
    NewEventRecord,
    OutboxQuery,
    PageRequest,
    ProcessingStatus,
    ReplayQuery,
    ReplayStatus,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.events import Event, parse_next_frame
from ohip_streaming.domain.masking import MaskingPolicy
from ohip_streaming.domain.messages import ExchangeKind, build_queue_message
from ohip_streaming.domain.offset import Offset

CHAIN, OTHER = "ZZT1", "ZZT2"
RECEIVED = datetime(2026, 10, 1, 12, 0, 0, 123456, tzinfo=UTC)


def uid(n: int, run: str = uuid.uuid4().hex[:8]) -> str:
    return f"zzt-{run}-{n}"


def event(offset: int, *, unique_id: str | None = None, chain: str = CHAIN, **kw: Any) -> Event:
    parsed = parse_next_frame(
        frame(str(offset), unique_id or uid(offset), **kw),
        chain_code=chain,
        subscription_id=SUBSCRIPTION_ID,
        received_at=RECEIVED,
        event_tz=ZoneInfo("UTC"),
    )
    assert isinstance(parsed, Event)
    return parsed


async def records(
    backend: Backend, *events: Event, ignored: frozenset[str] = frozenset()
) -> tuple[NewEventRecord, ...]:
    ids = await backend.events.reserve_event_ids(len(events))
    result = []
    for raw_id, ev in zip(ids, events, strict=True):
        if ev.unique_event_id in ignored:
            result.append(NewEventRecord(raw_id, ev, ProcessingStatus.IGNORED, None))
            continue
        message, _ = build_queue_message(
            ev, raw_event_id=raw_id, masking=MaskingPolicy(), module_codes={}
        )
        result.append(NewEventRecord(raw_id, ev, ProcessingStatus.RECEIVED, message))
    return tuple(result)


def batch(backend: Backend, recs: tuple[NewEventRecord, ...], **kw: Any) -> BatchToPersist:
    values: dict[str, Any] = {
        "chain_code": CHAIN,
        "lease_epoch": backend.epoch(f"consumer:{CHAIN}"),
        "events": recs,
        "rejected": (),
        "offset": recs[-1].event.offset if recs else None,
        "last_unique_event_id": recs[-1].event.unique_event_id if recs else None,
    }
    values.update(kw)
    return BatchToPersist(**values)


async def persist(backend: Backend, *events: Event, **kw: Any) -> Any:
    return await backend.events.persist_batch(batch(backend, await records(backend, *events), **kw))


# ------------------------------------------------------------------ lote do consumer


async def test_batch_inserts_dedups_and_keeps_outbox_order(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    first = [event(n) for n in (1, 2, 3)]
    recs = await records(backend, *first, ignored=frozenset({first[1].unique_event_id}))
    result = await backend.events.persist_batch(batch(backend, recs))

    assert result.inserted == tuple(e.unique_event_id for e in first)
    assert [o.unique_event_id for o in backend.outbox_rows(CHAIN)] == [
        first[0].unique_event_id,
        first[2].unique_event_id,  # IGNORED não gera outbox
    ]
    assert backend.offset(CHAIN) == Offset("3")

    same_uid = first[0]
    same_offset_and_key = event(2, unique_id=uid(902))  # outro uid, mesmo (chain, offset, pk)
    new = event(4)
    result = await persist(backend, same_uid, same_offset_and_key, new)

    assert result.inserted == (new.unique_event_id,)
    assert set(result.duplicates) == {same_uid.unique_event_id, same_offset_and_key.unique_event_id}
    ids = [o.id for o in backend.outbox_rows(CHAIN)]
    assert ids == sorted(ids)
    assert backend.raw_count(CHAIN) == 4
    assert backend.offset(CHAIN) == Offset("4")


async def test_row_failure_goes_to_dlq_and_the_rest_commits(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    bad = backend.broken(event(2))
    result = await persist(backend, event(1), bad, event(3))

    assert [f.unique_event_id for f in result.row_failures] == [bad.unique_event_id]
    assert len(result.inserted) == 2
    (item,) = backend.dlq_items(CHAIN)
    assert (item.stage, item.unique_event_id, item.offset) == ("CONSUME", bad.unique_event_id, "2")
    assert "12899" in item.error
    assert item.raw_message is not None
    assert bad.unique_event_id in item.raw_message
    assert len(backend.outbox_rows(CHAIN)) == 2
    assert backend.offset(CHAIN) == Offset("3")


async def test_rejected_messages_go_to_dlq_without_events(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    rejected = ConsumeDlqRecord("{lixo", "JSON inválido", Offset("7"), None)
    await backend.events.persist_batch(batch(backend, (), rejected=(rejected,), offset=Offset("7")))
    (item,) = backend.dlq_items(CHAIN)
    assert (item.offset, item.raw_message) == ("7", "{lixo")
    assert backend.offset(CHAIN) == Offset("7")
    assert backend.raw_count(CHAIN) == 0


async def test_zombie_writer_commits_nothing(backend: Backend) -> None:
    stale = backend.acquire(f"consumer:{CHAIN}")
    backend.acquire(f"consumer:{CHAIN}")  # outro processo assumiu
    recs = await records(backend, event(1))
    with pytest.raises(LeaseLostError):
        await backend.events.persist_batch(batch(backend, recs, lease_epoch=stale))
    assert backend.raw_count(CHAIN) == 0
    assert backend.outbox_rows(CHAIN) == []
    assert backend.offset(CHAIN) is None


async def test_large_payload_round_trip(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    detail = [{"elementName": f"E{i}", "oldValue": "", "newValue": "x" * 500} for i in range(200)]
    big = event(1, detail=detail)  # ~100 KB: CLOB, não VARCHAR2
    assert len(big.payload_json) > 32_767
    await persist(backend, big)
    stored = await backend.operations.find_event(big.unique_event_id)
    assert stored is not None
    assert stored.event.payload_json == big.payload_json
    assert stored.event.received_at == RECEIVED
    assert stored.event.offset == Offset("1")


# ------------------------------------------------------------------ retry de DLQ CONSUME


async def test_consume_retry_cycle(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    good = event(5)
    rejected = ConsumeDlqRecord(
        json.dumps(new_event("5", good.unique_event_id)),
        "falhou",
        Offset("5"),
        good.unique_event_id,
    )
    await backend.events.persist_batch(batch(backend, (), rejected=(rejected,), offset=Offset("5")))
    (item,) = backend.dlq_items(CHAIN)

    assert await backend.operations.request_consume_retry(item.id, "ana")
    assert not await backend.operations.request_consume_retry(item.id, "bia")  # já pedido
    (pending,) = await backend.events.pending_consume_retries(CHAIN, 10)
    assert pending.dlq_id == item.id
    assert await backend.events.pending_consume_retries(OTHER, 10) == []

    epoch = backend.epoch(f"consumer:{CHAIN}")
    await backend.events.mark_consume_retry_failed(item.id, "ainda inválido", epoch)
    (item,) = backend.dlq_items(CHAIN)
    assert (item.attempts, item.retry_requested, item.resolution) == (1, False, None)

    assert await backend.operations.request_consume_retry(item.id, "ana")
    result = await persist(
        backend, good, offset=None, last_unique_event_id=None, retry_of_dlq_id=item.id
    )
    assert result.inserted == (good.unique_event_id,)
    (item,) = backend.dlq_items(CHAIN)
    assert item.resolution == "RETRIED"
    assert backend.offset(CHAIN) == Offset("5")  # retry não mexe no offset
    assert not await backend.operations.request_consume_retry(item.id, "ana")  # resolvido


async def test_consume_retry_that_fails_again_keeps_the_item(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    bad = backend.broken(event(3))
    await persist(backend, bad)
    (item,) = backend.dlq_items(CHAIN)
    await backend.operations.request_consume_retry(item.id, "ana")

    await persist(backend, bad, offset=None, last_unique_event_id=None, retry_of_dlq_id=item.id)

    (item,) = backend.dlq_items(CHAIN)  # nenhum item novo
    assert (item.resolution, item.attempts, item.retry_requested) == (None, 1, False)


# ------------------------------------------------------------------ outbox e publisher


async def test_publisher_marks_and_dlq(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    await persist(backend, event(1), event(2), event(3))
    epoch = backend.acquire("publisher")
    assert CHAIN in await backend.outbox.chains_with_pending()

    head = await backend.outbox.fetch_head(CHAIN, 2)
    assert [r.message_id for r in head] == [o.unique_event_id for o in backend.outbox_rows(CHAIN)][
        :2
    ]
    assert head[0].exchange_name == "ohip.events"
    assert head[0].next_attempt_at.tzinfo is not None

    await backend.outbox.mark_sent(head[0].id, epoch)
    retry_at = datetime.now(UTC) + timedelta(seconds=30)
    await backend.outbox.record_failure(head[1].id, 1, retry_at, "nack", epoch)
    await backend.outbox.mark_failed(head[1].id, 10, "esgotado", epoch)

    rows = {o.id: o for o in backend.outbox_rows(CHAIN)}
    assert rows[head[0].id].status == "SENT"
    assert (rows[head[1].id].status, rows[head[1].id].attempts) == ("FAILED", 10)
    (dlq,) = backend.dlq_items(CHAIN)
    assert dlq.stage == "PUBLISH"
    assert [r.id for r in await backend.outbox.fetch_head(CHAIN, 10)] == [
        o.id for o in backend.outbox_rows(CHAIN) if o.status == "PENDING"
    ]

    backend.acquire("publisher")  # outro publisher assumiu
    with pytest.raises(LeaseLostError):
        await backend.outbox.mark_sent(head[0].id, epoch)


async def test_dlq_retries_are_atomic(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    await persist(backend, event(1))
    epoch = backend.acquire("publisher")
    (row,) = await backend.outbox.fetch_head(CHAIN, 1)
    await backend.outbox.mark_failed(row.id, 10, "esgotado", epoch)
    (item,) = backend.dlq_items(CHAIN)

    new_id = await backend.operations.retry_publish(item.id, row.id, "ana")
    assert new_id is not None
    assert await backend.operations.retry_publish(item.id, row.id, "bia") is None  # 2º clique
    rows = backend.outbox_rows(CHAIN)
    assert [o.status for o in rows] == ["FAILED", "PENDING"]
    assert rows[-1].id == new_id

    stored = await backend.operations.find_event(row.message_id)
    assert stored is not None
    message, _ = build_queue_message(
        stored.event,
        raw_event_id=stored.raw_event_id,
        masking=MaskingPolicy(),
        module_codes={},
        kind=ExchangeKind.REPROCESS,
    )
    outbox_id = await backend.operations.enqueue(
        chain_code=CHAIN,
        raw_event_id=stored.raw_event_id,
        message=message,
        exchange_name="ohip.reprocess",
    )
    assert backend.outbox_rows(CHAIN)[-1].id == outbox_id
    assert backend.outbox_rows(CHAIN)[-1].exchange_name == "ohip.reprocess"


# ------------------------------------------------------------------ replay e status


async def test_replay_lifecycle(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    await persist(backend, event(90), event(100))
    assert await backend.replay.last_offset(CHAIN) == Offset("100")
    assert await backend.replay.offset_received_at(CHAIN, Offset("90")) == RECEIVED
    assert await backend.replay.offset_received_at(CHAIN, Offset("55")) is None
    with pytest.raises(UnknownChainError):
        await backend.replay.last_offset("ZZNAOEXISTE")

    first = await backend.replay.create_request(CHAIN, Offset("90"), "lacuna", "ana")
    assert first.status is ReplayStatus.PENDING
    with pytest.raises(ReplayAlreadyPendingError):
        await backend.replay.create_request(CHAIN, Offset("90"), "de novo", "bia")
    await backend.replay.create_request(OTHER, Offset("1"), "outra chain", "ana")  # pode

    assert await backend.replay.cancel_request(first.id, "ana")
    assert not await backend.replay.cancel_request(first.id, "ana")
    epoch = backend.epoch(f"consumer:{CHAIN}")
    assert not await backend.replay.apply_request(first, epoch)  # cancelado

    second = await backend.replay.create_request(CHAIN, Offset("90"), "lacuna", "ana")
    assert await backend.replay.pending_request(CHAIN) == second
    assert await backend.replay.apply_request(second, epoch)
    assert backend.offset(CHAIN) == Offset("90")
    assert await backend.replay.pending_request(CHAIN) is None


async def test_consumer_status_and_disconnect_clock(backend: Backend) -> None:
    snapshot = await backend.status.disconnect_snapshot(CHAIN)
    assert (snapshot.last_disconnect_at, snapshot.last_state) == (None, ConsumerState.STOPPED)

    await backend.status.record_state(CHAIN, ConsumerState.SUBSCRIBED, "vm1:42")
    assert (await backend.status.disconnect_snapshot(CHAIN)).last_state is ConsumerState.SUBSCRIBED

    await backend.status.record_disconnect(CHAIN, ConsumerState.WAITING, 4409, "lockout")
    snapshot = await backend.status.disconnect_snapshot(CHAIN)
    assert snapshot.last_state is ConsumerState.WAITING
    assert snapshot.last_disconnect_at is not None
    assert snapshot.last_disconnect_at <= snapshot.db_now  # mesmo relógio (do banco)
    with pytest.raises(UnknownChainError):
        await backend.status.record_state("ZZNAOEXISTE", ConsumerState.STOPPED, "x")


# ------------------------------------------------------------------ lease (ADR-0008)


async def test_lease_has_one_owner_and_fences_writes(backend: Backend) -> None:
    lease = f"consumer:{CHAIN}"
    epoch = await backend.leases.acquire(lease, "vm1", 30)
    assert epoch is not None
    assert await backend.leases.acquire(lease, "vm2", 30) is None  # outro dono, prazo válido
    assert await backend.leases.acquire(lease, "vm1", 30) == epoch + 1  # o próprio dono renova
    epoch += 1
    assert await backend.leases.renew(lease, "vm1", epoch, 30)
    assert not await backend.leases.renew(lease, "vm1", epoch - 1, 30)  # epoch velho
    assert not await backend.leases.renew(lease, "vm2", epoch, 30)

    await persist(backend, event(1), lease_epoch=epoch)  # a barreira aceita o epoch do lease

    await backend.leases.release(lease, "vm2", epoch)  # não é dono: nada muda
    assert await backend.leases.acquire(lease, "vm2", 30) is None
    await backend.leases.release(lease, "vm1", epoch)
    new_epoch = await backend.leases.acquire(lease, "vm2", 30)
    assert new_epoch == epoch + 1
    with pytest.raises(LeaseLostError):
        await persist(backend, event(2), lease_epoch=epoch)  # o antigo dono virou zumbi


async def test_unprovisioned_lease(backend: Backend) -> None:
    with pytest.raises(LeaseNotProvisionedError):
        await backend.leases.acquire("consumer:ZZNAOEXISTE", "vm1", 30)


# ------------------------------------------------------------------ consultas da API (Fase 7)


async def test_monitoring_reads_what_the_consumer_wrote(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    first, second = event(1), event(2, hotel_id="ZZTH2")
    await persist(backend, first, second)
    await backend.replay.create_request(CHAIN, Offset("1"), "lacuna", "ana")

    page = await backend.monitoring.events(EventFilter(chain_code=CHAIN), PageRequest(limit=1))
    assert [e.unique_event_id for e in page.items] == [second.unique_event_id]
    assert page.next_cursor == page.items[0].raw_event_id
    rest = await backend.monitoring.events(
        EventFilter(chain_code=CHAIN), PageRequest(limit=1, cursor=page.next_cursor)
    )
    assert [e.unique_event_id for e in rest.items] == [first.unique_event_id]
    assert rest.next_cursor is None
    filtered = await backend.monitoring.events(
        EventFilter(
            chain_code=CHAIN,
            hotel_id="ZZTH2",
            module_name="reservation",
            event_name="UPDATE RESERVATION",
            received_from=RECEIVED,
            received_to=RECEIVED + timedelta(microseconds=1),
        ),
        PageRequest(),
    )
    assert [e.unique_event_id for e in filtered.items] == [second.unique_event_id]
    assert filtered.items[0].received_at == RECEIVED  # fração de segundo preservada

    record = await backend.monitoring.event(first.unique_event_id)
    assert record is not None
    assert record.stored.event.unique_event_id == first.unique_event_id
    assert [o.status for o in record.outbox] == ["PENDING"]
    assert await backend.monitoring.event("zzt-nao-existe") is None

    snapshot = await backend.monitoring.status()
    (chain,) = [c for c in snapshot.chains if c.chain_code == CHAIN]
    assert chain.last_offset == Offset("2")
    assert chain.outbox.pending == 2
    assert chain.outbox.oldest_pending_at is not None

    outbox = await backend.monitoring.outbox(OutboxQuery("PENDING", CHAIN), PageRequest())
    assert len(outbox.items) == 2
    age = await backend.monitoring.oldest_pending_age_s(CHAIN)
    assert age is not None
    assert age >= 0  # created_at e "agora" vêm do mesmo relógio (o do banco)
    assert await backend.monitoring.oldest_pending_age_s(OTHER) is None

    (replay,) = (await backend.monitoring.replays(ReplayQuery(CHAIN), PageRequest())).items
    assert (replay.status, replay.requested_by) == (ReplayStatus.PENDING, "ana")
    assert await backend.monitoring.replay(replay.id) == replay
    await backend.monitoring.ping()


async def test_monitoring_dlq_filters(backend: Backend) -> None:
    backend.acquire(f"consumer:{CHAIN}")
    rejected = ConsumeDlqRecord("{lixo", "JSON inválido", Offset("7"), None)
    await backend.events.persist_batch(batch(backend, (), rejected=(rejected,), offset=Offset("7")))

    open_items = await backend.monitoring.dlq(
        DlqQuery(stage=DlqStage.CONSUME, chain_code=CHAIN, resolved=False), PageRequest()
    )
    (item,) = open_items.items
    assert (item.offset, item.resolution) == ("7", None)
    resolved = await backend.monitoring.dlq(
        DlqQuery(chain_code=CHAIN, resolved=True), PageRequest()
    )
    assert resolved.items == ()
