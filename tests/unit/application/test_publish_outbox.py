"""PublishOutbox: cabeça por chain, nack, broker fora, canal fechado e tamanho (ADR-0002)."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from tests.fakes.memory import FakeClock, FakeMetrics, FakePublisher, InMemoryDatabase

from ohip_streaming.application.errors import (
    BrokerUnavailableError,
    ChannelClosedError,
    LeaseLostError,
)
from ohip_streaming.application.ports import DlqStage, PublishOutcome
from ohip_streaming.application.use_cases.publish_outbox import (
    ChainPublishResult,
    PublisherOptions,
    PublishOutbox,
)
from ohip_streaming.domain.messages import ExchangeKind, QueueMessage

ACK, NACK = PublishOutcome.ACKED, PublishOutcome.NACKED


class Harness:
    def __init__(
        self, script: list[PublishOutcome | Exception] | None = None, **options: int
    ) -> None:
        self.clock = FakeClock()
        self.db = InMemoryDatabase(clock=self.clock)
        self.epoch = self.db.acquire("publisher")
        self.publisher = FakePublisher(script or [])
        self.metrics = FakeMetrics()
        self.use_case = PublishOutbox(
            store=self.db,
            publisher=self.publisher,
            clock=self.clock,
            metrics=self.metrics,
            options=PublisherOptions(**options),
        )

    def add(
        self,
        chain: str,
        uid: str,
        body: str = '{"x":1}',
        kind: ExchangeKind = ExchangeKind.EVENTS,
    ) -> int:
        message = QueueMessage(kind, "ohip.rsv.UPDATE_RESERVATION", uid, body)
        record = self.db._outbox_record(chain, 0, message)
        record.id = self.db._next_id("outbox")
        self.db.outbox[record.id] = record
        return record.id

    def status(self, row_id: int) -> str:
        return self.db.outbox[row_id].status

    async def publish(self, chain: str = "C1") -> ChainPublishResult:
        return await self.use_case.publish_chain(chain, self.epoch)


async def test_publishes_in_order_with_message_id_and_headers() -> None:
    h = Harness()
    ids = [h.add("C1", f"u{n}") for n in range(3)]
    h.add("C2", "outra-chain")

    result = await h.publish("C1")

    assert result.sent == 3
    assert [m.message_id for m in h.publisher.published] == ["u0", "u1", "u2"]
    assert all(h.status(i) == "SENT" for i in ids)
    first = h.publisher.published[0]
    assert first.exchange == "ohip.events"
    assert first.headers == {"x-schema-version": 1}
    assert json.loads(first.body) == {"x": 1}
    assert await h.db.chains_with_pending() == ["C2"]


async def test_reprocess_rows_go_to_reprocess_exchange() -> None:
    h = Harness()
    h.add("C1", "u1", kind=ExchangeKind.REPROCESS)
    await h.publish()
    assert h.publisher.published[0].exchange == "ohip.reprocess"


async def test_nack_records_attempt_and_blocks_the_chain_head() -> None:
    h = Harness([NACK])
    first, second = h.add("C1", "u1"), h.add("C1", "u2")

    result = await h.publish()

    assert result.sent == 0
    assert result.blocked_until == h.clock.now() + timedelta(seconds=10)
    assert h.db.outbox[first].attempts == 1
    assert h.status(second) == "PENDING"
    assert len(h.publisher.attempts) == 1  # não pulou a cabeça

    again = await h.publish()  # ainda em backoff: nada é publicado
    assert again.blocked_until == result.blocked_until
    assert len(h.publisher.attempts) == 1

    h.clock.advance(10)
    assert (await h.publish()).sent == 2
    assert [m.message_id for m in h.publisher.published] == ["u1", "u2"]


async def test_exhausted_attempts_fail_to_dlq_and_chain_continues() -> None:
    h = Harness([NACK], max_attempts=3)
    first, second = h.add("C1", "u1"), h.add("C1", "u2")
    h.db.outbox[first].attempts = 2

    result = await h.publish()

    assert result.failed == 1
    assert result.sent == 1
    assert h.status(first) == "FAILED"
    assert h.status(second) == "SENT"
    (dlq,) = h.db.dlq.values()
    assert dlq.stage is DlqStage.PUBLISH
    assert dlq.outbox_id == first


async def test_oversized_message_fails_without_publishing() -> None:
    h = Harness(broker_max_message_bytes=10)
    big = h.add("C1", "u1", body="x" * 11)
    ok = h.add("C1", "u2", body="{}")

    result = await h.publish()

    assert h.status(big) == "FAILED"
    assert h.status(ok) == "SENT"
    assert [m.message_id for m in h.publisher.attempts] == ["u2"]
    assert result.failed == 1


async def test_broker_unavailable_propagates_without_counting_attempts() -> None:
    h = Harness([BrokerUnavailableError("conexão perdida")] * 20, channel_fail_attribution=3)
    row = h.add("C1", "u1")

    for _ in range(10):  # queda longa do broker
        with pytest.raises(BrokerUnavailableError):
            await h.publish()

    assert h.db.outbox[row].attempts == 0
    assert h.status(row) == "PENDING"
    assert h.db.dlq == {}


async def test_repeated_channel_closure_on_same_row_counts_as_message_failure() -> None:
    closed = ChannelClosedError("PRECONDITION_FAILED")
    h = Harness([closed, closed, closed], channel_fail_attribution=3)
    row = h.add("C1", "u1")

    for _ in range(2):
        with pytest.raises(ChannelClosedError):
            await h.publish()
    assert h.db.outbox[row].attempts == 0

    result = await h.publish()  # terceira queda na mesma linha: conta como falha dela

    assert h.db.outbox[row].attempts == 1
    assert result.blocked_until is not None
    assert "canal fechado" in (h.db.outbox[row].last_error or "")


async def test_lease_lost_stops_publishing() -> None:
    h = Harness()
    h.add("C1", "u1")
    h.db.acquire("publisher")  # outro publisher assumiu
    with pytest.raises(LeaseLostError):
        await h.publish()


async def test_channel_closures_are_counted_per_row_across_interleaved_chains() -> None:
    closed = ChannelClosedError("PRECONDITION_FAILED")
    h = Harness([closed] * 4, channel_fail_attribution=3)
    a, b = h.add("C1", "a1"), h.add("C2", "b1")

    for _ in range(2):  # C1 e C2 alternando: duas quedas em cada linha
        for chain in ("C1", "C2"):
            with pytest.raises(ChannelClosedError):
                await h.publish(chain)

    # Nenhuma linha chegou a 3 quedas: a contagem de uma não zera nem soma na outra.
    assert h.db.outbox[a].attempts == 0
    assert h.db.outbox[b].attempts == 0

    h.publisher.script = [closed, closed]
    await h.publish("C1")  # 3ª queda de a1: conta como falha dela
    await h.publish("C2")  # 3ª queda de b1: conta como falha dela
    assert h.db.outbox[a].attempts == 1
    assert h.db.outbox[b].attempts == 1


async def test_channel_counter_restarts_after_the_row_is_sent() -> None:
    closed = ChannelClosedError("PRECONDITION_FAILED")
    h = Harness([closed, closed, ACK], channel_fail_attribution=3)
    row = h.add("C1", "u1")
    for _ in range(2):
        with pytest.raises(ChannelClosedError):
            await h.publish()
    assert (await h.publish()).sent == 1
    assert h.use_case._channel_failures == {}
    assert h.db.outbox[row].attempts == 0


async def test_each_chain_publishes_in_its_own_lane() -> None:
    h = Harness()
    h.add("C1", "u1")
    h.add("C2", "u2")
    await h.publish("C1")
    await h.publish("C2")
    assert [(m.message_id, m.lane) for m in h.publisher.published] == [("u1", "C1"), ("u2", "C2")]
