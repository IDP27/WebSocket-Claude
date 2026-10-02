"""TokenProvider (cache, margem, emissão única, Redis fora) e LeaseKeeper (ADR-0008)."""

from __future__ import annotations

import asyncio
import random
from datetime import timedelta

import pytest
from tests.fakes.memory import (
    FakeClock,
    FakeMetrics,
    FakeTokenCache,
    FakeTokenIssuer,
    InMemoryDatabase,
    MemoryLeaseStore,
)

from ohip_streaming.application.errors import (
    AuthRejectedError,
    LeaseNotProvisionedError,
    StoreUnavailableError,
)
from ohip_streaming.application.ports import AccessToken
from ohip_streaming.application.use_cases.lease import LeaseKeeper, LeaseOptions
from ohip_streaming.application.use_cases.token_provider import TokenProvider, token_cache_key

KEY = token_cache_key("homologacao", "CHAIN1")

# ------------------------------------------------------------------ token


class TokenHarness:
    def __init__(self, *, margin_s: float = 300, lifetime_s: float = 3600) -> None:
        self.clock = FakeClock()
        self.issuer = FakeTokenIssuer(self.clock, lifetime_s=lifetime_s)
        self.cache = FakeTokenCache()
        self.metrics = FakeMetrics()
        self.provider = self.make()
        self.margin_s = margin_s

    def make(self, margin_s: float = 300) -> TokenProvider:
        return TokenProvider(
            issuer=self.issuer,
            cache=self.cache,
            clock=self.clock,
            metrics=self.metrics,
            cache_key=KEY,
            margin_s=margin_s,
        )


def test_cache_key_is_per_environment_and_chain() -> None:
    assert KEY == "ohip:token:homologacao:CHAIN1"  # DV-2


async def test_token_is_reused_until_the_margin() -> None:
    h = TokenHarness()
    first = await h.provider.get()
    h.clock.advance(3600 - 301)
    assert await h.provider.get() is first
    h.clock.advance(2)  # entrou na margem de 300 s antes do exp
    second = await h.provider.get()
    assert second.value == "token-2"
    assert h.metrics.total("ohip_token_issued_total") == 2


async def test_token_is_shared_through_the_cache() -> None:
    h = TokenHarness()
    token = await h.provider.get()
    stored, ttl = h.cache.values[KEY]
    assert stored == token
    assert ttl == 3600 - 300  # vale no cache até exp - margem

    other_process = h.make()
    assert (await other_process.get()).value == token.value
    assert h.issuer.issued == 1  # pedir token é cobrado: reaproveita


async def test_expired_cached_token_is_not_used() -> None:
    h = TokenHarness()
    h.cache.values[KEY] = (AccessToken("velho", h.clock.now() + timedelta(seconds=10)), 1)
    assert (await h.provider.get()).value == "token-1"


async def test_concurrent_callers_share_one_issue() -> None:
    h = TokenHarness()
    tokens = await asyncio.gather(*(h.provider.get() for _ in range(10)))
    assert {t.value for t in tokens} == {"token-1"}
    assert h.issuer.issued == 1


async def test_redis_down_never_blocks_the_token() -> None:
    h = TokenHarness()
    h.cache.broken = True
    assert (await h.provider.get()).value == "token-1"
    await h.provider.invalidate()
    assert (await h.provider.get()).value == "token-2"


async def test_invalidate_after_4401_forces_a_new_token() -> None:
    h = TokenHarness()
    await h.provider.get()
    await h.provider.invalidate()
    assert KEY not in h.cache.values
    assert (await h.provider.get()).value == "token-2"


async def test_rejected_credentials_propagate() -> None:
    h = TokenHarness()
    h.issuer.fail = [AuthRejectedError("401")]
    with pytest.raises(AuthRejectedError):
        await h.provider.get()


# ------------------------------------------------------------------ lease


LEASE = "consumer:CHAIN1"
OPTIONS = LeaseOptions(ttl_s=30, renew_interval_s=10, acquire_jitter_s=5)


class LeaseHarness:
    def __init__(self) -> None:
        self.db = InMemoryDatabase()
        self.db.provision_chain("CHAIN1")
        self.store = MemoryLeaseStore(self.db)
        self.metrics = FakeMetrics()

    def keeper(self, owner: str) -> LeaseKeeper:
        return LeaseKeeper(
            store=self.store,
            clock=self.db.clock,
            metrics=self.metrics,
            lease_name=LEASE,
            owner=owner,
            options=OPTIONS,
            rng=random.Random(1),  # noqa: S311 - jitter determinístico no teste
        )


async def test_acquire_and_renew() -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    epoch = await keeper.acquire()
    assert keeper.epoch == epoch == h.db.leases[LEASE]  # a barreira dos stores vê o mesmo epoch
    h.db.clock.advance(10)
    assert await keeper.renew_once()
    h.db.clock.advance(25)
    assert keeper.epoch == epoch  # renovado: ainda vale
    assert h.metrics.total("ohip_lease_acquired_total") == 1


async def test_second_instance_waits_until_the_lease_expires() -> None:
    h = LeaseHarness()
    await h.keeper("vm1").acquire()  # vm1 morre sem liberar
    passive = h.keeper("vm2")
    epoch = await passive.acquire()  # dorme ttl + jitter até o prazo vencer
    assert h.db.clock.sleeps[0] >= 30
    assert epoch == 2


async def test_renewal_refused_means_lost() -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    await keeper.acquire()
    h.db.clock.advance(31)  # travou mais que o ttl
    await h.keeper("vm2").acquire()

    assert not await keeper.renew_once()
    assert keeper.lost
    assert keeper.epoch is None
    await keeper.release()  # perdido: não libera o lease do novo dono
    assert h.store.holders[LEASE][0] == "vm2"


async def test_database_down_keeps_lease_only_until_local_deadline() -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    await keeper.acquire()
    h.store.fail = [StoreUnavailableError("fora")] * 5
    h.db.clock.advance(10)
    assert await keeper.renew_once()  # sem resposta, mas dentro do prazo local
    h.db.clock.advance(21)
    assert not await keeper.renew_once()  # prazo local venceu: para de gravar
    assert keeper.lost
    assert keeper.epoch is None


async def test_keep_returns_when_lost(monkeypatch: pytest.MonkeyPatch) -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    await keeper.acquire()
    real_renew = h.store.renew
    calls = 0

    async def renew_then_lose(name: str, owner: str, epoch: int, ttl_s: float) -> bool:
        nonlocal calls
        calls += 1
        return await real_renew(name, owner, epoch, ttl_s) if calls <= 2 else False

    monkeypatch.setattr(h.store, "renew", renew_then_lose)
    await keeper.keep()

    assert calls == 3
    assert h.db.clock.sleeps == [10, 10]  # renovou a cada renew_interval_s
    assert keeper.lost
    assert h.metrics.total("ohip_lease_lost_total") == 1


async def test_release_lets_the_next_owner_in_immediately() -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    await keeper.acquire()
    await keeper.release()
    assert keeper.epoch is None
    await h.keeper("vm2").acquire()
    assert h.db.clock.sleeps == []  # não precisou esperar o prazo


async def test_acquire_retries_when_database_is_down() -> None:
    h = LeaseHarness()
    h.store.fail = [StoreUnavailableError("fora")]
    assert await h.keeper("vm1").acquire() == 1
    assert len(h.db.clock.sleeps) == 1


async def test_release_without_database_is_harmless() -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    await keeper.acquire()
    h.store.fail = [StoreUnavailableError("fora")]
    await keeper.release()  # vence sozinho pelo prazo
    assert keeper.epoch is None


async def test_unprovisioned_lease_is_a_configuration_error() -> None:
    h = LeaseHarness()
    keeper = LeaseKeeper(
        store=h.store,
        clock=FakeClock(),
        metrics=h.metrics,
        lease_name="consumer:NAOEXISTE",
        owner="vm1",
        options=OPTIONS,
    )
    with pytest.raises(LeaseNotProvisionedError):
        await keeper.acquire()


async def test_renew_without_lease_is_false() -> None:
    assert not await LeaseHarness().keeper("vm1").renew_once()


async def test_short_lived_token_uses_half_its_life_as_margin() -> None:
    h = TokenHarness(lifetime_s=120)  # menor que a margem de 300 s
    first = await h.provider.get()
    assert await h.provider.get() is first  # não emite a cada chamada
    assert h.cache.values[KEY][1] == 60  # guardado até metade da vida
    assert h.metrics.total("ohip_token_short_lived_total") == 1
    h.clock.advance(61)
    assert (await h.provider.get()).value == "token-2"


async def test_short_lived_token_is_shared_between_processes() -> None:
    h = TokenHarness(lifetime_s=120)
    first = await h.provider.get()
    other_process = h.make()  # lê do mesmo cache, com a vida original do token
    assert (await other_process.get()).value == first.value
    assert h.issuer.issued == 1


async def test_invalidate_waits_for_an_ongoing_get() -> None:
    h = TokenHarness()
    stale = AccessToken("recusado", h.clock.now() + timedelta(hours=1))
    h.cache.values[KEY] = (stale, 3000)
    getting = asyncio.create_task(h.provider.get())
    await asyncio.sleep(0)
    await h.provider.invalidate()
    await getting
    assert (await h.provider.get()).value == "token-1"  # o recusado não volta


class SlowStore(MemoryLeaseStore):
    async def acquire(self, lease_name: str, owner: str, ttl_s: float) -> int | None:
        epoch = await super().acquire(lease_name, owner, ttl_s)
        self.db.clock.advance(20)  # chamada lenta: o banco já contou 20 s do prazo
        return epoch


async def test_acquire_counts_the_deadline_from_the_call_start() -> None:
    h = LeaseHarness()
    h.store = SlowStore(h.db)
    keeper = h.keeper("vm1")
    await keeper.acquire()
    h.db.clock.advance(11)  # 31 s desde o início da chamada
    assert keeper.epoch is None


async def test_renew_after_local_deadline_does_not_revive() -> None:
    h = LeaseHarness()
    keeper = h.keeper("vm1")
    await keeper.acquire()
    h.db.clock.advance(31)
    assert not await keeper.renew_once()
    assert keeper.lost
    assert h.store.holders[LEASE][1] < h.db.clock.now()  # não renovou no banco
