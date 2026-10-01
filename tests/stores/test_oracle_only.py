"""Comportamentos que só um Oracle de verdade prova (ADR-0011 §7, contrato do OperationsStore).

Sem DDL: os erros de espaço são levantados por PL/SQL com ``PRAGMA EXCEPTION_INIT``, que
produz o mesmo ORA-nnnnn que o driver recebe quando o tablespace enche.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest
from tests.stores.backends import OracleBackend, oracle_test_settings
from tests.stores.test_store_scenarios import CHAIN, batch, event, persist, records

from ohip_streaming.adapters.oracle.database import transaction
from ohip_streaming.application.errors import (
    BatchFailedError,
    LeaseLostError,
    StoreUnavailableError,
)

_settings = oracle_test_settings()
pytestmark = [
    pytest.mark.oracle,
    pytest.mark.skipif(_settings is None, reason="Oracle de teste não configurado"),
]

RAISE_SQL = """
DECLARE
    e EXCEPTION;
    PRAGMA EXCEPTION_INIT(e, -{code});
BEGIN
    RAISE e;
END;"""


@pytest.fixture
def oracle() -> Iterator[OracleBackend]:
    assert _settings is not None
    backend = OracleBackend.open(_settings)
    try:
        yield backend
    finally:
        backend.close()


@pytest.mark.parametrize(
    "code", [1653, 1654, 1688, 1691, 30036, 1536, 1628, 1631, 1632, 1650, 1658, 1683, 1692, 1000]
)
def test_space_errors_from_the_driver_are_unavailable(oracle: OracleBackend, code: int) -> None:
    connection = oracle.pool.acquire()
    try:
        with pytest.raises(StoreUnavailableError, match=f"ORA-{code:05d}"):
            transaction(
                connection,
                lambda cursor: cursor.execute(RAISE_SQL.format(code=code)),
                failure=BatchFailedError,
            )
    finally:
        oracle.pool.release(connection)


def test_other_errors_from_the_driver_are_batch_failures(oracle: OracleBackend) -> None:
    connection = oracle.pool.acquire()
    try:
        with pytest.raises(BatchFailedError, match="ORA-01476"):  # divisão por zero
            transaction(
                connection,
                lambda cursor: cursor.execute("SELECT 1/0 FROM dual"),
                failure=BatchFailedError,
            )
    finally:
        oracle.pool.release(connection)


async def test_concurrent_retry_publish_inserts_once(oracle: OracleBackend) -> None:
    """Outra sessão resolve o item enquanto o retry espera o lock: o retry não insere nada."""
    oracle.acquire(f"consumer:{CHAIN}")
    await persist(oracle, event(1))
    epoch = oracle.acquire("publisher")
    (row,) = await oracle.outbox.fetch_head(CHAIN, 1)
    await oracle.outbox.mark_failed(row.id, 10, "esgotado", epoch)
    (item,) = oracle.dlq_items(CHAIN)

    rival: Any = oracle.pool.acquire()
    try:
        cursor = rival.cursor()
        cursor.execute(
            "UPDATE ohip_dlq SET resolution = 'RETRIED', resolved_by = 'rival'"
            " WHERE id = :id AND resolution IS NULL",
            {"id": item.id},
        )
        assert cursor.rowcount == 1  # lock na linha, sem commit ainda

        retry = asyncio.create_task(oracle.operations.retry_publish(item.id, row.id, "ana"))
        await asyncio.sleep(1.0)
        assert not retry.done()  # esperando o lock do rival
        rival.commit()
        assert await asyncio.wait_for(retry, timeout=30) is None
    finally:
        oracle.pool.release(rival)

    assert [o.status for o in oracle.outbox_rows(CHAIN)] == ["FAILED"]  # nenhuma cópia


async def test_zombie_waiting_on_the_fence_loses_after_the_new_owner_commits(
    oracle: OracleBackend,
) -> None:
    """ADR-0008: o novo dono incrementa o epoch (sem commit); a barreira do zumbi espera o
    lock; depois do commit do rival, o UPDATE do zumbi não acha a linha e tudo é desfeito."""
    stale = oracle.acquire(f"consumer:{CHAIN}")
    recs = await records(oracle, event(1), event(2))

    rival: Any = oracle.pool.acquire()
    try:
        cursor = rival.cursor()
        cursor.execute(
            "UPDATE ohip_lease SET epoch = epoch + 1 WHERE lease_name = :n",
            {"n": f"consumer:{CHAIN}"},
        )
        zombie = asyncio.create_task(
            oracle.events.persist_batch(batch(oracle, recs, lease_epoch=stale))
        )
        await asyncio.sleep(1.0)
        assert not zombie.done()  # parado na barreira, esperando o lock do lease
        rival.commit()
        with pytest.raises(LeaseLostError):
            await asyncio.wait_for(zombie, timeout=30)
    finally:
        oracle.pool.release(rival)

    assert oracle.raw_count(CHAIN) == 0
    assert oracle.outbox_rows(CHAIN) == []
    assert oracle.offset(CHAIN) is None
