"""``LeaseStore`` no PostgreSQL (ADR-0008). Todo horário vem do banco.

As linhas são semeadas no provisionamento (``sql/003_seed_leases.sql``); a aplicação nunca as
cria (dois ``INSERT`` concorrentes numa linha inexistente dariam ``23505``). Cada operação é
uma transação curta na conexão de controle, separada da conexão de escrita.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from ohip_streaming.adapters.postgres.database import Cursor, Session, transaction
from ohip_streaming.application.errors import LeaseNotProvisionedError, StoreOperationError

T = TypeVar("T")

ACQUIRE_SQL = """
UPDATE ohip_lease
   SET epoch = epoch + 1,
       owner = %(owner)s,
       acquired_at = statement_timestamp(),
       expires_at = statement_timestamp() + make_interval(secs => %(ttl_s)s)
 WHERE lease_name = %(lease_name)s
   AND (expires_at < statement_timestamp() OR owner = %(owner)s)
RETURNING epoch"""

EXISTS_SQL = "SELECT COUNT(*) FROM ohip_lease WHERE lease_name = %(lease_name)s"

RENEW_SQL = """
UPDATE ohip_lease
   SET expires_at = statement_timestamp() + make_interval(secs => %(ttl_s)s)
 WHERE lease_name = %(lease_name)s
   AND owner = %(owner)s
   AND epoch = %(epoch)s"""

RELEASE_SQL = """
UPDATE ohip_lease
   SET expires_at = statement_timestamp()
 WHERE lease_name = %(lease_name)s
   AND owner = %(owner)s
   AND epoch = %(epoch)s"""


class PostgresLeaseStore:
    """Implementa ``LeaseStore``. Use a sessão de controle (``PooledSession``)."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def acquire(self, lease_name: str, owner: str, ttl_s: float) -> int | None:
        def body(cursor: Cursor) -> int | None:
            cursor.execute(ACQUIRE_SQL, {"lease_name": lease_name, "owner": owner, "ttl_s": ttl_s})
            row = cursor.fetchone()
            if row is not None:
                return int(row[0])
            cursor.execute(EXISTS_SQL, {"lease_name": lease_name})
            (count,) = cursor.fetchone()
            if not count:
                raise LeaseNotProvisionedError(f"lease {lease_name} não existe em OHIP_LEASE")
            return None  # outro dono com prazo válido

        return await self._run(body)

    async def renew(self, lease_name: str, owner: str, epoch: int, ttl_s: float) -> bool:
        def body(cursor: Cursor) -> bool:
            cursor.execute(
                RENEW_SQL,
                {"lease_name": lease_name, "owner": owner, "epoch": epoch, "ttl_s": ttl_s},
            )
            return cursor.rowcount == 1

        return await self._run(body)

    async def release(self, lease_name: str, owner: str, epoch: int) -> None:
        def body(cursor: Cursor) -> None:
            cursor.execute(RELEASE_SQL, {"lease_name": lease_name, "owner": owner, "epoch": epoch})

        await self._run(body)

    async def _run(self, body: Callable[[Cursor], T]) -> T:
        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )
