"""``LeaseStore`` no Oracle (ADR-0008). Todo horário vem do banco.

As linhas são semeadas no provisionamento (``sql/003_seed_leases.sql``); a aplicação nunca as
cria (dois ``MERGE`` concorrentes numa linha inexistente dariam ORA-00001). Cada operação é
uma transação curta na conexão de controle, separada da conexão de escrita.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from ohip_streaming.adapters.oracle.database import Cursor, Session, transaction
from ohip_streaming.application.errors import LeaseNotProvisionedError, StoreOperationError

T = TypeVar("T")

ACQUIRE_SQL = """
UPDATE ohip_lease
   SET epoch = epoch + 1,
       owner = :owner,
       acquired_at = SYS_EXTRACT_UTC(SYSTIMESTAMP),
       expires_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTODSINTERVAL(:ttl_s, 'SECOND')
 WHERE lease_name = :lease_name
   AND (expires_at < SYS_EXTRACT_UTC(SYSTIMESTAMP) OR owner = :owner)
RETURNING epoch INTO :epoch"""

EXISTS_SQL = "SELECT COUNT(*) FROM ohip_lease WHERE lease_name = :lease_name"

RENEW_SQL = """
UPDATE ohip_lease
   SET expires_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTODSINTERVAL(:ttl_s, 'SECOND')
 WHERE lease_name = :lease_name
   AND owner = :owner
   AND epoch = :epoch"""

RELEASE_SQL = """
UPDATE ohip_lease
   SET expires_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE lease_name = :lease_name
   AND owner = :owner
   AND epoch = :epoch"""


class OracleLeaseStore:
    """Implementa ``LeaseStore``. Use a sessão de controle (``PooledSession``)."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def acquire(self, lease_name: str, owner: str, ttl_s: float) -> int | None:
        def body(cursor: Cursor) -> int | None:
            epoch_var = cursor.var(int)
            cursor.execute(
                ACQUIRE_SQL,
                {"lease_name": lease_name, "owner": owner, "ttl_s": ttl_s, "epoch": epoch_var},
            )
            if cursor.rowcount == 1:
                (epoch,) = epoch_var.getvalue()
                return int(epoch)
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
