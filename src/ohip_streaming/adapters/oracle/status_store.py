"""``ConsumerStatusStore`` no Oracle: estado da conexão por chain (OHIP_CONSUMER_STATUS).

Os horários usados na regra dos 10 s vêm do relógio do banco (ADR-0007), nunca da VM.
O estado é informativo e não passa pela barreira de epoch (só a escrita de dados passa).
"""

from __future__ import annotations

from collections.abc import Callable

from ohip_streaming.adapters.oracle.database import Cursor, Session, query, transaction
from ohip_streaming.adapters.oracle.rows import (
    CLOSE_REASON_BYTES,
    from_db,
    from_db_required,
    truncate_bytes,
)
from ohip_streaming.application.errors import StoreOperationError, UnknownChainError
from ohip_streaming.application.ports import DisconnectSnapshot
from ohip_streaming.domain.connection import ConsumerState

RECORD_STATE_SQL = """
UPDATE ohip_consumer_status
   SET state = :state, instance_id = :instance_id, updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE chain_code = :chain_code"""

RECORD_DISCONNECT_SQL = """
UPDATE ohip_consumer_status
   SET state = :state,
       last_disconnect_at = SYS_EXTRACT_UTC(SYSTIMESTAMP),
       last_close_code = :close_code,
       last_close_reason = :close_reason,
       updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE chain_code = :chain_code"""

SNAPSHOT_SQL = """
SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP), last_disconnect_at, state
  FROM ohip_consumer_status
 WHERE chain_code = :chain_code"""


class OracleConsumerStatusStore:
    """Implementa ``ConsumerStatusStore``. Use a sessão de controle (``PooledSession``)."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def record_state(self, chain_code: str, state: ConsumerState, instance_id: str) -> None:
        await self._update(
            chain_code,
            lambda cursor: cursor.execute(
                RECORD_STATE_SQL,
                {"state": state.value, "instance_id": instance_id, "chain_code": chain_code},
            ),
        )

    async def record_disconnect(
        self,
        chain_code: str,
        state: ConsumerState,
        close_code: int | None,
        close_reason: str | None,
    ) -> None:
        reason = truncate_bytes(close_reason, CLOSE_REASON_BYTES) if close_reason else None
        await self._update(
            chain_code,
            lambda cursor: cursor.execute(
                RECORD_DISCONNECT_SQL,
                {
                    "state": state.value,
                    "close_code": close_code,
                    "close_reason": reason,
                    "chain_code": chain_code,
                },
            ),
        )

    async def disconnect_snapshot(self, chain_code: str) -> DisconnectSnapshot:
        rows = await self._session.run(
            lambda connection: query(
                connection, SNAPSHOT_SQL, {"chain_code": chain_code}, failure=StoreOperationError
            )
        )
        if not rows:
            raise UnknownChainError(chain_code)
        db_now, last_disconnect_at, state = rows[0]
        return DisconnectSnapshot(
            db_now=from_db_required(db_now),
            last_disconnect_at=from_db(last_disconnect_at),
            last_state=ConsumerState(state) if state else None,
        )

    async def _update(self, chain_code: str, statement: Callable[[Cursor], object]) -> None:
        def body(cursor: Cursor) -> None:
            statement(cursor)
            if cursor.rowcount != 1:
                raise UnknownChainError(chain_code)

        await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )
