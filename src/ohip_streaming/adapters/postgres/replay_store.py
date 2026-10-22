"""``ReplayStore`` no PostgreSQL (ARCHITECTURE §4.6, API.md).

O pedido é gravado pela API e aplicado pelo consumer com a conexão do OHIP fechada, numa
transação com barreira de epoch. O índice único parcial ``ohip_replay_ux_pending`` garante
no máximo um pedido ``PENDING`` por chain.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import psycopg

from ohip_streaming.adapters.postgres.database import Cursor, Session, fence, query, transaction
from ohip_streaming.adapters.postgres.errors import violated_constraint
from ohip_streaming.adapters.postgres.event_store import consumer_lease
from ohip_streaming.adapters.postgres.rows import from_db, from_db_required
from ohip_streaming.application.errors import (
    ReplayAlreadyPendingError,
    StoreOperationError,
    UnknownChainError,
)
from ohip_streaming.application.ports import ReplayRequest, ReplayStatus
from ohip_streaming.domain.offset import Offset

PENDING_INDEX = "ohip_replay_ux_pending"

LAST_OFFSET_SQL = "SELECT last_offset FROM ohip_offset WHERE chain_code = %(chain_code)s"

OFFSET_RECEIVED_AT_SQL = """
SELECT MIN(received_at)
  FROM ohip_event_raw
 WHERE chain_code = %(chain_code)s
   AND offset_value = %(offset_value)s"""

INSERT_REQUEST_SQL = """
INSERT INTO ohip_replay_request (id, chain_code, from_offset, reason, requested_by)
VALUES (
    nextval('ohip_replay_req_seq'), %(chain_code)s, %(from_offset)s, %(reason)s, %(requested_by)s
)
RETURNING id, created_at"""

PENDING_REQUEST_SQL = """
SELECT id, chain_code, from_offset, reason, requested_by, status, created_at
  FROM ohip_replay_request
 WHERE chain_code = %(chain_code)s
   AND status = 'PENDING'"""

APPLY_REQUEST_SQL = """
UPDATE ohip_replay_request
   SET status = 'APPLIED', applied_at = statement_timestamp()
 WHERE id = %(id)s
   AND status = 'PENDING'"""

MOVE_OFFSET_SQL = """
UPDATE ohip_offset
   SET last_offset = %(last_offset)s,
       last_unique_event_id = NULL,
       updated_at = statement_timestamp()
 WHERE chain_code = %(chain_code)s"""

CANCEL_REQUEST_SQL = """
UPDATE ohip_replay_request
   SET status = 'CANCELLED',
       cancelled_at = statement_timestamp(),
       cancelled_by = %(cancelled_by)s
 WHERE id = %(id)s
   AND status = 'PENDING'"""


def _request(row: Sequence[Any]) -> ReplayRequest:
    request_id, chain_code, from_offset, reason, requested_by, status, created_at = row
    return ReplayRequest(
        id=int(request_id),
        chain_code=chain_code,
        from_offset=Offset(from_offset),
        reason=reason,
        requested_by=requested_by,
        status=ReplayStatus(status),
        created_at=from_db_required(created_at),
    )


class PostgresReplayStore:
    """Implementa ``ReplayStore``. API e consumer usam ``PooledSession``/``DedicatedSession``."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def last_offset(self, chain_code: str) -> Offset | None:
        rows = await self._query(LAST_OFFSET_SQL, {"chain_code": chain_code})
        if not rows:
            raise UnknownChainError(chain_code)
        value = rows[0][0]
        return Offset(value) if value is not None else None

    async def offset_received_at(self, chain_code: str, offset: Offset) -> datetime | None:
        rows = await self._query(
            OFFSET_RECEIVED_AT_SQL, {"chain_code": chain_code, "offset_value": offset.value}
        )
        return from_db(rows[0][0]) if rows else None

    async def create_request(
        self, chain_code: str, from_offset: Offset, reason: str, requested_by: str
    ) -> ReplayRequest:
        def body(cursor: Cursor) -> ReplayRequest:
            try:
                cursor.execute(
                    INSERT_REQUEST_SQL,
                    {
                        "chain_code": chain_code,
                        "from_offset": from_offset.value,
                        "reason": reason,
                        "requested_by": requested_by,
                    },
                )
            except psycopg.IntegrityError as exc:
                if violated_constraint(exc) == PENDING_INDEX:
                    raise ReplayAlreadyPendingError(chain_code) from exc
                raise
            request_id, created_at = cursor.fetchone()
            return ReplayRequest(
                id=int(request_id),
                chain_code=chain_code,
                from_offset=from_offset,
                reason=reason,
                requested_by=requested_by,
                status=ReplayStatus.PENDING,
                created_at=from_db_required(created_at),
            )

        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    async def pending_request(self, chain_code: str) -> ReplayRequest | None:
        rows = await self._query(PENDING_REQUEST_SQL, {"chain_code": chain_code})
        return _request(rows[0]) if rows else None

    async def apply_request(self, request: ReplayRequest, lease_epoch: int) -> bool:
        def body(cursor: Cursor) -> bool:
            cursor.execute(APPLY_REQUEST_SQL, {"id": request.id})
            if cursor.rowcount != 1:  # cancelado entre a leitura e a aplicação
                return False
            cursor.execute(
                MOVE_OFFSET_SQL,
                {"last_offset": request.from_offset.value, "chain_code": request.chain_code},
            )
            if cursor.rowcount != 1:
                raise UnknownChainError(request.chain_code)
            fence(cursor, consumer_lease(request.chain_code), lease_epoch)
            return True

        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    async def cancel_request(self, request_id: int, cancelled_by: str) -> bool:
        def body(cursor: Cursor) -> bool:
            cursor.execute(CANCEL_REQUEST_SQL, {"id": request_id, "cancelled_by": cancelled_by})
            return cursor.rowcount == 1

        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    async def _query(self, statement: str, parameters: dict[str, Any]) -> list[Sequence[Any]]:
        return await self._session.run(
            lambda connection: query(connection, statement, parameters, failure=StoreOperationError)
        )
