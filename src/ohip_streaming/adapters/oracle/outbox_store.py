"""``OutboxStore`` no Oracle: cabeça da fila por chain e marcações do publisher (ADR-0002).

Toda marcação termina com a barreira de epoch do lease ``publisher`` (ADR-0008). As
atualizações são condicionadas a ``status = 'PENDING'``: uma linha que já saiu da fila não é
remarcada.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import oracledb

from ohip_streaming.adapters.oracle.database import Cursor, Session, fence, query, transaction
from ohip_streaming.adapters.oracle.rows import (
    ERROR_MESSAGE_BYTES,
    from_db_required,
    to_db,
    truncate_bytes,
)
from ohip_streaming.application.errors import StoreOperationError
from ohip_streaming.application.ports import OutboxRow
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

PUBLISHER_LEASE = "publisher"

CHAINS_WITH_PENDING_SQL = """
SELECT DISTINCT chain_code FROM ohip_outbox WHERE status = 'PENDING' ORDER BY chain_code"""

FETCH_HEAD_SQL = """
SELECT id, chain_code, exchange_name, routing_key, unique_event_id, message, schema_version,
       attempts, next_attempt_at
  FROM (SELECT id, chain_code, exchange_name, routing_key, unique_event_id, message,
               schema_version, attempts, next_attempt_at
          FROM ohip_outbox
         WHERE chain_code = :chain_code
           AND status = 'PENDING'
         ORDER BY id)
 WHERE ROWNUM <= :limit"""

MARK_SENT_SQL = """
UPDATE ohip_outbox
   SET status = 'SENT', sent_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE id = :id
   AND status = 'PENDING'"""

RECORD_FAILURE_SQL = """
UPDATE ohip_outbox
   SET attempts = :attempts, next_attempt_at = :next_attempt_at, last_error = :last_error
 WHERE id = :id
   AND status = 'PENDING'"""

MARK_FAILED_SQL = """
UPDATE ohip_outbox
   SET status = 'FAILED', attempts = :attempts, last_error = :last_error
 WHERE id = :id
   AND status = 'PENDING'"""

INSERT_DLQ_PUBLISH_SQL = """
INSERT INTO ohip_dlq (
    id, stage, chain_code, event_raw_id, outbox_id, unique_event_id, error_class,
    error_message, code_version
)
SELECT ohip_dlq_seq.NEXTVAL, 'PUBLISH', chain_code, event_raw_id, id, unique_event_id,
       :error_class, :error_message, :code_version
  FROM ohip_outbox
 WHERE id = :outbox_id"""

PUBLISH_ERROR_CLASS = "PUBLISH_FAILED"


class OracleOutboxStore:
    """Implementa ``OutboxStore``. Use com uma ``PooledSession``."""

    def __init__(self, session: Session, *, code_version: str) -> None:
        self._session = session
        self._code_version = code_version

    async def chains_with_pending(self) -> list[str]:
        rows = await self._session.run(
            lambda connection: query(
                connection, CHAINS_WITH_PENDING_SQL, {}, failure=StoreOperationError
            )
        )
        return [str(row[0]) for row in rows]

    async def fetch_head(self, chain_code: str, limit: int) -> list[OutboxRow]:
        rows = await self._session.run(
            lambda connection: query(
                connection,
                FETCH_HEAD_SQL,
                {"chain_code": chain_code, "limit": limit},
                failure=StoreOperationError,
            )
        )
        return [
            OutboxRow(
                id=int(row_id),
                chain_code=chain,
                exchange_name=exchange,
                routing_key=routing_key,
                message_id=message_id,
                body=body,
                schema_version=int(schema_version),
                attempts=int(attempts),
                next_attempt_at=from_db_required(next_attempt_at),
            )
            for (
                row_id,
                chain,
                exchange,
                routing_key,
                message_id,
                body,
                schema_version,
                attempts,
                next_attempt_at,
            ) in rows
        ]

    async def mark_sent(self, row_id: int, lease_epoch: int) -> None:
        def body(cursor: Cursor) -> None:
            cursor.execute(MARK_SENT_SQL, {"id": row_id})
            self._check_pending(cursor, row_id)
            fence(cursor, PUBLISHER_LEASE, lease_epoch)

        await self._write(body)

    async def record_failure(
        self, row_id: int, attempts: int, next_attempt_at: datetime, error: str, lease_epoch: int
    ) -> None:
        def body(cursor: Cursor) -> None:
            cursor.setinputsizes(next_attempt_at=oracledb.DB_TYPE_TIMESTAMP)  # com fração
            cursor.execute(
                RECORD_FAILURE_SQL,
                {
                    "id": row_id,
                    "attempts": attempts,
                    "next_attempt_at": to_db(next_attempt_at),
                    "last_error": truncate_bytes(error, ERROR_MESSAGE_BYTES),
                },
            )
            self._check_pending(cursor, row_id)
            fence(cursor, PUBLISHER_LEASE, lease_epoch)

        await self._write(body)

    async def mark_failed(self, row_id: int, attempts: int, error: str, lease_epoch: int) -> None:
        message = truncate_bytes(error, ERROR_MESSAGE_BYTES)

        def body(cursor: Cursor) -> None:
            cursor.execute(
                MARK_FAILED_SQL, {"id": row_id, "attempts": attempts, "last_error": message}
            )
            if self._check_pending(cursor, row_id):
                cursor.execute(
                    INSERT_DLQ_PUBLISH_SQL,
                    {
                        "outbox_id": row_id,
                        "error_class": PUBLISH_ERROR_CLASS,
                        "error_message": message,
                        "code_version": self._code_version,
                    },
                )
            fence(cursor, PUBLISHER_LEASE, lease_epoch)

        await self._write(body)

    async def _write(self, body: Callable[[Cursor], None]) -> None:
        await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    @staticmethod
    def _check_pending(cursor: Cursor, row_id: int) -> bool:
        if cursor.rowcount == 1:
            return True
        # Só o dono do lease marca linhas; isto indica edição manual no banco.
        log.warning("outbox_linha_fora_da_fila", outbox_id=row_id)
        return False
