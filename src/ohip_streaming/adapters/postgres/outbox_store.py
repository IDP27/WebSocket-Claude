"""``OutboxStore`` no PostgreSQL: cabeça da fila por chain e marcações do publisher (ADR-0002).

Toda marcação termina com a barreira de epoch do lease ``publisher`` (ADR-0008). As
atualizações são condicionadas a ``status = 'PENDING'``: uma linha que já saiu da fila não é
remarcada.
"""

from __future__ import annotations

from datetime import datetime

from ohip_streaming.adapters.postgres.database import Cursor, Session, fence, read, write
from ohip_streaming.adapters.postgres.outbox_chains import OUTBOX_CHAINS_CTE
from ohip_streaming.adapters.postgres.rows import (
    ERROR_MESSAGE_BYTES,
    from_db_required,
    to_db,
    truncate_bytes,
)
from ohip_streaming.application.ports import OutboxRow
from ohip_streaming.domain.leases import PUBLISHER_LEASE
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

# Por chain, pelo índice ohip_outbox_ix_head: nunca varre o histórico (B2, outbox_chains.py).
CHAINS_WITH_PENDING_SQL = (
    OUTBOX_CHAINS_CTE  # noqa: S608 - fragmento fixo deste pacote
    + """
SELECT c.chain_code
  FROM chains c
 WHERE c.chain_code IS NOT NULL
   AND EXISTS (SELECT 1 FROM ohip_outbox o
                WHERE o.chain_code = c.chain_code AND o.status = 'PENDING')
 ORDER BY c.chain_code"""
)

FETCH_HEAD_SQL = """
SELECT id, chain_code, exchange_name, routing_key, unique_event_id, message, schema_version,
       attempts, next_attempt_at
  FROM ohip_outbox
 WHERE chain_code = %(chain_code)s
   AND status = 'PENDING'
 ORDER BY id
 LIMIT %(limit)s"""

MARK_SENT_SQL = """
UPDATE ohip_outbox
   SET status = 'SENT', sent_at = statement_timestamp()
 WHERE id = %(id)s
   AND status = 'PENDING'"""

RECORD_FAILURE_SQL = """
UPDATE ohip_outbox
   SET attempts = %(attempts)s, next_attempt_at = %(next_attempt_at)s, last_error = %(last_error)s
 WHERE id = %(id)s
   AND status = 'PENDING'"""

MARK_FAILED_SQL = """
UPDATE ohip_outbox
   SET status = 'FAILED', attempts = %(attempts)s, last_error = %(last_error)s
 WHERE id = %(id)s
   AND status = 'PENDING'"""

INSERT_DLQ_PUBLISH_SQL = """
INSERT INTO ohip_dlq (
    id, stage, chain_code, event_raw_id, outbox_id, unique_event_id, error_class,
    error_message, code_version
)
SELECT nextval('ohip_dlq_seq'), 'PUBLISH', chain_code, event_raw_id, id, unique_event_id,
       %(error_class)s, %(error_message)s, %(code_version)s
  FROM ohip_outbox
 WHERE id = %(outbox_id)s"""

PUBLISH_ERROR_CLASS = "PUBLISH_FAILED"


class PostgresOutboxStore:
    """Implementa ``OutboxStore``. Use com uma ``PooledSession``."""

    def __init__(self, session: Session, *, code_version: str) -> None:
        self._session = session
        self._code_version = code_version

    async def chains_with_pending(self) -> list[str]:
        rows = await read(self._session, CHAINS_WITH_PENDING_SQL, {})
        return [str(row[0]) for row in rows]

    async def fetch_head(self, chain_code: str, limit: int) -> list[OutboxRow]:
        rows = await read(self._session, FETCH_HEAD_SQL, {"chain_code": chain_code, "limit": limit})
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

        await write(self._session, body)

    async def record_failure(
        self, row_id: int, attempts: int, next_attempt_at: datetime, error: str, lease_epoch: int
    ) -> None:
        def body(cursor: Cursor) -> None:
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

        await write(self._session, body)

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

        await write(self._session, body)

    @staticmethod
    def _check_pending(cursor: Cursor, row_id: int) -> bool:
        if cursor.rowcount == 1:
            return True
        # Só o dono do lease marca linhas; isto indica edição manual no banco.
        log.warning("outbox_linha_fora_da_fila", outbox_id=row_id)
        return False
