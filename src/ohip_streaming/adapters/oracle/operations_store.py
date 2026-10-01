"""``OperationsStore`` no Oracle: ações da API sobre eventos e DLQ (ARCHITECTURE §4.4).

Retries são **uma transação** condicionada a ``resolution IS NULL``, nesta ordem (contrato do
port): ``UPDATE`` do item primeiro, que trava a linha e serializa pedidos concorrentes;
``rowcount = 0`` → rollback sem inserir nada; só então o ``INSERT`` na outbox.

A API não é dona de lease: o que ela insere na outbox é lido pelo publisher, que publica.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeVar

import oracledb

from ohip_streaming.adapters.oracle.database import Cursor, Session, query, transaction
from ohip_streaming.adapters.oracle.rows import stored_event_from_row
from ohip_streaming.application.errors import StoreOperationError
from ohip_streaming.application.ports import DlqItem, DlqStage, StoredEvent
from ohip_streaming.domain.messages import QueueMessage

T = TypeVar("T")

# Colunas na ordem de rows.stored_event_from_row.
EVENT_BY_UID_SQL = """
SELECT id, chain_code, subscription_id, event_ts, payload, processing_status, received_at
  FROM ohip_event_raw
 WHERE unique_event_id = :uid"""
EVENT_BY_ID_SQL = """
SELECT id, chain_code, subscription_id, event_ts, payload, processing_status, received_at
  FROM ohip_event_raw
 WHERE id = :id"""

ENQUEUE_SQL = """
INSERT INTO ohip_outbox (
    id, event_raw_id, chain_code, unique_event_id, exchange_name, routing_key, message,
    schema_version
) VALUES (
    ohip_outbox_seq.NEXTVAL, :event_raw_id, :chain_code, :unique_event_id, :exchange_name,
    :routing_key, :message, :schema_version
)
RETURNING id INTO :new_id"""

DLQ_ITEM_SQL = """
SELECT id, stage, chain_code, event_raw_id, outbox_id, unique_event_id, resolution
  FROM ohip_dlq
 WHERE id = :id"""

REQUEST_CONSUME_RETRY_SQL = """
UPDATE ohip_dlq
   SET retry_requested_at = SYS_EXTRACT_UTC(SYSTIMESTAMP), retry_requested_by = :requested_by
 WHERE id = :id
   AND stage = 'CONSUME'
   AND resolution IS NULL
   AND retry_requested_at IS NULL"""

RESOLVE_SQL = """
UPDATE ohip_dlq
   SET resolution = 'RETRIED',
       resolved_at = SYS_EXTRACT_UTC(SYSTIMESTAMP),
       resolved_by = :resolved_by,
       retry_requested_at = NULL,
       retry_requested_by = NULL
 WHERE id = :id
   AND resolution IS NULL"""

NEXT_OUTBOX_ID_SQL = "SELECT ohip_outbox_seq.NEXTVAL FROM dual"

# INSERT ... SELECT não aceita RETURNING no 11g: o id vem antes, da sequence.
COPY_OUTBOX_SQL = """
INSERT INTO ohip_outbox (
    id, event_raw_id, chain_code, unique_event_id, exchange_name, routing_key, message,
    schema_version
)
SELECT :new_id, event_raw_id, chain_code, unique_event_id, exchange_name, routing_key, message,
       schema_version
  FROM ohip_outbox
 WHERE id = :outbox_id"""


class OracleOperationsStore:
    """Implementa ``OperationsStore``. Use com uma ``PooledSession``."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def find_event(self, unique_event_id: str) -> StoredEvent | None:
        rows = await self._query(EVENT_BY_UID_SQL, {"uid": unique_event_id})
        return stored_event_from_row(rows[0]) if rows else None

    async def get_event(self, raw_event_id: int) -> StoredEvent | None:
        rows = await self._query(EVENT_BY_ID_SQL, {"id": raw_event_id})
        return stored_event_from_row(rows[0]) if rows else None

    async def enqueue(
        self, *, chain_code: str, raw_event_id: int, message: QueueMessage, exchange_name: str
    ) -> int:
        return await self._write(
            lambda cursor: self._insert_message(
                cursor, chain_code, raw_event_id, message, exchange_name
            )
        )

    async def get_dlq_item(self, item_id: int) -> DlqItem | None:
        rows = await self._query(DLQ_ITEM_SQL, {"id": item_id})
        if not rows:
            return None
        item_id_, stage, chain_code, raw_id, outbox_id, uid, resolution = rows[0]
        return DlqItem(
            id=int(item_id_),
            stage=DlqStage(stage),
            chain_code=chain_code,
            event_raw_id=int(raw_id) if raw_id is not None else None,
            outbox_id=int(outbox_id) if outbox_id is not None else None,
            unique_event_id=uid,
            resolved=resolution is not None,
        )

    async def request_consume_retry(self, item_id: int, requested_by: str) -> bool:
        def body(cursor: Cursor) -> bool:
            cursor.execute(REQUEST_CONSUME_RETRY_SQL, {"id": item_id, "requested_by": requested_by})
            return cursor.rowcount == 1

        return await self._write(body)

    async def retry_publish(self, item_id: int, outbox_id: int, requested_by: str) -> int | None:
        def body(cursor: Cursor) -> int | None:
            if not self._resolve(cursor, item_id, requested_by):
                return None
            cursor.execute(NEXT_OUTBOX_ID_SQL)
            (new_id,) = cursor.fetchone()
            cursor.execute(COPY_OUTBOX_SQL, {"new_id": new_id, "outbox_id": outbox_id})
            if cursor.rowcount != 1:
                raise StoreOperationError(f"linha {outbox_id} da outbox não existe")
            return int(new_id)

        return await self._write(body)

    async def enqueue_and_resolve(
        self,
        item_id: int,
        *,
        chain_code: str,
        raw_event_id: int,
        message: QueueMessage,
        exchange_name: str,
        requested_by: str,
    ) -> int | None:
        def body(cursor: Cursor) -> int | None:
            if not self._resolve(cursor, item_id, requested_by):
                return None
            return self._insert_message(cursor, chain_code, raw_event_id, message, exchange_name)

        return await self._write(body)

    # ------------------------------------------------------------------- apoio

    @staticmethod
    def _resolve(cursor: Cursor, item_id: int, resolved_by: str) -> bool:
        """Primeiro comando dos retries: trava o item; 0 linhas = já resolvido."""
        cursor.execute(RESOLVE_SQL, {"id": item_id, "resolved_by": resolved_by})
        return bool(cursor.rowcount == 1)

    @staticmethod
    def _insert_message(
        cursor: Cursor, chain_code: str, raw_event_id: int, message: QueueMessage, exchange: str
    ) -> int:
        new_id = cursor.var(int)
        cursor.setinputsizes(message=oracledb.DB_TYPE_CLOB)
        cursor.execute(
            ENQUEUE_SQL,
            {
                "event_raw_id": raw_event_id,
                "chain_code": chain_code,
                "unique_event_id": message.message_id,
                "exchange_name": exchange,
                "routing_key": message.routing_key,
                "message": message.body,
                "schema_version": message.schema_version,
                "new_id": new_id,
            },
        )
        (value,) = new_id.getvalue()
        return int(value)

    async def _query(self, statement: str, parameters: dict[str, Any]) -> list[Sequence[Any]]:
        return await self._session.run(
            lambda connection: query(connection, statement, parameters, failure=StoreOperationError)
        )

    async def _write(self, body: Callable[[Cursor], T]) -> T:
        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )
