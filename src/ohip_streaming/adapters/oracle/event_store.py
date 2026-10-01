"""``EventStore`` no Oracle: a transação do lote do consumer (ARCHITECTURE §4.1, ADR-0008/0009).

Ordem dentro da transação (o contrato está em ``EventStore.persist_batch``):

1. ``INSERT`` dos eventos com ``executemany(batcherrors=True)``: ORA-00001 numa constraint de
   dedup = duplicado; erro de recurso por linha (ex.: ORA-01653) = banco indisponível (o lote
   inteiro é desfeito); qualquer outro erro por linha → DLQ ``CONSUME``;
2. outbox dos eventos gravados, na ordem de chegada; 3. DLQ; 4. item de retry (se houver);
5. offset; 6. barreira de epoch; commit.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import oracledb

from ohip_streaming.adapters.oracle.database import (
    Cursor,
    Session,
    executemany,
    fence,
    query,
    transaction,
)
from ohip_streaming.adapters.oracle.errors import (
    describe,
    is_dedup_violation,
    is_unavailable,
)
from ohip_streaming.adapters.oracle.rows import (
    ERROR_CLASS_BYTES,
    ERROR_MESSAGE_BYTES,
    from_db_required,
    to_db,
    truncate_bytes,
)
from ohip_streaming.application.errors import (
    BatchFailedError,
    StoreOperationError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    BatchResult,
    BatchToPersist,
    ConsumeRetryItem,
    RowFailure,
)
from ohip_streaming.domain.messages import ExchangeKind
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

RESERVE_IDS_SQL = "SELECT ohip_event_raw_seq.NEXTVAL FROM dual CONNECT BY LEVEL <= :count"

INSERT_RAW_SQL = """
INSERT INTO ohip_event_raw (
    id, chain_code, hotel_id, offset_value, unique_event_id, subscription_id, module_name,
    event_name, primary_key, publisher_id, action_instance_id, event_ts, payload,
    processing_status, received_at
) VALUES (
    :id, :chain_code, :hotel_id, :offset_value, :unique_event_id, :subscription_id, :module_name,
    :event_name, :primary_key, :publisher_id, :action_instance_id, :event_ts, :payload,
    :processing_status, :received_at
)"""

INSERT_OUTBOX_SQL = """
INSERT INTO ohip_outbox (
    id, event_raw_id, chain_code, unique_event_id, exchange_name, routing_key, message,
    schema_version
) VALUES (
    ohip_outbox_seq.NEXTVAL, :event_raw_id, :chain_code, :unique_event_id, :exchange_name,
    :routing_key, :message, :schema_version
)"""

INSERT_DLQ_CONSUME_SQL = """
INSERT INTO ohip_dlq (
    id, stage, chain_code, unique_event_id, offset_value, raw_message, error_class,
    error_message, code_version
) VALUES (
    ohip_dlq_seq.NEXTVAL, 'CONSUME', :chain_code, :unique_event_id, :offset_value, :raw_message,
    :error_class, :error_message, :code_version
)"""

RESOLVE_RETRIED_SQL = """
UPDATE ohip_dlq
   SET resolution = 'RETRIED',
       resolved_at = SYS_EXTRACT_UTC(SYSTIMESTAMP),
       resolved_by = :resolved_by,
       retry_requested_at = NULL,
       retry_requested_by = NULL
 WHERE id = :id
   AND resolution IS NULL"""

RETRY_FAILED_SQL = """
UPDATE ohip_dlq
   SET error_class = :error_class,
       error_message = :error_message,
       attempts = attempts + 1,
       retry_requested_at = NULL,
       retry_requested_by = NULL
 WHERE id = :id
   AND resolution IS NULL"""

RETRY_FAILED_RETURNING_SQL = RETRY_FAILED_SQL + "\nRETURNING chain_code INTO :chain_code"

UPDATE_OFFSET_SQL = """
UPDATE ohip_offset
   SET last_offset = :last_offset,
       last_unique_event_id = :last_unique_event_id,
       updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE chain_code = :chain_code"""

PENDING_RETRIES_SQL = """
SELECT id, raw_message, created_at
  FROM (SELECT id, raw_message, created_at
          FROM ohip_dlq
         WHERE stage = 'CONSUME'
           AND chain_code = :chain_code
           AND resolution IS NULL
           AND retry_requested_at IS NOT NULL
         ORDER BY id)
 WHERE ROWNUM <= :limit"""

REJECTED_ERROR_CLASS = "REJECTED"  # mensagem recusada pelo domínio (não virou evento)
RETRY_RESOLVER = "consumer"


def consumer_lease(chain_code: str) -> str:
    return f"consumer:{chain_code}"


class OracleEventStore:
    """Implementa ``EventStore``. Use com uma ``DedicatedSession`` (um thread por consumer)."""

    def __init__(
        self,
        session: Session,
        *,
        exchange_names: Mapping[ExchangeKind, str],
        code_version: str,
    ) -> None:
        missing = set(ExchangeKind) - set(exchange_names)
        if missing:  # falhar na composição, nunca no meio da transação do lote
            raise ValueError(f"exchange_names sem {sorted(k.value for k in missing)}")
        self._session = session
        self._exchange_names = dict(exchange_names)
        self._code_version = code_version

    # ------------------------------------------------------------------- port

    async def reserve_event_ids(self, count: int) -> list[int]:
        if count <= 0:
            return []
        rows = await self._session.run(
            lambda connection: query(
                connection, RESERVE_IDS_SQL, {"count": count}, failure=StoreOperationError
            )
        )
        return sorted(int(row[0]) for row in rows)

    async def persist_batch(self, batch: BatchToPersist) -> BatchResult:
        return await self._session.run(
            lambda connection: transaction(
                connection, lambda cursor: self._persist(cursor, batch), failure=BatchFailedError
            )
        )

    async def pending_consume_retries(self, chain_code: str, limit: int) -> list[ConsumeRetryItem]:
        rows = await self._session.run(
            lambda connection: query(
                connection,
                PENDING_RETRIES_SQL,
                {"chain_code": chain_code, "limit": limit},
                failure=StoreOperationError,
            )
        )
        return [
            ConsumeRetryItem(int(dlq_id), raw or "", from_db_required(created_at))
            for dlq_id, raw, created_at in rows
        ]

    async def mark_consume_retry_failed(self, dlq_id: int, reason: str, lease_epoch: int) -> None:
        def body(cursor: Cursor) -> None:
            chain_var = cursor.var(str)
            cursor.execute(
                RETRY_FAILED_RETURNING_SQL,
                {
                    **self._retry_failure(dlq_id, REJECTED_ERROR_CLASS, reason),
                    "chain_code": chain_var,
                },
            )
            if cursor.rowcount != 1:  # item resolvido ou removido nesse meio-tempo
                log.warning("retry_dlq_item_nao_aberto", dlq_id=dlq_id)
                return
            (chain_code,) = chain_var.getvalue()
            fence(cursor, consumer_lease(chain_code), lease_epoch)

        await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    # ------------------------------------------------------------------- lote

    def _persist(self, cursor: Cursor, batch: BatchToPersist) -> BatchResult:
        inserted, duplicates, failures = self._insert_events(cursor, batch)
        self._insert_outbox(cursor, batch, set(inserted))
        if batch.retry_of_dlq_id is None:
            self._insert_dlq(cursor, batch, failures)
        else:
            self._settle_retry(cursor, batch.retry_of_dlq_id, inserted, duplicates, failures)
        if batch.offset is not None:
            cursor.execute(
                UPDATE_OFFSET_SQL,
                {
                    "last_offset": batch.offset.value,
                    "last_unique_event_id": batch.last_unique_event_id,
                    "chain_code": batch.chain_code,
                },
            )
            if cursor.rowcount != 1:
                raise UnknownChainError(f"chain {batch.chain_code} sem linha em OHIP_OFFSET")
        fence(cursor, consumer_lease(batch.chain_code), batch.lease_epoch)
        return BatchResult(tuple(inserted), tuple(duplicates), tuple(failures))

    def _insert_events(
        self, cursor: Cursor, batch: BatchToPersist
    ) -> tuple[list[str], list[str], list[RowFailure]]:
        if not batch.events:
            return [], [], []
        rows = [
            {
                "id": record.raw_event_id,
                "chain_code": record.event.chain_code,
                "hotel_id": record.event.hotel_id,
                "offset_value": record.event.offset.value,
                "unique_event_id": record.event.unique_event_id,
                "subscription_id": record.event.subscription_id,
                "module_name": record.event.module_name,
                "event_name": record.event.event_name,
                "primary_key": record.event.primary_key,
                "publisher_id": record.event.publisher_id,
                "action_instance_id": record.event.action_instance_id,
                "event_ts": to_db(record.event.event_ts),
                "payload": record.event.payload_json,
                "processing_status": record.status.value,
                "received_at": to_db(record.event.received_at),
            }
            for record in batch.events
        ]
        # Tipos fixos: um None na primeira linha não pode decidir o tipo da coluna.
        errors = executemany(
            cursor,
            INSERT_RAW_SQL,
            rows,
            input_sizes={
                "payload": oracledb.DB_TYPE_CLOB,
                "event_ts": oracledb.DB_TYPE_TIMESTAMP,
                "received_at": oracledb.DB_TYPE_TIMESTAMP,
            },
            batcherrors=True,
        )
        row_errors = {error.offset: error for error in errors}

        inserted: list[str] = []
        duplicates: list[str] = []
        failures: list[RowFailure] = []
        for index, record in enumerate(batch.events):
            uid = record.event.unique_event_id
            error = row_errors.get(index)
            if error is None:
                inserted.append(uid)
            elif is_dedup_violation(error):
                duplicates.append(uid)
            elif is_unavailable(error):
                # Falta de recurso aparece por linha: é sistêmica, desfaz o lote inteiro.
                raise StoreUnavailableError(describe(error))
            else:
                failures.append(RowFailure(uid, error.full_code, describe(error)))
        return inserted, duplicates, failures

    def _insert_outbox(self, cursor: Cursor, batch: BatchToPersist, inserted: set[str]) -> None:
        rows = [
            {
                "event_raw_id": record.raw_event_id,
                "chain_code": record.event.chain_code,
                "unique_event_id": record.outbox.message_id,
                "exchange_name": self._exchange_names[record.outbox.kind],
                "routing_key": record.outbox.routing_key,
                "message": record.outbox.body,
                "schema_version": record.outbox.schema_version,
            }
            for record in batch.events  # ordem de chegada = ordem da outbox
            if record.outbox is not None and record.event.unique_event_id in inserted
        ]
        if rows:
            executemany(
                cursor, INSERT_OUTBOX_SQL, rows, input_sizes={"message": oracledb.DB_TYPE_CLOB}
            )

    def _insert_dlq(
        self, cursor: Cursor, batch: BatchToPersist, failures: list[RowFailure]
    ) -> None:
        payloads = {r.event.unique_event_id: r.event for r in batch.events}
        rows: list[dict[str, Any]] = [
            self._dlq_row(
                batch.chain_code,
                unique_event_id=failure.unique_event_id,
                offset=payloads[failure.unique_event_id].offset.value,
                raw_message=payloads[failure.unique_event_id].payload_json,
                error_class=failure.error_class,
                error_message=failure.error_message,
            )
            for failure in failures
        ]
        rows.extend(
            self._dlq_row(
                batch.chain_code,
                unique_event_id=rejected.unique_event_id,
                offset=rejected.offset.value if rejected.offset else None,
                raw_message=rejected.raw_message,
                error_class=REJECTED_ERROR_CLASS,
                error_message=rejected.reason,
            )
            for rejected in batch.rejected
        )
        if rows:
            executemany(
                cursor,
                INSERT_DLQ_CONSUME_SQL,
                rows,
                input_sizes={"raw_message": oracledb.DB_TYPE_CLOB},
            )

    def _settle_retry(
        self,
        cursor: Cursor,
        dlq_id: int,
        inserted: list[str],
        duplicates: list[str],
        failures: list[RowFailure],
    ) -> None:
        """Retry de DLQ CONSUME: resolve o item ou registra a falha nele (sem item novo)."""
        if failures and not (inserted or duplicates):
            failure = failures[0]
            cursor.execute(
                RETRY_FAILED_SQL,
                self._retry_failure(dlq_id, failure.error_class, failure.error_message),
            )
        else:
            cursor.execute(RESOLVE_RETRIED_SQL, {"id": dlq_id, "resolved_by": RETRY_RESOLVER})
        if cursor.rowcount != 1:
            log.warning("retry_dlq_item_nao_aberto", dlq_id=dlq_id)

    def _dlq_row(
        self,
        chain_code: str,
        *,
        unique_event_id: str | None,
        offset: str | None,
        raw_message: str,
        error_class: str,
        error_message: str,
    ) -> dict[str, Any]:
        return {
            "chain_code": chain_code,
            "unique_event_id": unique_event_id,
            "offset_value": offset,
            "raw_message": raw_message,
            "error_class": truncate_bytes(error_class, ERROR_CLASS_BYTES),
            "error_message": truncate_bytes(error_message, ERROR_MESSAGE_BYTES),
            "code_version": self._code_version,
        }

    @staticmethod
    def _retry_failure(dlq_id: int, error_class: str, message: str) -> dict[str, Any]:
        return {
            "id": dlq_id,
            "error_class": truncate_bytes(error_class, ERROR_CLASS_BYTES),
            "error_message": truncate_bytes(message, ERROR_MESSAGE_BYTES),
        }
