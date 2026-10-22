"""``EventStore`` no PostgreSQL: a transação do lote do consumer (ARCHITECTURE §4.1, ADR-0022).

Ordem dentro da transação (o contrato está em ``EventStore.persist_batch``):

1. eventos brutos (ADR-0022):

   - caminho rápido: um ``executemany`` com ``ON CONFLICT ON CONSTRAINT ohip_event_raw_uq_evt
     DO NOTHING RETURNING``: linha com retorno = gravada; sem retorno = duplicado (``uq_evt``);
   - erro de dado (classes 22/23, NUL ou surrogate recusados pelo driver) → desfaz o savepoint
     e grava **linha a linha**, cada uma no seu savepoint: ``23505`` em ``uq_off`` = duplicado;
     indisponibilidade desfaz o lote inteiro; erro de dado da linha (inclusive ``23505`` na PK)
     → DLQ ``CONSUME``; qualquer outro erro → ``BatchFailedError``;

2. outbox dos eventos gravados, na ordem de chegada; 3. DLQ; 4. item de retry (se houver);
5. offset; 6. barreira de epoch; commit.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import psycopg

from ohip_streaming.adapters.postgres.database import (
    Cursor,
    Session,
    executemany,
    fence,
    read,
    savepoint,
    write,
)
from ohip_streaming.adapters.postgres.errors import (
    describe,
    error_code,
    is_dedup_violation,
    is_row_data_error,
    is_unavailable,
    violated_constraint,
)
from ohip_streaming.adapters.postgres.rows import (
    ERROR_CLASS_BYTES,
    ERROR_MESSAGE_BYTES,
    clean_text,
    from_db_required,
    to_db,
    truncate_bytes,
)
from ohip_streaming.application.errors import (
    BatchFailedError,
    StoreUnavailableError,
    UnknownChainError,
)
from ohip_streaming.application.ports import (
    BatchResult,
    BatchToPersist,
    ConsumeRetryItem,
    NewEventRecord,
    RowFailure,
)
from ohip_streaming.domain.leases import consumer_lease
from ohip_streaming.domain.messages import ExchangeKind
from ohip_streaming.logging import get_logger

log = get_logger(__name__)

RESERVE_IDS_SQL = "SELECT nextval('ohip_event_raw_seq') FROM generate_series(1, %(count)s)"

UQ_EVT = "ohip_event_raw_uq_evt"

INSERT_RAW_SQL = """
INSERT INTO ohip_event_raw (
    id, chain_code, hotel_id, offset_value, unique_event_id, subscription_id, module_name,
    event_name, primary_key, publisher_id, action_instance_id, event_ts, payload,
    processing_status, received_at
) VALUES (
    %(id)s, %(chain_code)s, %(hotel_id)s, %(offset_value)s, %(unique_event_id)s,
    %(subscription_id)s, %(module_name)s, %(event_name)s, %(primary_key)s, %(publisher_id)s,
    %(action_instance_id)s, %(event_ts)s, %(payload)s, %(processing_status)s, %(received_at)s
)
ON CONFLICT ON CONSTRAINT ohip_event_raw_uq_evt DO NOTHING
RETURNING unique_event_id"""

INSERT_OUTBOX_SQL = """
INSERT INTO ohip_outbox (
    id, event_raw_id, chain_code, unique_event_id, exchange_name, routing_key, message,
    schema_version
) VALUES (
    nextval('ohip_outbox_seq'), %(event_raw_id)s, %(chain_code)s, %(unique_event_id)s,
    %(exchange_name)s, %(routing_key)s, %(message)s, %(schema_version)s
)"""

INSERT_DLQ_CONSUME_SQL = """
INSERT INTO ohip_dlq (
    id, stage, chain_code, unique_event_id, offset_value, raw_message, error_class,
    error_message, code_version
) VALUES (
    nextval('ohip_dlq_seq'), 'CONSUME', %(chain_code)s, %(unique_event_id)s, %(offset_value)s,
    %(raw_message)s, %(error_class)s, %(error_message)s, %(code_version)s
)"""

RESOLVE_RETRIED_SQL = """
UPDATE ohip_dlq
   SET resolution = 'RETRIED',
       resolved_at = statement_timestamp(),
       resolved_by = %(resolved_by)s,
       retry_requested_at = NULL,
       retry_requested_by = NULL
 WHERE id = %(id)s
   AND resolution IS NULL"""

RETRY_FAILED_SQL = """
UPDATE ohip_dlq
   SET error_class = %(error_class)s,
       error_message = %(error_message)s,
       attempts = attempts + 1,
       retry_requested_at = NULL,
       retry_requested_by = NULL
 WHERE id = %(id)s
   AND resolution IS NULL"""

RETRY_FAILED_RETURNING_SQL = RETRY_FAILED_SQL + "\nRETURNING chain_code"

UPDATE_OFFSET_SQL = """
UPDATE ohip_offset
   SET last_offset = %(last_offset)s,
       last_unique_event_id = %(last_unique_event_id)s,
       updated_at = statement_timestamp()
 WHERE chain_code = %(chain_code)s"""

PENDING_RETRIES_SQL = """
SELECT id, raw_message, created_at
  FROM ohip_dlq
 WHERE stage = 'CONSUME'
   AND chain_code = %(chain_code)s
   AND resolution IS NULL
   AND retry_requested_at IS NOT NULL
 ORDER BY id
 LIMIT %(limit)s"""

REJECTED_ERROR_CLASS = "REJECTED"  # mensagem recusada pelo domínio (não virou evento)
RETRY_RESOLVER = "consumer"

_Classified = tuple[list[str], list[str], list[RowFailure]]


def encodes_cleanly(row: Mapping[str, Any]) -> bool:
    """A linha passa pelo driver? NUL e surrogate isolado são recusados do lado do cliente;
    linhas assim vão direto ao caminho lento (sem depender do estado do pipeline)."""
    for value in row.values():
        if isinstance(value, str):
            if "\x00" in value:
                return False
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                return False
    return True


class PostgresEventStore:
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
        rows = await read(self._session, RESERVE_IDS_SQL, {"count": count})
        return sorted(int(row[0]) for row in rows)

    async def persist_batch(self, batch: BatchToPersist) -> BatchResult:
        return await write(
            self._session, lambda cursor: self._persist(cursor, batch), failure=BatchFailedError
        )

    async def pending_consume_retries(self, chain_code: str, limit: int) -> list[ConsumeRetryItem]:
        rows = await read(
            self._session, PENDING_RETRIES_SQL, {"chain_code": chain_code, "limit": limit}
        )
        return [
            ConsumeRetryItem(int(dlq_id), raw or "", from_db_required(created_at))
            for dlq_id, raw, created_at in rows
        ]

    async def mark_consume_retry_failed(self, dlq_id: int, reason: str, lease_epoch: int) -> None:
        def body(cursor: Cursor) -> None:
            cursor.execute(
                RETRY_FAILED_RETURNING_SQL,
                self._retry_failure(dlq_id, REJECTED_ERROR_CLASS, reason),
            )
            row = cursor.fetchone()
            if row is None:  # item resolvido ou removido nesse meio-tempo
                log.warning("retry_dlq_item_nao_aberto", dlq_id=dlq_id)
                return
            fence(cursor, consumer_lease(row[0]), lease_epoch)

        await write(self._session, body)

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

    def _insert_events(self, cursor: Cursor, batch: BatchToPersist) -> _Classified:
        if not batch.events:
            return [], [], []
        rows = [self._raw_row(record) for record in batch.events]
        if all(encodes_cleanly(row) for row in rows):
            try:
                with savepoint(cursor):
                    results = executemany(cursor, INSERT_RAW_SQL, rows, returning=True)
            except psycopg.Error as exc:
                if is_unavailable(exc):
                    raise StoreUnavailableError(describe(exc)) from exc
                if not is_row_data_error(exc):
                    raise BatchFailedError(describe(exc)) from exc
                log.info("lote_caminho_lento", motivo=error_code(exc), linhas=len(rows))
            else:
                return self._classify_fast(batch, results)
        return self._insert_one_by_one(cursor, batch, rows)

    @staticmethod
    def _classify_fast(batch: BatchToPersist, results: list[list[Any]]) -> _Classified:
        inserted: list[str] = []
        duplicates: list[str] = []
        for record, returned in zip(batch.events, results, strict=True):
            uid = record.event.unique_event_id
            (inserted if returned else duplicates).append(uid)  # sem retorno = uq_evt
        return inserted, duplicates, []

    def _insert_one_by_one(
        self, cursor: Cursor, batch: BatchToPersist, rows: list[dict[str, Any]]
    ) -> _Classified:
        """Caminho lento (ADR-0022): cada linha no seu savepoint, na ordem de chegada."""
        inserted: list[str] = []
        duplicates: list[str] = []
        failures: list[RowFailure] = []
        for record, row in zip(batch.events, rows, strict=True):
            uid = record.event.unique_event_id
            try:
                with savepoint(cursor):
                    cursor.execute(INSERT_RAW_SQL, row)
                    returned = cursor.fetchone()
            except (psycopg.Error, UnicodeEncodeError) as exc:
                if is_unavailable(exc):
                    # Falta de recurso aparece por linha: é sistêmica, desfaz o lote inteiro.
                    raise StoreUnavailableError(describe(exc)) from exc
                if is_dedup_violation(exc):
                    duplicates.append(uid)
                    log.warning(  # uq_off com outro uniqueEventId: offset reutilizado? (D-6)
                        "evento_duplicado_por_offset",
                        unique_event_id=uid,
                        constraint=violated_constraint(exc),
                    )
                elif is_row_data_error(exc):  # inclusive 23505 na PK (classe 23)
                    failures.append(RowFailure(uid, error_code(exc), describe(exc)))
                else:  # erro sistêmico: o lote falha (bisseção e disjuntor, ADR-0009/0011)
                    raise BatchFailedError(describe(exc)) from exc
                continue
            (inserted if returned is not None else duplicates).append(uid)
        return inserted, duplicates, failures

    @staticmethod
    def _raw_row(record: NewEventRecord) -> dict[str, Any]:
        event = record.event
        return {
            "id": record.raw_event_id,
            "chain_code": event.chain_code,
            "hotel_id": event.hotel_id,
            "offset_value": event.offset.value,
            "unique_event_id": event.unique_event_id,
            "subscription_id": event.subscription_id,
            "module_name": event.module_name,
            "event_name": event.event_name,
            "primary_key": event.primary_key,
            "publisher_id": event.publisher_id,
            "action_instance_id": event.action_instance_id,
            "event_ts": to_db(event.event_ts),
            "payload": event.payload_json,
            "processing_status": record.status.value,
            "received_at": to_db(event.received_at),
        }

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
            for record in batch.events  # ordem de chegada = ordem da outbox (ADR-0022 §4)
            if record.outbox is not None and record.event.unique_event_id in inserted
        ]
        if rows:
            executemany(cursor, INSERT_OUTBOX_SQL, rows)

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
            executemany(cursor, INSERT_DLQ_CONSUME_SQL, rows)

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
            "unique_event_id": clean_text(unique_event_id) if unique_event_id else None,
            "offset_value": offset,
            "raw_message": clean_text(raw_message),
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
