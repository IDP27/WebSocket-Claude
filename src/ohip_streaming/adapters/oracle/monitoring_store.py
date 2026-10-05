"""``MonitoringStore`` no Oracle 11g: leituras da API de controle (ADR-0017).

- Paginação por chave com ``ROWNUM`` (sem ``FETCH FIRST``): ``id`` decrescente, ``id < :cursor``,
  e ``limit + 1`` linhas para saber se há próxima página.
- Filtros montados só com fragmentos fixos deste módulo; todo valor vai por bind.
- Horários com o relógio do banco; TIMESTAMP de filtro vai como TIMESTAMP (``setinputsizes``),
  senão o driver mandaria DATE e cortaria a fração de segundo.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any, TypeVar

import oracledb

from ohip_streaming.adapters.oracle.database import Session, query
from ohip_streaming.adapters.oracle.rows import (
    from_db,
    from_db_required,
    stored_event_from_row,
    to_db,
)
from ohip_streaming.application.errors import StoreOperationError
from ohip_streaming.application.ports import (
    ChainStatus,
    DlqEntry,
    DlqQuery,
    DlqStage,
    EventFilter,
    EventRecord,
    EventSummary,
    OutboxCounts,
    OutboxEntry,
    OutboxQuery,
    Page,
    PageRequest,
    ProcessingStatus,
    ReplayEntry,
    ReplayQuery,
    ReplayStatus,
    StatusSnapshot,
)
from ohip_streaming.domain.connection import ConsumerState
from ohip_streaming.domain.offset import Offset

T = TypeVar("T")

PING_SQL = "SELECT 1 FROM dual"
DB_NOW_SQL = "SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP) FROM dual"

STATUS_SQL = """
SELECT s.chain_code, s.state, s.instance_id, s.subscription_id, o.last_offset,
       s.last_message_at, s.reconnects, s.consecutive_failures, s.next_attempt_at,
       s.last_close_code, s.last_close_reason, s.last_disconnect_at, s.token_expires_at
  FROM ohip_consumer_status s
  LEFT JOIN ohip_offset o ON o.chain_code = s.chain_code
 ORDER BY s.chain_code"""

OUTBOX_COUNTS_SQL = """
SELECT chain_code, status, COUNT(*), MIN(created_at)
  FROM ohip_outbox
 WHERE status IN ('PENDING', 'FAILED')
 GROUP BY chain_code, status"""

DLQ_OPEN_SQL = """
SELECT chain_code, stage, COUNT(*)
  FROM ohip_dlq
 WHERE resolved_at IS NULL
   AND chain_code IS NOT NULL
 GROUP BY chain_code, stage"""

# Atraso em segundos (received_at - event_ts) dos eventos dos últimos 5 minutos.
RECENT_EVENTS_SQL = """
SELECT chain_code, COUNT(*), PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lag_s)
  FROM (SELECT chain_code,
               EXTRACT(DAY FROM (received_at - event_ts)) * 86400
             + EXTRACT(HOUR FROM (received_at - event_ts)) * 3600
             + EXTRACT(MINUTE FROM (received_at - event_ts)) * 60
             + EXTRACT(SECOND FROM (received_at - event_ts)) AS lag_s
          FROM ohip_event_raw
         WHERE received_at >= SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL(300, 'SECOND'))
 GROUP BY chain_code"""

EVENT_COLUMNS = (
    "id, unique_event_id, chain_code, hotel_id, offset_value, module_name, event_name, "
    "primary_key, event_ts, received_at, persisted_at, processing_status"
)
OUTBOX_COLUMNS = (
    "id, event_raw_id, chain_code, unique_event_id, exchange_name, routing_key, status, "
    "attempts, next_attempt_at, last_error, created_at, sent_at"
)
DLQ_COLUMNS = (
    "id, stage, chain_code, event_raw_id, outbox_id, unique_event_id, offset_value, "
    "error_class, error_message, code_version, attempts, created_at, retry_requested_at, "
    "retry_requested_by, resolved_at, resolved_by, resolution"
)
REPLAY_COLUMNS = (
    "id, chain_code, from_offset, reason, requested_by, status, error_message, created_at, "
    "applied_at, cancelled_at, cancelled_by"
)

# Colunas 0 a 6 na ordem de rows.stored_event_from_row.
EVENT_DETAIL_SQL = """
SELECT id, chain_code, subscription_id, event_ts, payload, processing_status, received_at,
       persisted_at, updated_at
  FROM ohip_event_raw
 WHERE unique_event_id = :uid"""
EVENT_OUTBOX_SQL = f"""
SELECT {OUTBOX_COLUMNS}
  FROM ohip_outbox
 WHERE event_raw_id = :raw_id
 ORDER BY id"""  # noqa: S608 - colunas fixas deste módulo
EVENT_DLQ_SQL = f"""
SELECT {DLQ_COLUMNS}
  FROM ohip_dlq
 WHERE event_raw_id = :raw_id
 ORDER BY id"""  # noqa: S608 - colunas fixas deste módulo

OLDEST_PENDING_SQL = """
SELECT MIN(created_at), SYS_EXTRACT_UTC(SYSTIMESTAMP)
  FROM ohip_outbox
 WHERE status = 'PENDING'"""

REPLAY_BY_ID_SQL = f"""
SELECT {REPLAY_COLUMNS}
  FROM ohip_replay_request
 WHERE id = :id"""  # noqa: S608 - colunas fixas deste módulo


def page_sql(columns: str, table: str, conditions: Sequence[str]) -> str:
    """``SELECT`` paginado por chave. ``conditions`` são fragmentos fixos deste módulo."""
    where = "".join(f"\n           AND {condition}" for condition in conditions)
    return f"""
SELECT *
  FROM (SELECT {columns}
          FROM {table}
         WHERE 1 = 1{where}
         ORDER BY id DESC)
 WHERE ROWNUM <= :fetch"""  # noqa: S608 - fragmentos fixos; valores por bind


def oldest_pending_sql(chain_code: str | None) -> str:
    return OLDEST_PENDING_SQL + ("\n   AND chain_code = :chain_code" if chain_code else "")


def _timestamp(value: datetime) -> datetime | None:
    return to_db(value)


def event_conditions(
    query_: EventFilter, page: PageRequest
) -> tuple[list[str], dict[str, Any], dict[str, Any]]:
    """Fragmentos, binds e tipos dos binds do filtro de ``/events``."""
    conditions: list[str] = []
    params: dict[str, Any] = {}
    sizes: dict[str, Any] = {}
    equals = (
        ("chain_code", query_.chain_code),
        ("hotel_id", query_.hotel_id),
        ("event_name", query_.event_name),
        ("primary_key", query_.primary_key),
    )
    for column, value in equals:
        if value is not None:
            conditions.append(f"{column} = :{column}")
            params[column] = value
    if query_.module_name is not None:
        conditions.append("UPPER(module_name) = :module_name")
        params["module_name"] = query_.module_name.upper()
    if query_.received_from is not None:
        conditions.append("received_at >= :received_from")
        params["received_from"] = _timestamp(query_.received_from)
        sizes["received_from"] = oracledb.DB_TYPE_TIMESTAMP
    if query_.received_to is not None:
        conditions.append("received_at < :received_to")
        params["received_to"] = _timestamp(query_.received_to)
        sizes["received_to"] = oracledb.DB_TYPE_TIMESTAMP
    if query_.processing_status is not None:
        conditions.append("processing_status = :processing_status")
        params["processing_status"] = query_.processing_status.value
    _cursor(conditions, params, page)
    return conditions, params, sizes


def outbox_conditions(query_: OutboxQuery, page: PageRequest) -> tuple[list[str], dict[str, Any]]:
    conditions = ["status = :status"]
    params: dict[str, Any] = {"status": query_.status}
    if query_.chain_code is not None:
        conditions.append("chain_code = :chain_code")
        params["chain_code"] = query_.chain_code
    _cursor(conditions, params, page)
    return conditions, params


def dlq_conditions(query_: DlqQuery, page: PageRequest) -> tuple[list[str], dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}
    if query_.stage is not None:
        conditions.append("stage = :stage")
        params["stage"] = query_.stage.value
    if query_.chain_code is not None:
        conditions.append("chain_code = :chain_code")
        params["chain_code"] = query_.chain_code
    if query_.resolved is not None:
        conditions.append("resolved_at IS NOT NULL" if query_.resolved else "resolved_at IS NULL")
    _cursor(conditions, params, page)
    return conditions, params


def replay_conditions(query_: ReplayQuery, page: PageRequest) -> tuple[list[str], dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {}
    if query_.chain_code is not None:
        conditions.append("chain_code = :chain_code")
        params["chain_code"] = query_.chain_code
    if query_.status is not None:
        conditions.append("status = :status")
        params["status"] = query_.status.value
    _cursor(conditions, params, page)
    return conditions, params


def _cursor(conditions: list[str], params: dict[str, Any], page: PageRequest) -> None:
    if page.cursor is not None:
        conditions.append("id < :cursor")
        params["cursor"] = page.cursor
    params["fetch"] = page.limit + 1


def _page(rows: Sequence[T], page: PageRequest, key: Callable[[T], int]) -> Page[T]:
    items = tuple(rows[: page.limit])
    has_more = len(rows) > page.limit
    return Page(items, key(items[-1]) if has_more and items else None)


def _int(value: Any) -> int:
    return int(value) if value is not None else 0


def _optional_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def event_summary(row: Sequence[Any]) -> EventSummary:
    (raw_id, uid, chain_code, hotel_id, offset, module, event_name, primary_key, event_ts,
     received_at, persisted_at, status) = row  # fmt: skip
    return EventSummary(
        raw_event_id=int(raw_id),
        unique_event_id=uid,
        chain_code=chain_code,
        hotel_id=hotel_id,
        offset=Offset(offset),
        module_name=module,
        event_name=event_name,
        primary_key=primary_key,
        event_ts=from_db(event_ts),
        received_at=from_db_required(received_at),
        persisted_at=from_db_required(persisted_at),
        processing_status=ProcessingStatus(status),
    )


def outbox_entry(row: Sequence[Any]) -> OutboxEntry:
    (row_id, raw_id, chain_code, uid, exchange, key, status, attempts, next_attempt_at,
     last_error, created_at, sent_at) = row  # fmt: skip
    return OutboxEntry(
        id=int(row_id),
        event_raw_id=int(raw_id),
        chain_code=chain_code,
        unique_event_id=uid,
        exchange_name=exchange,
        routing_key=key,
        status=status,
        attempts=_int(attempts),
        next_attempt_at=from_db_required(next_attempt_at),
        last_error=last_error,
        created_at=from_db_required(created_at),
        sent_at=from_db(sent_at),
    )


def dlq_entry(row: Sequence[Any]) -> DlqEntry:
    (item_id, stage, chain_code, raw_id, outbox_id, uid, offset, error_class, error_message,
     code_version, attempts, created_at, retry_at, retry_by, resolved_at, resolved_by,
     resolution) = row  # fmt: skip
    return DlqEntry(
        id=int(item_id),
        stage=DlqStage(stage),
        chain_code=chain_code,
        event_raw_id=_optional_int(raw_id),
        outbox_id=_optional_int(outbox_id),
        unique_event_id=uid,
        offset=offset,
        error_class=error_class,
        error_message=error_message,
        code_version=code_version,
        attempts=_int(attempts),
        created_at=from_db_required(created_at),
        retry_requested_at=from_db(retry_at),
        retry_requested_by=retry_by,
        resolved_at=from_db(resolved_at),
        resolved_by=resolved_by,
        resolution=resolution,
    )


def replay_entry(row: Sequence[Any]) -> ReplayEntry:
    (request_id, chain_code, from_offset, reason, requested_by, status, error_message,
     created_at, applied_at, cancelled_at, cancelled_by) = row  # fmt: skip
    return ReplayEntry(
        id=int(request_id),
        chain_code=chain_code,
        from_offset=Offset(from_offset),
        reason=reason,
        requested_by=requested_by,
        status=ReplayStatus(status),
        error_message=error_message,
        created_at=from_db_required(created_at),
        applied_at=from_db(applied_at),
        cancelled_at=from_db(cancelled_at),
        cancelled_by=cancelled_by,
    )


class OracleMonitoringStore:
    """Implementa ``MonitoringStore``. Na API, com ``InlineSession`` (ADR-0017)."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def ping(self) -> None:
        await self._query(PING_SQL, {})

    async def status(self) -> StatusSnapshot:
        def work(connection: Any) -> StatusSnapshot:
            ((db_now,),) = self._run(connection, DB_NOW_SQL, {})
            statuses = self._run(connection, STATUS_SQL, {})
            outbox_rows = self._run(connection, OUTBOX_COUNTS_SQL, {})
            dlq_rows = self._run(connection, DLQ_OPEN_SQL, {})
            recent_rows = self._run(connection, RECENT_EVENTS_SQL, {})
            return _snapshot(db_now, statuses, outbox_rows, dlq_rows, recent_rows)

        return await self._session.run(work)

    async def events(self, query_: EventFilter, page: PageRequest) -> Page[EventSummary]:
        conditions, params, sizes = event_conditions(query_, page)
        rows = await self._query(
            page_sql(EVENT_COLUMNS, "ohip_event_raw", conditions), params, sizes
        )
        return _page([event_summary(r) for r in rows], page, lambda e: e.raw_event_id)

    async def event(self, unique_event_id: str) -> EventRecord | None:
        def work(connection: Any) -> EventRecord | None:
            rows = self._run(connection, EVENT_DETAIL_SQL, {"uid": unique_event_id})
            if not rows:
                return None
            row = rows[0]
            stored = stored_event_from_row(row[:7])
            linked = {"raw_id": stored.raw_event_id}
            return EventRecord(
                stored=stored,
                persisted_at=from_db_required(row[7]),
                updated_at=from_db(row[8]),
                outbox=tuple(
                    outbox_entry(r) for r in self._run(connection, EVENT_OUTBOX_SQL, linked)
                ),
                dlq=tuple(dlq_entry(r) for r in self._run(connection, EVENT_DLQ_SQL, linked)),
            )

        return await self._session.run(work)

    async def outbox(self, query_: OutboxQuery, page: PageRequest) -> Page[OutboxEntry]:
        conditions, params = outbox_conditions(query_, page)
        rows = await self._query(page_sql(OUTBOX_COLUMNS, "ohip_outbox", conditions), params)
        return _page([outbox_entry(r) for r in rows], page, lambda e: e.id)

    async def oldest_pending_age_s(self, chain_code: str | None) -> float | None:
        params = {"chain_code": chain_code} if chain_code else {}
        rows = await self._query(oldest_pending_sql(chain_code), params)
        if not rows or rows[0][0] is None:
            return None
        oldest, db_now = rows[0]
        return (from_db_required(db_now) - from_db_required(oldest)).total_seconds()

    async def dlq(self, query_: DlqQuery, page: PageRequest) -> Page[DlqEntry]:
        conditions, params = dlq_conditions(query_, page)
        rows = await self._query(page_sql(DLQ_COLUMNS, "ohip_dlq", conditions), params)
        return _page([dlq_entry(r) for r in rows], page, lambda e: e.id)

    async def replays(self, query_: ReplayQuery, page: PageRequest) -> Page[ReplayEntry]:
        conditions, params = replay_conditions(query_, page)
        rows = await self._query(
            page_sql(REPLAY_COLUMNS, "ohip_replay_request", conditions), params
        )
        return _page([replay_entry(r) for r in rows], page, lambda e: e.id)

    async def replay(self, request_id: int) -> ReplayEntry | None:
        rows = await self._query(REPLAY_BY_ID_SQL, {"id": request_id})
        return replay_entry(rows[0]) if rows else None

    # ------------------------------------------------------------------- apoio

    @staticmethod
    def _run(
        connection: Any,
        statement: str,
        params: dict[str, Any],
        sizes: dict[str, Any] | None = None,
    ) -> list[Sequence[Any]]:
        return query(connection, statement, params, failure=StoreOperationError, input_sizes=sizes)

    async def _query(
        self, statement: str, params: dict[str, Any], sizes: dict[str, Any] | None = None
    ) -> list[Sequence[Any]]:
        return await self._session.run(lambda c: self._run(c, statement, params, sizes))


def _snapshot(
    db_now: datetime,
    statuses: Sequence[Sequence[Any]],
    outbox_rows: Sequence[Sequence[Any]],
    dlq_rows: Sequence[Sequence[Any]],
    recent_rows: Sequence[Sequence[Any]],
) -> StatusSnapshot:
    pending: dict[str, tuple[int, datetime | None]] = {}
    failed: dict[str, int] = {}
    for chain_code, status, count, oldest in outbox_rows:
        if status == "PENDING":
            pending[chain_code] = (int(count), from_db(oldest))
        else:
            failed[chain_code] = int(count)
    dlq: dict[str, dict[DlqStage, int]] = {}
    for chain_code, stage, count in dlq_rows:
        dlq.setdefault(chain_code, {})[DlqStage(stage)] = int(count)
    recent = {
        chain_code: (int(count), float(p95) if p95 is not None else None)
        for chain_code, count, p95 in recent_rows
    }

    chains = []
    for row in statuses:
        (chain_code, state, instance_id, subscription_id, last_offset, last_message_at,
         reconnects, failures, next_attempt_at, close_code, close_reason, disconnect_at,
         token_expires_at) = row  # fmt: skip
        pending_count, oldest = pending.get(chain_code, (0, None))
        events_5m, p95 = recent.get(chain_code, (0, None))
        chains.append(
            ChainStatus(
                chain_code=chain_code,
                state=ConsumerState(state),
                instance_id=instance_id,
                subscription_id=subscription_id,
                last_offset=Offset(last_offset) if last_offset is not None else None,
                last_message_at=from_db(last_message_at),
                lag_seconds_p95_5m=p95,
                events_last_5m=events_5m,
                reconnects_total=_int(reconnects),
                consecutive_failures=_int(failures),
                next_attempt_at=from_db(next_attempt_at),
                last_close_code=_optional_int(close_code),
                last_close_reason=close_reason,
                last_disconnect_at=from_db(disconnect_at),
                token_expires_at=from_db(token_expires_at),
                outbox=OutboxCounts(pending_count, failed.get(chain_code, 0), oldest),
                dlq_open={stage: dlq.get(chain_code, {}).get(stage, 0) for stage in DlqStage},
            )
        )
    return StatusSnapshot(generated_at=from_db_required(db_now), chains=tuple(chains))
