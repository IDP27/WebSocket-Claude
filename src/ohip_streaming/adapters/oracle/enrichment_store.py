"""``EnrichmentStore`` no Oracle 11g (ARCHITECTURE §4.3, ADR-0019).

- ``apply``: numa transação, um ``MERGE`` condicional por escrita e o ``processing_status``.
  ``WHEN MATCHED`` só atualiza se o evento não for mais antigo (``source_offset_num``): N
  enrichers em paralelo não sobrescrevem estado novo com antigo, e reprocessar é idempotente.
- Tabela e colunas vêm das regras (código, não entrada externa) e ainda assim são validadas
  antes de entrar no SQL; todo valor vai por bind.
- Sem lease nem barreira de epoch: a ordem é garantida pela condição do ``MERGE``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Final

from ohip_streaming.adapters.oracle.database import Cursor, Session, query, transaction
from ohip_streaming.adapters.oracle.operations_store import EVENT_BY_ID_SQL
from ohip_streaming.adapters.oracle.rows import (
    ERROR_CLASS_BYTES,
    ERROR_MESSAGE_BYTES,
    stored_event_from_row,
    truncate_bytes,
)
from ohip_streaming.application.errors import NotFoundError, StoreOperationError
from ohip_streaming.application.ports import DlqStage, DomainWrite, ProcessingStatus, StoredEvent

# Identificador do 11g: até 30 caracteres, sem aspas (ARCHITECTURE §5).
_IDENTIFIER_RE: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,29}$")
SOURCE_COLUMNS: Final = ("source_event_raw_id", "source_offset_num")
_ENRICH_STAGES: Final = frozenset({DlqStage.NORMALIZE, DlqStage.ENRICH})

UPDATE_STATUS_SQL = """
UPDATE ohip_event_raw
   SET processing_status = :status, updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE id = :raw_event_id"""

INSERT_DLQ_SQL = """
INSERT INTO ohip_dlq (
    id, stage, chain_code, event_raw_id, unique_event_id, offset_value, error_class,
    error_message, code_version
)
SELECT ohip_dlq_seq.NEXTVAL, :stage, chain_code, id, unique_event_id, offset_value,
       :error_class, :error_message, :code_version
  FROM ohip_event_raw
 WHERE id = :raw_event_id"""


def _identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise StoreOperationError(f"identificador inválido para o Oracle 11g: {name!r}")
    return name.lower()


def merge_sql(write: DomainWrite) -> tuple[str, dict[str, Any]]:
    """``MERGE`` condicional de uma escrita e os seus binds.

    Binds numerados (``:k1``, ``:v1``…): o nome da coluna pode ter 30 caracteres, e um bind
    derivado dele passaria do limite de identificador do 11g (ORA-00972). Chave com ``None``
    é recusada: ``t.col = NULL`` nunca casa, e o ``MERGE`` inseriria uma linha nova a cada
    evento (hotel ausente vira ``'#CHAIN'`` na regra, ADR-0019 §4)."""
    table = _identifier(write.table)
    keys = [_identifier(k) for k in write.key]
    values = [_identifier(v) for v in write.values]
    if not keys:
        raise StoreOperationError(f"escrita em {table} sem chave")
    clash = (set(keys) & set(values)) | (set(keys + values) & set(SOURCE_COLUMNS))
    if clash:
        raise StoreOperationError(f"coluna repetida na escrita em {table}: {sorted(clash)}")
    nulls = sorted(k for k, value in zip(keys, write.key.values(), strict=True) if value is None)
    if nulls:
        raise StoreOperationError(f"chave nula na escrita em {table}: {nulls}")

    params: dict[str, Any] = {f"k{i}": value for i, value in enumerate(write.key.values(), start=1)}
    params.update({f"v{i}": value for i, value in enumerate(write.values.values(), start=1)})
    params["source_event_raw_id"] = write.source_event_raw_id
    params["source_offset_num"] = write.source_offset_num

    value_binds = [f":v{i}" for i in range(1, len(values) + 1)]
    source = ", ".join(f":k{i} AS {k}" for i, k in enumerate(keys, start=1))
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    updates = [f"t.{v} = {bind}" for v, bind in zip(values, value_binds, strict=True)] + [
        "t.source_event_raw_id = :source_event_raw_id",
        "t.source_offset_num = :source_offset_num",
    ]
    columns = [*keys, *values, *SOURCE_COLUMNS]
    inserted = [
        *(f"s.{k}" for k in keys),
        *value_binds,
        ":source_event_raw_id",
        ":source_offset_num",
    ]
    statement = f"""
MERGE INTO {table} t
USING (SELECT {source} FROM dual) s
   ON ({on})
 WHEN MATCHED THEN UPDATE SET {", ".join(updates)}
      WHERE t.source_offset_num <= :source_offset_num
 WHEN NOT MATCHED THEN INSERT ({", ".join(columns)})
      VALUES ({", ".join(inserted)})"""  # noqa: S608 - identificadores validados; valores por bind
    return statement, params


class OracleEnrichmentStore:
    """Implementa ``EnrichmentStore``. Use com ``PooledSession`` (executor do enricher)."""

    def __init__(self, session: Session, *, code_version: str) -> None:
        self._session = session
        self._code_version = code_version

    async def get_event(self, raw_event_id: int) -> StoredEvent | None:
        rows = await self._session.run(
            lambda connection: query(
                connection, EVENT_BY_ID_SQL, {"id": raw_event_id}, failure=StoreOperationError
            )
        )
        return stored_event_from_row(rows[0]) if rows else None

    async def apply(
        self, raw_event_id: int, writes: Sequence[DomainWrite], status: ProcessingStatus
    ) -> int:
        statements = [merge_sql(write) for write in writes]  # valida antes de abrir transação

        def body(cursor: Cursor) -> int:
            skipped = 0
            for statement, params in statements:
                cursor.execute(statement, params)
                if cursor.rowcount == 0:  # existe e é mais novo: a condição barrou
                    skipped += 1
            cursor.execute(
                UPDATE_STATUS_SQL, {"status": status.value, "raw_event_id": raw_event_id}
            )
            if cursor.rowcount != 1:
                raise NotFoundError(f"evento bruto {raw_event_id} não encontrado")
            return skipped

        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    async def add_dlq(
        self, raw_event_id: int, stage: DlqStage, error_class: str, error: str
    ) -> None:
        if stage not in _ENRICH_STAGES:
            raise ValueError(f"o enricher só grava DLQ NORMALIZE/ENRICH, não {stage}")

        def body(cursor: Cursor) -> None:
            cursor.execute(
                INSERT_DLQ_SQL,
                {
                    "stage": stage.value,
                    "error_class": truncate_bytes(error_class, ERROR_CLASS_BYTES),
                    "error_message": truncate_bytes(error, ERROR_MESSAGE_BYTES),
                    "code_version": self._code_version,
                    "raw_event_id": raw_event_id,
                },
            )
            if cursor.rowcount != 1:
                raise NotFoundError(f"evento bruto {raw_event_id} não encontrado")
            cursor.execute(
                UPDATE_STATUS_SQL,
                {"status": ProcessingStatus.FAILED.value, "raw_event_id": raw_event_id},
            )

        await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )
