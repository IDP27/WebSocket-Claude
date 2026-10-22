"""``EnrichmentStore`` no PostgreSQL (ARCHITECTURE §4.3, ADR-0019, ADR-0022 §5).

- ``apply``: numa transação, um ``INSERT ... ON CONFLICT DO UPDATE`` condicional por escrita e
  o ``processing_status``. O ``UPDATE`` só acontece se o evento não for mais antigo
  (``source_offset_num``): N enrichers em paralelo não sobrescrevem estado novo com antigo, e
  reprocessar é idempotente.
- Tabela e colunas vêm das regras (código, não entrada externa) e ainda assim são validadas
  antes de entrar no SQL; todo valor vai por bind.
- Sem lease nem barreira de epoch: a ordem é garantida pela condição do ``ON CONFLICT``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Final

from ohip_streaming.adapters.postgres.database import Cursor, Session, read, write
from ohip_streaming.adapters.postgres.operations_store import EVENT_BY_ID_SQL
from ohip_streaming.adapters.postgres.rows import (
    ERROR_CLASS_BYTES,
    ERROR_MESSAGE_BYTES,
    stored_event_from_row,
    truncate_bytes,
)
from ohip_streaming.application.errors import NotFoundError, StoreOperationError
from ohip_streaming.application.ports import DlqStage, DomainWrite, ProcessingStatus, StoredEvent

# Identificador sem aspas, até 63 caracteres (limite do PostgreSQL).
_IDENTIFIER_RE: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")
SOURCE_COLUMNS: Final = ("source_event_raw_id", "source_offset_num")
_ENRICH_STAGES: Final = frozenset({DlqStage.NORMALIZE, DlqStage.ENRICH})

UPDATE_STATUS_SQL = """
UPDATE ohip_event_raw
   SET processing_status = %(status)s, updated_at = statement_timestamp()
 WHERE id = %(raw_event_id)s"""

INSERT_DLQ_SQL = """
INSERT INTO ohip_dlq (
    id, stage, chain_code, event_raw_id, unique_event_id, offset_value, error_class,
    error_message, code_version
)
SELECT nextval('ohip_dlq_seq'), %(stage)s, chain_code, id, unique_event_id, offset_value,
       %(error_class)s, %(error_message)s, %(code_version)s
  FROM ohip_event_raw
 WHERE id = %(raw_event_id)s"""


def _identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise StoreOperationError(f"identificador inválido: {name!r}")
    return name.lower()


def merge_sql(write: DomainWrite) -> tuple[str, dict[str, Any]]:
    """``INSERT ... ON CONFLICT DO UPDATE`` condicional de uma escrita e os seus binds.

    Equivale ao ``MERGE`` condicional do Oracle, mas é atômico com N enrichers em paralelo
    (ADR-0022 §5). A tabela de domínio precisa de um índice único nas colunas da chave (o ADR
    dela declara). Chave com ``None`` é recusada: ``NULL`` nunca conflita, e cada evento
    inseriria uma linha nova (hotel ausente vira ``'#CHAIN'`` na regra, ADR-0019 §4)."""
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

    columns = [*keys, *values, *SOURCE_COLUMNS]
    inserted = [
        *(f"%(k{i})s" for i in range(1, len(keys) + 1)),
        *(f"%(v{i})s" for i in range(1, len(values) + 1)),
        "%(source_event_raw_id)s",
        "%(source_offset_num)s",
    ]
    updates = [f"{column} = EXCLUDED.{column}" for column in [*values, *SOURCE_COLUMNS]]
    statement = f"""
INSERT INTO {table} AS t ({", ".join(columns)})
VALUES ({", ".join(inserted)})
ON CONFLICT ({", ".join(keys)}) DO UPDATE SET {", ".join(updates)}
 WHERE t.source_offset_num <= EXCLUDED.source_offset_num"""  # noqa: S608 - identificadores validados; valores por bind
    return statement, params


class PostgresEnrichmentStore:
    """Implementa ``EnrichmentStore``. Use com ``PooledSession`` (executor do enricher)."""

    def __init__(self, session: Session, *, code_version: str) -> None:
        self._session = session
        self._code_version = code_version

    async def get_event(self, raw_event_id: int) -> StoredEvent | None:
        rows = await read(self._session, EVENT_BY_ID_SQL, {"id": raw_event_id})
        return stored_event_from_row(rows[0]) if rows else None

    async def apply(
        self, raw_event_id: int, writes: Sequence[DomainWrite], status: ProcessingStatus
    ) -> int:
        statements = [merge_sql(write) for write in writes]  # valida antes de abrir transação

        def body(cursor: Cursor) -> int:
            skipped = 0
            for statement, params in statements:
                cursor.execute(statement, params)
                if cursor.rowcount == 0:  # existe e é mais novo: a condição barrou o UPDATE
                    skipped += 1
            cursor.execute(
                UPDATE_STATUS_SQL, {"status": status.value, "raw_event_id": raw_event_id}
            )
            if cursor.rowcount != 1:
                raise NotFoundError(f"evento bruto {raw_event_id} não encontrado")
            return skipped

        return await write(self._session, body)

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

        await write(self._session, body)
