"""``PurgeStore`` no PostgreSQL (ARCHITECTURE §5.1, ADR-0020 §2).

``DELETE ... WHERE id IN (SELECT id ... LIMIT %(n)s)`` com commit por lote; corte pelo relógio
do banco (``statement_timestamp()``, que permite usar índice). Usa os
índices existentes (``resolved_at``, ``sent_at``, ``received_at``, ``event_raw_id``,
``outbox_id``); o passo ``OUTBOX_FAILED`` filtra poucas linhas (``FAILED`` é exceção).
Planos de execução conferidos no contêiner de teste e a conferir no banco real (Q-17). Use com
o usuário ``ohip_purge`` (``sql/postgres/900_grants_example.sql``).
"""

from __future__ import annotations

from typing import Final

from ohip_streaming.adapters.postgres.database import Cursor, Session, read, write
from ohip_streaming.application.ports import PurgeTarget

CUTOFF = "statement_timestamp() - make_interval(days => %(days)s)"

# Tabela com alias e filtro de cada alvo. Linhas PENDING/FAILED e DLQ aberta seguram o bruto.
_FILTERS: Final[dict[PurgeTarget, tuple[str, str]]] = {
    PurgeTarget.DLQ: ("ohip_dlq d", f"d.resolved_at < {CUTOFF}"),
    PurgeTarget.OUTBOX: (
        "ohip_outbox o",
        f"o.status = 'SENT' AND o.sent_at < {CUTOFF}"  # noqa: S608 - texto fixo
        " AND NOT EXISTS (SELECT 1 FROM ohip_dlq d WHERE d.outbox_id = o.id)",
    ),
    PurgeTarget.OUTBOX_FAILED: (
        "ohip_outbox o",
        f"o.status = 'FAILED' AND o.created_at < {CUTOFF}"  # noqa: S608 - texto fixo
        " AND NOT EXISTS (SELECT 1 FROM ohip_dlq d WHERE d.outbox_id = o.id)",
    ),
    PurgeTarget.RAW: (
        "ohip_event_raw r",
        f"r.received_at < {CUTOFF}"  # noqa: S608 - texto fixo
        " AND NOT EXISTS (SELECT 1 FROM ohip_outbox o WHERE o.event_raw_id = r.id)"
        " AND NOT EXISTS (SELECT 1 FROM ohip_dlq d WHERE d.event_raw_id = r.id)",
    ),
}


def delete_sql(target: PurgeTarget) -> str:
    table, where = _FILTERS[target]
    name, alias = table.split()
    subquery = f"SELECT {alias}.id FROM {table} WHERE {where} LIMIT %(n)s"  # noqa: S608 - texto fixo
    return f"DELETE FROM {name} WHERE id IN ({subquery})"  # noqa: S608 - texto fixo


def count_sql(target: PurgeTarget) -> str:
    table, where = _FILTERS[target]
    return f"SELECT COUNT(*) FROM {table} WHERE {where}"  # noqa: S608 - texto fixo


class PostgresPurgeStore:
    """Implementa ``PurgeStore``. Use com ``PooledSession`` e prazo de chamada folgado."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def purge_batch(self, target: PurgeTarget, retention_days: int, batch_size: int) -> int:
        statement = delete_sql(target)
        params = {"days": retention_days, "n": batch_size}

        def body(cursor: Cursor) -> int:
            cursor.execute(statement, params)
            return int(cursor.rowcount)

        return await write(self._session, body)

    async def count_expired(self, target: PurgeTarget, retention_days: int) -> int:
        statement = count_sql(target)
        rows = await read(self._session, statement, {"days": retention_days})
        return int(rows[0][0]) if rows else 0
