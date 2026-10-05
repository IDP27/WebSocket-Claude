"""``PurgeStore`` no Oracle 11g (ARCHITECTURE §5.1, ADR-0020 §2).

``DELETE ... AND ROWNUM <= :n`` com commit por lote; corte pelo relógio do banco. Usa os
índices existentes (``resolved_at``, ``sent_at``, ``received_at``, ``event_raw_id``,
``outbox_id``); o passo ``OUTBOX_FAILED`` filtra poucas linhas (``FAILED`` é exceção).
Planos de execução a conferir no Oracle real (Q-17). Use com o usuário ``ohip_purge``
(``sql/900_grants_example.sql``).
"""

from __future__ import annotations

from typing import Final

from ohip_streaming.adapters.oracle.database import Cursor, Session, query, transaction
from ohip_streaming.application.errors import StoreOperationError
from ohip_streaming.application.ports import PurgeTarget

CUTOFF = "SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL(:days, 'DAY')"

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
    return f"DELETE FROM {table} WHERE {where} AND ROWNUM <= :n"  # noqa: S608 - texto fixo


def count_sql(target: PurgeTarget) -> str:
    table, where = _FILTERS[target]
    return f"SELECT COUNT(*) FROM {table} WHERE {where}"  # noqa: S608 - texto fixo


class OraclePurgeStore:
    """Implementa ``PurgeStore``. Use com ``PooledSession`` e prazo de chamada folgado."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def purge_batch(self, target: PurgeTarget, retention_days: int, batch_size: int) -> int:
        statement = delete_sql(target)
        params = {"days": retention_days, "n": batch_size}

        def body(cursor: Cursor) -> int:
            cursor.execute(statement, params)
            return int(cursor.rowcount)

        return await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )

    async def count_expired(self, target: PurgeTarget, retention_days: int) -> int:
        statement = count_sql(target)
        rows = await self._session.run(
            lambda connection: query(
                connection, statement, {"days": retention_days}, failure=StoreOperationError
            )
        )
        return int(rows[0][0]) if rows else 0
