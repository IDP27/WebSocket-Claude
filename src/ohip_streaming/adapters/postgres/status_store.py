"""``ConsumerStatusStore`` no PostgreSQL: estado da conexão por chain (OHIP_CONSUMER_STATUS).

Os horários usados na regra dos 10 s vêm do relógio do banco (ADR-0007), nunca da VM; os
de saúde (mensagem, ping/pong) vêm do processo e são informativos (ADR-0020 §1). O estado
não passa pela barreira de epoch (só a escrita de dados passa).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from ohip_streaming.adapters.postgres.database import Cursor, Session, query, transaction
from ohip_streaming.adapters.postgres.rows import (
    CLOSE_REASON_BYTES,
    from_db,
    from_db_required,
    to_db,
    truncate_bytes,
)
from ohip_streaming.application.errors import StoreOperationError, UnknownChainError
from ohip_streaming.application.ports import ConnectionHealth, DisconnectSnapshot
from ohip_streaming.domain.connection import ConsumerState

RECORD_STATE_SQL = """
UPDATE ohip_consumer_status
   SET state = %(state)s, instance_id = %(instance_id)s, updated_at = statement_timestamp()
 WHERE chain_code = %(chain_code)s"""

RECORD_SUBSCRIBED_SQL = """
UPDATE ohip_consumer_status
   SET state = 'SUBSCRIBED', instance_id = %(instance_id)s, subscription_id = %(subscription_id)s,
       connected_at = statement_timestamp(), token_expires_at = %(token_expires_at)s,
       next_attempt_at = NULL, updated_at = statement_timestamp()
 WHERE chain_code = %(chain_code)s"""

_DISCONNECT_SET = """
UPDATE ohip_consumer_status
   SET state = %(state)s,
       last_disconnect_at = statement_timestamp(),
       last_close_code = %(close_code)s,
       last_close_reason = %(close_reason)s,
       consecutive_failures = %(consecutive_failures)s,
       reconnects = reconnects + %(reconnect)s,
       updated_at = statement_timestamp(),"""

# Duas variantes (com e sem próxima tentativa): um bind nulo dentro de make_interval daria um
# intervalo nulo sem aviso; a variante explícita deixa o NULL só onde ele é a intenção.
RECORD_DISCONNECT_SQL = (
    _DISCONNECT_SET
    + """
       next_attempt_at = statement_timestamp() + make_interval(secs => %(wait_s)s)
 WHERE chain_code = %(chain_code)s"""
)
RECORD_FINAL_DISCONNECT_SQL = (
    _DISCONNECT_SET
    + """
       next_attempt_at = NULL
 WHERE chain_code = %(chain_code)s"""
)

# Colunas de saúde, na ordem de ConnectionHealth (nomes fixos: nunca vêm de fora).
HEALTH_COLUMNS = ("last_message_at", "last_ping_at", "last_pong_at", "last_rtt_ms")

SNAPSHOT_SQL = """
SELECT statement_timestamp(), last_disconnect_at, state
  FROM ohip_consumer_status
 WHERE chain_code = %(chain_code)s"""


class PostgresConsumerStatusStore:
    """Implementa ``ConsumerStatusStore``. Use a sessão de controle (``PooledSession``)."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def record_state(self, chain_code: str, state: ConsumerState, instance_id: str) -> None:
        await self._update(
            chain_code,
            lambda cursor: cursor.execute(
                RECORD_STATE_SQL,
                {"state": state.value, "instance_id": instance_id, "chain_code": chain_code},
            ),
        )

    async def record_subscribed(
        self,
        chain_code: str,
        instance_id: str,
        subscription_id: str,
        token_expires_at: datetime,
    ) -> None:
        params = {
            "instance_id": instance_id,
            "subscription_id": subscription_id,
            "token_expires_at": to_db(token_expires_at),
            "chain_code": chain_code,
        }
        await self._update(chain_code, lambda cursor: cursor.execute(RECORD_SUBSCRIBED_SQL, params))

    async def record_health(self, chain_code: str, health: ConnectionHealth) -> None:
        statement, params = health_sql(chain_code, health)
        if statement is None:
            return  # nada novo
        await self._update(chain_code, lambda cursor: cursor.execute(statement, params))

    async def record_disconnect(
        self,
        chain_code: str,
        state: ConsumerState,
        close_code: int | None,
        close_reason: str | None,
        *,
        consecutive_failures: int = 0,
        reconnect: bool = False,
        next_attempt_in_s: float | None = None,
    ) -> None:
        reason = truncate_bytes(close_reason, CLOSE_REASON_BYTES) if close_reason else None
        params: dict[str, Any] = {
            "state": state.value,
            "close_code": close_code,
            "close_reason": reason,
            "consecutive_failures": consecutive_failures,
            "reconnect": 1 if reconnect else 0,
            "chain_code": chain_code,
        }
        statement = RECORD_FINAL_DISCONNECT_SQL
        if next_attempt_in_s is not None:
            statement = RECORD_DISCONNECT_SQL
            params["wait_s"] = round(max(0.0, next_attempt_in_s), 3)
        await self._update(chain_code, lambda cursor: cursor.execute(statement, params))

    async def disconnect_snapshot(self, chain_code: str) -> DisconnectSnapshot:
        rows = await self._session.run(
            lambda connection: query(
                connection, SNAPSHOT_SQL, {"chain_code": chain_code}, failure=StoreOperationError
            )
        )
        if not rows:
            raise UnknownChainError(chain_code)
        db_now, last_disconnect_at, state = rows[0]
        return DisconnectSnapshot(
            db_now=from_db_required(db_now),
            last_disconnect_at=from_db(last_disconnect_at),
            last_state=ConsumerState(state) if state else None,
        )

    async def _update(self, chain_code: str, statement: Callable[[Cursor], object]) -> None:
        def body(cursor: Cursor) -> None:
            statement(cursor)
            if cursor.rowcount != 1:
                raise UnknownChainError(chain_code)

        await self._session.run(
            lambda connection: transaction(connection, body, failure=StoreOperationError)
        )


def health_sql(chain_code: str, health: ConnectionHealth) -> tuple[str | None, dict[str, Any]]:
    """``UPDATE`` só das colunas com valor (um ``NULL`` não apaga o último dado bom)."""
    values = (
        to_db(health.last_message_at),
        to_db(health.last_ping_at),
        to_db(health.last_pong_at),
        health.rtt_ms,
    )
    params: dict[str, Any] = {
        column: value
        for column, value in zip(HEALTH_COLUMNS, values, strict=True)
        if value is not None
    }
    if not params:
        return None, {}
    sets = ", ".join(f"{column} = %({column})s" for column in params)
    params["chain_code"] = chain_code
    statement = f"""
UPDATE ohip_consumer_status
   SET {sets}, updated_at = statement_timestamp()
 WHERE chain_code = %(chain_code)s"""  # noqa: S608 - colunas de uma lista fixa; valores por bind
    return statement, params
