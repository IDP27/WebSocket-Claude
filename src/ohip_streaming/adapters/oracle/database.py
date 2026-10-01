"""Pool Thick, sessões e transações (ADR-0003).

Todo acesso é síncrono e roda num executor via ``run_in_executor`` (leva o contexto de log):

- ``DedicatedSession``: uma conexão fixa e **um** thread. É a conexão de escrita do consumer:
  um único thread preserva a ordem das transações da chain.
- ``PooledSession``: conexão do pool por chamada. Publisher, controle e API.

Uma falha de indisponibilidade descarta a conexão; a próxima chamada pega outra.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import oracledb

from ohip_streaming.adapters.oracle.errors import translate
from ohip_streaming.application.errors import (
    ApplicationError,
    LeaseLostError,
    StoreUnavailableError,
)
from ohip_streaming.logging import get_logger, run_in_executor

if TYPE_CHECKING:
    from ohip_streaming.config import OracleSettings

log = get_logger(__name__)
T = TypeVar("T")

FENCE_SQL = """
UPDATE ohip_lease
   SET last_write_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
 WHERE lease_name = :lease_name
   AND epoch = :epoch"""


class Cursor(Protocol):
    """O subconjunto de ``oracledb.Cursor`` usado pelos stores (os testes usam um fake)."""

    rowcount: int
    arraysize: int
    outputtypehandler: Any
    connection: Any

    def execute(self, statement: str, parameters: Any = None) -> Any: ...

    def executemany(self, statement: str, parameters: Any, *, batcherrors: bool = False) -> Any: ...

    def getbatcherrors(self) -> list[Any]: ...

    def fetchone(self) -> Any: ...

    def fetchall(self) -> list[Any]: ...

    def setinputsizes(self, *args: Any, **kwargs: Any) -> Any: ...

    def var(self, typ: Any, *args: Any, **kwargs: Any) -> Any: ...

    def close(self) -> None: ...


class Pool(Protocol):
    def acquire(self) -> Any: ...

    def release(self, connection: Any) -> None: ...

    def drop(self, connection: Any) -> None: ...

    def close(self, force: bool = False) -> None: ...


_client_initialized = False


def open_pool(settings: OracleSettings) -> Pool:
    """Inicializa o Instant Client (modo Thick, uma vez por processo) e cria o pool."""
    global _client_initialized
    if not _client_initialized:
        lib_dir = str(settings.client_lib_dir) if settings.client_lib_dir else None
        oracledb.init_oracle_client(lib_dir=lib_dir)
        _client_initialized = True
    pool = oracledb.create_pool(
        user=settings.user,
        password=settings.password.get_secret_value(),
        dsn=settings.dsn,
        min=settings.pool_min,
        max=settings.pool_max,
        increment=1,
        getmode=oracledb.POOL_GETMODE_TIMEDWAIT,
        wait_timeout=settings.pool_wait_timeout_ms,
        stmtcachesize=50,
    )
    log.info("oracle_pool_aberto", dsn=settings.dsn, min=settings.pool_min, max=settings.pool_max)
    return pool


def _clob_as_text(cursor: Any, metadata: Any) -> Any:
    """Lê CLOB como ``str`` (payloads e mensagens cabem em memória; sem objetos LOB abertos)."""
    if metadata.type_code is oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    return None


def new_cursor(connection: Any) -> Cursor:
    cursor: Cursor = connection.cursor()
    cursor.outputtypehandler = _clob_as_text
    return cursor


def executemany(
    cursor: Cursor,
    statement: str,
    rows: list[dict[str, Any]],
    *,
    input_sizes: dict[str, Any],
    batcherrors: bool = False,
) -> list[Any]:
    """``executemany`` num cursor próprio da mesma conexão (mesma transação).

    Os tipos de ``setinputsizes`` ficam presos ao cursor; um cursor por comando evita que
    vazem para o próximo ``execute``. Devolve os erros por linha (com ``batcherrors``).
    """
    bulk: Cursor = cursor.connection.cursor()
    try:
        bulk.setinputsizes(**input_sizes)
        bulk.executemany(statement, rows, batcherrors=batcherrors)
        return list(bulk.getbatcherrors()) if batcherrors else []
    finally:
        bulk.close()


def fence(cursor: Cursor, lease_name: str, epoch: int) -> None:
    """Barreira de epoch (ADR-0008): **último comando** antes do commit."""
    cursor.execute(FENCE_SQL, {"lease_name": lease_name, "epoch": epoch})
    if cursor.rowcount != 1:
        raise LeaseLostError(f"epoch {epoch} não é mais o dono de {lease_name}")


def _rollback(connection: Any) -> None:
    try:
        connection.rollback()
    except oracledb.Error:  # conexão já perdida: o banco desfaz sozinho
        log.warning("oracle_rollback_falhou", exc_info=True)


def transaction(
    connection: Any, body: Callable[[Cursor], T], *, failure: type[ApplicationError]
) -> T:
    """Roda ``body`` e faz commit. Qualquer erro → rollback e erro da aplicação.

    Commit que falha por queda de conexão tem resultado **desconhecido**: sobe como
    indisponível; quem chama recomeça do último offset confirmado e a deduplicação absorve
    o que já tinha sido gravado (ADR-0009).
    """
    cursor = new_cursor(connection)
    try:
        result = body(cursor)
        connection.commit()
        return result
    except ApplicationError:
        _rollback(connection)
        raise
    except oracledb.Error as exc:
        _rollback(connection)
        raise translate(exc, failure) from exc
    except BaseException:
        # Bug do lado Python no meio do corpo: o DML já executado não pode ficar pendente
        # na conexão (o próximo commit o levaria junto).
        _rollback(connection)
        raise
    finally:
        cursor.close()


def query(
    connection: Any,
    statement: str,
    parameters: dict[str, Any],
    *,
    failure: type[ApplicationError],
) -> list[Sequence[Any]]:
    """SELECT fora de transação de escrita."""
    cursor = new_cursor(connection)
    try:
        cursor.execute(statement, parameters)
        return list(cursor.fetchall())
    except oracledb.Error as exc:
        raise translate(exc, failure) from exc
    finally:
        cursor.close()


class Session(Protocol):
    async def run(self, work: Callable[[Any], T]) -> T: ...


class DedicatedSession:
    """Uma conexão fixa, um thread (escrita do consumer, ADR-0003)."""

    def __init__(self, pool: Pool, *, call_timeout_ms: int, executor: Executor | None = None):
        self._pool = pool
        self._call_timeout_ms = call_timeout_ms
        self._executor = executor or ThreadPoolExecutor(1, thread_name_prefix="oracle-escrita")
        self._connection: Any = None

    async def run(self, work: Callable[[Any], T]) -> T:
        return await run_in_executor(self._executor, self._call, work)

    def _call(self, work: Callable[[Any], T]) -> T:
        if self._connection is None:
            self._connection = _acquire(self._pool, self._call_timeout_ms)
        try:
            return work(self._connection)
        except StoreUnavailableError:
            self._discard()
            raise
        except ApplicationError:  # rollback feito; a conexão está boa
            raise
        except BaseException:  # inesperado: não reaproveita uma conexão em estado incerto
            self._discard()
            raise

    def _discard(self) -> None:
        connection, self._connection = self._connection, None
        try:
            self._pool.drop(connection)
        except oracledb.Error:
            log.warning("oracle_descartar_conexao_falhou", exc_info=True)

    def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            self._pool.release(connection)


class PooledSession:
    """Conexão do pool por chamada."""

    def __init__(self, pool: Pool, *, call_timeout_ms: int, executor: Executor | None = None):
        self._pool = pool
        self._call_timeout_ms = call_timeout_ms
        self._executor = executor

    async def run(self, work: Callable[[Any], T]) -> T:
        return await run_in_executor(self._executor, self.call, work)

    def call(self, work: Callable[[Any], T]) -> T:
        """Versão síncrona: rotas ``def`` do FastAPI chamam direto (ADR-0003)."""
        connection = _acquire(self._pool, self._call_timeout_ms)
        try:
            result = work(connection)
        except StoreUnavailableError:
            _drop(self._pool, connection)
            raise
        except BaseException:
            self._pool.release(connection)
            raise
        self._pool.release(connection)
        return result


def _acquire(pool: Pool, call_timeout_ms: int) -> Any:
    try:
        connection = pool.acquire()
    except oracledb.Error as exc:
        raise translate(exc, StoreUnavailableError) from exc
    connection.call_timeout = call_timeout_ms
    return connection


def _drop(pool: Pool, connection: Any) -> None:
    try:
        pool.drop(connection)
    except oracledb.Error:
        log.warning("oracle_descartar_conexao_falhou", exc_info=True)
