"""Pool, sessões e transações do PostgreSQL com psycopg 3 síncrono (ADR-0021 §2 e §3).

Todo acesso é síncrono e roda num executor via ``run_in_executor`` (leva o contexto de log):

- ``DedicatedSession``: uma conexão fixa e **um** thread. É a conexão de escrita do consumer:
  um único thread preserva a ordem das transações da chain.
- ``PooledSession``: conexão do pool por chamada. Publisher, enricher, controle e expurgo.
- ``InlineSession``: ``PooledSession`` no thread de quem chama. Só a API (ADR-0017).

Toda conexão abre com ``autocommit=True``: a escrita roda dentro de ``transaction`` (um
``with conn.transaction()``; blocos aninhados viram savepoints, ADR-0022) e a leitura roda
fora de transação. Assim nenhuma conexão fica "idle in transaction" entre uma chamada e
outra. Uma falha de indisponibilidade descarta a conexão; a próxima chamada pega outra.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import psycopg
from psycopg_pool import ConnectionPool

from ohip_streaming.adapters.postgres.errors import translate
from ohip_streaming.application.errors import (
    ApplicationError,
    LeaseLostError,
    StoreOperationError,
    StoreUnavailableError,
)
from ohip_streaming.logging import get_logger, run_in_executor

if TYPE_CHECKING:
    from ohip_streaming.config import Environment, PostgresSettings

log = get_logger(__name__)
T = TypeVar("T")

FENCE_SQL = """
UPDATE ohip_lease
   SET last_write_at = statement_timestamp()
 WHERE lease_name = %(lease_name)s
   AND epoch = %(epoch)s"""


VERIFIED_TLS = frozenset({"verify-full", "verify-ca"})


class Cursor(Protocol):
    """O subconjunto de ``psycopg.Cursor`` usado pelos stores (os testes usam um fake)."""

    rowcount: int
    connection: Any

    def execute(self, query: Any, params: Any = None) -> Any: ...

    def executemany(self, query: Any, params_seq: Any, *, returning: bool = False) -> None: ...

    def results(self) -> Iterator[Any]: ...

    def fetchone(self) -> Any: ...

    def fetchall(self) -> list[Any]: ...

    def close(self) -> None: ...


class Pool(Protocol):
    def getconn(self, timeout: float | None = None) -> Any: ...

    def putconn(self, conn: Any) -> None: ...

    def close(self, timeout: float = 5.0) -> None: ...


def connection_kwargs(
    settings: PostgresSettings,
    *,
    environment: Environment,
    application_name: str,
    statement_timeout_ms: int | None = None,
) -> dict[str, Any]:
    """Parâmetros de conexão (a senha não vai para log). Fora de desenvolvimento, só TLS com
    certificado conferido (``verify-full`` ou ``verify-ca``): ``allow``/``prefer`` caem para
    texto puro sem avisar e ``require`` não confere o servidor (RNF-07)."""
    from ohip_streaming.config import Environment

    if environment is not Environment.DESENVOLVIMENTO and settings.sslmode not in VERIFIED_TLS:
        raise ValueError(
            f"POSTGRES_SSLMODE={settings.sslmode} só é aceito em APP_ENVIRONMENT=desenvolvimento;"
            " use verify-full ou verify-ca"
        )
    timeout = (
        settings.statement_timeout_ms if statement_timeout_ms is None else statement_timeout_ms
    )
    kwargs: dict[str, Any] = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.db,
        "user": settings.user,
        "password": settings.password.get_secret_value(),
        "sslmode": settings.sslmode,
        "application_name": application_name,
        "options": f"-c TimeZone=UTC -c statement_timeout={timeout}",
        "autocommit": True,
    }
    if settings.sslrootcert is not None:
        kwargs["sslrootcert"] = str(settings.sslrootcert)
    return kwargs


def open_pool(
    settings: PostgresSettings,
    *,
    environment: Environment,
    application_name: str,
    statement_timeout_ms: int | None = None,
) -> ConnectionPool[Any]:
    """Cria o pool (``autocommit``, UTC, ``statement_timeout``). Não espera conexão: com o banco
    fora na partida, o processo sobe e o ``getconn`` estoura em ``POSTGRES_POOL_WAIT_TIMEOUT_MS``
    (banco indisponível) até o banco voltar."""
    kwargs = connection_kwargs(
        settings,
        environment=environment,
        application_name=application_name,
        statement_timeout_ms=statement_timeout_ms,
    )
    pool: ConnectionPool[Any] = ConnectionPool(
        "",
        kwargs=kwargs,
        min_size=settings.pool_min,
        max_size=settings.pool_max,
        timeout=settings.pool_wait_timeout_ms / 1000,
        name=application_name,
        open=True,
    )
    log.info(
        "postgres_pool_aberto",
        host=settings.host,
        db=settings.db,
        min=settings.pool_min,
        max=settings.pool_max,
    )
    return pool


def executemany(
    cursor: Cursor, statement: str, rows: Sequence[dict[str, Any]], *, returning: bool = False
) -> list[list[Any]]:
    """``executemany`` na mesma transação. Com ``returning``, devolve as linhas de cada
    execução, na ordem dos parâmetros (uma lista vazia quando o comando não retornou nada)."""
    cursor.executemany(statement, rows, returning=returning)
    if not returning:
        return []
    results = [list(result.fetchall()) for result in cursor.results()]
    if len(results) != len(rows):  # nunca classificar com resultados deslocados (ADR-0022)
        raise psycopg.ProgrammingError(
            f"executemany devolveu {len(results)} resultados para {len(rows)} linhas"
        )
    return results


@contextmanager
def savepoint(cursor: Cursor) -> Iterator[None]:
    """Savepoint dentro da transação corrente: erro no bloco desfaz só o bloco (ADR-0022)."""
    with cursor.connection.transaction():
        yield


def fence(cursor: Cursor, lease_name: str, epoch: int) -> None:
    """Barreira de epoch (ADR-0008): **último comando** antes do commit."""
    cursor.execute(FENCE_SQL, {"lease_name": lease_name, "epoch": epoch})
    if cursor.rowcount != 1:
        raise LeaseLostError(f"epoch {epoch} não é mais o dono de {lease_name}")


def transaction(
    connection: Any, body: Callable[[Cursor], T], *, failure: type[ApplicationError]
) -> T:
    """Roda ``body`` numa transação e faz commit. Qualquer erro → rollback e erro da aplicação.

    Commit que falha por queda de conexão tem resultado **desconhecido**: sobe como
    indisponível; quem chama recomeça do último offset confirmado e a deduplicação absorve
    o que já tinha sido gravado (ADR-0009).
    """
    cursor: Cursor | None = None
    try:
        cursor = connection.cursor()  # numa conexão já fechada, falha aqui: também traduzido
        with connection.transaction():
            return body(cursor)
    except ApplicationError:
        raise  # rollback já feito pelo bloco
    except psycopg.Error as exc:
        raise translate(exc, failure) from exc
    finally:
        if cursor is not None:
            cursor.close()


def query(
    connection: Any,
    statement: str,
    parameters: dict[str, Any],
    *,
    failure: type[ApplicationError],
) -> list[Sequence[Any]]:
    """``SELECT`` fora de transação (``autocommit``): nada fica aberto depois da leitura."""
    cursor: Cursor | None = None
    try:
        cursor = connection.cursor()
        cursor.execute(statement, parameters)
        return list(cursor.fetchall())
    except psycopg.Error as exc:
        raise translate(exc, failure) from exc
    finally:
        if cursor is not None:
            cursor.close()


class Session(Protocol):
    async def run(self, work: Callable[[Any], T]) -> T: ...


async def read(
    session: Session,
    statement: str,
    parameters: dict[str, Any],
    *,
    failure: type[ApplicationError] = StoreOperationError,
) -> list[Sequence[Any]]:
    """Leitura fora de transação pela sessão (refatoração 12: um apoio para todos os stores)."""
    return await session.run(
        lambda connection: query(connection, statement, parameters, failure=failure)
    )


async def write(
    session: Session,
    body: Callable[[Cursor], T],
    *,
    failure: type[ApplicationError] = StoreOperationError,
) -> T:
    """Escrita numa transação pela sessão: ``body`` roda com o cursor e o commit vem no fim."""
    return await session.run(lambda connection: transaction(connection, body, failure=failure))


class DedicatedSession:
    """Uma conexão fixa, um thread (escrita do consumer, ADR-0021 §2)."""

    def __init__(self, pool: Pool, *, executor: Executor | None = None) -> None:
        self._pool = pool
        self._owns_executor = executor is None  # o próprio, encerrado no close()
        self._executor = executor or ThreadPoolExecutor(1, thread_name_prefix="postgres-escrita")
        self._connection: Any = None

    async def run(self, work: Callable[[Any], T]) -> T:
        return await run_in_executor(self._executor, self._call, work)

    def _call(self, work: Callable[[Any], T]) -> T:
        if self._connection is None:
            self._connection = _acquire(self._pool)
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
        _drop(self._pool, connection)

    def close(self) -> None:
        if self._connection is not None:
            connection, self._connection = self._connection, None
            self._pool.putconn(connection)
        if self._owns_executor:
            self._executor.shutdown(wait=False)


class PooledSession:
    """Conexão do pool por chamada."""

    def __init__(self, pool: Pool, *, executor: Executor | None = None) -> None:
        self._pool = pool
        self._executor = executor

    async def run(self, work: Callable[[Any], T]) -> T:
        return await run_in_executor(self._executor, self.call, work)

    def call(self, work: Callable[[Any], T]) -> T:
        """Versão síncrona: rotas ``def`` do FastAPI chamam direto (ADR-0017)."""
        connection = _acquire(self._pool)
        try:
            result = work(connection)
        except StoreUnavailableError:
            _drop(self._pool, connection)
            raise
        except BaseException:
            self._pool.putconn(connection)
            raise
        self._pool.putconn(connection)
        return result


class InlineSession:
    """``PooledSession`` sem executor: o trabalho roda no thread de quem chama.

    Só para a API (ADR-0017): a rota ``def`` já roda num thread do threadpool e executa o caso
    de uso com ``asyncio.run`` nesse mesmo thread. O banco nunca é chamado do event loop do
    Uvicorn, e mandar para outro executor só trocaria de thread.
    """

    def __init__(self, pooled: PooledSession) -> None:
        self._pooled = pooled

    async def run(self, work: Callable[[Any], T]) -> T:
        return self._pooled.call(work)


def _acquire(pool: Pool) -> Any:
    try:
        return pool.getconn()
    except Exception as exc:  # PoolTimeout, pool fechado, banco fora
        raise translate(exc, StoreUnavailableError) from exc


def _drop(pool: Pool, connection: Any) -> None:
    """Fecha a conexão e a devolve: o pool descarta conexões fechadas e abre outra."""
    try:
        connection.close()
    except Exception:  # já perdida
        log.warning("postgres_fechar_conexao_falhou", exc_info=True)
    try:
        pool.putconn(connection)
    except Exception:
        log.warning("postgres_descartar_conexao_falhou", exc_info=True)
