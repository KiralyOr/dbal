"""PostgreSQL implementation of DatabaseService."""

import threading
from contextlib import contextmanager
from queue import Empty, Queue
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from dbal.service import DatabaseService
from dbal.types import Params, ParamsList


class PostgresDatabaseService(DatabaseService):
    """PostgreSQL backend using psycopg2.

    Thread-safe via a connection pool (Queue). Each transaction() call
    acquires a dedicated connection and returns it on exit.
    """

    def __init__(self, dsn: str, pool_size: int = 4):
        self._dsn = dsn
        self._pool_size = pool_size
        self._pool: Queue = Queue(maxsize=pool_size)
        self._local = threading.local()

    def connect(self) -> None:
        for _ in range(self._pool_size):
            conn = psycopg2.connect(self._dsn)
            conn.autocommit = False
            self._pool.put(conn)

    def close(self) -> None:
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break

    def _acquire(self):
        return self._pool.get(timeout=30)

    def _release(self, conn) -> None:
        self._pool.put(conn)

    def _get_conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn
        raise RuntimeError(
            "No active transaction. Wrap calls in a `with service.transaction():` block."
        )

    @contextmanager
    def transaction(self) -> Iterator[None]:
        conn = self._acquire()
        self._local.conn = conn
        try:
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._local.conn = None
            self._release(conn)

    def execute(self, sql: str, params: Params | None = None) -> list[dict[str, Any]]:
        conn = self._get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if cur.description is None:
                return []
            return [dict(row) for row in cur.fetchall()]

    def execute_many(self, sql: str, params_list: ParamsList) -> None:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, params_list)

    def execute_ddl(self, sql: str) -> None:
        conn = self._acquire()
        try:
            with conn.cursor() as cur:
                for statement in sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        cur.execute(statement)
            conn.commit()
        finally:
            self._release(conn)

    def batch_insert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        if not rows:
            return
        cols = ", ".join(columns)
        placeholders = ", ".join("%s" for _ in columns)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        self.execute_many(sql, rows)

    def upsert(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple],
        conflict_columns: list[str],
    ) -> None:
        if not rows:
            return
        cols = ", ".join(columns)
        placeholders = ", ".join("%s" for _ in columns)
        conflict_cols = ", ".join(conflict_columns)
        update_cols = [c for c in columns if c not in conflict_columns]
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        if update_cols:
            sql = (
                f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
            )
        else:
            sql = (
                f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_cols}) DO NOTHING"
            )
        self.execute_many(sql, rows)
