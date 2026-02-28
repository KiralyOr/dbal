"""SQLite implementation of DatabaseService."""

import sqlite3
import threading
from contextlib import contextmanager
from queue import Empty, Queue
from typing import Any, Iterator

from dbal.service import DatabaseService
from dbal.types import Params, ParamsList


class SQLiteDatabaseService(DatabaseService):
    """SQLite backend using stdlib sqlite3.

    Thread-safe via a connection pool (Queue). Each transaction() call
    acquires a dedicated connection and returns it on exit.
    """

    def __init__(self, db_path: str, pool_size: int = 4):
        self._db_path = db_path
        self._pool_size = pool_size
        self._pool: Queue[sqlite3.Connection] = Queue(maxsize=pool_size)
        self._local = threading.local()

    def connect(self) -> None:
        for _ in range(self._pool_size):
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._pool.put(conn)

    def close(self) -> None:
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break

    def _acquire(self) -> sqlite3.Connection:
        return self._pool.get(timeout=30)

    def _release(self, conn: sqlite3.Connection) -> None:
        self._pool.put(conn)

    def _get_conn(self) -> sqlite3.Connection:
        """Get the connection for the current transaction, or acquire one for a single op."""
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
        cursor = conn.execute(sql, params or ())
        if cursor.description is None:
            return []
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_many(self, sql: str, params_list: ParamsList) -> None:
        conn = self._get_conn()
        conn.executemany(sql, params_list)

    def execute_ddl(self, sql: str) -> None:
        conn = self._acquire()
        try:
            conn.executescript(sql)
            conn.commit()
        finally:
            self._release(conn)

    def batch_insert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        if not rows:
            return
        cols = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
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
        placeholders = ", ".join("?" for _ in columns)
        conflict_cols = ", ".join(conflict_columns)
        update_cols = [c for c in columns if c not in conflict_columns]
        update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)

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
