"""Abstract DatabaseService interface."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Iterator

from dbal.task1_database.types import Params, ParamsList


class DatabaseService(ABC):
    """Database-agnostic interface for all DB operations.

    Design principles:
    - Stateless: no mutable state beyond the connection pool
    - Thread-safe: each transaction() acquires its own connection
    - DB-agnostic: callers program against this ABC, never a concrete backend
    """

    @abstractmethod
    def connect(self) -> None:
        """Initialize the connection pool."""

    @abstractmethod
    def close(self) -> None:
        """Close all connections and release resources."""

    @abstractmethod
    def execute(self, sql: str, params: Params | None = None) -> list[dict[str, Any]]:
        """Execute a single SQL statement and return rows as dicts."""

    @abstractmethod
    def execute_many(self, sql: str, params_list: ParamsList) -> None:
        """Execute a SQL statement for each parameter set."""

    @abstractmethod
    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager: acquires a connection, commits on success, rolls back on error."""

    @abstractmethod
    def execute_ddl(self, sql: str) -> None:
        """Execute DDL statements (CREATE TABLE, CREATE INDEX, etc.)."""

    @abstractmethod
    def batch_insert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        """Insert multiple rows into a table."""

    @abstractmethod
    def upsert(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple],
        conflict_columns: list[str],
    ) -> None:
        """Insert rows, updating on conflict with the specified columns."""
