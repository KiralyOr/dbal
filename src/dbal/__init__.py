"""Database Abstraction Layer — factory and public API."""

from dbal.task1_database.service import DatabaseService
from dbal.task1_database.sqlite_service import SQLiteDatabaseService


def create_service(db_url: str, pool_size: int = 4) -> DatabaseService:
    """Create a DatabaseService from a connection URL.

    Supported schemes:
    - sqlite:///path/to/db  or  sqlite:///:memory:
    - postgresql://user:pass@host:port/dbname
    """
    if db_url.startswith("sqlite"):
        # Extract path: sqlite:///foo.db -> foo.db, sqlite:///:memory: -> :memory:
        path = db_url.split(":///", 1)[1] if ":///" in db_url else ":memory:"
        return SQLiteDatabaseService(path, pool_size)
    elif db_url.startswith("postgresql"):
        from dbal.task1_database.postgres_service import PostgresDatabaseService

        return PostgresDatabaseService(db_url, pool_size)
    else:
        raise ValueError(f"Unsupported database URL scheme: {db_url}")
