# Database Service Design Rationale

## Multi-DB Extension Points

The `DatabaseService` ABC defines the contract. Adding a new backend (e.g., MySQL) requires:
1. Implement `DatabaseService` in a new module (e.g., `mysql_service.py`).
2. Handle dialect differences: paramstyle (`?` vs `%s`), upsert syntax (`ON DUPLICATE KEY UPDATE` for MySQL), and DDL quirks.
3. Register the new scheme in the `create_service()` factory function.

Business logic (ingestion, FX storage) never imports a concrete backend — only the `DatabaseService` interface. This means swapping SQLite for PostgreSQL (or adding MySQL) requires zero changes to ingestion or FX code.

## Statelessness Rationale

The service holds no mutable state beyond the connection pool. There is no "current transaction" at the instance level — each `transaction()` call acquires a fresh connection from the pool and binds it to the calling thread via `threading.local()`. This means:
- Multiple threads can use the same service safely.
- The service can be stopped and restarted without leaking state.
- There is no risk of one thread's transaction affecting another.

## Transaction Boundaries

Each chunk of rows is wrapped in a single `with service.transaction():` block. This provides:
- **Atomicity**: The entire chunk either commits or rolls back.
- **Defined scope**: Callers explicitly control where transactions begin and end.
- **No implicit transactions**: The service raises an error if you try to execute SQL outside a transaction, preventing accidental auto-commit behavior.
