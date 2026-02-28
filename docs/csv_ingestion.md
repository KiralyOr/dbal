# CSV Ingestion

## Duplication Prevention

### How Duplicates Can Arise

1. **Process restart**: Ingestion crashes at row 5000, restarts from row 1 — re-inserts rows 1-5000.
2. **Manual re-run**: Operator runs the script twice on the same file.
3. **Overlapping files**: Two CSV files contain some of the same `(date, bill_id)` pairs.

### Prevention Strategy

- **Database-level unique constraint**: `PRIMARY KEY (date, bill_id)` makes it impossible to insert two rows with the same key, regardless of application logic.
- **Upsert on conflict**: `INSERT ... ON CONFLICT (date, bill_id) DO UPDATE SET ...` ensures retries overwrite existing data rather than failing or duplicating.
- **Chunked transactions**: If a chunk fails, it rolls back completely. On retry, the same chunk is re-applied as an upsert — previously committed rows get updated (no-op if data is identical), and new rows get inserted.

### Mid-Batch Crash Behavior

If the process crashes while processing chunk N:
- Chunks 1 through N-1 are already committed and safe.
- Chunk N's transaction was never committed, so it rolls back.
- On restart, chunk N is re-processed via upsert — safe and idempotent.

## Production Failure Scenarios

### SIGTERM During Ingestion

When the process receives SIGTERM mid-ingestion:

- **Completed chunks are safe.** Each chunk of rows is committed in its own transaction. If SIGTERM arrives between chunks, all previously committed data remains in the database.
- **The in-flight chunk rolls back.** If SIGTERM interrupts a chunk mid-transaction, the database connection closes, and the DB engine automatically rolls back the uncommitted transaction. No partial chunk is ever visible.
- **Restart is safe.** Because all writes use `INSERT ... ON CONFLICT DO UPDATE` (upsert), restarting ingestion over the same CSV file simply re-applies already-committed rows as no-op updates and resumes where it left off. The operation is fully idempotent.

### Database Connection Drop Mid-Transaction

If the database connection drops (network failure, DB restart) while a transaction is open:

- **The transaction is never committed.** An uncommitted transaction on a dropped connection is treated by the DB engine as abandoned and is rolled back automatically.
- **The application sees an exception.** The `transaction()` context manager catches the exception and propagates it after cleanup. No silent data loss.
- **On retry, upsert semantics prevent duplicates.** The same chunk can be re-applied safely.

### Parallel Instances Running Simultaneously

Multiple instances of the ingestion or FX fetch process can run concurrently:

- **No shared in-process state.** Each instance creates its own `DatabaseService` with its own connection pool. There is no global mutable state.
- **Upsert prevents duplicates.** If two instances process the same CSV or fetch the same FX rates, the `ON CONFLICT DO UPDATE` clause ensures the last writer wins, and no duplicate rows are created. The `(date, bill_id)` and `(date, currency)` primary keys enforce uniqueness at the database level.
- **Transaction isolation handles contention.** Each chunk runs in its own transaction. The database's isolation level (WAL mode for SQLite, default READ COMMITTED for PostgreSQL) ensures concurrent transactions see consistent data without deadlocking on typical insert/upsert workloads.
