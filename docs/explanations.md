# Task 4 — Production Behavior & Observability

## Part A: Production Failure Scenarios

### 1. SIGTERM During Ingestion

When the process receives SIGTERM mid-ingestion:

- **Completed chunks are safe.** Each chunk of rows is committed in its own transaction. If SIGTERM arrives between chunks, all previously committed data remains in the database.
- **The in-flight chunk rolls back.** If SIGTERM interrupts a chunk mid-transaction, the database connection closes, and the DB engine automatically rolls back the uncommitted transaction. No partial chunk is ever visible.
- **Restart is safe.** Because all writes use `INSERT ... ON CONFLICT DO UPDATE` (upsert), restarting ingestion over the same CSV file simply re-applies already-committed rows as no-op updates and resumes where it left off. The operation is fully idempotent.

### 2. Database Connection Drop Mid-Transaction

If the database connection drops (network failure, DB restart) while a transaction is open:

- **The transaction is never committed.** An uncommitted transaction on a dropped connection is treated by the DB engine as abandoned and is rolled back automatically.
- **The application sees an exception.** The `transaction()` context manager catches the exception and propagates it after cleanup. No silent data loss.
- **On retry, upsert semantics prevent duplicates.** The same chunk can be re-applied safely.

### 3. Parallel Instances Running Simultaneously

Multiple instances of the ingestion or FX fetch process can run concurrently:

- **No shared in-process state.** Each instance creates its own `DatabaseService` with its own connection pool. There is no global mutable state.
- **Upsert prevents duplicates.** If two instances process the same CSV or fetch the same FX rates, the `ON CONFLICT DO UPDATE` clause ensures the last writer wins, and no duplicate rows are created. The `(date, bill_id)` and `(date, currency)` primary keys enforce uniqueness at the database level.
- **Transaction isolation handles contention.** Each chunk runs in its own transaction. The database's isolation level (WAL mode for SQLite, default READ COMMITTED for PostgreSQL) ensures concurrent transactions see consistent data without deadlocking on typical insert/upsert workloads.

## Part B: Data Quality & Observability Signals

### Signals to Monitor

1. **Row counts per ingestion run.** Log the total rows ingested and compare against expected file size. A significant deviation signals data issues or parsing failures.

2. **Malformed row count.** The ingestion process logs and skips rows that fail parsing (bad dates, non-numeric revenue). A spike in skipped rows indicates upstream data quality problems.

3. **Duplicate detection queries.** After ingestion, run:
   ```sql
   SELECT date, bill_id, COUNT(*) FROM usage_data GROUP BY date, bill_id HAVING COUNT(*) > 1;
   ```
   With the primary key constraint, this should always return zero. A non-zero result indicates a schema or upsert misconfiguration.

4. **Revenue range checks.** Query for negative or anomalously large revenue values:
   ```sql
   SELECT * FROM usage_data WHERE product1_revenue < 0 OR product2_revenue < 0;
   SELECT * FROM usage_data WHERE product1_revenue > 1000000;
   ```

5. **Currency coverage.** Verify expected currencies appear in the data:
   ```sql
   SELECT currency, COUNT(*) FROM usage_data GROUP BY currency;
   ```
   Missing currencies may indicate upstream filtering or format changes.

6. **FX rate staleness.** For the `fx_rates` table, check that rates exist for each date present in `usage_data`:
   ```sql
   SELECT DISTINCT u.date FROM usage_data u
   LEFT JOIN fx_rates f ON u.date = f.date AND u.currency = f.currency
   WHERE f.date IS NULL AND u.currency != 'USD';
   ```

7. **Ingestion timing metrics.** Log wall-clock time per chunk and total ingestion time. Degradation over time may indicate index bloat or lock contention.

## Task 1 — Database Service Design Rationale

### Multi-DB Extension Points

The `DatabaseService` ABC defines the contract. Adding a new backend (e.g., MySQL) requires:
1. Implement `DatabaseService` in a new module (e.g., `mysql_service.py`).
2. Handle dialect differences: paramstyle (`?` vs `%s`), upsert syntax (`ON DUPLICATE KEY UPDATE` for MySQL), and DDL quirks.
3. Register the new scheme in the `create_service()` factory function.

Business logic (ingestion, FX storage) never imports a concrete backend — only the `DatabaseService` interface. This means swapping SQLite for PostgreSQL (or adding MySQL) requires zero changes to ingestion or FX code.

### Statelessness Rationale

The service holds no mutable state beyond the connection pool. There is no "current transaction" at the instance level — each `transaction()` call acquires a fresh connection from the pool and binds it to the calling thread via `threading.local()`. This means:
- Multiple threads can use the same service safely.
- The service can be stopped and restarted without leaking state.
- There is no risk of one thread's transaction affecting another.

### Transaction Boundaries

Each chunk of rows is wrapped in a single `with service.transaction():` block. This provides:
- **Atomicity**: The entire chunk either commits or rolls back.
- **Defined scope**: Callers explicitly control where transactions begin and end.
- **No implicit transactions**: The service raises an error if you try to execute SQL outside a transaction, preventing accidental auto-commit behavior.

## Task 2 — Duplication Prevention

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

## Task 3 — FX Rate Idempotency

### Retry Safety

The FX client uses exponential backoff (1s, 2s, 4s) with up to 3 retries. Each retry is a fresh HTTP request to the CurrencyLayer API — the API is read-only and safe to retry.

### Storage Idempotency

`store_rates()` uses `INSERT ... ON CONFLICT (date, currency) DO UPDATE SET rate_to_usd = ...`. Storing the same rate twice for the same `(date, currency)` simply updates the existing row. This is safe for:
- Retried fetches (same data, no duplicates)
- Corrected fetches (updated rate overwrites the old one)

### Crash During Insertion

If the process crashes after fetching rates but before the `store_rates()` transaction commits, no rates are stored. On restart, rates are re-fetched and re-stored via upsert — no partial state.
