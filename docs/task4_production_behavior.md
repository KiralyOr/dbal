# Production Behavior & Data Observability

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

### Crash During FX Rate Insertion

If the process crashes after fetching rates but before the `store_rates()` transaction commits, no rates are stored. On restart, rates are re-fetched and re-stored via upsert — no partial state.

## Data Quality & Observability Signals

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
