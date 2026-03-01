# CSV Ingestion

## Schema

```sql
CREATE TABLE IF NOT EXISTS usage_data (
    date              DATE        NOT NULL,
    bill_id           INTEGER     NOT NULL,
    currency          VARCHAR(3)  NOT NULL,
    name              VARCHAR(255) NOT NULL,
    product1_revenue  DECIMAL(15,6) NOT NULL,
    product2_revenue  DECIMAL(15,6) NOT NULL,
    PRIMARY KEY (date, bill_id)
);
CREATE INDEX IF NOT EXISTS idx_usage_date ON usage_data(date);
CREATE INDEX IF NOT EXISTS idx_usage_currency ON usage_data(currency);
```

### Primary key: `(date, bill_id)`

The composite primary key `(date, bill_id)` uniquely identifies each row. A single `bill_id` can appear on different dates, and a single date has many bills, but the combination is unique. This constraint is enforced at the database level — no application logic is needed to prevent duplicates.

### Indexes

- `idx_usage_date` — speeds up queries filtering by date (e.g., "all usage for October 2021").
- `idx_usage_currency` — speeds up queries filtering by currency (e.g., "all ILS rows for FX conversion").

### Design choices

- **`DECIMAL(15,6)` for revenue** — avoids floating-point rounding errors that `FLOAT` or `REAL` would introduce. Revenue values require exact representation for financial calculations.
- **`VARCHAR(3)` for currency** — ISO 4217 currency codes are exactly 3 characters. The constraint prevents invalid data from entering the table.
- **`DATE` type for date** — stored as `YYYY-MM-DD` (converted from the CSV's `DD/MM/YYYY` format during parsing). This enables date arithmetic and range queries natively in SQL.
- **`IF NOT EXISTS`** — makes schema creation idempotent. The table can be created on every run without errors.

## Ingestion flow

1. **Schema creation** — `execute_ddl()` runs the DDL above. Idempotent via `IF NOT EXISTS`.
2. **Streaming read** — `chunked_reader()` opens the CSV and yields chunks of parsed rows (default 5,000 rows per chunk). The full file is never loaded into memory.
3. **Date parsing** — each row's `DD/MM/YYYY` date string is converted to `YYYY-MM-DD`.
4. **Chunked transactions** — each chunk is wrapped in `with service.transaction()`. The entire chunk either commits or rolls back atomically.
5. **Upsert** — `INSERT ... ON CONFLICT (date, bill_id) DO UPDATE SET ...` ensures retries overwrite existing data rather than failing or creating duplicates.
6. **Malformed row handling** — rows that fail parsing (bad dates, non-numeric revenue, missing columns) are logged and skipped without aborting the batch.

## Scaling

The ingestion is designed to handle CSV files with up to 20 million rows:

- **Constant memory usage** — the file is read in chunks of N rows (default 5,000). At no point is the entire file loaded into memory. Memory usage is proportional to the chunk size, not the file size.
- **Chunk size is configurable** — the `--chunk-size` CLI flag allows tuning the trade-off between transaction overhead (smaller chunks = more commits) and memory usage (larger chunks = more rows in memory).
- **No intermediate data structures** — rows are parsed from CSV directly into tuples and passed to the database. There is no intermediate DataFrame, dict-of-lists, or other structure that would multiply memory usage.
- **Database indexes are created upfront** — the `CREATE INDEX IF NOT EXISTS` statements run before ingestion. Indexes are maintained incrementally by the database as rows are inserted, which is more efficient than building indexes after a bulk load for append-heavy workloads.
- **Batched writes** — `executemany()` sends an entire chunk to the database in a single call, which is significantly faster than individual `INSERT` statements. The database can optimize the write path for batch operations.

## Duplication Prevention

### How duplicates can arise

1. **Process restart** — ingestion crashes at row 5,000, restarts from row 1, re-inserts rows 1–5,000.
2. **Manual re-run** — operator runs the script twice on the same file.
3. **Overlapping files** — two CSV files contain some of the same `(date, bill_id)` pairs.

### Prevention strategy

- **Database-level unique constraint** — `PRIMARY KEY (date, bill_id)` makes it impossible to insert two rows with the same key, regardless of application logic.
- **Upsert on conflict** — `INSERT ... ON CONFLICT (date, bill_id) DO UPDATE SET ...` ensures retries overwrite existing data rather than failing or duplicating.
- **Chunked transactions** — if a chunk fails, it rolls back completely. On retry, the same chunk is re-applied as an upsert — previously committed rows are updated (no-op if data is identical), and new rows are inserted.

## Reliability Constraints

### May be terminated at any time

If the process receives SIGTERM or is killed mid-ingestion:

- **Completed chunks are safe.** Each chunk is committed in its own transaction. Chunks 1 through N-1 are already in the database.
- **The in-flight chunk rolls back.** If the process is killed while a chunk's transaction is open, the database connection closes, and the DB engine automatically rolls back the uncommitted transaction. No partial chunk is ever visible in the database.
- **No corrupt state.** The database either has a fully committed chunk or nothing from that chunk — never a half-written set of rows.

### May be restarted automatically

On restart, the ingestion re-processes the entire CSV file from the beginning. This is safe because:

- Every write is an **upsert** (`ON CONFLICT DO UPDATE`). Already-committed rows are overwritten with the same values (a no-op). The chunk that was in-flight when the process was killed is re-applied cleanly.
- The schema creation (`CREATE TABLE IF NOT EXISTS`) is idempotent — it does nothing if the table already exists.
- There is **no checkpoint or offset tracking** to manage. The database itself is the source of truth. This eliminates an entire class of bugs related to stale or corrupt checkpoint files.

### May run multiple instances in parallel

Multiple instances of the ingestion process can run concurrently against the same database:

- **No shared in-process state.** Each instance creates its own `DatabaseService` with its own connection pool. There are no global variables, shared files, or inter-process locks.
- **Upsert prevents duplicates across instances.** If two instances process the same CSV or overlapping CSVs, the `ON CONFLICT DO UPDATE` clause ensures the last writer wins, and no duplicate rows are created. The primary key enforces uniqueness at the database level.
- **Transaction isolation handles contention.** Each chunk runs in its own transaction. SQLite uses WAL (Write-Ahead Logging) mode, which allows concurrent readers alongside a single writer. PostgreSQL uses READ COMMITTED isolation by default, so concurrent transactions see only committed data without dirty reads.

### Must remain stateless

The ingestion process holds **no state that survives a restart**:

- No checkpoint files, offset tracking, or progress markers.
- No in-memory caches or accumulated state between chunks.
- The database is the only durable state. After each chunk commits, the process could be killed and restarted with zero data loss.
- The `DatabaseService` itself holds no mutable state beyond the connection pool — there is no "current transaction" at the instance level.
