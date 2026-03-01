# DBAL — Database Abstraction Layer

A Python package providing database access through a clean, extensible interface. Built for reliability: every operation is stateless, idempotent, and crash-safe.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 Business Logic                  │
│          (Ingestion, FX Rate Fetching)          │
└──────────────────────┬──────────────────────────┘
                       │
              DatabaseService ABC
                       │
         ┌─────────────┼─────────────┐
         │             │             │
    ┌────▼────┐  ┌─────▼─────┐  ┌───▼───┐
    │ SQLite  │  │ PostgreSQL│  │  ...  │
    └─────────┘  └───────────┘  └───────┘
```

Business logic depends only on the abstract `DatabaseService` interface. Swapping backends requires zero changes to ingestion or FX code.

## Package Structure

```
src/dbal/
  __init__.py               # Factory: create_service()
  task1_database/            # Task 1: Database service
    service.py               # Abstract DatabaseService ABC
    types.py                 # Shared types (Row, Params, ParamsList)
    sqlite_service.py        # SQLite backend
    postgres_service.py      # PostgreSQL backend
  task2_ingestion/           # Task 2: CSV ingestion
    csv_ingest.py            # Chunked CSV ingestion orchestrator
    schema.py                # Usage data table DDL
  task3_fx/                  # Task 3: FX rate fetching
    client.py                # CurrencyLayer API client with retry/backoff
    store.py                 # FX rate persistence
scripts/                     # CLI entry points
tests/                       # Unit tests (SQLite) and E2E tests (PostgreSQL)
docs/                        # Design documentation (including Task 4)
data/                        # Sample data files
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Copy `.env.example` to `.env` and fill in your values:
- `DB_URL` — database connection string (e.g., `sqlite:///data.db`)
- `CURRENCY_LAYER_API_KEY` — API key for CurrencyLayer

---

## Task 1 — Database Service

An abstract `DatabaseService` interface (`service.py`) with two concrete backends: SQLite and PostgreSQL. All business logic programs against the ABC, never a specific backend.

### Interface

The ABC defines the following operations:

| Method | Purpose |
|---|---|
| `connect()` / `close()` | Manage the connection pool lifecycle |
| `transaction()` | Context manager — acquires a connection, commits on success, rolls back on error |
| `execute(sql, params)` | Run a query and return rows as `list[dict]` |
| `execute_many(sql, params_list)` | Execute a statement for each parameter set |
| `execute_ddl(sql)` | Run DDL statements (CREATE TABLE, CREATE INDEX) outside a transaction |
| `batch_insert(table, columns, rows)` | Insert multiple rows |
| `upsert(table, columns, rows, conflict_columns)` | Insert rows, updating on conflict |

### Backend differences

Each backend handles its own SQL dialect internally:

| Concern | SQLite | PostgreSQL |
|---|---|---|
| Paramstyle | `?` | `%s` |
| Upsert keyword | `excluded.col` | `EXCLUDED.col` |
| DDL execution | `executescript()` | Statement-by-statement split on `;` |
| Journal mode | WAL pragma on connect | Managed by server config |
| Connection pool | `queue.Queue` of `sqlite3.Connection` | `queue.Queue` of `psycopg2` connections |

### Concurrency safety

Both backends use a `Queue`-based connection pool (`queue.Queue`, which is inherently thread-safe). Each `transaction()` call acquires a dedicated connection via `pool.get()` and binds it to the calling thread via `threading.local()`. This guarantees:

- **No shared connections** — Two threads never use the same connection simultaneously. If all connections are in use, additional threads block (up to 30s) rather than corrupting state.
- **Thread-local binding** — Within a transaction, every SQL call (`execute`, `upsert`, etc.) reads the connection from `threading.local()`, so each thread always operates on its own connection.
- **No shared mutable state** — The service holds no instance-level "current transaction" or cursor. The only mutable state is the pool itself (thread-safe `Queue`) and the per-thread local.
- **Database-level isolation** — SQLite uses WAL mode, allowing concurrent readers alongside a single writer. PostgreSQL uses READ COMMITTED by default, so concurrent transactions see only committed data.

### Correctness under failure

The `transaction()` context manager is the central safety mechanism:

```python
@contextmanager
def transaction(self):
    conn = self._acquire()
    self._local.conn = conn
    try:
        yield
        conn.commit()       # only reached if no exception
    except Exception:
        conn.rollback()     # any failure → full rollback
        raise
    finally:
        self._local.conn = None
        self._release(conn) # connection always returned to pool
```

This guarantees:

- **Atomicity** — If any operation inside the `with` block raises, `commit()` is never reached, `rollback()` is called, and the exception propagates. The database never contains a partial write from a failed transaction.
- **No connection leaks** — The `finally` block always clears the thread-local and returns the connection to the pool, regardless of success or failure. A leaked connection would eventually starve the pool and deadlock the application.
- **No operations outside transactions** — `_get_conn()` raises `RuntimeError` if called without an active transaction, preventing accidental auto-commit writes that could leave inconsistent state on failure.
- **DDL isolation** — `execute_ddl()` acquires its own connection, runs the DDL, commits, and releases — outside the transaction mechanism. This prevents a DDL failure from corrupting a data transaction.
- **Connection drop** — If the database connection drops mid-transaction, the `execute` or `commit` call raises an exception. The `except` block calls `rollback()`, and the DB engine treats an abandoned connection as an implicit rollback. No partial state is ever committed.
- **Process kill (SIGTERM)** — Any uncommitted transaction is never committed. Both SQLite and PostgreSQL automatically roll back transactions on connection close. Since every write goes through `upsert`, the caller can safely restart and re-apply the same data.

### Factory

`create_service(db_url)` inspects the URL scheme and returns the appropriate backend:
```python
from dbal import create_service

service = create_service("sqlite:///data.db")       # SQLite
service = create_service("postgresql://u:p@host/db") # PostgreSQL
```

---

## Task 2 — CSV Ingestion

Streaming ingestion of CSV usage data (designed for files up to 20M rows) into the `usage_data` table. The ingestion is chunked, idempotent, and crash-safe.

### Schema

```sql
CREATE TABLE usage_data (
    date              DATE        NOT NULL,
    bill_id           INTEGER     NOT NULL,
    currency          VARCHAR(3)  NOT NULL,
    name              VARCHAR(255) NOT NULL,
    product1_revenue  DECIMAL(15,6) NOT NULL,
    product2_revenue  DECIMAL(15,6) NOT NULL,
    PRIMARY KEY (date, bill_id)
);
```

The composite primary key `(date, bill_id)` prevents duplicate rows regardless of how many times ingestion runs.

### How it works

1. **Streaming read** — `chunked_reader()` yields chunks of parsed rows (default 5000) without loading the full file into memory.
2. **Date parsing** — Converts `DD/MM/YYYY` from the CSV to `YYYY-MM-DD` for the database.
3. **Chunked transactions** — Each chunk is wrapped in `with service.transaction()`. The entire chunk either commits or rolls back.
4. **Upsert on conflict** — `INSERT ... ON CONFLICT (date, bill_id) DO UPDATE SET ...` ensures retries overwrite existing data rather than failing or creating duplicates.
5. **Malformed row handling** — Rows that fail parsing (bad dates, non-numeric values) are logged and skipped without aborting the batch.

### Crash recovery

If the process is killed mid-ingestion:
- Chunks 1 through N-1 are already committed and safe.
- The in-flight chunk N rolls back automatically (uncommitted transaction).
- On restart, the entire CSV is re-processed. Already-committed rows are updated as no-ops via upsert; the failed chunk resumes cleanly.

### Usage

```bash
python -m scripts.ingest_csv --db-url sqlite:///data.db --file data/usage_data.csv --chunk-size 5000
```

Or programmatically:
```python
from dbal import create_service
from dbal.task2_ingestion import ingest_csv

service = create_service("sqlite:///data.db")
service.connect()
total = ingest_csv(service, "data/usage_data.csv", chunk_size=5000)
service.close()
```

---

## Task 3 — FX Rate Fetching

Integration with the CurrencyLayer API to fetch and store historical USD exchange rates (ILS, EUR, GBP). The design is idempotent, retry-safe, and secrets-free in code.

### Schema

```sql
CREATE TABLE fx_rates (
    date          DATE        NOT NULL,
    currency      VARCHAR(3)  NOT NULL,
    rate_to_usd   DECIMAL(15,6) NOT NULL,
    fetched_at    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, currency)
);
```

### FX client hierarchy

An abstract `FXClient` interface with two implementations:

- **`CurrencyLayerClient`** — Calls the live CurrencyLayer API (`/historical` endpoint). Features exponential backoff retry (delays: 1s, 2s, 4s) with up to 3 attempts. The API key is read from the `CURRENCY_LAYER_API_KEY` environment variable at the CLI level and passed in — no secrets are stored in code.
- **`MockCurrencyLayerClient`** — Returns fixed rates (ILS: 3.21, EUR: 0.86, GBP: 0.73) for testing and development without an API key.

### Storage

`store_rates()` converts the fetched `{currency: rate}` dict into rows and upserts them:
```
INSERT INTO fx_rates (date, currency, rate_to_usd)
VALUES (...)
ON CONFLICT (date, currency) DO UPDATE SET rate_to_usd = EXCLUDED.rate_to_usd
```

This means:
- Storing the same rates twice is a no-op (same values overwritten).
- Storing corrected rates overwrites the old values — last write wins.

### Usage

```bash
# With mock client (no API key needed)
python -m scripts.fetch_rates --db-url sqlite:///data.db --date 2021-10-01 --currencies ILS EUR GBP --mock

# With live API
export CURRENCY_LAYER_API_KEY=your_key_here
python -m scripts.fetch_rates --db-url sqlite:///data.db --date 2021-10-01 --currencies ILS EUR GBP
```

---

## Task 4 — Production Behavior

Design documentation for production failure scenarios and data observability. See `docs/task4_production_behavior.md` for the full writeup. Key scenarios addressed:

### Failure scenarios

| Scenario | Behavior |
|---|---|
| **SIGTERM during ingestion** | Completed chunks are safe (committed). The in-flight chunk rolls back. Restart re-applies via upsert — fully idempotent. |
| **DB connection drop mid-transaction** | The uncommitted transaction is rolled back by the DB engine. The application sees an exception. Retry is safe via upsert. |
| **Parallel instances** | No shared in-process state. Upsert prevents duplicates across instances. DB isolation (WAL for SQLite, READ COMMITTED for PostgreSQL) handles contention. |
| **Crash during FX insertion** | If `store_rates()` hasn't committed, no rates are stored. Re-fetch and re-store on restart — safe and idempotent. |

### Data observability signals

1. **Row counts per run** — compare against expected file size to detect data issues.
2. **Malformed row count** — spikes indicate upstream data quality problems.
3. **Revenue range checks** — query for negative or anomalously large values.
4. **Currency coverage** — verify expected currencies appear in the data.
5. **FX rate staleness** — check that rates exist for every date and currency in `usage_data`.
6. **Ingestion timing** — track wall-clock time per chunk to detect performance degradation.

---

## Running Tests

### Unit tests (SQLite, no external dependencies)

```bash
pytest              # 18 tests, runs in ~0.1s
pytest -v           # verbose output
pytest -k test_upsert  # specific test
```

### E2E tests (PostgreSQL via Podman)

```bash
# 1. Start PostgreSQL (port 5433, db: dbal_test, user/pass: dbal/dbal)
podman-compose up -d

# 2. Run E2E tests
pytest -m e2e -v

# 3. Tear down
podman-compose down
```

E2E tests are marked with `@pytest.mark.e2e` and excluded from default `pytest` runs. They exercise the full flow (ingestion + FX storage) against a real PostgreSQL 16 instance.

## Linting & Formatting

```bash
ruff check src/ tests/
black src/ tests/
```

## Design Principles

- **Stateless** — No in-process state survives restarts. All state lives in the database.
- **Idempotent** — Every write uses upsert semantics (`INSERT ... ON CONFLICT DO UPDATE`). Safe to retry any operation.
- **Crash-safe** — Chunked transactions ensure partial failures roll back cleanly. Restart picks up where it left off.
- **Database-agnostic** — Abstract interface decouples business logic from the storage engine.
- **Streaming** — CSV ingestion processes data in chunks to handle files with 20M+ rows without exceeding memory.

See `docs/` for detailed design rationale on each component.
