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
  database/          # Connection management, transactions, pooling
  ingestion/         # Chunked CSV reader and ingestion orchestrator
  fx/                # CurrencyLayer API client and rate storage
  schema/            # Table DDL (usage, fx_rates)
scripts/             # CLI entry points
tests/               # Test suite
data/                # Sample data files
docs/                # Design documentation
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

## Usage

### Ingest CSV data

```bash
python -m scripts.ingest_csv --db-url sqlite:///data.db --file data/usage_data.csv
```

### Fetch FX rates

```bash
python -m scripts.fetch_rates --db-url sqlite:///data.db --date 2021-10-01 --currencies ILS EUR GBP
```

## Running Tests

```bash
pytest                # all tests
pytest -v             # verbose
pytest -k test_upsert # specific test
```

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
