# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python **database abstraction layer (DBAL)** package providing connection management, transactions, batch inserts, upserts, concurrency safety, and multi-DB extensibility. The package includes:

1. **Task 1 — Database Service** — Abstract interface with SQLite backend (PostgreSQL-ready)
2. **Task 2 — CSV Ingestion** — Streaming ingestion of usage data (up to 20M rows) with idempotency and crash safety
3. **Task 3 — FX Rate Fetching** — CurrencyLayer API integration storing ILS/EUR/GBP rates, idempotent and secrets-free
4. **Task 4 — Production Behavior** — SIGTERM handling, connection drops, parallel instances, data observability

## Architecture

### Core Design Principles
- **Stateless**: No in-process state survives restarts; all state lives in the database
- **Idempotent**: Every write operation is safe to retry (upserts via unique constraints)
- **Database-agnostic**: Business logic depends on an abstract interface, not a concrete DB engine
- **Streaming**: CSV ingestion processes chunks (e.g., 10k rows) to support 20M-row files without OOM

### Package Structure
```
src/dbal/
  __init__.py               # Factory: create_service()
  task1_database/
    __init__.py              # Exports DatabaseService, types
    service.py               # Abstract DatabaseService ABC
    types.py                 # Shared types (Row, Params, ParamsList)
    sqlite_service.py        # SQLite backend
    postgres_service.py      # PostgreSQL backend
  task2_ingestion/
    __init__.py              # Exports ingest_csv, parse_date, parse_row
    csv_ingest.py            # Chunked CSV ingestion orchestrator
    schema.py                # Usage data table DDL
  task3_fx/
    __init__.py              # Exports FX clients and store functions
    client.py                # CurrencyLayer API client with retry/backoff
    store.py                 # FX rate persistence
scripts/
  ingest_csv.py              # CLI entry point for CSV ingestion
  fetch_rates.py             # CLI entry point for FX rate fetching
tests/
  conftest.py                # Shared pytest fixtures
  test_task1_database.py     # Database service tests
  test_task2_ingestion.py    # CSV ingestion tests
  test_task3_fx.py           # FX client and store tests
docs/
  task1_database_service.md  # Database service design rationale
  task2_csv_ingestion.md     # CSV ingestion design
  task3_fx_rates.md          # FX rate integration design
  task4_production_behavior.md # Production failure scenarios & observability
  requirements.md            # Original requirements spec
data/
  usage_data.csv             # Sample usage data
pyproject.toml
.env.example                 # DB_URL, CURRENCY_LAYER_API_KEY, etc.
```

### Key Schema Design
- **usage_data** table: `(date, bill_id)` as composite primary key; prevents duplicates on retry
- **fx_rates** table: `(date, currency)` as primary key; upsert-safe

### CSV Data Format
Columns: `Date_`, `Bill_ID`, `Currency`, `Name`, `Product1 revenue`, `Product2 revenue`
- Dates are `DD/MM/YYYY`
- Currencies include USD, ILS, EUR, GBP

## Commands

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run a single test
pytest tests/test_task2_ingestion.py::TestIngestion::test_ingest_basic -v

# Lint
ruff check src/ tests/

# Format
black src/ tests/

# Ingest CSV
python -m scripts.ingest_csv --db-url sqlite:///data.db --file data/usage_data.csv

# Fetch FX rates
python -m scripts.fetch_rates --db-url sqlite:///data.db --date 2021-10-01 --currencies ILS EUR GBP --mock
```

Environment variables (use `.env` file, never commit secrets):
- `DB_URL` — SQLAlchemy connection string
- `CURRENCY_LAYER_API_KEY` — CurrencyLayer API key
