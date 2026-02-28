# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python home assignment implementing a **database abstraction layer (DBAL)** for a Senior Data Engineer role. The project is greenfield — only the assignment doc (`Home assignment_DE.docx.md`) and sample data (`Data for Home Assignment - Data Engineer.csv`) exist at the start.

The assignment has four tasks:
1. **Database Service** — Python package with connection management, transactions, batch inserts, upserts, concurrency safety, and multi-DB extensibility
2. **CSV Ingestion** — Streaming ingestion of usage data (up to 20M rows) with idempotency and crash safety
3. **FX Rate Fetching** — CurrencyLayer API integration storing ILS/EUR/GBP rates, idempotent and secrets-free
4. **Production Behavior & Observability** — Written explanations of SIGTERM handling, mid-transaction drops, parallel instances, and data quality signals

## Architecture

### Core Design Principles
- **Stateless**: No in-process state survives restarts; all state lives in the database
- **Idempotent**: Every write operation is safe to retry (upserts via unique constraints)
- **Database-agnostic**: Business logic depends on an abstract interface, not a concrete DB engine
- **Streaming**: CSV ingestion processes chunks (e.g., 10k rows) to support 20M-row files without OOM

### Expected Package Structure
```
src/dbal/
  database/
    base.py          # Abstract DatabaseService protocol/ABC
    postgres.py      # PostgreSQL implementation (or sqlite for local dev)
    connection.py    # Thread-safe connection pooling
    transaction.py   # Context manager for transaction boundaries
  ingestion/
    csv_reader.py    # Chunked CSV reader
    ingest.py        # Ingestion orchestrator
  fx/
    client.py        # CurrencyLayer API client with retry/backoff
    store.py         # FX rate persistence
  schema/
    usage.py         # Usage data table DDL
    fx_rates.py      # FX rates table DDL
scripts/
  ingest_csv.py      # CLI entry point for CSV ingestion
  fetch_rates.py     # CLI entry point for FX rate fetching
tests/
pyproject.toml       # or setup.py
.env.example         # DB_URL, CURRENCY_LAYER_API_KEY, etc.
```

### Key Schema Design
- **usage** table: `(date, bill_id)` as composite primary key or unique constraint; prevents duplicates on retry
- **fx_rates** table: `(date, currency)` as unique constraint; upsert-safe

### CSV Data Format
Columns: `Date_`, `Bill_ID`, `Currency`, `Name`, `Product1 revenue`, `Product2 revenue`
- Dates are `DD/MM/YYYY`
- Currencies include USD, ILS, EUR, GBP

## Commands

Once the project is set up, typical commands will be:

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run a single test
pytest tests/test_ingestion.py::test_chunk_processing -v

# Lint
ruff check src/ tests/

# Format
black src/ tests/

# Ingest CSV
python scripts/ingest_csv.py --file "Data for Home Assignment - Data Engineer.csv"

# Fetch FX rates
python scripts/fetch_rates.py --date 2021-10-01 --currencies ILS EUR GBP
```

Environment variables (use `.env` file, never commit secrets):
- `DB_URL` — SQLAlchemy connection string
- `CURRENCY_LAYER_API_KEY` — CurrencyLayer API key

## Assignment Deliverables

Each task requires both **code** and a **written explanation**. The explanations cover:
- Task 1: Multi-DB extension points, statelessness rationale, transaction boundaries
- Task 2: Duplication scenarios, prevention strategy, DB-level guarantees, transaction scope, mid-batch crash behavior
- Task 3: Retry safety, idempotency guarantee, crash-during-insertion behavior
- Task 4: SIGTERM handling, connection drop behavior, parallel instance safety, observability signals
