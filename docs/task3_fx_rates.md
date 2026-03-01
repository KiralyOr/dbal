# FX Rate Integration

## Schema

```sql
CREATE TABLE IF NOT EXISTS fx_rates (
    date          DATE        NOT NULL,
    currency      VARCHAR(3)  NOT NULL,
    rate_to_usd   DECIMAL(15,6) NOT NULL,
    fetched_at    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, currency)
);
```

### Primary key: `(date, currency)`

Each row stores the exchange rate for one currency on one date. The composite primary key ensures there is exactly one rate per `(date, currency)` pair — no duplicates, regardless of how many times the fetch runs.

### Design choices

- **`DECIMAL(15,6)` for rate** — avoids floating-point rounding. Exchange rates require precise representation (e.g., `3.210000` not `3.2099999...`).
- **`fetched_at` with `DEFAULT CURRENT_TIMESTAMP`** — records when the rate was last written. Useful for auditing and debugging stale data, without requiring application code to set it.
- **`IF NOT EXISTS`** — makes schema creation idempotent. Safe to run on every startup.

## How retries are handled safely

The `CurrencyLayerClient` uses **exponential backoff** with up to 3 retries:

| Attempt | Delay before retry |
|---|---|
| 1st failure | 1 second |
| 2nd failure | 2 seconds |
| 3rd failure | no retry — exception is raised |

Retries are safe because:

- **The API call is read-only.** Fetching rates from CurrencyLayer does not modify any state on the API side. Each retry is a fresh HTTP GET request that returns the same data.
- **Each retry is independent.** There is no accumulated state between attempts. A failed request is discarded entirely, and the next attempt starts from scratch.
- **The retry loop catches transient errors.** Network timeouts (`requests.RequestException`), malformed responses (`KeyError`), and API errors (`RuntimeError` from non-success responses) all trigger a retry. Permanent failures (e.g., invalid API key) will exhaust retries and propagate the exception to the caller.

## How idempotency is guaranteed

`store_rates()` uses upsert semantics:

```sql
INSERT INTO fx_rates (date, currency, rate_to_usd)
VALUES (...)
ON CONFLICT (date, currency) DO UPDATE SET rate_to_usd = EXCLUDED.rate_to_usd
```

This means:

- **First insert** — the row is created normally.
- **Repeated insert with same data** — the `ON CONFLICT` clause fires, and the `rate_to_usd` column is overwritten with the same value. The row is unchanged — effectively a no-op.
- **Repeated insert with updated data** — the `ON CONFLICT` clause fires, and the `rate_to_usd` column is overwritten with the new value. The `fetched_at` timestamp is also updated (via `DEFAULT CURRENT_TIMESTAMP`), providing an audit trail.

This guarantees that:

- Running the fetch script twice for the same date and currencies produces the same result as running it once.
- No duplicate rows are ever created, regardless of how many times the process runs.
- Corrected rates (e.g., if the API returned a different value on a later call) automatically overwrite the old values — last write wins.

## What happens if the process crashes during rate insertion

The `store_rates()` function wraps all inserts in a single transaction:

```python
def store_rates(service, date, rates):
    rows = [(date, currency, rate) for currency, rate in rates.items()]
    with service.transaction():
        service.upsert(FX_TABLE, FX_COLUMNS, rows, FX_CONFLICT_COLUMNS)
```

**Crash before `store_rates()` is called** — rates were fetched from the API but never written. No data reaches the database. On restart, rates are re-fetched and stored normally.

**Crash during the transaction (inside `with service.transaction()`)** — the transaction was never committed. The database connection closes, and the DB engine automatically rolls back the uncommitted transaction. No partial set of rates is ever visible in the database (e.g., you will never see ILS and EUR stored but GBP missing from a single fetch).

**Crash after the transaction commits** — all rates are safely stored. A restart would re-fetch and re-store via upsert, which is a no-op if the data hasn't changed.

In all three cases, the database is left in a consistent state. There is no scenario where a crash produces partial or corrupt data.
