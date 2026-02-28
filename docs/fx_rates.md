# FX Rate Integration

## Idempotency

### Retry Safety

The FX client uses exponential backoff (1s, 2s, 4s) with up to 3 retries. Each retry is a fresh HTTP request to the CurrencyLayer API — the API is read-only and safe to retry.

### Storage Idempotency

`store_rates()` uses `INSERT ... ON CONFLICT (date, currency) DO UPDATE SET rate_to_usd = ...`. Storing the same rate twice for the same `(date, currency)` simply updates the existing row. This is safe for:
- Retried fetches (same data, no duplicates)
- Corrected fetches (updated rate overwrites the old one)

### Crash During Insertion

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
