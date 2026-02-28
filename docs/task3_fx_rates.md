# FX Rate Integration

## Idempotency

### Retry Safety

The FX client uses exponential backoff (1s, 2s, 4s) with up to 3 retries. Each retry is a fresh HTTP request to the CurrencyLayer API — the API is read-only and safe to retry.

### Storage Idempotency

`store_rates()` uses `INSERT ... ON CONFLICT (date, currency) DO UPDATE SET rate_to_usd = ...`. Storing the same rate twice for the same `(date, currency)` simply updates the existing row. This is safe for:
- Retried fetches (same data, no duplicates)
- Corrected fetches (updated rate overwrites the old one)
