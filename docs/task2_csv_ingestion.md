# CSV Ingestion

## Duplication Prevention

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
