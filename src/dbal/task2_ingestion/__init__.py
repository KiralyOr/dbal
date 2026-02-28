"""Task 2 — CSV Ingestion: chunked, idempotent CSV-to-database ingestion."""

from dbal.task2_ingestion.csv_ingest import ingest_csv, parse_date, parse_row

__all__ = ["ingest_csv", "parse_date", "parse_row"]
