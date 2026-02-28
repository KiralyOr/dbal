"""Chunked CSV ingestion into the usage_data table."""

import csv
import logging
from datetime import datetime
from pathlib import Path

from dbal.service import DatabaseService
from ingestion.schema import (
    USAGE_COLUMNS,
    USAGE_CONFLICT_COLUMNS,
    USAGE_TABLE,
    USAGE_TABLE_DDL,
)

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> str:
    """Convert DD/MM/YYYY to YYYY-MM-DD."""
    return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")


def parse_row(row: list[str]) -> tuple:
    """Parse a CSV row into a typed tuple matching USAGE_COLUMNS."""
    date = parse_date(row[0])
    bill_id = int(row[1])
    currency = row[2].strip()
    name = row[3].strip()
    product1_revenue = float(row[4])
    product2_revenue = float(row[5])
    return (date, bill_id, currency, name, product1_revenue, product2_revenue)


def chunked_reader(file_path: str | Path, chunk_size: int = 5000):
    """Yield chunks of parsed rows from a CSV file.

    Never loads the full file into memory — reads and yields chunk_size rows at a time.
    """
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        chunk: list[tuple] = []
        for row_num, row in enumerate(reader, start=2):
            if not row or all(cell.strip() == "" for cell in row):
                continue
            try:
                chunk.append(parse_row(row))
            except (ValueError, IndexError) as e:
                logger.warning("Skipping malformed row %d: %s — %s", row_num, row, e)
                continue

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk


def ingest_csv(
    service: DatabaseService,
    file_path: str | Path,
    chunk_size: int = 5000,
) -> int:
    """Ingest a CSV file into usage_data.

    Each chunk is an atomic transaction — if the process is killed mid-file,
    committed chunks are safe and the current chunk rolls back.

    Returns the total number of rows ingested.
    """
    service.execute_ddl(USAGE_TABLE_DDL)

    total = 0
    for i, chunk in enumerate(chunked_reader(file_path, chunk_size)):
        with service.transaction():
            service.upsert(USAGE_TABLE, USAGE_COLUMNS, chunk, USAGE_CONFLICT_COLUMNS)
        total += len(chunk)
        logger.info("Chunk %d: upserted %d rows (total: %d)", i + 1, len(chunk), total)

    logger.info("Ingestion complete: %d rows total", total)
    return total
