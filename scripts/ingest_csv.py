"""CLI entry point for CSV ingestion.

Usage:
    python -m scripts.ingest_csv --db-url sqlite:///data.db --file data.csv [--chunk-size 5000]
"""

import argparse
import logging

from dbal import create_service
from ingestion.csv_ingest import ingest_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest CSV usage data into the database")
    parser.add_argument(
        "--db-url", required=True, help="Database URL (sqlite:/// or postgresql://)"
    )
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Rows per transaction chunk")
    args = parser.parse_args()

    service = create_service(args.db_url)
    service.connect()
    try:
        total = ingest_csv(service, args.file, args.chunk_size)
        logger.info("Done. %d rows ingested.", total)
    finally:
        service.close()


if __name__ == "__main__":
    main()
