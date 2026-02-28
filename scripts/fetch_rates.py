"""CLI entry point for FX rate fetching.

Usage:
    python -m scripts.fetch_rates --db-url sqlite:///data.db --date 2021-10-01 --currencies ILS EUR GBP [--mock]
"""

import argparse
import logging
import os
import sys

from dbal import create_service
from fx.client import CurrencyLayerClient, MockCurrencyLayerClient
from fx.store import ensure_fx_schema, store_rates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and store FX rates")
    parser.add_argument("--db-url", required=True, help="Database URL")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--currencies", nargs="+", required=True, help="Currency codes")
    parser.add_argument("--mock", action="store_true", help="Use mock FX client (for testing)")
    args = parser.parse_args()

    if args.mock:
        client = MockCurrencyLayerClient()
    else:
        api_key = os.environ.get("CURRENCY_LAYER_API_KEY")
        if not api_key:
            logger.error("CURRENCY_LAYER_API_KEY not set. Use --mock for testing.")
            sys.exit(1)
        client = CurrencyLayerClient(api_key)

    service = create_service(args.db_url)
    service.connect()
    try:
        ensure_fx_schema(service)
        rates = client.fetch_rates(args.date, args.currencies)
        logger.info("Fetched rates for %s: %s", args.date, rates)
        store_rates(service, args.date, rates)
        logger.info("Done.")
    finally:
        service.close()


if __name__ == "__main__":
    main()
