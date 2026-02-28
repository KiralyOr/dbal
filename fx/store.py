"""FX rate persistence and schema."""

import logging

from dbal.service import DatabaseService

logger = logging.getLogger(__name__)

FX_RATES_DDL = """
CREATE TABLE IF NOT EXISTS fx_rates (
    date          DATE        NOT NULL,
    currency      VARCHAR(3)  NOT NULL,
    rate_to_usd   DECIMAL(15,6) NOT NULL,
    fetched_at    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, currency)
);
"""

FX_TABLE = "fx_rates"
FX_COLUMNS = ["date", "currency", "rate_to_usd"]
FX_CONFLICT_COLUMNS = ["date", "currency"]


def ensure_fx_schema(service: DatabaseService) -> None:
    """Create the fx_rates table if it doesn't exist."""
    service.execute_ddl(FX_RATES_DDL)


def store_rates(service: DatabaseService, date: str, rates: dict[str, float]) -> None:
    """Upsert FX rates into the fx_rates table.

    Idempotent: ON CONFLICT (date, currency) DO UPDATE.
    """
    rows = [(date, currency, rate) for currency, rate in rates.items()]
    with service.transaction():
        service.upsert(FX_TABLE, FX_COLUMNS, rows, FX_CONFLICT_COLUMNS)
    logger.info("Stored %d FX rates for %s", len(rows), date)
