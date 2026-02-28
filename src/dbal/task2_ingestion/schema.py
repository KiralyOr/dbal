"""Usage data table schema."""

USAGE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS usage_data (
    date          DATE        NOT NULL,
    bill_id       INTEGER     NOT NULL,
    currency      VARCHAR(3)  NOT NULL,
    name          VARCHAR(255) NOT NULL,
    product1_revenue DECIMAL(15,6) NOT NULL,
    product2_revenue DECIMAL(15,6) NOT NULL,
    PRIMARY KEY (date, bill_id)
);
CREATE INDEX IF NOT EXISTS idx_usage_date ON usage_data(date);
CREATE INDEX IF NOT EXISTS idx_usage_currency ON usage_data(currency);
"""

USAGE_TABLE = "usage_data"
USAGE_COLUMNS = ["date", "bill_id", "currency", "name", "product1_revenue", "product2_revenue"]
USAGE_CONFLICT_COLUMNS = ["date", "bill_id"]
