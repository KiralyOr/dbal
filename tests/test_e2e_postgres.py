"""E2E tests that run against a real PostgreSQL instance.

Requires: docker compose up -d
Run with: pytest tests/test_e2e_postgres.py -v -m e2e
"""

from pathlib import Path

import pytest

from dbal import create_service
from dbal.task2_ingestion import ingest_csv
from dbal.task3_fx.store import ensure_fx_schema, store_rates

PG_URL = "postgresql://dbal:dbal@localhost:5433/dbal_test"
CSV_PATH = Path(__file__).parent.parent / "data" / "usage_data.csv"

pytestmark = pytest.mark.e2e


@pytest.fixture()
def pg_service():
    """Connect to Postgres, drop/recreate tables between tests."""
    service = create_service(PG_URL)
    service.connect()
    try:
        # Clean slate
        service.execute_ddl("DROP TABLE IF EXISTS usage_data")
        service.execute_ddl("DROP TABLE IF EXISTS fx_rates")
        yield service
    finally:
        service.close()


class TestIngestion:
    def test_ingest_csv(self, pg_service):
        total = ingest_csv(pg_service, CSV_PATH, chunk_size=5)
        assert total == 20

        with pg_service.transaction():
            rows = pg_service.execute("SELECT COUNT(*) AS cnt FROM usage_data")
        assert rows[0]["cnt"] == 20

    def test_ingest_idempotent(self, pg_service):
        ingest_csv(pg_service, CSV_PATH)
        ingest_csv(pg_service, CSV_PATH)

        with pg_service.transaction():
            rows = pg_service.execute("SELECT COUNT(*) AS cnt FROM usage_data")
        assert rows[0]["cnt"] == 20


class TestFxRates:
    def test_fx_store_and_retrieve(self, pg_service):
        ensure_fx_schema(pg_service)
        store_rates(pg_service, "2021-10-01", {"ILS": 3.22, "EUR": 0.86})

        with pg_service.transaction():
            rows = pg_service.execute(
                "SELECT currency, rate_to_usd FROM fx_rates WHERE date = %s ORDER BY currency",
                ("2021-10-01",),
            )
        assert len(rows) == 2
        by_currency = {r["currency"]: float(r["rate_to_usd"]) for r in rows}
        assert by_currency["EUR"] == pytest.approx(0.86)
        assert by_currency["ILS"] == pytest.approx(3.22)

    def test_fx_idempotent(self, pg_service):
        ensure_fx_schema(pg_service)
        store_rates(pg_service, "2021-10-01", {"ILS": 3.22})
        store_rates(pg_service, "2021-10-01", {"ILS": 3.25})

        with pg_service.transaction():
            rows = pg_service.execute(
                "SELECT rate_to_usd FROM fx_rates WHERE date = %s AND currency = %s",
                ("2021-10-01", "ILS"),
            )
        assert len(rows) == 1
        assert float(rows[0]["rate_to_usd"]) == pytest.approx(3.25)


class TestFullPipeline:
    def test_full_pipeline(self, pg_service):
        total = ingest_csv(pg_service, CSV_PATH)
        assert total == 20

        ensure_fx_schema(pg_service)
        store_rates(pg_service, "2021-10-01", {"ILS": 3.22, "EUR": 0.86, "GBP": 0.73})

        with pg_service.transaction():
            usage_rows = pg_service.execute("SELECT COUNT(*) AS cnt FROM usage_data")
            fx_rows = pg_service.execute("SELECT COUNT(*) AS cnt FROM fx_rates")

        assert usage_rows[0]["cnt"] == 20
        assert fx_rows[0]["cnt"] == 3
