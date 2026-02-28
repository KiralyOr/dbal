"""Tests for FX client and store."""

import pytest

from fx.client import MockCurrencyLayerClient
from fx.store import ensure_fx_schema, store_rates


class TestMockClient:
    def test_fetch_rates(self):
        client = MockCurrencyLayerClient()
        rates = client.fetch_rates("2021-10-01", ["ILS", "EUR", "GBP"])
        assert rates["ILS"] == pytest.approx(3.21)
        assert rates["EUR"] == pytest.approx(0.86)
        assert rates["GBP"] == pytest.approx(0.73)

    def test_fetch_unknown_currency(self):
        client = MockCurrencyLayerClient()
        rates = client.fetch_rates("2021-10-01", ["XYZ"])
        assert rates["XYZ"] == 1.0  # default


class TestFXStore:
    def test_store_and_retrieve(self, db_service):
        ensure_fx_schema(db_service)
        store_rates(db_service, "2021-10-01", {"ILS": 3.21, "EUR": 0.86})

        with db_service.transaction():
            rows = db_service.execute("SELECT * FROM fx_rates ORDER BY currency")
        assert len(rows) == 2
        assert rows[0]["currency"] == "EUR"
        assert float(rows[0]["rate_to_usd"]) == pytest.approx(0.86)

    def test_store_idempotent(self, db_service):
        ensure_fx_schema(db_service)
        store_rates(db_service, "2021-10-01", {"ILS": 3.21})
        store_rates(db_service, "2021-10-01", {"ILS": 3.25})  # updated rate

        with db_service.transaction():
            rows = db_service.execute("SELECT * FROM fx_rates")
        assert len(rows) == 1
        assert float(rows[0]["rate_to_usd"]) == pytest.approx(3.25)
