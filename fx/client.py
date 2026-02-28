"""CurrencyLayer API client with retry and mock support."""

import logging
import time
from abc import ABC, abstractmethod

import requests

logger = logging.getLogger(__name__)

CURRENCYLAYER_BASE_URL = "http://api.currencylayer.com/historical"


class FXClient(ABC):
    """Abstract interface for fetching FX rates."""

    @abstractmethod
    def fetch_rates(self, date: str, currencies: list[str]) -> dict[str, float]:
        """Fetch exchange rates for the given date and currencies.

        Args:
            date: Date string in YYYY-MM-DD format.
            currencies: List of currency codes (e.g., ["ILS", "EUR", "GBP"]).

        Returns:
            Dict mapping currency code to rate vs USD, e.g. {"ILS": 3.21, "EUR": 0.86}.
        """


class CurrencyLayerClient(FXClient):
    """Real CurrencyLayer API client with exponential backoff retry."""

    def __init__(self, api_key: str, max_retries: int = 3, base_delay: float = 1.0):
        self._api_key = api_key
        self._max_retries = max_retries
        self._base_delay = base_delay

    def fetch_rates(self, date: str, currencies: list[str]) -> dict[str, float]:
        params = {
            "access_key": self._api_key,
            "date": date,
            "currencies": ",".join(currencies),
            "source": "USD",
        }

        for attempt in range(self._max_retries):
            try:
                resp = requests.get(CURRENCYLAYER_BASE_URL, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                if not data.get("success"):
                    error = data.get("error", {})
                    raise RuntimeError(
                        f"CurrencyLayer API error {error.get('code')}: {error.get('info')}"
                    )

                quotes = data["quotes"]
                # Quotes come as "USDILS", "USDEUR", etc.
                return {cur: quotes[f"USD{cur}"] for cur in currencies}

            except (requests.RequestException, KeyError, RuntimeError) as e:
                if attempt < self._max_retries - 1:
                    delay = self._base_delay * (2**attempt)
                    logger.warning(
                        "Attempt %d failed: %s. Retrying in %.1fs...",
                        attempt + 1,
                        e,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    raise


class MockCurrencyLayerClient(FXClient):
    """Mock client returning fixed rates for testing."""

    MOCK_RATES = {
        "ILS": 3.21,
        "EUR": 0.86,
        "GBP": 0.73,
        "USD": 1.0,
    }

    def fetch_rates(self, date: str, currencies: list[str]) -> dict[str, float]:
        return {cur: self.MOCK_RATES.get(cur, 1.0) for cur in currencies}
