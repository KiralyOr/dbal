"""Shared test fixtures."""

import pytest

from dbal import create_service


@pytest.fixture
def db_service(tmp_path):
    """Provide a fresh SQLite DatabaseService for each test."""
    db_path = tmp_path / "test.db"
    service = create_service(f"sqlite:///{db_path}")
    service.connect()
    yield service
    service.close()
