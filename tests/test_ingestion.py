"""Tests for CSV ingestion."""

import csv
from pathlib import Path

import pytest

from ingestion.csv_ingest import ingest_csv, parse_date, parse_row


class TestParseHelpers:
    def test_parse_date(self):
        assert parse_date("01/10/2021") == "2021-10-01"
        assert parse_date("31/12/2023") == "2023-12-31"

    def test_parse_row(self):
        row = ["01/10/2021", "1", "USD", "Cust1", "5833", "4041.904278"]
        result = parse_row(row)
        assert result[0] == "2021-10-01"
        assert result[1] == 1
        assert result[2] == "USD"
        assert result[3] == "Cust1"
        assert float(result[4]) == 5833.0
        assert float(result[5]) == pytest.approx(4041.904278)


class TestIngestion:
    def _write_csv(self, path: Path, rows: list[list[str]]) -> Path:
        csv_file = path / "test.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Date_", "Bill_ID", "Currency", "Name", "Product1 revenue", "Product2 revenue"]
            )
            writer.writerows(rows)
        return csv_file

    def test_ingest_basic(self, db_service, tmp_path):
        csv_file = self._write_csv(
            tmp_path,
            [
                ["01/10/2021", "1", "USD", "Cust1", "100.5", "200.3"],
                ["01/10/2021", "2", "EUR", "Cust2", "300", "400"],
            ],
        )
        total = ingest_csv(db_service, csv_file, chunk_size=10)
        assert total == 2

        with db_service.transaction():
            rows = db_service.execute("SELECT * FROM usage_data ORDER BY bill_id")
        assert len(rows) == 2
        assert rows[0]["currency"] == "USD"
        assert rows[1]["bill_id"] == 2

    def test_ingest_idempotent(self, db_service, tmp_path):
        csv_file = self._write_csv(
            tmp_path,
            [
                ["01/10/2021", "1", "USD", "Cust1", "100", "200"],
            ],
        )
        ingest_csv(db_service, csv_file)
        ingest_csv(db_service, csv_file)  # second run should not duplicate

        with db_service.transaction():
            rows = db_service.execute("SELECT * FROM usage_data")
        assert len(rows) == 1

    def test_ingest_chunked(self, db_service, tmp_path):
        data = [["01/10/2021", str(i), "USD", f"Cust{i}", "10", "20"] for i in range(1, 26)]
        csv_file = self._write_csv(tmp_path, data)
        total = ingest_csv(db_service, csv_file, chunk_size=10)
        assert total == 25

        with db_service.transaction():
            rows = db_service.execute("SELECT COUNT(*) as cnt FROM usage_data")
        assert rows[0]["cnt"] == 25

    def test_ingest_malformed_rows_skipped(self, db_service, tmp_path):
        csv_file = self._write_csv(
            tmp_path,
            [
                ["01/10/2021", "1", "USD", "Cust1", "100", "200"],
                ["bad_date", "2", "USD", "Cust2", "100", "200"],
                ["02/10/2021", "3", "EUR", "Cust3", "50", "75"],
            ],
        )
        total = ingest_csv(db_service, csv_file)
        assert total == 2
