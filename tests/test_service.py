"""Tests for DatabaseService (SQLite backend)."""

import threading

import pytest


class TestDatabaseService:
    def test_execute_ddl_and_insert(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        with db_service.transaction():
            db_service.execute("INSERT INTO t (id, name) VALUES (?, ?)", (1, "alice"))
            rows = db_service.execute("SELECT * FROM t")
        assert rows == [{"id": 1, "name": "alice"}]

    def test_execute_many(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        with db_service.transaction():
            db_service.execute_many(
                "INSERT INTO t (id, val) VALUES (?, ?)",
                [(1, "a"), (2, "b"), (3, "c")],
            )
            rows = db_service.execute("SELECT * FROM t ORDER BY id")
        assert len(rows) == 3
        assert rows[0]["val"] == "a"

    def test_batch_insert(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        with db_service.transaction():
            db_service.batch_insert("t", ["id", "val"], [(1, "x"), (2, "y")])
            rows = db_service.execute("SELECT * FROM t ORDER BY id")
        assert len(rows) == 2

    def test_upsert_insert_and_update(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        with db_service.transaction():
            db_service.upsert("t", ["id", "val"], [(1, "old")], ["id"])
        with db_service.transaction():
            db_service.upsert("t", ["id", "val"], [(1, "new"), (2, "fresh")], ["id"])
            rows = db_service.execute("SELECT * FROM t ORDER BY id")
        assert rows == [{"id": 1, "val": "new"}, {"id": 2, "val": "fresh"}]

    def test_transaction_rollback_on_error(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        with pytest.raises(ValueError):
            with db_service.transaction():
                db_service.execute("INSERT INTO t (id, val) VALUES (?, ?)", (1, "x"))
                raise ValueError("simulated failure")

        with db_service.transaction():
            rows = db_service.execute("SELECT * FROM t")
        assert rows == []

    def test_requires_transaction(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        with pytest.raises(RuntimeError, match="No active transaction"):
            db_service.execute("SELECT 1")

    def test_concurrent_transactions(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        errors = []

        def worker(n):
            try:
                with db_service.transaction():
                    db_service.execute("INSERT INTO t (id, val) VALUES (?, ?)", (n, n * 10))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        with db_service.transaction():
            rows = db_service.execute("SELECT * FROM t ORDER BY id")
        assert len(rows) == 4

    def test_upsert_empty_rows(self, db_service):
        db_service.execute_ddl("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        with db_service.transaction():
            db_service.upsert("t", ["id", "val"], [], ["id"])
            rows = db_service.execute("SELECT * FROM t")
        assert rows == []
