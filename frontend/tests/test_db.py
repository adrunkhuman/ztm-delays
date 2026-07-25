from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pytest
from flask import Flask

from ztm_frontend import db

EXPECTED_REQUEST_CONNECTIONS = 2


class FakeConnection:
    def __init__(self, generation: str) -> None:
        self.generation = generation
        self.description = [("generation",)]
        self.closed = False

    def execute(self, _sql: str, _params: object) -> FakeConnection:
        return self

    def fetchall(self) -> list[tuple[str]]:
        return [(self.generation,)]

    def close(self) -> None:
        self.closed = True


def test_request_queries_share_one_snapshot_and_close_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_generation = ["old"]
    connections: list[FakeConnection] = []

    def connect(_path: str, *, read_only: bool) -> FakeConnection:
        assert read_only is True
        connection = FakeConnection(current_generation[0])
        connections.append(connection)
        return connection

    monkeypatch.setattr(db.duckdb, "connect", connect)
    app = Flask(__name__)
    app.teardown_appcontext(db.close_request_connections)

    @app.get("/")
    def read_twice() -> dict[str, str]:
        first = db.fetch_one(Path("serving.duckdb"), "select generation")
        current_generation[0] = "new"
        second = db.fetch_one(Path("serving.duckdb"), "select generation")
        assert first is not None
        assert second is not None
        return {"first": first["generation"], "second": second["generation"]}

    client = app.test_client()
    first_response = client.get("/")
    second_response = client.get("/")

    assert first_response.json == {"first": "old", "second": "old"}
    assert second_response.json == {"first": "new", "second": "new"}
    assert len(connections) == EXPECTED_REQUEST_CONNECTIONS
    assert all(connection.closed for connection in connections)


def test_query_outside_request_closes_its_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection("current")

    def connect(_path: str, *, read_only: bool) -> FakeConnection:
        assert read_only is True
        return connection

    monkeypatch.setattr(db.duckdb, "connect", connect)

    assert db.fetch_one(Path("serving.duckdb"), "select generation") == {"generation": "current"}
    assert connection.closed is True


def test_frontend_reads_partitioned_catalog_view(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "parquet" / "mart_trip_daily" / "service_date=2026-07-02" / "generation=test"
    parquet_dir.mkdir(parents=True)
    parquet_path = parquet_dir / "part.parquet"
    db_path = tmp_path / "serving.duckdb"
    escaped_parquet_path = parquet_path.as_posix().replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"copy (select date '2026-07-02' as service_date, 'trip-1' as trip_id) "
            f"to '{escaped_parquet_path}' (format parquet)"
        )
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            f"create view mart_trip_daily as select * from read_parquet('{escaped_parquet_path}', hive_partitioning=false)"  # noqa: S608
        )

    assert db.fetch_one(db_path, "select service_date, trip_id from mart_trip_daily") == {
        "service_date": date(2026, 7, 2),
        "trip_id": "trip-1",
    }


@pytest.mark.skipif(os.name == "nt", reason="Windows cannot replace an open DuckDB file")
def test_request_keeps_open_duckdb_generation_after_atomic_replacement(tmp_path: Path) -> None:
    db_path = tmp_path / "serving.duckdb"
    replacement_path = tmp_path / "replacement.duckdb"
    for path, generation in ((db_path, "old"), (replacement_path, "new")):
        with duckdb.connect(str(path)) as connection:
            connection.execute("create table export_generation as select ? as generation", [generation])

    app = Flask(__name__)
    app.teardown_appcontext(db.close_request_connections)

    @app.get("/")
    def replace_between_reads() -> dict[str, str]:
        first = db.fetch_one(db_path, "select generation from export_generation")
        replacement_path.replace(db_path)
        second = db.fetch_one(db_path, "select generation from export_generation")
        assert first is not None
        assert second is not None
        return {"first": first["generation"], "second": second["generation"]}

    response = app.test_client().get("/")

    assert response.json == {"first": "old", "second": "old"}
    assert db.fetch_one(db_path, "select generation from export_generation") == {"generation": "new"}
