from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import duckdb
from flask import g, has_request_context

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path


def _connection(db_path: Path) -> tuple[duckdb.DuckDBPyConnection, bool]:
    if not has_request_context():
        return duckdb.connect(str(db_path), read_only=True), True

    connections = cast("dict[Path, duckdb.DuckDBPyConnection] | None", g.get("ztm_duckdb_connections"))
    if connections is None:
        connections = {}
        g.ztm_duckdb_connections = connections
    connection = connections.get(db_path)
    if connection is None:
        connection = duckdb.connect(str(db_path), read_only=True)
        connections[db_path] = connection
    return connection, False


def close_request_connections(_error: BaseException | None = None) -> None:
    """Close DuckDB snapshots held for the current Flask request."""
    connections = cast("dict[Path, duckdb.DuckDBPyConnection]", g.pop("ztm_duckdb_connections", {}))
    for connection in connections.values():
        connection.close()


@contextmanager
def read_connection(db_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Reuse the request's snapshot; outside a request, close the connection on exit."""
    connection, close_after_query = _connection(db_path)
    try:
        yield connection
    finally:
        if close_after_query:
            connection.close()


def fetch_all(db_path: Path, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    """Run a DuckDB query and return rows as dictionaries."""
    with read_connection(db_path) as connection:
        result = connection.execute(sql, params)
        columns = [column[0] for column in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def fetch_one(db_path: Path, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
    """Return the first row from a read-only DuckDB query."""
    rows = fetch_all(db_path, sql, params)
    if not rows:
        return None
    return rows[0]
