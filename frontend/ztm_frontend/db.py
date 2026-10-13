from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import duckdb
from flask import g, has_app_context

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path


def _open_connection(db_path: Path, *, fresh: bool) -> duckdb.DuckDBPyConnection:
    if not fresh:
        return duckdb.connect(str(db_path), read_only=True)
    # DuckDB can reuse an old top-level instance at this pathname after a rename.
    # A read-only attachment in a private instance opens the published file instead.
    connection = duckdb.connect()
    try:
        # ATTACH does not accept a bound filename; escape it as a SQL string literal.
        quoted = str(db_path).replace("'", "''")
        connection.execute(f"attach '{quoted}' as snapshot (read_only)")
        connection.execute("use snapshot")
    except Exception:
        connection.close()
        raise
    return connection


def _connection(db_path: Path, *, fresh: bool) -> tuple[duckdb.DuckDBPyConnection, bool]:
    if not has_app_context():
        return _open_connection(db_path, fresh=fresh), True

    connections = cast("dict[Path, duckdb.DuckDBPyConnection] | None", g.get("ztm_duckdb_connections"))
    if connections is None:
        connections = {}
        g.ztm_duckdb_connections = connections
    connection = connections.get(db_path)
    if connection is None:
        connection = _open_connection(db_path, fresh=fresh)
        connections[db_path] = connection
    elif fresh:
        raise RuntimeError("cannot replace a snapshot already open in this Flask context")
    return connection, False


def close_request_connections(_error: BaseException | None = None) -> None:
    """Close DuckDB snapshots held for the current Flask context."""
    connections = cast("dict[Path, duckdb.DuckDBPyConnection]", g.pop("ztm_duckdb_connections", {}))
    for connection in connections.values():
        connection.close()


@contextmanager
def read_connection(db_path: Path, *, fresh: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Reuse a context snapshot; fresh=True opens a publication without old-instance reuse."""
    connection, close_after_query = _connection(db_path, fresh=fresh)
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
