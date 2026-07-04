from __future__ import annotations

from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


def fetch_all(db_path: Path, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    """Run one read-only DuckDB query and return rows as dictionaries."""
    with duckdb.connect(str(db_path), read_only=True) as connection:
        result = connection.execute(sql, params)
        columns = [column[0] for column in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def fetch_one(db_path: Path, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
    """Return the first row from a read-only DuckDB query."""
    rows = fetch_all(db_path, sql, params)
    if not rows:
        return None
    return rows[0]
