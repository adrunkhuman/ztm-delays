"""DuckDB connections bounded for a small shared machine: memory capped, spills to disk."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from ztm_planner.settings import Resources


def connect(resources: Resources, path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection that spills instead of exceeding the memory limit."""
    Path(resources.temp_dir).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(path)
    con.execute(f"set threads = {int(resources.threads)}")
    con.execute("set memory_limit = ?", [resources.memory_limit])
    con.execute("set temp_directory = ?", [resources.temp_dir])
    con.execute("set preserve_insertion_order = false")
    return con


@contextmanager
def yielding_memory(con: duckdb.DuckDBPyConnection, resources: Resources, idle_limit: str = "256MB") -> Iterator[None]:
    """Let DuckDB evict its buffer pool (tables are on disk) while another library needs the memory."""
    con.execute("set memory_limit = ?", [idle_limit])
    try:
        yield
    finally:
        con.execute("set memory_limit = ?", [resources.memory_limit])


def one(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> tuple:
    """The single row a query must return."""
    row = con.execute(sql, params or []).fetchone()
    if row is None:
        raise RuntimeError(f"query returned no row: {sql[:120]}")
    return row
