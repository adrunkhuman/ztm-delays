"""Turn segment rows into model-ready rows: lookup levels, recent conditions, weather, line ids, row ids."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from ztm_planner import features, lookup
from ztm_planner.weather import WEATHER_FEATURES

if TYPE_CHECKING:
    import duckdb


def model_rows(
    con: duckdb.DuckDBPyConnection, rows: str, target: str, prefix: str, daily: str, asof: date | None
) -> None:
    """Create ``target`` from featured segment rows using lookup tables ``prefix``."""
    lookup.apply(con, rows, f"{target}_lk", prefix)
    finish(con, f"{target}_lk", target, daily, asof)
    con.execute(f"drop table {target}_lk")


def finish(con: duckdb.DuckDBPyConnection, looked_up: str, target: str, daily: str, asof: date | None) -> None:
    """Add recent conditions, weather, line ids and a stable ``row_id`` to rows that carry lookup levels."""
    features.add_recent(con, looked_up, daily, f"{target}_rc", asof)
    weather = ", ".join(f"w.{c}" for c in WEATHER_FEATURES)
    con.execute(
        f"""
        create or replace table {target} as
        -- row_id is assigned once here; every later read orders by it, so no (memory-hungry) sort is needed
        select r.*, {weather}, lm.line_id, row_number() over () as row_id
        from {target}_rc r
        left join weather w on w.wx_ts = r.wx_ts
        left join line_map lm on lm.line = r.line
        """
    )
    con.execute(f"drop table {target}_rc")
