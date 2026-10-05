"""Write planner.duckdb exactly as contracts/planner_artifact_v1.json describes, then swap it in atomically."""

from __future__ import annotations

import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING

from ztm_planner.db import one

if TYPE_CHECKING:
    import duckdb

# Mirrors contracts/planner_artifact_v1.json (a test keeps them equal): table -> (column, type, nullable).
CONTRACT: dict[str, tuple[tuple[str, str, bool], ...]] = {
    "planner_metadata": (
        ("build_id", "VARCHAR", False), ("built_at", "TIMESTAMP", False), ("model_version", "VARCHAR", False),
        ("first_date", "DATE", False), ("last_date", "DATE", False),
    ),
    "planner_stop_group": (
        ("stop_group_id", "VARCHAR", False), ("name", "VARCHAR", False), ("search_key", "VARCHAR", False),
        ("lines", "VARCHAR[]", False), ("visits", "INTEGER", False),
    ),
    "planner_trip": (
        ("trip_key", "BIGINT", False), ("service_date", "DATE", False), ("mode", "VARCHAR", False),
        ("line", "VARCHAR", False), ("headsign", "VARCHAR", False),
    ),
    "planner_stop": (
        ("trip_key", "BIGINT", False), ("stop_sequence", "INTEGER", False), ("stop_id", "VARCHAR", False),
        ("stop_group_id", "VARCHAR", False), ("stop_name", "VARCHAR", False), ("scheduled_sod", "INTEGER", False),
        ("usual_delay_s", "INTEGER", False), ("late_delay_s", "INTEGER", False),
        ("leave_by_offset_s", "INTEGER", True), ("ride_from_start_s", "DOUBLE", False),
        ("expected_sod", "INTEGER", False), ("can_alight", "BOOLEAN", False),
    ),
    "planner_range": (
        ("is_tram", "BOOLEAN", False), ("weekday", "BOOLEAN", False), ("hour", "INTEGER", False),
        ("min_ride_s", "DOUBLE", False), ("max_ride_s", "DOUBLE", False), ("low_ratio", "DOUBLE", False),
        ("high_ratio", "DOUBLE", False),
    ),
    "planner_footpath": (
        ("from_stop_id", "VARCHAR", False), ("to_stop_id", "VARCHAR", False), ("distance_m", "INTEGER", True),
        ("walk_s", "INTEGER", False),
    ),
}  # fmt: skip
# Unique columns, also in the repository contract; a trip key shared by two trips would merge them.
KEYS: dict[str, tuple[str, ...]] = {"planner_trip": ("trip_key",), "planner_stop": ("trip_key", "stop_sequence")}
ORDER = {
    "planner_stop": "stop_group_id, trip_key, stop_sequence",
    "planner_trip": "trip_key",
    "planner_footpath": "from_stop_id, walk_s",
}


def search_key(text: str) -> str:
    """Accent-free lowercase stop name; must equal the frontend's ``planner.search_key``."""
    decomposed = unicodedata.normalize("NFKD", text.lower().replace("ł", "l"))
    return " ".join("".join(c for c in decomposed if not unicodedata.combining(c)).split())


def write(con: duckdb.DuckDBPyConnection, output: Path, sources: dict[str, str]) -> None:
    """Materialize ``sources`` (contract table -> query in ``con``) into ``output``, validated, atomically."""
    if set(sources) != set(CONTRACT):
        raise ValueError(f"artifact sources must be exactly {sorted(CONTRACT)}")
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name + ".tmp")
    tmp.unlink(missing_ok=True)
    con.execute(f"attach '{tmp}' as artifact")
    try:
        for table, columns in CONTRACT.items():
            select = ", ".join(f"{name}::{kind} as {name}" for name, kind, _ in columns)
            order = f" order by {ORDER[table]}" if table in ORDER else ""
            con.execute(f"create table artifact.{table} as select {select} from ({sources[table]}){order}")
            _validate(con, table, columns)
    finally:
        con.execute("detach artifact")
    tmp.chmod(0o644)  # the frontend container reads as a different user
    tmp.replace(output)


def _validate(con: duckdb.DuckDBPyConnection, table: str, columns: tuple[tuple[str, str, bool], ...]) -> None:
    rows = one(con, f"select count(*) from artifact.{table}")[0]
    if rows == 0:
        raise ValueError(f"artifact table {table} is empty")
    for name, _, nullable in columns:
        if not nullable:
            nulls = one(con, f"select count(*) from artifact.{table} where {name} is null")[0]
            if nulls:
                raise ValueError(f"artifact {table}.{name} has {nulls} nulls")
    if table in KEYS:
        key = ", ".join(KEYS[table])
        groups = f"select 1 from artifact.{table} group by {key} having count(*) > 1"
        duplicated = one(con, f"select count(*) from ({groups})")[0]
        if duplicated:
            raise ValueError(f"artifact {table} has {duplicated} duplicated ({key}) keys")
