"""GPS partition discovery and dbt-compatible normalization."""

from collections import defaultdict
from datetime import date
from pathlib import Path

import duckdb
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from ztm_matcher.errors import MatcherError, fail
from ztm_matcher.schemas import NORMALIZED_GPS_SCHEMA, RAW_GPS_SCHEMA


def discover(root: Path, processing_date: date) -> tuple[list[Path], dict[str, list[int]]]:
    """Discover deterministic bus/tram date/hour partitions and report missing hours."""
    if not root.is_dir():
        raise fail("missing_input", f"GPS root does not exist: {root}", 10)
    files: list[Path] = []
    missing: dict[str, list[int]] = {}
    for mode in ("bus", "tram"):
        base = root / f"vehicle_type={mode}" / f"date={processing_date}"
        hours: dict[int, list[Path]] = defaultdict(list)
        for path in base.glob("hour=*/*.parquet") if base.is_dir() else ():
            try:
                hour = int(path.parent.name.removeprefix("hour="))
            except ValueError:
                continue
            if 0 <= hour < 24:
                hours[hour].append(path)
        missing[mode] = [hour for hour in range(24) if hour not in hours]
        files.extend(path for hour in sorted(hours) for path in sorted(hours[hour]))
    if not files:
        raise fail("missing_input", f"no GPS Parquet files for Warsaw date {processing_date}", 10)
    return files, missing


def normalize(
    connection: duckdb.DuckDBPyConnection,
    files: list[Path],
    day: date,
    output: Path,
    *,
    lines: set[str] | None = None,
    vehicle_number: str | None = None,
) -> int:
    """Port stg_gps__pings with Warsaw bounds, newest ingestion dedup, and ordering."""
    for path in files:
        try:
            if pq.read_schema(path) != RAW_GPS_SCHEMA:
                raise fail("schema_drift", f"raw GPS schema differs from raw-gps-v1: {path}", 11)
        except MatcherError:
            raise
        except Exception as exc:
            raise fail("invalid_data", f"cannot read GPS Parquet schema: {path}", 12) from exc
    try:
        connection.register("raw_gps", ds.dataset([str(path) for path in files], format="parquet"))
        diagnostic_filters = []
        if lines:
            quoted_lines = ", ".join(f"'{line.replace("'", "''")}'" for line in sorted(lines))
            diagnostic_filters.append(f'cast("Lines" as varchar) in ({quoted_lines})')
        if vehicle_number:
            quoted_vehicle = vehicle_number.replace("'", "''")
            diagnostic_filters.append(f"cast(\"VehicleNumber\" as varchar) = '{quoted_vehicle}'")
        diagnostic_sql = "".join(f"\n                and {condition}" for condition in diagnostic_filters)
        connection.execute(
            f"""
            create or replace temp view normalized_gps as
            with source as (
              select cast("Lines" as varchar) line,
                coalesce(nullif(regexp_replace(cast("Brigade" as varchar), '^0+', ''), ''), '0') brigade,
                cast("Lat" as double) lat, cast("Lon" as double) lon, "Time" gps_time,
                cast("VehicleNumber" as varchar) vehicle_number, cast(vehicle_type as bigint) vehicle_type,
                ingested_at, cast(timezone('Europe/Warsaw', "Time") as date) gps_date
              from raw_gps
                  where "Time" >= (date '{day}'::timestamp at time zone 'Europe/Warsaw')
                    and "Time" < ((date '{day}' + interval 1 day)::timestamp at time zone 'Europe/Warsaw')
                and regexp_full_match(cast("Brigade" as varchar), '^[0-9]+$')
                and regexp_full_match(cast("VehicleNumber" as varchar), '^[0-9]+$')
                and cast("Lat" as double) between 51.0 and 53.5 and cast("Lon" as double) between 19.5 and 22.5
                {diagnostic_sql}
            ), dedup as (
              select *, row_number() over (
                partition by vehicle_type, vehicle_number, gps_time
                order by ingested_at desc, line, brigade, lat, lon
              ) rank from source
            ) select line, brigade, lat, lon, gps_time, vehicle_number, vehicle_type, ingested_at, gps_date from dedup
              where rank = 1 order by vehicle_type, vehicle_number, gps_time, ingested_at, line, brigade
        """,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        quoted = str(output).replace("'", "''")
        connection.execute(f"copy normalized_gps to '{quoted}' (format parquet, compression zstd)")
        count = connection.execute("select count(*) from normalized_gps").fetchone()
        if count is None:
            raise fail("invalid_output", "DuckDB did not return a normalized row count", 15)
        rows = int(count[0])
    except duckdb.Error as exc:
        if any(word in str(exc).lower() for word in ("out of memory", "temp", "disk")):
            raise fail("resource_limit", "DuckDB exceeded a configured resource limit", 14) from exc
        raise fail("invalid_data", "DuckDB failed while normalizing GPS", 12) from exc
    if pq.read_schema(output) != NORMALIZED_GPS_SCHEMA:
        raise fail("schema_drift", "normalized GPS output differs from normalized-gps-v1", 11)
    return rows


def hourly_counts(connection: duckdb.DuckDBPyConnection) -> list[dict[str, int]]:
    """Return deterministic normalized row counts by type and Warsaw hour."""
    rows = connection.execute(
        "select vehicle_type, extract(hour from timezone('Europe/Warsaw', gps_time))::int, count(*) "
        "from normalized_gps group by 1, 2 order by 1, 2"
    ).fetchall()
    return [{"vehicle_type": int(row[0]), "hour": int(row[1]), "rows": int(row[2])} for row in rows]
