from __future__ import annotations

import argparse
import json
import os
import re
import sys
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

resource_module = import_module("resource") if os.name == "posix" else None

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_WARNINGS = 100


class SemanticValidationError(RuntimeError):
    """A stable serving contract failed."""


class QueryResult(Protocol):
    """Minimal DuckDB query result used by validation."""

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return one query row."""
        ...

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all query rows."""
        ...


class Connection(Protocol):
    """Minimal DuckDB connection used by validation."""

    def execute(self, query: str, parameters: list[object] | None = None) -> QueryResult:
        """Execute a DuckDB query."""
        ...


def validate_duckdb(  # noqa: PLR0913
    path: Path,
    requested_dates: tuple[str, ...],
    memory_limit_mb: int,
    temp_limit_mb: int,
    threads: int,
    temp_directory: Path,
) -> dict[str, object]:
    """Validate bounded semantic contracts in a candidate serving database."""
    _apply_process_memory_limit(memory_limit_mb)
    duckdb = import_module("duckdb")

    temp_directory.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path), read_only=True) as connection:
        escaped_temp_directory = temp_directory.as_posix().replace("'", "''")
        connection.execute(f"set temp_directory = '{escaped_temp_directory}'")
        connection.execute(f"set max_temp_directory_size = '{temp_limit_mb}MB'")
        connection.execute(f"set memory_limit = '{memory_limit_mb}MB'")
        connection.execute(f"set threads = {threads}")
        connection.execute("set preserve_insertion_order = false")

        _validate_required_columns(connection)
        _require_zero(
            connection,
            "dim_serving_date",
            """
            select count(*)
            from (
                select service_date
                from dim_serving_date
                group by service_date
                having count(*) != 1
            )
            """,
        )
        _require_zero(
            connection,
            "dim_serving_date_keys",
            """
            select count(*)
            from dim_serving_date
            where service_date is null
               or service_date_key is null
               or service_date_key != cast(service_date as varchar)
            """,
        )
        _require_zero(
            connection,
            "dim_serving_date_window_alignment",
            """
            select count(*)
            from (
                (select service_date from dim_serving_date)
                except
                (select distinct source_end_date from mart_mode_window_summary where window_type = 'day')
            )
            """,
        )
        _require_zero(
            connection,
            "mode_window_dim_serving_date_alignment",
            """
            select count(*)
            from (
                (select distinct source_end_date from mart_mode_window_summary where window_type = 'day')
                except
                (select service_date from dim_serving_date)
            )
            """,
        )
        _validate_pipeline_status(connection)

        available_dates = {
            str(row[0]) for row in connection.execute("select service_date from dim_serving_date").fetchall()
        }
        latest_row = connection.execute("select max(service_date)::varchar from dim_serving_date").fetchone()
        latest_date = str(latest_row[0]) if latest_row and latest_row[0] is not None else None
        checked_dates = tuple(dict.fromkeys((*requested_dates, *((latest_date,) if latest_date else ()))))
        fact_dates = tuple(value for value in checked_dates if value in available_dates)
        if fact_dates:
            _validate_trip_event_relationships(connection, fact_dates)

        warnings = _semantic_warnings(connection, checked_dates, available_dates)
        return _validation_report(checked_dates, warnings)


def _validate_required_columns(connection: Connection) -> None:
    required = {
        "dim_serving_date": {"service_date", "service_date_key"},
        "mart_mode_window_summary": {"source_end_date", "window_type", "mode"},
        "mart_pipeline_status": {
            "service_date",
            "vehicle_type",
            "mode",
            "expected_hours",
            "present_hours",
            "missing_hours",
            "completeness_ratio",
            "is_complete_day",
            "gps_row_count",
            "max_vehicle_count",
            "mean_hourly_coverage_ratio",
            "min_hourly_coverage_ratio",
            "max_gap_seconds",
            "pings_total",
            "trips_observed",
            "trips_complete",
            "trips_partial",
            "trips_broken",
            "broken_rate",
            "expected_trips",
            "observed_trips",
            "service_coverage_ratio",
            "expected_service_minutes",
            "observed_service_minutes",
            "stop_arrivals_count",
            "latest_gtfs_snapshot_id",
            "latest_gtfs_snapshot_at",
            "gtfs_snapshot_age_hours",
            "schedule_versions_active",
            "status_generated_at",
        },
        "mart_trip_daily": {"gtfs_snapshot_id", "service_date", "trip_id", "vehicle_number"},
        "fct_expected_stop_event": {
            "gtfs_snapshot_id",
            "service_date",
            "trip_id",
            "vehicle_number",
            "stop_sequence",
        },
        "export_metadata": {"export_id"},
    }
    for table_name, required_columns in required.items():
        actual_columns = {
            str(row[0])
            for row in connection.execute(
                "select column_name from information_schema.columns where table_schema = 'main' and table_name = ?",
                [table_name],
            ).fetchall()
        }
        missing = sorted(required_columns - actual_columns)
        if missing:
            raise SemanticValidationError(f"required_columns:{table_name}: missing {', '.join(missing)}")


def _validate_pipeline_status(connection: Connection) -> None:
    _require_zero(
        connection,
        "mart_pipeline_status_unique_mode_date",
        """
        select count(*)
        from (
            select service_date, mode
            from mart_pipeline_status
            group by service_date, mode
            having count(*) != 1
        )
        """,
    )
    _require_zero(
        connection,
        "mart_pipeline_status_contract",
        """
        select count(*)
        from mart_pipeline_status
        where service_date is null
           or vehicle_type is null or vehicle_type not in (1, 2)
           or mode is null or mode not in ('bus', 'tram')
           or (vehicle_type = 1 and mode != 'bus')
           or (vehicle_type = 2 and mode != 'tram')
           or expected_hours is null or expected_hours <= 0
           or present_hours is null or present_hours < 0 or present_hours > expected_hours
           or missing_hours is null or len(missing_hours) != expected_hours - present_hours
           or completeness_ratio is null or completeness_ratio < 0 or completeness_ratio > 1
           or is_complete_day is null or is_complete_day != (present_hours = expected_hours)
           or gps_row_count is null or gps_row_count < 0
           or max_vehicle_count is null or max_vehicle_count < 0
           or mean_hourly_coverage_ratio is null or mean_hourly_coverage_ratio < 0 or mean_hourly_coverage_ratio > 1
           or min_hourly_coverage_ratio is null or min_hourly_coverage_ratio < 0 or min_hourly_coverage_ratio > 1
           or max_gap_seconds is null or max_gap_seconds < 0
           or pings_total is null or pings_total < 0
           or trips_observed is null or trips_observed < 0
           or trips_complete is null or trips_complete < 0
           or trips_partial is null or trips_partial < 0
           or trips_broken is null or trips_broken < 0
           or trips_observed != trips_complete + trips_partial + trips_broken
           or broken_rate is null or broken_rate < 0 or broken_rate > 1
           or expected_trips is null or expected_trips < 0
           or observed_trips is null or observed_trips < 0 or observed_trips > expected_trips
           or service_coverage_ratio < 0 or service_coverage_ratio > 1
           or expected_service_minutes is null or expected_service_minutes < 0
           or observed_service_minutes is null or observed_service_minutes < 0
           or stop_arrivals_count is null or stop_arrivals_count < 0
           or latest_gtfs_snapshot_id is null or latest_gtfs_snapshot_at is null
           or gtfs_snapshot_age_hours is null or gtfs_snapshot_age_hours < 0
           or schedule_versions_active is null or schedule_versions_active < 0
           or status_generated_at is null
        """,
    )


def _validate_trip_event_relationships(connection: Connection, checked_dates: tuple[str, ...]) -> None:
    _require_zero(
        connection,
        "mart_trip_daily_unique_key",
        """
        select count(*)
        from (
            select service_date, trip_id, vehicle_number
            from mart_trip_daily
            where service_date in (select unnest(?::varchar[])::date)
            group by all
            having count(*) != 1
        )
        """,
        [list(checked_dates)],
    )
    _require_zero(
        connection,
        "fct_expected_stop_event_unique_key",
        """
        select count(*)
        from (
            select gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
            from fct_expected_stop_event
            where service_date in (select unnest(?::varchar[])::date)
            group by all
            having count(*) != 1
        )
        """,
        [list(checked_dates)],
    )
    _require_zero(
        connection,
        "fct_expected_stop_event_orphan_trip",
        """
        select count(*)
        from fct_expected_stop_event as event
        left join mart_trip_daily as trip using (gtfs_snapshot_id, service_date, trip_id, vehicle_number)
        where event.service_date in (select unnest(?::varchar[])::date)
          and trip.trip_id is null
        """,
        [list(checked_dates)],
    )
    _require_zero(
        connection,
        "mart_trip_daily_without_stop_events",
        """
        select count(*)
        from mart_trip_daily as trip
        left join (
            select distinct gtfs_snapshot_id, service_date, trip_id, vehicle_number
            from fct_expected_stop_event
            where service_date in (select unnest(?::varchar[])::date)
        ) as event using (gtfs_snapshot_id, service_date, trip_id, vehicle_number)
        where trip.service_date in (select unnest(?::varchar[])::date)
          and event.trip_id is null
        """,
        [list(checked_dates), list(checked_dates)],
    )


def _semantic_warnings(
    connection: Connection, checked_dates: tuple[str, ...], available_dates: set[str]
) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []
    for service_date in checked_dates:
        if service_date not in available_dates:
            warnings.append({"code": "serving_date_absent", "service_date": service_date})
            continue
        rows = connection.execute(
            """
            select mode, is_complete_day, completeness_ratio, expected_trips, service_coverage_ratio
            from mart_pipeline_status
            where service_date = ?
            order by mode
            """,
            [service_date],
        ).fetchall()
        modes = {str(row[0]) for row in rows}
        warnings.extend(
            {"code": "pipeline_status_mode_absent", "service_date": service_date, "mode": mode}
            for mode in sorted({"bus", "tram"} - modes)
        )
        for mode, is_complete_day, completeness_ratio, expected_trips, coverage_ratio in rows:
            if not is_complete_day:
                warnings.append(
                    {
                        "code": "incomplete_gps_day",
                        "service_date": service_date,
                        "mode": str(mode),
                        "completeness_ratio": float(completeness_ratio),
                    }
                )
            if expected_trips > 0 and coverage_ratio == 0:
                warnings.append(
                    {
                        "code": "zero_service_coverage",
                        "service_date": service_date,
                        "mode": str(mode),
                        "expected_trips": int(expected_trips),
                    }
                )
    return warnings


def _validation_report(checked_dates: tuple[str, ...], warnings: list[dict[str, object]]) -> dict[str, object]:
    return {
        "status": "warning" if warnings else "pass",
        "checked_dates": list(checked_dates),
        "warning_count": len(warnings),
        "warnings_truncated": len(warnings) > MAX_WARNINGS,
        "warnings": warnings[:MAX_WARNINGS],
    }


def _require_zero(
    connection: Connection,
    contract: str,
    query: str,
    parameters: list[object] | None = None,
) -> None:
    row = connection.execute(query, parameters).fetchone()
    failures = int(row[0]) if row is not None else 1
    if failures:
        raise SemanticValidationError(f"{contract}: {failures} invalid rows")


def _apply_process_memory_limit(memory_limit_mb: int) -> None:
    if resource_module is None:
        return

    memory_limit_bytes = memory_limit_mb * 1024 * 1024
    resource_module.setrlimit(resource_module.RLIMIT_AS, (memory_limit_bytes, memory_limit_bytes))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--date", action="append", default=[])
    parser.add_argument("--memory-limit-mb", type=int, required=True)
    parser.add_argument("--temp-limit-mb", type=int, required=True)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--temp-directory", type=Path, required=True)
    args = parser.parse_args()
    if any(not DATE_PATTERN.fullmatch(value) for value in args.date):
        parser.error("--date values must be ISO dates")
    if min(args.memory_limit_mb, args.temp_limit_mb, args.threads) <= 0:
        parser.error("resource limits must be positive")
    return args


def main() -> int:
    """Run the isolated semantic validator CLI."""
    args = _parse_args()
    try:
        report = validate_duckdb(
            args.path,
            tuple(args.date),
            args.memory_limit_mb,
            args.temp_limit_mb,
            args.threads,
            args.temp_directory,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"semantic validation failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
