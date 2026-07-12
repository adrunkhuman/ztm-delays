"""Deterministic, bounded local evidence gate for overnight reconstruction artifacts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ztm_matcher.errors import MatcherError, fail
from ztm_matcher.schemas import (
    RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
    RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_TRIP_FACT_SCHEMA,
    TRIP_UNIVERSE_SCHEMA,
)

RANKING_ARRIVAL_FLOOR = 20
DEGRADED_PROCESSING_DATES = frozenset(date(2026, 7, day) for day in range(5, 8))
_N_LINE = re.compile(r"^N\d")
_ARTIFACTS = (
    ("reconstruction_trip_facts", RECONSTRUCTION_TRIP_FACT_SCHEMA),
    ("reconstruction_stop_arrivals", RECONSTRUCTION_STOP_ARRIVAL_SCHEMA),
    ("reconstruction_expected_stop_events", RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA),
    ("trip_universe", TRIP_UNIVERSE_SCHEMA),
)
_VIOLATIONS = (
    "artifact_schema_mismatch",
    "duplicate_trip_grain",
    "duplicate_stop_arrival_grain",
    "duplicate_expected_event_grain",
    "duplicate_trip_universe_grain",
    "null_lineage",
    "multiple_processing_dates",
    "multiple_snapshot_ids",
    "gps_date_processing_date_mismatch",
    "degraded_processing_date",
    "prior_service_date_mismatch",
    "missing_expected_trip",
    "expected_event_count_mismatch",
    "arrival_without_expected_event",
    "observed_expected_without_arrival",
    "arrival_expected_mismatch",
    "non_observed_expected_has_observation_data",
    "observed_source_gps_date_mismatch",
    "lineage_mismatch",
    "trip_universe_mismatch",
    "non_monotone_stop_arrival_sequence",
    "non_monotone_expected_schedule_sequence",
    "non_monotone_expected_observed_sequence",
    "incoherent_complete_prior_endpoints",
    "no_prior_service_evidence",
    "no_healthy_n_line_at_ranking_floor",
)


def _artifact_path(input_dir: Path, name: str) -> Path:
    path = input_dir / f"{name}.parquet"
    if not path.is_file():
        raise fail("missing_input", f"overnight proof requires {path.name}", 10)
    return path


def _quoted(path: Path) -> str:
    return str(path).replace("'", "''")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _scalar(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    value = connection.execute(query).fetchone()
    return int(value[0]) if value and value[0] is not None else 0


def _base_report(*, hashes: dict[str, str], violations: dict[str, int]) -> dict[str, Any]:
    return {
        "report_version": "overnight-proof-v2",
        "artifact_hashes": hashes,
        "processing_dates": [],
        "snapshot_ids": [],
        "prior_service_trip_quality_counts": {},
        "prior_n_line_complete_ranking_arrival_counts": {},
        "ranking_arrival_floor": RANKING_ARRIVAL_FLOOR,
        "ranking_floor_unchanged": RANKING_ARRIVAL_FLOOR == 20,
        "eligible_n_lines": [],
        "ineligible_n_lines": [],
        "contract_violation_counts": violations,
        "has_prior_service_evidence": False,
        "has_healthy_n_line_at_ranking_floor": False,
        "passed": False,
    }


def _validate_artifacts(input_dir: Path) -> tuple[dict[str, Path], dict[str, str], int]:
    paths: dict[str, Path] = {}
    hashes: dict[str, str] = {}
    mismatches = 0
    for name, schema in _ARTIFACTS:
        path = _artifact_path(input_dir, name)
        try:
            hashes[name] = _sha256(path)
            if pq.read_schema(path) != schema:
                mismatches += 1
        except (OSError, pa.ArrowException, ValueError) as exc:
            raise fail("invalid_data", f"cannot read {path.name}: {exc}", 12) from exc
        paths[name] = path
    return paths, hashes, mismatches


def _create_views(connection: duckdb.DuckDBPyConnection, paths: dict[str, Path]) -> None:
    try:
        for view, artifact in (
            ("trips", "reconstruction_trip_facts"),
            ("arrivals", "reconstruction_stop_arrivals"),
            ("expected_events", "reconstruction_expected_stop_events"),
            ("trip_universe", "trip_universe"),
        ):
            connection.execute(f"create view {view} as select * from read_parquet('{_quoted(paths[artifact])}')")
    except duckdb.Error as exc:
        raise fail("invalid_data", f"cannot query overnight proof artifacts: {exc}", 12) from exc


def build_overnight_proof_report(input_dir: Path) -> dict[str, Any]:
    """Validate artifacts with DuckDB aggregates; never materialize their rows in Python."""
    paths, hashes, schema_mismatches = _validate_artifacts(input_dir)
    violations = {name: 0 for name in _VIOLATIONS}
    if schema_mismatches:
        violations["artifact_schema_mismatch"] = schema_mismatches
        return _base_report(hashes=hashes, violations=violations)

    connection = duckdb.connect()
    try:
        _create_views(connection, paths)
        violations["duplicate_trip_grain"] = _scalar(
            connection,
            """select coalesce(sum(row_count - 1), 0) from (
                select count(*) row_count from trips
                group by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                having count(*) > 1
            )""",
        )
        violations["duplicate_stop_arrival_grain"] = _scalar(
            connection,
            """select coalesce(sum(row_count - 1), 0) from (
                select count(*) row_count from arrivals
                group by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence
                having count(*) > 1
            )""",
        )
        violations["duplicate_expected_event_grain"] = _scalar(
            connection,
            """select coalesce(sum(row_count - 1), 0) from (
                select count(*) row_count from expected_events
                group by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence
                having count(*) > 1
            )""",
        )
        violations["duplicate_trip_universe_grain"] = _scalar(
            connection,
            """select coalesce(sum(row_count - 1), 0) from (
                select count(*) row_count from trip_universe
                group by gtfs_snapshot_id, processing_date, service_date, duty_chain_id, trip_id
                having count(*) > 1
            )""",
        )
        violations["null_lineage"] = _scalar(
            connection,
            """select count(*) from (
                select 1 from trips where gtfs_snapshot_id is null or processing_date is null or gps_date is null
                    or service_date is null or trip_id is null or vehicle_number is null
                union all
                select 1 from arrivals where gtfs_snapshot_id is null or processing_date is null or gps_date is null
                    or service_date is null or trip_id is null or vehicle_number is null
                union all
                select 1 from expected_events
                where gtfs_snapshot_id is null or processing_date is null or gps_date is null
                    or service_date is null or trip_id is null or vehicle_number is null
            )""",
        )
        processing_dates = [
            row[0]
            for row in connection.execute(
                """select distinct processing_date from (
                    select processing_date from trips union select processing_date from arrivals
                    union select processing_date from expected_events union select processing_date from trip_universe
                ) where processing_date is not null order by processing_date"""
            ).fetchall()
        ]
        snapshot_ids = [
            str(row[0])
            for row in connection.execute(
                """select distinct gtfs_snapshot_id from (
                    select gtfs_snapshot_id from trips union select gtfs_snapshot_id from arrivals
                    union select gtfs_snapshot_id from expected_events union select gtfs_snapshot_id from trip_universe
                ) where gtfs_snapshot_id is not null and gtfs_snapshot_id != '' order by gtfs_snapshot_id"""
            ).fetchall()
        ]
        violations["multiple_processing_dates"] = int(len(processing_dates) != 1)
        violations["multiple_snapshot_ids"] = int(len(snapshot_ids) != 1)
        violations["gps_date_processing_date_mismatch"] = _scalar(
            connection,
            """select count(*) from (
                select 1 from trips where gps_date is distinct from processing_date
                union all select 1 from arrivals where gps_date is distinct from processing_date
                union all select 1 from expected_events where gps_date is distinct from processing_date
            )""",
        )
        violations["degraded_processing_date"] = sum(day in DEGRADED_PROCESSING_DATES for day in processing_dates)
        violations["prior_service_date_mismatch"] = _scalar(
            connection,
            """select count(*) from (
                select processing_date, service_date from trips
                union all select processing_date, service_date from arrivals
                union all select processing_date, service_date from expected_events
            ) where service_date < processing_date and service_date != processing_date - interval '1 day'""",
        )
        violations["missing_expected_trip"] = _scalar(
            connection,
            """select count(*) from trips left join expected_events using
                (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
                group by trips.gtfs_snapshot_id, trips.processing_date, trips.service_date, trips.trip_id,
                    trips.vehicle_number having count(expected_events.stop_sequence) = 0""",
        )
        violations["expected_event_count_mismatch"] = _scalar(
            connection,
            """select count(*) from trips left join expected_events using
                (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
                group by trips.gtfs_snapshot_id, trips.processing_date, trips.service_date, trips.trip_id,
                    trips.vehicle_number, trips.passenger_stops_expected, trips.optional_passenger_stops_expected
                having count(expected_events.stop_sequence) != trips.passenger_stops_expected
                    + trips.optional_passenger_stops_expected""",
        )
        violations["lineage_mismatch"] = _scalar(
            connection,
            """select count(*) from (
                select 1 from arrivals a left join trips t using
                    (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
                where t.trip_id is null or a.gps_date is distinct from t.gps_date
                    or a.line is distinct from t.line or a.brigade is distinct from t.brigade
                    or a.mode is distinct from t.mode
                    or a.is_zone1_public_ranking_trip is distinct from t.is_zone1_public_ranking_trip
                union all
                select 1 from expected_events e left join trips t using
                    (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
                where t.trip_id is null or e.gps_date is distinct from t.gps_date
                    or e.line is distinct from t.line or e.brigade is distinct from t.brigade
                    or e.mode is distinct from t.mode
            )""",
        )
        violations["trip_universe_mismatch"] = _scalar(
            connection,
            """with universe_trip as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, count(*) universe_count,
                    min(is_zone1_public_ranking_trip) ranking_trip
                from trip_universe group by all
            ) select count(*) from trips left join universe_trip using
                (gtfs_snapshot_id, processing_date, service_date, trip_id)
            where universe_count != 1 or trips.is_zone1_public_ranking_trip is distinct from ranking_trip""",
        )
        violations["arrival_without_expected_event"] = _scalar(
            connection,
            """select count(*) from arrivals a left join expected_events e using
                (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence)
            where e.stop_sequence is null""",
        )
        violations["arrival_expected_mismatch"] = _scalar(
            connection,
            """select count(*) from arrivals a inner join expected_events e using
                (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence)
            where e.observation_status != 'observed' or e.actual_arrival_time is distinct from a.actual_arrival_time
                or e.delay_seconds is distinct from a.delay_seconds
                or e.source_gps_date is distinct from a.source_gps_date""",
        )
        violations["observed_expected_without_arrival"] = _scalar(
            connection,
            """select count(*) from expected_events e left join arrivals a using
                (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence)
            where e.observation_status = 'observed' and a.stop_sequence is null""",
        )
        violations["non_observed_expected_has_observation_data"] = _scalar(
            connection,
            """select count(*) from expected_events
            where observation_status != 'observed' and (
                actual_arrival_time is not null or delay_seconds is not null or source_gps_date is not null
            )""",
        )
        violations["observed_source_gps_date_mismatch"] = _scalar(
            connection,
            """select count(*) from (
                select 1 from arrivals where source_gps_date is distinct from processing_date
                union all select 1 from expected_events
                    where observation_status = 'observed' and source_gps_date is distinct from processing_date
            )""",
        )
        violations["non_monotone_stop_arrival_sequence"] = _scalar(
            connection,
            """with sequenced as (
                select *, lag(actual_arrival_time) over trip_window previous_arrival
                from arrivals window trip_window as (
                    partition by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                    order by stop_sequence
                )
            ) select count(*) from (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number from sequenced
                group by all having bool_or(actual_arrival_time is null or actual_arrival_time < previous_arrival)
            )""",
        )
        violations["non_monotone_expected_schedule_sequence"] = _scalar(
            connection,
            """with sequenced as (
                select *, lag(scheduled_arrival_time) over trip_window previous_schedule
                from expected_events window trip_window as (
                    partition by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                    order by stop_sequence
                )
            ) select count(*) from (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number from sequenced
                group by all having bool_or(
                    scheduled_arrival_time is null or scheduled_arrival_time < previous_schedule
                )
            )""",
        )
        violations["non_monotone_expected_observed_sequence"] = _scalar(
            connection,
            """with sequenced as (
                select *, lag(actual_arrival_time) over trip_window previous_arrival
                from expected_events where actual_arrival_time is not null window trip_window as (
                    partition by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                    order by stop_sequence
                )
            ) select count(*) from (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number from sequenced
                group by all having bool_or(actual_arrival_time < previous_arrival)
            )""",
        )
        violations["incoherent_complete_prior_endpoints"] = _scalar(
            connection,
            """with regular_arrivals as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number,
                    count(*) arrival_count, min(stop_sequence) first_sequence, max(stop_sequence) last_sequence,
                    min(actual_arrival_time) actual_start, max(actual_arrival_time) actual_end
                from arrivals where stop_service_class = 'regular'
                group by all
            ) select count(*) from trips t left join regular_arrivals a using
                (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
            where t.service_date < t.processing_date and t.trip_quality = 'complete' and (
                not t.is_first_stop_observed or not t.is_last_stop_observed
                or t.scheduled_start_time is null or t.scheduled_end_time is null
                or t.actual_start_time is null or t.actual_end_time is null
                or t.scheduled_start_time > t.scheduled_end_time or t.actual_start_time > t.actual_end_time
                or coalesce(a.arrival_count, 0) != t.passenger_stops_detected
                or a.first_sequence is distinct from t.first_detected_stop_sequence
                or a.last_sequence is distinct from t.last_detected_stop_sequence
                or a.actual_start is distinct from t.actual_start_time
                or a.actual_end is distinct from t.actual_end_time
            )""",
        )
        prior_count = _scalar(connection, "select count(*) from trips where service_date < processing_date")
        if not prior_count:
            violations["no_prior_service_evidence"] = 1
        prior_quality_counts = {
            str(row[0]): int(row[1])
            for row in connection.execute(
                """select trip_quality, count(*) from trips where service_date < processing_date
                group by trip_quality order by trip_quality"""
            ).fetchall()
        }
        ranking_counts = {
            str(row[0]): int(row[1])
            for row in connection.execute(
                """select t.line, count(a.stop_sequence) filter (
                    where t.trip_quality = 'complete' and t.is_zone1_public_ranking_trip
                ) ranking_arrival_count
                from trips t left join arrivals a using
                    (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
                where t.service_date < t.processing_date and regexp_matches(t.line, '^N[0-9]')
                group by t.line order by t.line"""
            ).fetchall()
        }
        eligible_n_lines = [line for line, count in ranking_counts.items() if count >= RANKING_ARRIVAL_FLOOR]
        ineligible_n_lines = [line for line in ranking_counts if line not in eligible_n_lines]
        if not eligible_n_lines:
            violations["no_healthy_n_line_at_ranking_floor"] = 1
    except duckdb.Error as exc:
        raise fail("invalid_data", f"cannot validate overnight proof artifacts: {exc}", 12) from exc
    finally:
        connection.close()

    report = _base_report(hashes=hashes, violations=violations)
    report.update(
        {
            "processing_dates": [str(value) for value in processing_dates],
            "snapshot_ids": snapshot_ids,
            "prior_service_trip_quality_counts": prior_quality_counts,
            "prior_n_line_complete_ranking_arrival_counts": ranking_counts,
            "eligible_n_lines": eligible_n_lines,
            "ineligible_n_lines": ineligible_n_lines,
            "has_prior_service_evidence": bool(prior_count),
            "has_healthy_n_line_at_ranking_floor": bool(eligible_n_lines),
            "passed": not any(violations.values()),
        }
    )
    return report


def write_overnight_proof_report(input_dir: Path, report_json: Path) -> dict[str, Any]:
    """Build and persist the deterministic report used by the command-line gate."""
    report = build_overnight_proof_report(input_dir)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def write_failed_overnight_proof_report(report_json: Path, error: MatcherError) -> None:
    """Persist a stable failed report when required proof input cannot be read."""
    violations = {name: 0 for name in _VIOLATIONS}
    report = _base_report(hashes={}, violations=violations) | {"error": {"code": error.code, "message": error.message}}
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
