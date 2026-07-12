"""Deterministic local evidence gate for overnight reconstruction artifacts."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ztm_matcher.errors import fail
from ztm_matcher.schemas import (
    RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
    RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_TRIP_FACT_SCHEMA,
)

RANKING_ARRIVAL_FLOOR = 20
DEGRADED_PROCESSING_DATES = frozenset(date(2026, 7, day) for day in range(5, 8))
_N_LINE = re.compile(r"^N\d")
_ARTIFACTS = (
    ("reconstruction_trip_facts", RECONSTRUCTION_TRIP_FACT_SCHEMA),
    ("reconstruction_stop_arrivals", RECONSTRUCTION_STOP_ARRIVAL_SCHEMA),
    ("reconstruction_expected_stop_events", RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA),
)
_VIOLATIONS = (
    "artifact_schema_mismatch",
    "duplicate_trip_grain",
    "duplicate_stop_arrival_grain",
    "duplicate_expected_event_grain",
    "null_lineage",
    "multiple_processing_dates",
    "multiple_snapshot_ids",
    "degraded_processing_date",
    "prior_service_date_mismatch",
    "missing_expected_trip",
    "expected_event_count_mismatch",
    "arrival_without_expected_event",
    "observed_expected_without_arrival",
    "arrival_expected_mismatch",
    "observed_source_gps_date_mismatch",
    "lineage_mismatch",
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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _as_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _lineage(row: dict[str, Any]) -> tuple[object, ...]:
    return tuple(
        row.get(name) for name in ("gtfs_snapshot_id", "processing_date", "service_date", "trip_id", "vehicle_number")
    )


def _event_grain(row: dict[str, Any]) -> tuple[object, ...]:
    return (*_lineage(row), row.get("stop_sequence"))


def _duplicates(rows: list[dict[str, Any]], grain: str) -> int:
    counts = Counter(tuple(row.get(field) for field in grain.split(",")) for row in rows)
    return sum(count - 1 for count in counts.values() if count > 1)


def _is_prior(row: dict[str, Any]) -> bool:
    processing_date = _as_date(row.get("processing_date"))
    service_date = _as_date(row.get("service_date"))
    return processing_date is not None and service_date is not None and service_date < processing_date


def _is_n_line(line: object) -> bool:
    return isinstance(line, str) and _N_LINE.match(line) is not None


def _ordered(start: Any, end: Any) -> bool:
    return start is not None and end is not None and start <= end


def _non_monotone(rows: list[dict[str, Any]], field: str, *, non_null_only: bool = False) -> bool:
    values = [
        row.get(field)
        for row in sorted(rows, key=lambda row: (row.get("stop_sequence") is None, row.get("stop_sequence")))
    ]
    if non_null_only:
        values = [value for value in values if value is not None]
    return any(
        previous is None or current is None or current < previous
        for previous, current in zip(values, values[1:], strict=False)
    )


def build_overnight_proof_report(input_dir: Path) -> dict[str, Any]:
    """Validate local fact artifacts and return a stable overnight-evidence report."""
    artifacts: dict[str, list[dict[str, Any]]] = {}
    hashes: dict[str, str] = {}
    violations = Counter({name: 0 for name in _VIOLATIONS})
    for name, schema in _ARTIFACTS:
        path = _artifact_path(input_dir, name)
        hashes[name] = _sha256(path)
        if pq.read_schema(path) != schema:
            violations["artifact_schema_mismatch"] += 1
        artifacts[name] = pq.read_table(path).to_pylist()

    trips = artifacts["reconstruction_trip_facts"]
    arrivals = artifacts["reconstruction_stop_arrivals"]
    expected = artifacts["reconstruction_expected_stop_events"]
    violations["duplicate_trip_grain"] = _duplicates(
        trips, "gtfs_snapshot_id,processing_date,service_date,trip_id,vehicle_number"
    )
    violations["duplicate_stop_arrival_grain"] = _duplicates(
        arrivals, "gtfs_snapshot_id,processing_date,service_date,trip_id,vehicle_number,stop_sequence"
    )
    violations["duplicate_expected_event_grain"] = _duplicates(
        expected, "gtfs_snapshot_id,processing_date,service_date,trip_id,vehicle_number,stop_sequence"
    )

    all_rows = [*trips, *arrivals, *expected]
    for row in all_rows:
        if any(value is None for value in _lineage(row)):
            violations["null_lineage"] += 1
    processing_dates = sorted(
        {value for row in all_rows if (value := _as_date(row.get("processing_date"))) is not None}
    )
    snapshot_ids = sorted(
        {value for row in all_rows if isinstance((value := row.get("gtfs_snapshot_id")), str) and value}
    )
    if len(processing_dates) != 1:
        violations["multiple_processing_dates"] += 1
    if len(snapshot_ids) != 1:
        violations["multiple_snapshot_ids"] += 1
    if any(processing_date in DEGRADED_PROCESSING_DATES for processing_date in processing_dates):
        violations["degraded_processing_date"] += 1

    for row in all_rows:
        processing_date = _as_date(row.get("processing_date"))
        service_date = _as_date(row.get("service_date"))
        if processing_date is not None and service_date is not None and service_date < processing_date:
            if service_date != processing_date - timedelta(days=1):
                violations["prior_service_date_mismatch"] += 1

    trip_by_lineage = {_lineage(row): row for row in trips}
    expected_by_grain = {_event_grain(row): row for row in expected}
    arrival_by_grain = {_event_grain(row): row for row in arrivals}
    expected_by_lineage: dict[tuple[object, ...], list[dict[str, Any]]] = defaultdict(list)
    arrivals_by_lineage: dict[tuple[object, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in expected:
        expected_by_lineage[_lineage(row)].append(row)
    for row in arrivals:
        arrivals_by_lineage[_lineage(row)].append(row)

    for trip_key in trip_by_lineage:
        trip_expected = expected_by_lineage[trip_key]
        if not trip_expected:
            violations["missing_expected_trip"] += 1
        elif len(trip_expected) != (
            int(trip_by_lineage[trip_key].get("passenger_stops_expected") or 0)
            + int(trip_by_lineage[trip_key].get("optional_passenger_stops_expected") or 0)
        ):
            violations["expected_event_count_mismatch"] += 1
    for row in [*arrivals, *expected]:
        trip = trip_by_lineage.get(_lineage(row))
        if trip is None or any(row.get(field) != trip.get(field) for field in ("line", "brigade", "mode")):
            violations["lineage_mismatch"] += 1
    for row in arrivals:
        processing_date = _as_date(row.get("processing_date"))
        if row.get("source_gps_date") != processing_date:
            violations["observed_source_gps_date_mismatch"] += 1
        event = expected_by_grain.get(_event_grain(row))
        if event is None:
            violations["arrival_without_expected_event"] += 1
        elif (
            event.get("observation_status") != "observed"
            or event.get("actual_arrival_time") != row.get("actual_arrival_time")
            or event.get("delay_seconds") != row.get("delay_seconds")
            or event.get("source_gps_date") != row.get("source_gps_date")
        ):
            violations["arrival_expected_mismatch"] += 1
    for row in expected:
        if row.get("observation_status") != "observed":
            continue
        processing_date = _as_date(row.get("processing_date"))
        if row.get("source_gps_date") != processing_date:
            violations["observed_source_gps_date_mismatch"] += 1
        if _event_grain(row) not in arrival_by_grain:
            violations["observed_expected_without_arrival"] += 1

    for rows in arrivals_by_lineage.values():
        if _non_monotone(rows, "actual_arrival_time"):
            violations["non_monotone_stop_arrival_sequence"] += 1
    for rows in expected_by_lineage.values():
        if _non_monotone(rows, "scheduled_arrival_time"):
            violations["non_monotone_expected_schedule_sequence"] += 1
        if _non_monotone(rows, "actual_arrival_time", non_null_only=True):
            violations["non_monotone_expected_observed_sequence"] += 1

    prior_trips = [row for row in trips if _is_prior(row)]
    for trip in prior_trips:
        if trip.get("trip_quality") != "complete":
            continue
        trip_arrivals = [
            row for row in arrivals_by_lineage[_lineage(trip)] if row.get("stop_service_class") == "regular"
        ]
        sequences = [row.get("stop_sequence") for row in trip_arrivals]
        arrival_times = [row.get("actual_arrival_time") for row in trip_arrivals]
        coherent = (
            bool(trip.get("is_first_stop_observed"))
            and bool(trip.get("is_last_stop_observed"))
            and trip.get("scheduled_start_time") is not None
            and trip.get("scheduled_end_time") is not None
            and trip.get("actual_start_time") is not None
            and trip.get("actual_end_time") is not None
            and _ordered(trip.get("scheduled_start_time"), trip.get("scheduled_end_time"))
            and _ordered(trip.get("actual_start_time"), trip.get("actual_end_time"))
            and len(trip_arrivals) == trip.get("passenger_stops_detected")
            and sequences
            and all(sequence is not None for sequence in sequences)
            and all(arrival_time is not None for arrival_time in arrival_times)
            and min(sequences) == trip.get("first_detected_stop_sequence")
            and max(sequences) == trip.get("last_detected_stop_sequence")
            and min(arrival_times) == trip.get("actual_start_time")
            and max(arrival_times) == trip.get("actual_end_time")
        )
        if not coherent:
            violations["incoherent_complete_prior_endpoints"] += 1

    prior_quality_counts = dict(sorted(Counter(str(row.get("trip_quality")) for row in prior_trips).items()))
    n_lines = sorted({str(row.get("line")) for row in prior_trips if _is_n_line(row.get("line"))})
    prior_complete_arrival_counts = {
        line: sum(
            1
            for row in arrivals
            if _is_prior(row)
            and row.get("line") == line
            and trip_by_lineage.get(_lineage(row), {}).get("trip_quality") == "complete"
        )
        for line in n_lines
    }
    eligible_n_lines = [line for line in n_lines if prior_complete_arrival_counts[line] >= RANKING_ARRIVAL_FLOOR]
    ineligible_n_lines = [line for line in n_lines if line not in eligible_n_lines]
    if not prior_trips:
        violations["no_prior_service_evidence"] += 1
    if not eligible_n_lines:
        violations["no_healthy_n_line_at_ranking_floor"] += 1

    contract_violations = {name: violations[name] for name in _VIOLATIONS}
    return {
        "report_version": "overnight-proof-v1",
        "artifact_hashes": hashes,
        "processing_dates": [str(value) for value in processing_dates],
        "snapshot_ids": snapshot_ids,
        "prior_service_trip_quality_counts": prior_quality_counts,
        "prior_n_line_complete_trip_arrival_counts": prior_complete_arrival_counts,
        "ranking_arrival_floor": RANKING_ARRIVAL_FLOOR,
        "ranking_floor_unchanged": RANKING_ARRIVAL_FLOOR == 20,
        "eligible_n_lines": eligible_n_lines,
        "ineligible_n_lines": ineligible_n_lines,
        "contract_violation_counts": contract_violations,
        "has_prior_service_evidence": bool(prior_trips),
        "has_healthy_n_line_at_ranking_floor": bool(eligible_n_lines),
        "passed": not any(contract_violations.values()),
    }


def write_overnight_proof_report(input_dir: Path, report_json: Path) -> dict[str, Any]:
    """Build and persist the deterministic report used by the command-line gate."""
    report = build_overnight_proof_report(input_dir)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
