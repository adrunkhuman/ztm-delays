from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ztm_matcher.cli import main
from ztm_matcher.overnight_proof import RANKING_ARRIVAL_FLOOR, build_overnight_proof_report
from ztm_matcher.schemas import (
    RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
    RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_TRIP_FACT_SCHEMA,
)


def _write_artifacts(
    output: Path,
    *,
    processing_date: date = date(2026, 7, 9),
    include_prior: bool = True,
    include_current_same_trip_id: bool = False,
) -> None:
    output.mkdir()
    trip_rows: list[dict[str, object]] = []
    arrival_rows: list[dict[str, object]] = []
    expected_rows: list[dict[str, object]] = []
    service_dates = [processing_date - timedelta(days=1)] if include_prior else []
    if include_current_same_trip_id:
        service_dates.append(processing_date)
    for service_date in service_dates:
        scheduled_start = datetime.combine(service_date + timedelta(days=1), datetime.min.time(), UTC)
        actual_start = scheduled_start + timedelta(minutes=2)
        trip = dict.fromkeys(RECONSTRUCTION_TRIP_FACT_SCHEMA.names)
        trip.update(
            {
                "gtfs_snapshot_id": "snapshot-2026-07-09",
                "processing_date": processing_date,
                "gps_date": processing_date,
                "service_date": service_date,
                "trip_id": "reused-trip-id",
                "vehicle_number": "9001",
                "line": "N42",
                "brigade": "0042",
                "mode": "bus",
                "scheduled_start_time": scheduled_start,
                "scheduled_end_time": scheduled_start + timedelta(minutes=19),
                "actual_start_time": actual_start,
                "actual_end_time": actual_start + timedelta(minutes=19),
                "start_delay_seconds": 120,
                "end_delay_seconds": 120,
                "passenger_stops_expected": RANKING_ARRIVAL_FLOOR,
                "passenger_stops_detected": RANKING_ARRIVAL_FLOOR,
                "detected_stop_ratio": 1.0,
                "optional_passenger_stops_expected": 0,
                "optional_passenger_stops_detected": 0,
                "first_detected_stop_sequence": 0,
                "last_detected_stop_sequence": RANKING_ARRIVAL_FLOOR - 1,
                "max_stop_sequence_gap": 1,
                "max_ping_gap_seconds": 60,
                "max_speed_mps": 8.0,
                "is_first_stop_observed": True,
                "is_last_stop_observed": True,
                "has_non_monotonic_stop_progression": False,
                "has_impossible_speed_jump": False,
                "has_stale_stop_progression": False,
                "trip_quality": "complete",
                "quality_flags": [],
                "service_observation_class": "regular",
                "service_observation_flags": [],
            }
        )
        trip_rows.append(trip)
        for sequence in range(RANKING_ARRIVAL_FLOOR):
            scheduled = scheduled_start + timedelta(minutes=sequence)
            actual = actual_start + timedelta(minutes=sequence)
            common = {
                "gtfs_snapshot_id": trip["gtfs_snapshot_id"],
                "processing_date": processing_date,
                "gps_date": processing_date,
                "source_gps_date": processing_date,
                "service_date": service_date,
                "trip_id": trip["trip_id"],
                "vehicle_number": trip["vehicle_number"],
                "line": trip["line"],
                "brigade": trip["brigade"],
                "mode": trip["mode"],
                "stop_id": f"stop-{sequence}",
                "stop_group_id": f"group-{sequence}",
                "stop_sequence": sequence,
                "pickup_type": 0,
                "drop_off_type": 0,
                "stop_service_class": "regular",
                "scheduled_arrival_time": scheduled,
                "scheduled_departure_time": scheduled,
                "actual_arrival_time": actual,
                "delay_seconds": 120,
                "trip_quality": "complete",
                "quality_flags": [],
                "service_observation_class": "regular",
                "service_observation_flags": [],
            }
            arrival = dict.fromkeys(RECONSTRUCTION_STOP_ARRIVAL_SCHEMA.names)
            arrival.update(
                common
                | {
                    "detection_method": "segment_crossing",
                    "stop_match_radius_m": 50.0,
                    "stop_distance_m": 1.0,
                    "prev_ping_distance_m": 10.0,
                    "next_ping_distance_m": 10.0,
                    "segment_start_time": actual - timedelta(seconds=10),
                    "segment_end_time": actual,
                    "segment_duration_seconds": 10,
                    "alignment_confidence": "high",
                    "alignment_evidence": [],
                }
            )
            expected = dict.fromkeys(RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA.names)
            expected.update(common | {"observation_status": "observed", "uncertainty_evidence": []})
            arrival_rows.append(arrival)
            expected_rows.append(expected)
    pq.write_table(
        pa.Table.from_pylist(trip_rows, schema=RECONSTRUCTION_TRIP_FACT_SCHEMA),
        output / "reconstruction_trip_facts.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(arrival_rows, schema=RECONSTRUCTION_STOP_ARRIVAL_SCHEMA),
        output / "reconstruction_stop_arrivals.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(expected_rows, schema=RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA),
        output / "reconstruction_expected_stop_events.parquet",
    )


def test_overnight_proof_accepts_prior_service_lineage_and_reused_current_trip_id(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, include_current_same_trip_id=True)

    report = build_overnight_proof_report(artifacts)

    assert report["passed"]
    assert report["processing_dates"] == ["2026-07-09"]
    assert report["prior_service_trip_quality_counts"] == {"complete": 1}
    assert report["prior_n_line_complete_trip_arrival_counts"] == {"N42": 20}
    assert report["eligible_n_lines"] == ["N42"]
    assert report["ineligible_n_lines"] == []


def test_overnight_proof_rejects_degraded_processing_date(tmp_path: Path) -> None:
    artifacts, report_json = tmp_path / "artifacts", tmp_path / "report.json"
    _write_artifacts(artifacts, processing_date=date(2026, 7, 5))

    exit_code = main(["overnight-proof", "--input-dir", str(artifacts), "--report-json", str(report_json)])

    report = json.loads(report_json.read_text())
    assert exit_code == 1
    assert not report["passed"]
    assert report["contract_violation_counts"]["degraded_processing_date"] == 1


def test_overnight_proof_requires_prior_service_evidence(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, include_prior=False, include_current_same_trip_id=True)

    report = build_overnight_proof_report(artifacts)

    assert not report["passed"]
    assert report["contract_violation_counts"]["no_prior_service_evidence"] == 1
    assert report["contract_violation_counts"]["no_healthy_n_line_at_ranking_floor"] == 1


def test_overnight_proof_report_is_deterministic_and_keeps_ranking_floor(tmp_path: Path) -> None:
    artifacts, first, second = tmp_path / "artifacts", tmp_path / "first.json", tmp_path / "second.json"
    _write_artifacts(artifacts)

    assert main(["overnight-proof", "--input-dir", str(artifacts), "--report-json", str(first)]) == 0
    assert main(["overnight-proof", "--input-dir", str(artifacts), "--report-json", str(second)]) == 0

    assert first.read_bytes() == second.read_bytes()
    report = json.loads(first.read_text())
    assert RANKING_ARRIVAL_FLOOR == 20
    assert report["ranking_arrival_floor"] == 20
    assert report["ranking_floor_unchanged"]
