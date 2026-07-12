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
    TRIP_UNIVERSE_SCHEMA,
)


def _write_artifacts(
    output: Path,
    *,
    processing_date: date = date(2026, 7, 9),
    include_prior: bool = True,
    include_current_same_trip_id: bool = False,
    ranking_eligible: bool = True,
) -> None:
    output.mkdir()
    trip_rows: list[dict[str, object]] = []
    arrival_rows: list[dict[str, object]] = []
    expected_rows: list[dict[str, object]] = []
    universe_rows: list[dict[str, object]] = []
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
                "is_zone1_public_ranking_trip": ranking_eligible,
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
                "is_zone1_public_ranking_trip": ranking_eligible,
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
        universe = dict.fromkeys(TRIP_UNIVERSE_SCHEMA.names)
        universe.update(
            {
                "gtfs_snapshot_id": trip["gtfs_snapshot_id"],
                "processing_date": processing_date,
                "service_date": service_date,
                "duty_chain_id": "duty-42",
                "trip_id": trip["trip_id"],
                "line": trip["line"],
                "mode": trip["mode"],
                "direction_id": 0,
                "origin_stop_id": "stop-0",
                "destination_stop_id": f"stop-{RANKING_ARRIVAL_FLOOR - 1}",
                "ordered_stop_ids": "|".join(f"stop-{index}" for index in range(RANKING_ARRIVAL_FLOOR)),
                "stop_count": RANKING_ARRIVAL_FLOOR,
                "non_zone1_stop_count": 0,
                "is_public_service_segment": True,
                "is_public_passenger_segment": True,
                "terminal_pair_trip_count": 1,
                "terminal_pair_rank": 1,
                "is_short_turn_part_trip": False,
                "is_zone1_only": True,
                "is_zone1_public_ranking_trip": ranking_eligible,
            }
        )
        universe_rows.append(universe)
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
    pq.write_table(pa.Table.from_pylist(universe_rows, schema=TRIP_UNIVERSE_SCHEMA), output / "trip_universe.parquet")


def test_overnight_proof_accepts_prior_service_lineage_and_reused_current_trip_id(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, include_current_same_trip_id=True)

    report = build_overnight_proof_report(artifacts)

    assert report["passed"]
    assert report["processing_dates"] == ["2026-07-09"]
    assert report["prior_service_trip_quality_counts"] == {"complete": 1}
    assert report["prior_n_line_complete_ranking_arrival_counts"] == {"N42": 20}
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


def test_overnight_proof_counts_only_persisted_ranking_universe_arrivals(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, ranking_eligible=False)

    report = build_overnight_proof_report(artifacts)

    assert report["prior_n_line_complete_ranking_arrival_counts"] == {"N42": 0}
    assert report["eligible_n_lines"] == []
    assert report["ineligible_n_lines"] == ["N42"]
    assert report["contract_violation_counts"]["no_healthy_n_line_at_ranking_floor"] == 1


def test_overnight_proof_rejects_non_observed_event_data_and_gps_date_drift(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts)
    expected_path = artifacts / "reconstruction_expected_stop_events.parquet"
    expected_table = pq.read_table(expected_path)
    expected_rows = expected_table.to_pylist()
    expected_rows[0].update(
        {
            "observation_status": "missed",
            "actual_arrival_time": None,
            "delay_seconds": None,
            "source_gps_date": date(2026, 7, 9),
            "gps_date": date(2026, 7, 8),
        }
    )
    pq.write_table(pa.Table.from_pylist(expected_rows, schema=expected_table.schema), expected_path)

    report = build_overnight_proof_report(artifacts)

    assert report["contract_violation_counts"]["non_observed_expected_has_observation_data"] == 1
    assert report["contract_violation_counts"]["gps_date_processing_date_mismatch"] == 1


def test_overnight_proof_cli_writes_stable_failed_report_for_corrupt_input(tmp_path: Path) -> None:
    artifacts, report_json = tmp_path / "artifacts", tmp_path / "report.json"
    _write_artifacts(artifacts)
    (artifacts / "trip_universe.parquet").write_bytes(b"not parquet")

    exit_code = main(["overnight-proof", "--input-dir", str(artifacts), "--report-json", str(report_json)])

    report = json.loads(report_json.read_text())
    assert exit_code == 12
    assert not report["passed"]
    assert report["error"]["code"] == "invalid_data"


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
