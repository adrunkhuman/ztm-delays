from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ztm_matcher.runtime as runtime
from ztm_matcher import ReconstructionRun, RunConfig
from ztm_matcher.cli import main
from ztm_matcher.errors import MatcherError
from ztm_matcher.gps import normalize
from ztm_matcher.gtfs import Snapshot, load, select
from ztm_matcher.overnight_proof import build_overnight_proof_report
from ztm_matcher.runtime import STOP_ALIGNMENT_ROW_GROUP_ROWS, _flush_stop_alignment_rows
from ztm_matcher.schemas import (
    DUTY_EXECUTION_SCHEMA,
    NORMALIZED_GPS_SCHEMA,
    PASSENGER_STOP_ARRIVAL_SCHEMA,
    RAW_GPS_SCHEMA,
    RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
    RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_TRIP_FACT_SCHEMA,
    STOP_CROSSING_SCHEMA,
    TRIP_UNIVERSE_SCHEMA,
)
from ztm_matcher.semantics import duties, iter_stop_semantics


def _gps(root: Path, rows: list[dict[str, object]]) -> None:
    path = root / "vehicle_type=bus" / "date=2026-01-15" / "hour=01"
    path.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=RAW_GPS_SCHEMA), path / "part.parquet")


def _gtfs(path: Path, block: str = "block-1", prior: bool = True) -> None:
    tables = {
        "trips.txt": [
            [
                "trip_id",
                "route_id",
                "service_id",
                "trip_headsign",
                "direction_id",
                "block_id",
                "block_short_name",
                "shape_id",
            ],
            ["overnight", "r1", "old", "Old", "0", block, "0012", "s"],
            ["today", "r1", "new", "New", "0", block, "0012", "s"],
        ],
        "stop_times.txt": [
            ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "pickup_type", "drop_off_type"],
            ["overnight", "100001", "1", "25:00:00", "25:00:00", "0", "0"],
            ["overnight", "100002", "2", "25:05:00", "25:05:00", "0", "0"],
            ["today", "100002", "1", "01:00:00", "01:00:00", "0", "0"],
            ["today", "100003", "2", "01:05:00", "01:05:00", "1", "1"],
        ],
        "stops.txt": [
            ["stop_id", "stop_name", "stop_code", "stop_lat", "stop_lon", "zone_id", "stop_name_stem", "town_name"],
            ["100001", "A", "1", "52.2", "21.0", "1", "A", "Warszawa"],
            ["100002", "B", "2", "52.201", "21.001", "1", "B", "Warszawa"],
            ["100003", "C", "3", "52.202", "21.002", "1", "C", "Warszawa"],
        ],
        "shapes.txt": [["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"], ["s", "52.2", "21", "1"]],
        "routes.txt": [["route_id", "route_short_name", "route_type"], ["r1", "187", "3"]],
        "calendar_dates.txt": [
            ["service_id", "date", "exception_type"],
            ["old", "20260114", "1" if prior else "2"],
            ["new", "20260115", "1"],
        ],
    }
    with zipfile.ZipFile(path, "w") as archive:
        for name, rows in tables.items():
            stream = io.StringIO()
            csv.writer(stream).writerows(rows)
            archive.writestr(name, stream.getvalue())


def _ranking_gtfs(path: Path, *, include_zone_id: bool = True) -> None:
    """Schedule fixtures covering each local ranking-universe exclusion."""
    tables = {
        "trips.txt": [
            [
                "trip_id",
                "route_id",
                "service_id",
                "trip_headsign",
                "direction_id",
                "block_id",
                "block_short_name",
                "shape_id",
            ],
            ["full-one", "n42", "svc", "Full", "0", "one", "1", "s"],
            ["full-two", "n42", "svc", "Full", "0", "two", "2", "s"],
            ["short", "n42", "svc", "Short", "0", "three", "3", "s"],
            ["non-zone1", "n43", "svc", "Outside", "0", "four", "4", "s"],
            ["depot", "n44", "svc", "Depot", "0", "five", "5", "s"],
            ["technical", "n45", "svc", "Technical", "0", "six", "6", "s"],
            ["all-technical", "n46", "svc", "Technical", "0", "seven", "7", "s"],
            ["malformed-previous", "n47", "svc", "Previous", "0", "eight", "8", "s"],
            ["malformed", "n47", "svc", "Malformed", "0", "eight", "8", "s"],
        ],
        "stop_times.txt": [
            ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "pickup_type", "drop_off_type"],
            *[
                [trip, stop, str(index), f"0{index}:00:00", f"0{index}:00:00", "0", "0"]
                for trip in ("full-one", "full-two")
                for index, stop in enumerate(("100001", "100002", "100003", "100004"), 1)
            ],
            ["short", "100002", "1", "01:00:00", "01:00:00", "0", "0"],
            ["short", "100003", "2", "02:00:00", "02:00:00", "0", "0"],
            ["non-zone1", "200001", "1", "01:00:00", "01:00:00", "0", "0"],
            ["non-zone1", "200002", "2", "02:00:00", "02:00:00", "0", "0"],
            ["depot", "300001", "1", "01:00:00", "01:00:00", "0", "0"],
            ["depot", "300002", "2", "02:00:00", "02:00:00", "0", "0"],
            ["technical", "400001", "1", "01:00:00", "01:00:00", "1", "1"],
            ["technical", "400002", "2", "02:00:00", "02:00:00", "0", "0"],
            ["all-technical", "500001", "1", "01:00:00", "01:00:00", "1", "1"],
            ["all-technical", "500002", "2", "02:00:00", "02:00:00", "1", "1"],
            ["malformed-previous", "100001", "1", "01:00:00", "01:00:00", "0", "0"],
            ["malformed-previous", "100004", "2", "02:00:00", "02:00:00", "0", "0"],
            ["malformed", "100001", "1", "01:30:00", "01:30:00", "0", "0"],
            ["malformed", "100004", "2", "02:30:00", "02:30:00", "0", "0"],
        ],
        "stops.txt": [
            ["stop_id", "stop_name", "stop_code", "stop_lat", "stop_lon", "zone_id", "stop_name_stem", "town_name"],
            ["100001", "A", "1", "52.20", "21.00", "1", "A", "Warszawa"],
            ["100002", "B", "2", "52.201", "21.001", "1+2", "B", "Warszawa"],
            ["100003", "C", "3", "52.202", "21.002", "1", "C", "Warszawa"],
            ["100004", "D", "4", "52.203", "21.003", "1", "D", "Warszawa"],
            ["200001", "E", "5", "52.204", "21.004", "2", "E", "Warszawa"],
            ["200002", "F", "6", "52.205", "21.005", "2", "F", "Warszawa"],
            ["300001", "Zajezdnia", "7", "52.206", "21.006", "1", "Z", "Warszawa"],
            ["300002", "G", "8", "52.207", "21.007", "1", "G", "Warszawa"],
            ["400001", "H", "9", "52.208", "21.008", "1", "H", "Warszawa"],
            ["400002", "I", "10", "52.209", "21.009", "1", "I", "Warszawa"],
            ["500001", "J", "11", "52.210", "21.010", "1", "J", "Warszawa"],
            ["500002", "K", "12", "52.211", "21.011", "1", "K", "Warszawa"],
        ],
        "shapes.txt": [["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"], ["s", "52.2", "21", "1"]],
        "routes.txt": [
            ["route_id", "route_short_name", "route_type"],
            ["n42", "N42", "3"],
            ["n43", "N43", "3"],
            ["n44", "N44", "3"],
            ["n45", "N45", "3"],
            ["n46", "N46", "3"],
            ["n47", "N47", "3"],
        ],
        "calendar_dates.txt": [
            ["service_id", "date", "exception_type"],
            ["svc", "20260114", "1"],
            ["svc", "20260115", "1"],
        ],
    }
    with zipfile.ZipFile(path, "w") as archive:
        for name, rows in tables.items():
            stream = io.StringIO()
            if name == "stops.txt" and not include_zone_id:
                rows = [row[:5] + row[6:] for row in rows]
            csv.writer(stream).writerows(rows)
            archive.writestr(name, stream.getvalue())


def _trip_universe_inputs(zip_path: Path) -> tuple[Snapshot, list[dict[str, Any]], set[tuple[date, str]]]:
    snapshot = load(zip_path, "synthetic", date(2026, 1, 15))
    duty_rows = duties(select(snapshot, date(2026, 1, 15)), snapshot)
    settled_passenger_trips = {
        (semantic["service_date"], semantic["trip_id"])
        for semantic in iter_stop_semantics(duty_rows, snapshot)
        if semantic["are_passenger_boundaries_settled"]
        and semantic["is_passenger_stop"]
        and semantic["stop_execution_class"] == "passenger"
    }
    return snapshot, duty_rows, settled_passenger_trips


def _row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "Lines": "187",
        "Brigade": "0012",
        "Lat": 52.2,
        "Lon": 21.0,
        "Time": datetime(2026, 1, 14, 23, 30, tzinfo=UTC),
        "VehicleNumber": "2",
        "vehicle_type": 1,
        "ingested_at": datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
    }
    row.update(changes)
    return row


def test_normalize_applies_diagnostic_line_and_vehicle_filters(tmp_path: Path) -> None:
    gps = tmp_path / "gps"
    _gps(
        gps,
        [
            _row(Lines="187", VehicleNumber="2"),
            _row(Lines="Z26", VehicleNumber="3", Time=datetime(2026, 1, 15, 0, 30, tzinfo=UTC)),
            _row(Lines="Z26", VehicleNumber="4", Time=datetime(2026, 1, 15, 0, 31, tzinfo=UTC)),
        ],
    )
    source = next(gps.rglob("*.parquet"))
    output = tmp_path / "normalized.parquet"

    with duckdb.connect() as connection:
        rows = normalize(
            connection,
            [source],
            date(2026, 1, 15),
            output,
            lines={"Z26"},
            vehicle_number="3",
        )

    assert rows == 1
    assert pq.read_table(output).to_pylist()[0]["line"] == "Z26"
    assert pq.read_table(output).to_pylist()[0]["vehicle_number"] == "3"


def test_normalize_keeps_same_vehicle_number_separate_across_types(tmp_path: Path) -> None:
    gps = tmp_path / "gps"
    _gps(
        gps,
        [
            _row(VehicleNumber="4244", vehicle_type=1, Lines="122", Lat=52.1, Lon=20.9),
            _row(VehicleNumber="4244", vehicle_type=2, Lines="31", Lat=52.3, Lon=21.1),
        ],
    )
    output = tmp_path / "normalized.parquet"

    with duckdb.connect() as connection:
        rows = normalize(connection, [next(gps.rglob("*.parquet"))], date(2026, 1, 15), output)

    normalized = pq.read_table(output).to_pylist()
    assert rows == 2
    assert [(row["vehicle_type"], row["vehicle_number"], row["line"]) for row in normalized] == [
        (1, "4244", "122"),
        (2, "4244", "31"),
    ]


def test_vehicle_streams_partition_same_number_by_type(tmp_path: Path) -> None:
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        tmp_path / "snapshot.zip",
        tmp_path / "output",
        tmp_path / "output" / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        run.normalized_path = run._work() / "normalized_gps.parquet"
        rows = [
            {
                "line": line,
                "brigade": "1",
                "lat": 52.2,
                "lon": 21.0,
                "gps_time": datetime(2026, 1, 15, 1, 0, tzinfo=UTC),
                "vehicle_number": "4244",
                "vehicle_type": vehicle_type,
                "ingested_at": datetime(2026, 1, 15, 1, 0, tzinfo=UTC),
                "gps_date": date(2026, 1, 15),
            }
            for vehicle_type, line in ((1, "122"), (2, "31"))
        ]
        pq.write_table(pa.Table.from_pylist(rows, schema=NORMALIZED_GPS_SCHEMA), run.normalized_path)

        streams = list(run.iter_vehicle_streams())

    assert [(stream.vehicle_type, stream.vehicle_number, stream.pings.num_rows) for stream in streams] == [
        (1, "4244", 1),
        (2, "4244", 1),
    ]


def _write_stop_alignment_fixture(run: ReconstructionRun, vehicle_numbers: tuple[str, ...]) -> None:
    run.prepare_schedule()
    work = run._work()
    semantic = next(
        row for row in pq.read_table(work / "stop_semantics.parquet").to_pylist() if row["trip_id"] == "today"
    )
    start = datetime(2026, 1, 15, 1, 0, tzinfo=UTC)
    executions = []
    pings = []
    for vehicle_number in vehicle_numbers:
        execution = dict.fromkeys(DUTY_EXECUTION_SCHEMA.names)
        execution.update(
            {
                "service_date": semantic["service_date"],
                "processing_date": semantic["processing_date"],
                "gtfs_snapshot_id": semantic["gtfs_snapshot_id"],
                "duty_chain_id": semantic["duty_chain_id"],
                "duty_chain_source": semantic["duty_chain_source"],
                "duty_chain_source_id": semantic["duty_chain_source_id"],
                "trip_order": semantic["trip_order"],
                "trip_id": semantic["trip_id"],
                "line": "187",
                "brigade": "0012",
                "mode": "bus",
                "vehicle_number": vehicle_number,
                "vehicle_type": 1,
                "execution_status": "executed",
                "confidence": "high",
                "execution_evidence": [],
                "source_ping_start_time": start,
                "source_ping_end_time": start + timedelta(minutes=2),
                "source_ping_count": 3,
                "ownership_interval_start_time": start,
                "ownership_interval_end_time": start + timedelta(minutes=2),
            }
        )
        executions.append(execution)
        for gps_time, lat, lon in (
            (start, 52.201, 21.001),
            (start + timedelta(minutes=1), 52.202, 21.002),
            (start + timedelta(minutes=2), 52.2021, 21.0021),
        ):
            pings.append(
                {
                    "line": "187",
                    "brigade": "0012",
                    "lat": lat,
                    "lon": lon,
                    "gps_time": gps_time,
                    "vehicle_number": vehicle_number,
                    "vehicle_type": 1,
                    "ingested_at": gps_time,
                    "gps_date": date(2026, 1, 15),
                }
            )
    pq.write_table(pa.Table.from_pylist(executions, schema=DUTY_EXECUTION_SCHEMA), work / "duty_execution.parquet")
    run.normalized_path = work / "normalized_gps.parquet"
    pq.write_table(pa.Table.from_pylist(pings, schema=NORMALIZED_GPS_SCHEMA), run.normalized_path)


def _run_stop_alignment_fixture(tmp_path: Path, name: str, workers: int) -> tuple[Path, dict[str, int]]:
    tmp_path.mkdir()
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / name
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
        alignment_workers=workers,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1", "2"))
        counts = run.align_stops()
        assert not (run._work() / ".stop_alignment_inputs.parquet").exists()
        assert not (run._work() / ".stop-alignment-shards").exists()
    return output, counts


def test_prepares_normalized_gps_schedule_semantics_manifest_and_groups(tmp_path: Path) -> None:
    root, zip_path, output = tmp_path / "gps", tmp_path / "snapshot.zip", tmp_path / "output"
    _gps(
        root,
        [
            _row(),
            _row(VehicleNumber="1", Brigade="0000"),
            _row(ingested_at=datetime(2026, 1, 15, 0, 2, tzinfo=UTC)),
            _row(Brigade="D1"),
            _row(VehicleNumber="not-a-number"),
        ],
    )
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        result = run.prepare()
        groups = list(run.iter_vehicle_streams())
    assert result["metrics"]["normalized_rows"] == 2
    assert [group.vehicle_number for group in groups] == ["1", "2"]
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["missing_hours"]["tram"] == list(range(24))
    assert (output / "duty_schedule.parquet").is_file()
    assert (output / "duty_execution.parquet").is_file()
    assert (output / "operational_stop_crossings.parquet").is_file()
    assert (output / "passenger_stop_arrivals.parquet").is_file()
    assert not (output / ".stop_alignment_inputs.parquet").exists()
    assert result["metrics"]["duty_execution_rows"] == 2
    execution = pq.read_table(output / "duty_execution.parquet")
    assert execution.schema == DUTY_EXECUTION_SCHEMA
    assert execution.num_rows == 2
    assert pq.read_table(output / "operational_stop_crossings.parquet").schema == STOP_CROSSING_SCHEMA
    assert pq.read_table(output / "passenger_stop_arrivals.parquet").schema == PASSENGER_STOP_ARRIVAL_SCHEMA
    assert result["metrics"]["operational_stop_crossings"] >= result["metrics"]["passenger_stop_arrivals"]
    assert result["metrics"]["stop_alignment_vehicle_groups"] == 0
    assert result["metrics"]["stop_alignment_execution_trips"] == 0
    assert result["metrics"]["stop_alignment_workers"] == 1
    assert result["metrics"]["stop_alignment_worker_chunks"] == 0
    assert all(
        row["ownership_interval_start_time"] is None
        or row["ownership_interval_start_time"] <= row["ownership_interval_end_time"]
        for row in execution.to_pylist()
    )
    semantics = pq.read_table(output / "stop_semantics.parquet").to_pylist()
    assert any(row["stop_execution_class"] == "technical_suffix" for row in semantics)


def test_prepare_diagnostic_trip_filters_schedule_and_gps(tmp_path: Path) -> None:
    root, zip_path, output = tmp_path / "gps", tmp_path / "snapshot.zip", tmp_path / "output"
    _gps(
        root,
        [
            _row(VehicleNumber="2"),
            _row(Lines="r1", VehicleNumber="3", Time=datetime(2026, 1, 15, 0, 30, tzinfo=UTC)),
        ],
    )
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
        diagnostic_vehicle_number="3",
        diagnostic_trip_id="today",
    )

    with ReconstructionRun(config) as run:
        result = run.prepare()

    assert result["metrics"]["schedule_rows"] == 1
    assert result["metrics"]["normalized_rows"] == 1
    assert result["manifest"]["config"]["diagnostic_trip_id"] == "today"
    assert {row["trip_id"] for row in pq.read_table(output / "duty_schedule.parquet").to_pylist()} == {"today"}


def test_prepare_schedule_accepts_comma_separated_diagnostic_lines(tmp_path: Path) -> None:
    zip_path = tmp_path / "snapshot.zip"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        tmp_path / "output",
        tmp_path / "metrics.json",
        diagnostic_line="187, missing",
    )

    run = ReconstructionRun(config)
    with pytest.raises(RuntimeError, match="stop before publication"):
        with run:
            schedule_rows = run.prepare_schedule()
            raise RuntimeError("stop before publication")

    assert schedule_rows == 2
    assert run.diagnostic_lines == {"r1"}


def test_stop_alignment_inputs_are_removed_after_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, zip_path, output = tmp_path / "gps", tmp_path / "snapshot.zip", tmp_path / "failed"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )

    def fail_after_materialization(*_: object) -> None:
        inputs = run._work() / ".stop_alignment_inputs.parquet"
        assert inputs.is_file()
        assert "semantic_stop_id" in pq.read_schema(inputs).names
        raise RuntimeError("alignment failed")

    def fail_flush(*_: object, **__: object) -> None:
        raise RuntimeError("flush failed")

    with pytest.raises(RuntimeError, match="alignment failed"):
        with ReconstructionRun(config) as run:
            run.prepare_schedule()
            work = run._work()
            semantic = pq.read_table(work / "stop_semantics.parquet").to_pylist()[0]
            execution = dict.fromkeys(DUTY_EXECUTION_SCHEMA.names)
            execution.update(
                {
                    "service_date": semantic["service_date"],
                    "processing_date": semantic["processing_date"],
                    "gtfs_snapshot_id": semantic["gtfs_snapshot_id"],
                    "duty_chain_id": semantic["duty_chain_id"],
                    "duty_chain_source": semantic["duty_chain_source"],
                    "duty_chain_source_id": semantic["duty_chain_source_id"],
                    "trip_order": semantic["trip_order"],
                    "trip_id": semantic["trip_id"],
                    "line": "187",
                    "brigade": "0012",
                    "mode": "bus",
                    "vehicle_number": "2",
                    "vehicle_type": 1,
                    "execution_status": "executed",
                    "confidence": "high",
                    "execution_evidence": [],
                    "source_ping_count": 2,
                    "ownership_interval_start_time": datetime(2026, 1, 15, tzinfo=UTC),
                    "ownership_interval_end_time": datetime(2026, 1, 15, tzinfo=UTC) + timedelta(minutes=5),
                }
            )
            pq.write_table(
                pa.Table.from_pylist([execution], schema=DUTY_EXECUTION_SCHEMA), work / "duty_execution.parquet"
            )
            run.normalized_path = work / "normalized_gps.parquet"
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "line": "187",
                            "brigade": "0012",
                            "lat": 52.2,
                            "lon": 21.0,
                            "gps_time": datetime(2026, 1, 15, tzinfo=UTC),
                            "vehicle_number": "2",
                            "vehicle_type": 1,
                            "ingested_at": datetime(2026, 1, 15, tzinfo=UTC),
                            "gps_date": date(2026, 1, 15),
                        }
                    ],
                    schema=NORMALIZED_GPS_SCHEMA,
                ),
                run.normalized_path,
            )
            monkeypatch.setattr("ztm_matcher.runtime.align_stop_crossings", fail_after_materialization)
            monkeypatch.setattr("ztm_matcher.runtime._flush_stop_alignment_rows", fail_flush)
            run.align_stops()

    assert not list(tmp_path.glob(".failed.incomplete-*/.stop_alignment_inputs.parquet"))
    assert not list(tmp_path.glob(".failed.incomplete-*/.stop-alignment-shards"))


def test_stop_alignment_worker_writes_bounded_row_groups(tmp_path: Path) -> None:
    path = tmp_path / "crossings.parquet"
    rows: list[dict[str, object]] = [{}] * (STOP_ALIGNMENT_ROW_GROUP_ROWS * 2 + 1)
    writer = pq.ParquetWriter(path, STOP_CROSSING_SCHEMA, compression="zstd")
    try:
        _flush_stop_alignment_rows(rows, writer, STOP_CROSSING_SCHEMA)
        assert len(rows) == 1
        _flush_stop_alignment_rows(rows, writer, STOP_CROSSING_SCHEMA, final=True)
    finally:
        writer.close()

    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_row_groups == 3
    assert [parquet.metadata.row_group(index).num_rows for index in range(parquet.metadata.num_row_groups)] == [
        STOP_ALIGNMENT_ROW_GROUP_ROWS,
        STOP_ALIGNMENT_ROW_GROUP_ROWS,
        1,
    ]


def test_stop_alignment_parallel_workers_match_serial_artifacts_and_clean_shards(tmp_path: Path) -> None:
    serial_one, serial_one_counts = _run_stop_alignment_fixture(tmp_path / "serial-one", "output", 1)
    serial_two, serial_two_counts = _run_stop_alignment_fixture(tmp_path / "serial-two", "output", 1)
    parallel, parallel_counts = _run_stop_alignment_fixture(tmp_path / "parallel", "output", 2)
    parallel_repeat, parallel_repeat_counts = _run_stop_alignment_fixture(tmp_path / "parallel-repeat", "output", 2)

    assert serial_one_counts == serial_two_counts
    assert serial_one_counts["vehicle_groups"] == 2
    assert serial_one_counts["execution_trips"] == 2
    assert parallel_counts == serial_one_counts | {"worker_chunks": 2}
    assert parallel_repeat_counts == parallel_counts
    for name in ("operational_stop_crossings.parquet", "passenger_stop_arrivals.parquet"):
        assert (serial_one / name).read_bytes() == (serial_two / name).read_bytes()
        assert (parallel / name).read_bytes() == (parallel_repeat / name).read_bytes()
        serial_rows = pq.read_table(serial_one / name).to_pylist()
        parallel_rows = pq.read_table(parallel / name).to_pylist()
        assert parallel_rows == serial_rows
        expected_row_groups = (len(parallel_rows) + STOP_ALIGNMENT_ROW_GROUP_ROWS - 1) // STOP_ALIGNMENT_ROW_GROUP_ROWS
        assert pq.ParquetFile(serial_one / name).metadata.num_row_groups == expected_row_groups
        assert pq.ParquetFile(parallel / name).metadata.num_row_groups == expected_row_groups
        assert [(row["vehicle_number"], row["trip_id"], row["stop_sequence"]) for row in parallel_rows] == sorted(
            (row["vehicle_number"], row["trip_id"], row["stop_sequence"]) for row in parallel_rows
        )
    assert not (parallel / ".stop-alignment-shards").exists()


def test_stop_alignment_worker_scans_normalized_gps_once_per_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
        alignment_workers=1,
    )
    normalized_queries: list[str] = []

    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1", "2", "3"))
        normalized_path = str(run.normalized_path).replace("'", "''")
        connect = runtime.duckdb.connect

        class CountingConnection:
            def __init__(self) -> None:
                self.connection = connect()

            def execute(self, query: str, *args: object, **kwargs: object) -> object:
                if normalized_path in query:
                    normalized_queries.append(query)
                return self.connection.execute(query, *args, **kwargs)

            def close(self) -> None:
                self.connection.close()

        monkeypatch.setattr(runtime.duckdb, "connect", CountingConnection)
        counts = run.align_stops()

    assert counts["vehicle_groups"] == 3
    assert len(normalized_queries) == 1
    assert "count(" not in normalized_queries[0].lower()
    assert (
        "concat(cast(vehicle_type as varchar), ':', vehicle_number) in (select unnest(?))"
        in normalized_queries[0].lower()
    )


def test_stop_alignment_enforces_vehicle_row_limit_while_grouping(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
        max_vehicle_rows=2,
    )

    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        with pytest.raises(MatcherError, match=r"vehicle \(1, '1'\) exceeds max_vehicle_rows"):
            run.align_stops()


def test_fact_construction_publishes_high_confidence_direct_adapters(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        run.align_stops()
        counts = run.build_facts()
        work = run._work()
        trips = pq.read_table(work / "reconstruction_trip_facts.parquet")
        arrivals = pq.read_table(work / "reconstruction_stop_arrivals.parquet")
        expected = pq.read_table(work / "reconstruction_expected_stop_events.parquet")

    assert counts["reconstruction_trip_facts"] == trips.num_rows == 1
    assert trips.schema == RECONSTRUCTION_TRIP_FACT_SCHEMA
    assert arrivals.schema == RECONSTRUCTION_STOP_ARRIVAL_SCHEMA
    assert expected.schema == RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA
    assert trips.to_pylist()[0]["trip_quality"] == "complete"
    assert trips.to_pylist()[0]["is_zone1_public_ranking_trip"]
    assert all(row["source_gps_date"] == date(2026, 1, 15) for row in arrivals.to_pylist())
    assert all(row["is_zone1_public_ranking_trip"] for row in arrivals.to_pylist())
    assert [row["observation_status"] for row in expected.to_pylist()] == ["observed"]
    assert all(row["source_gps_date"] == date(2026, 1, 15) for row in expected.to_pylist())


def test_fact_speed_metrics_ignore_same_number_from_other_vehicle_type(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("4244",))
        run.align_stops()
        normalized = pq.read_table(run.normalized_path).to_pylist()
        start = datetime(2026, 1, 15, 1, 0, tzinfo=UTC)
        normalized.append(
            {
                "line": "31",
                "brigade": "1",
                "lat": 51.0,
                "lon": 19.5,
                "gps_time": start,
                "vehicle_number": "4244",
                "vehicle_type": 2,
                "ingested_at": start,
                "gps_date": date(2026, 1, 15),
            }
        )
        pq.write_table(pa.Table.from_pylist(normalized, schema=NORMALIZED_GPS_SCHEMA), run.normalized_path)

        run.build_facts()
        trip = pq.read_table(run._work() / "reconstruction_trip_facts.parquet").to_pylist()[0]

    assert trip["trip_quality"] == "complete"
    assert not trip["has_impossible_speed_jump"]


def test_schedule_trip_universe_classifies_zone_depot_technical_and_short_turns(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _ranking_gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        run.prepare_schedule()
        assert not list(run._work().glob(".trip_universe.compact.*.parquet"))
        assert not list(run._work().glob(".trip_universe.parquet.*.tmp"))
        rows = [
            row
            for row in pq.read_table(run._work() / "trip_universe.parquet").to_pylist()
            if row["service_date"] == date(2026, 1, 15)
        ]
        semantics = pq.read_table(run._work() / "stop_semantics.parquet").to_pylist()

    universe = {row["trip_id"]: row for row in rows}
    assert semantics[0]["zone_id"] is not None
    assert next(row for row in semantics if row["stop_id"] == "100002")["effective_zone_id"] == "1"
    assert universe["full-one"]["is_zone1_public_ranking_trip"]
    assert universe["full-two"]["is_zone1_public_ranking_trip"]
    assert universe["non-zone1"]["non_zone1_stop_count"] == 2
    assert not universe["non-zone1"]["is_zone1_public_ranking_trip"]
    assert not universe["depot"]["is_public_passenger_segment"]
    assert universe["technical"]["is_public_passenger_segment"]
    assert not universe["all-technical"]["is_public_passenger_segment"]
    assert not universe["malformed"]["is_public_passenger_segment"]
    assert universe["short"]["is_short_turn_part_trip"]
    assert universe["short"]["terminal_pair_rank"] == 2
    assert not universe["short"]["is_zone1_public_ranking_trip"]


def test_trip_universe_writes_from_in_memory_schedule_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _ranking_gtfs(zip_path)
    snapshot, duty_rows, settled_passenger_trips = _trip_universe_inputs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )

    def unexpected_duckdb_access() -> None:
        raise AssertionError("trip-universe writer must not query DuckDB")

    with ReconstructionRun(config) as run:
        monkeypatch.setattr(run, "_connection", unexpected_duckdb_access)
        universe_path = run._work() / "trip_universe.parquet"
        run._write_trip_universe(duty_rows, snapshot, settled_passenger_trips, universe_path)
        universe = {row["trip_id"]: row for row in pq.read_table(universe_path).to_pylist()}

    assert pq.read_schema(output / "trip_universe.parquet") == TRIP_UNIVERSE_SCHEMA
    assert universe["technical"]["is_public_passenger_segment"]
    assert not universe["all-technical"]["is_public_passenger_segment"]
    assert not universe["malformed"]["is_public_passenger_segment"]
    assert not universe["non-zone1"]["is_zone1_public_ranking_trip"]
    assert not universe["short"]["is_zone1_public_ranking_trip"]


def test_trip_universe_short_turns_compare_stop_id_occurrences(tmp_path: Path) -> None:
    def trip(trip_id: str, stop_ids: list[str], rank: int) -> dict[str, object]:
        row = dict.fromkeys(TRIP_UNIVERSE_SCHEMA.names)
        row.update(
            {
                "trip_id": trip_id,
                "stop_count": len(stop_ids),
                "stop_ids": stop_ids,
                "non_zone1_stop_count": 0,
                "is_public_passenger_segment": True,
                "terminal_pair_rank": rank,
            }
        )
        return row

    rows = [
        trip("full-one", ["origin", "a|b", "destination"], 1),
        trip("full-two", ["origin", "a|b", "destination"], 1),
        trip("not-short", ["a", "b"], 2),
        trip("short", ["a|b"], 2),
    ]
    output_rows: list[dict[str, object]] = []

    with pq.ParquetWriter(tmp_path / "unused.parquet", TRIP_UNIVERSE_SCHEMA) as writer:
        runtime._write_trip_universe_group(rows, output_rows, writer)

    short_turn_parts = {row["trip_id"]: row["is_short_turn_part_trip"] for row in output_rows}
    assert short_turn_parts == {"full-one": False, "full-two": False, "not-short": False, "short": True}


def test_trip_universe_cleans_temporary_files_and_closes_writer_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _ranking_gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )

    parquet_writer = pq.ParquetWriter

    class FailingWriter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self._writer = parquet_writer(*args, **kwargs)
            self.closed = False

        def write_table(self, table: pa.Table) -> None:
            raise OSError("simulated parquet write failure")

        def close(self) -> None:
            self._writer.close()
            self.closed = True

    writer: FailingWriter | None = None

    def failing_writer(*args: object, **kwargs: object) -> FailingWriter:
        nonlocal writer
        writer = FailingWriter(*args, **kwargs)
        return writer

    with ReconstructionRun(config) as run:
        work = run._work()
        snapshot, duty_rows, settled_passenger_trips = _trip_universe_inputs(zip_path)
        monkeypatch.setattr(runtime.pq, "ParquetWriter", failing_writer)
        failed_output = work / "failed_trip_universe.parquet"
        with pytest.raises(OSError, match="simulated parquet write failure"):
            run._write_trip_universe(duty_rows, snapshot, settled_passenger_trips, failed_output)

    assert writer is not None
    assert writer.closed
    assert not failed_output.exists()
    assert not list(work.glob(".failed_trip_universe.compact.*.parquet"))
    assert not list(work.glob(".failed_trip_universe.parquet.*.tmp"))


def test_missing_zone_column_is_conservatively_non_eligible_in_trip_universe(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _ranking_gtfs(zip_path, include_zone_id=False)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        run.prepare_schedule()
        universe = pq.read_table(run._work() / "trip_universe.parquet").to_pylist()

    full_trip = next(
        row for row in universe if row["trip_id"] == "full-one" and row["service_date"] == date(2026, 1, 15)
    )
    assert full_trip["non_zone1_stop_count"] == 4
    assert not full_trip["is_zone1_public_ranking_trip"]


def test_prior_service_gtfs_after_midnight_gps_produces_complete_lineage(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        run.prepare_schedule()
        work = run._work()
        semantic_rows = [
            row for row in pq.read_table(work / "stop_semantics.parquet").to_pylist() if row["trip_id"] == "overnight"
        ]
        semantic = semantic_rows[0]
        start = datetime(2026, 1, 15, 0, 1, 30, tzinfo=UTC)
        end = datetime(2026, 1, 15, 0, 7, 30, tzinfo=UTC)
        execution = dict.fromkeys(DUTY_EXECUTION_SCHEMA.names)
        execution.update(
            {
                "service_date": semantic["service_date"],
                "processing_date": semantic["processing_date"],
                "gtfs_snapshot_id": semantic["gtfs_snapshot_id"],
                "duty_chain_id": semantic["duty_chain_id"],
                "duty_chain_source": semantic["duty_chain_source"],
                "duty_chain_source_id": semantic["duty_chain_source_id"],
                "trip_order": semantic["trip_order"],
                "trip_id": semantic["trip_id"],
                "line": "N42",
                "brigade": "0012",
                "mode": "bus",
                "vehicle_number": "42",
                "vehicle_type": 1,
                "execution_status": "executed",
                "confidence": "high",
                "execution_evidence": [],
                "source_ping_start_time": start,
                "source_ping_end_time": end,
                "source_ping_count": 7,
                "ownership_interval_start_time": start,
                "ownership_interval_end_time": end,
            }
        )
        pq.write_table(pa.Table.from_pylist([execution], schema=DUTY_EXECUTION_SCHEMA), work / "duty_execution.parquet")
        run.normalized_path = work / "normalized_gps.parquet"
        pings = []
        for minute, lat, lon in (
            (0, 52.1995, 20.9995),
            (1, 52.2, 21.0),
            (2, 52.20025, 21.00025),
            (3, 52.2005, 21.0005),
            (4, 52.20075, 21.00075),
            (5, 52.201, 21.001),
            (6, 52.2015, 21.0015),
        ):
            gps_time = start + timedelta(minutes=minute)
            pings.append(
                {
                    "line": "N42",
                    "brigade": "0012",
                    "lat": lat,
                    "lon": lon,
                    "gps_time": gps_time,
                    "vehicle_number": "42",
                    "vehicle_type": 1,
                    "ingested_at": gps_time,
                    "gps_date": date(2026, 1, 15),
                }
            )
        pq.write_table(pa.Table.from_pylist(pings, schema=NORMALIZED_GPS_SCHEMA), run.normalized_path)
        direct_arrivals = []
        for index, stop in enumerate(semantic_rows):
            scheduled = datetime(2026, 1, 15, 0, index * 5, tzinfo=UTC)
            actual = scheduled + timedelta(minutes=2)
            direct = dict.fromkeys(PASSENGER_STOP_ARRIVAL_SCHEMA.names)
            direct.update(
                {
                    "gtfs_snapshot_id": semantic["gtfs_snapshot_id"],
                    "service_date": semantic["service_date"],
                    "processing_date": semantic["processing_date"],
                    "duty_chain_id": semantic["duty_chain_id"],
                    "trip_id": semantic["trip_id"],
                    "line": "N42",
                    "brigade": "0012",
                    "mode": "bus",
                    "vehicle_number": "42",
                    "vehicle_type": 1,
                    "stop_id": stop["stop_id"],
                    "stop_group_id": stop["stop_group_id"],
                    "stop_sequence": stop["stop_sequence"],
                    "pickup_type": stop["pickup_type"],
                    "drop_off_type": stop["drop_off_type"],
                    "stop_service_class": stop["stop_service_class"],
                    "stop_execution_class": stop["stop_execution_class"],
                    "classification_confidence": stop["classification_confidence"],
                    "classification_reason": stop["classification_reason"],
                    "classification_evidence": stop["classification_evidence"],
                    "are_passenger_boundaries_settled": stop["are_passenger_boundaries_settled"],
                    "is_passenger_stop": stop["is_passenger_stop"],
                    "scheduled_arrival_time": scheduled,
                    "scheduled_departure_time": scheduled,
                    "actual_arrival_time": actual,
                    "arrival_delay_seconds": 120,
                    "segment_start_time": actual - timedelta(seconds=30),
                    "segment_end_time": actual,
                    "segment_duration_seconds": 30,
                    "segment_distance_m": 2.0,
                    "segment_start_distance_m": 20.0,
                    "segment_end_distance_m": 20.0,
                    "stop_match_radius_m": 50.0,
                    "detection_method": "segment_crossing",
                    "alignment_confidence": "high",
                    "alignment_evidence": [],
                }
            )
            direct_arrivals.append(direct)
        pq.write_table(
            pa.Table.from_pylist(direct_arrivals, schema=PASSENGER_STOP_ARRIVAL_SCHEMA),
            work / "passenger_stop_arrivals.parquet",
        )
        run.build_facts()
        trips = pq.read_table(work / "reconstruction_trip_facts.parquet").to_pylist()
        arrivals = pq.read_table(work / "reconstruction_stop_arrivals.parquet").to_pylist()
        expected = pq.read_table(work / "reconstruction_expected_stop_events.parquet").to_pylist()
        report = build_overnight_proof_report(work)

    assert trips[0]["service_date"] == date(2026, 1, 14)
    assert trips[0]["processing_date"] == date(2026, 1, 15)
    assert trips[0]["vehicle_number"] == "42"
    assert trips[0]["line"] == "N42"
    assert trips[0]["scheduled_start_time"] == datetime(2026, 1, 15, 0, 0, tzinfo=UTC)
    assert trips[0]["scheduled_end_time"] == datetime(2026, 1, 15, 0, 5, tzinfo=UTC)
    assert trips[0]["trip_quality"] == "complete"
    assert trips[0]["start_delay_seconds"] == 120
    assert trips[0]["end_delay_seconds"] == 120
    assert [row["stop_sequence"] for row in arrivals] == [1, 2]
    assert [row["actual_arrival_time"] for row in arrivals] == sorted(row["actual_arrival_time"] for row in arrivals)
    assert [row["source_gps_date"] for row in arrivals] == [date(2026, 1, 15)] * 2
    assert [row["observation_status"] for row in expected] == ["observed", "observed"]
    assert [row["scheduled_arrival_time"] for row in expected] == [
        datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
    ]
    assert [row["source_gps_date"] for row in expected] == [date(2026, 1, 15)] * 2
    assert report["prior_n_line_complete_ranking_arrival_counts"] == {"N42": 2}
    assert report["contract_violation_counts"]["no_healthy_n_line_at_ranking_floor"] == 1
    assert all(
        count == 0
        for name, count in report["contract_violation_counts"].items()
        if name != "no_healthy_n_line_at_ranking_floor"
    )


@pytest.mark.parametrize(
    ("service_date", "scheduled_start", "scheduled_end"),
    [
        (
            date(2026, 3, 29),
            datetime(2026, 3, 29, 1, 30, tzinfo=UTC),
            datetime(2026, 3, 29, 23, 0, tzinfo=UTC),
        ),
        (
            date(2026, 10, 25),
            datetime(2026, 10, 25, 0, 30, tzinfo=UTC),
            datetime(2026, 10, 26, 0, 0, tzinfo=UTC),
        ),
    ],
)
def test_fact_scheduled_times_use_warsaw_wall_clock_across_dst_and_overnight(
    tmp_path: Path, service_date: date, scheduled_start: datetime, scheduled_end: datetime
) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        run.align_stops()
        work = run._work()
        execution_path = work / "duty_execution.parquet"
        executions = pq.read_table(execution_path).to_pylist()
        executions[0]["service_date"] = service_date
        pq.write_table(pa.Table.from_pylist(executions, schema=DUTY_EXECUTION_SCHEMA), execution_path)

        semantics_path = work / "stop_semantics.parquet"
        semantic_table = pq.read_table(semantics_path)
        semantics = semantic_table.to_pylist()
        trip_semantics = [row for row in semantics if row["trip_id"] == executions[0]["trip_id"]]
        for index, row in enumerate(trip_semantics):
            row.update(
                {
                    "service_date": service_date,
                    "arrival_time_seconds": 2 * 3600 + 30 * 60 if index == 0 else 25 * 3600,
                    "departure_time_seconds": 2 * 3600 + 30 * 60 if index == 0 else 25 * 3600,
                    "stop_execution_class": "passenger",
                    "stop_service_class": "regular",
                    "is_passenger_stop": True,
                    "are_passenger_boundaries_settled": True,
                    "first_passenger_stop_sequence": trip_semantics[0]["stop_sequence"],
                    "last_passenger_stop_sequence": trip_semantics[-1]["stop_sequence"],
                }
            )
        pq.write_table(pa.Table.from_pylist(semantics, schema=semantic_table.schema), semantics_path)
        universe_path = work / "trip_universe.parquet"
        universe_table = pq.read_table(universe_path)
        universe_rows = universe_table.to_pylist()
        for row in universe_rows:
            if row["trip_id"] == executions[0]["trip_id"]:
                row["service_date"] = service_date
        pq.write_table(pa.Table.from_pylist(universe_rows, schema=universe_table.schema), universe_path)

        arrivals_path = work / "passenger_stop_arrivals.parquet"
        arrival_table = pq.read_table(arrivals_path)
        template = arrival_table.to_pylist()[0]
        direct_arrivals = []
        for index, semantic in enumerate(trip_semantics):
            direct = template | {
                "service_date": service_date,
                "stop_id": semantic["stop_id"],
                "stop_group_id": semantic["stop_group_id"],
                "stop_sequence": semantic["stop_sequence"],
                "pickup_type": semantic["pickup_type"],
                "drop_off_type": semantic["drop_off_type"],
                "stop_service_class": "regular",
                "stop_execution_class": "passenger",
                "is_passenger_stop": True,
                "are_passenger_boundaries_settled": True,
                "alignment_confidence": "high",
                "actual_arrival_time": datetime(2026, 1, 15, 1, index, tzinfo=UTC),
                "arrival_delay_seconds": 0,
            }
            direct_arrivals.append(direct)
        pq.write_table(pa.Table.from_pylist(direct_arrivals, schema=arrival_table.schema), arrivals_path)

        run.build_facts()
        trips = pq.read_table(work / "reconstruction_trip_facts.parquet").to_pylist()
        expected = pq.read_table(work / "reconstruction_expected_stop_events.parquet").to_pylist()

    assert trips[0]["scheduled_start_time"] == scheduled_start
    assert trips[0]["scheduled_end_time"] == scheduled_end
    assert [row["scheduled_arrival_time"] for row in expected] == [scheduled_start, scheduled_end]
    assert [row["scheduled_departure_time"] for row in expected] == [scheduled_start, scheduled_end]


def test_medium_direct_arrivals_are_uncertain_not_fact_evidence(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        run.align_stops()
        work = run._work()
        semantics_path = work / "stop_semantics.parquet"
        semantic_table = pq.read_table(semantics_path)
        semantics = semantic_table.to_pylist()
        trip_semantics = [row for row in semantics if row["trip_id"] == "today"]
        for index, row in enumerate(trip_semantics):
            row.update(
                {
                    "stop_execution_class": "passenger",
                    "stop_service_class": "regular" if index == 0 else "request",
                    "is_passenger_stop": True,
                    "are_passenger_boundaries_settled": True,
                    "first_passenger_stop_sequence": trip_semantics[0]["stop_sequence"],
                    "last_passenger_stop_sequence": trip_semantics[-1]["stop_sequence"],
                }
            )
        pq.write_table(pa.Table.from_pylist(semantics, schema=semantic_table.schema), semantics_path)

        arrivals_path = work / "passenger_stop_arrivals.parquet"
        arrival_table = pq.read_table(arrivals_path)
        template = arrival_table.to_pylist()[0]
        source_time = datetime(2026, 1, 14, 23, 30, tzinfo=UTC)
        direct_arrivals = []
        for index, semantic in enumerate(trip_semantics):
            direct_arrivals.append(
                template
                | {
                    "stop_id": semantic["stop_id"],
                    "stop_group_id": semantic["stop_group_id"],
                    "stop_sequence": semantic["stop_sequence"],
                    "pickup_type": semantic["pickup_type"],
                    "drop_off_type": semantic["drop_off_type"],
                    "stop_service_class": "regular" if index == 0 else "request",
                    "stop_execution_class": "passenger",
                    "is_passenger_stop": True,
                    "are_passenger_boundaries_settled": True,
                    "alignment_confidence": "medium",
                    "actual_arrival_time": datetime(2026, 1, 15, 1, index, tzinfo=UTC),
                    "arrival_delay_seconds": index,
                    "segment_start_time": source_time,
                }
            )
        pq.write_table(pa.Table.from_pylist(direct_arrivals, schema=arrival_table.schema), arrivals_path)
        gps_table = pq.read_table(run.normalized_path)
        source_ping = gps_table.to_pylist()[0] | {"gps_time": source_time, "gps_date": date(2026, 1, 14)}
        pq.write_table(
            pa.Table.from_pylist([*gps_table.to_pylist(), source_ping], schema=NORMALIZED_GPS_SCHEMA),
            run.normalized_path,
        )

        run.build_facts()
        trips = pq.read_table(work / "reconstruction_trip_facts.parquet").to_pylist()
        stop_arrivals = pq.read_table(work / "reconstruction_stop_arrivals.parquet").to_pylist()
        expected = pq.read_table(work / "reconstruction_expected_stop_events.parquet").to_pylist()

    trip = trips[0]
    assert (trip["passenger_stops_detected"], trip["optional_passenger_stops_detected"]) == (0, 0)
    assert (trip["actual_start_time"], trip["actual_end_time"]) == (None, None)
    assert trip["trip_quality"] == "broken"
    assert stop_arrivals == []
    assert [row["observation_status"] for row in expected] == ["uncertain", "uncertain"]
    assert all(row["actual_arrival_time"] is None and row["delay_seconds"] is None for row in expected)
    assert all(row["uncertainty_evidence"] == ["alignment_ambiguous_or_medium"] for row in expected)
    assert all(row["source_gps_date"] is None for row in expected)


def test_matching_failure_absent_events_are_uncertain_without_lineage(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        run.align_stops()
        work = run._work()
        semantics_path = work / "stop_semantics.parquet"
        semantic_table = pq.read_table(semantics_path)
        semantics = semantic_table.to_pylist()
        trip_semantics = [row for row in semantics if row["trip_id"] == "today"]
        for index, row in enumerate(trip_semantics):
            row.update(
                {
                    "stop_execution_class": "passenger",
                    "stop_service_class": "regular" if index == 0 else "request",
                    "is_passenger_stop": True,
                    "are_passenger_boundaries_settled": True,
                    "first_passenger_stop_sequence": trip_semantics[0]["stop_sequence"],
                    "last_passenger_stop_sequence": trip_semantics[-1]["stop_sequence"],
                }
            )
        pq.write_table(pa.Table.from_pylist(semantics, schema=semantic_table.schema), semantics_path)

        arrivals_path = work / "passenger_stop_arrivals.parquet"
        arrival_schema = pq.read_schema(arrivals_path)
        pq.write_table(pa.Table.from_pylist([], schema=arrival_schema), arrivals_path)

        run.build_facts()
        trips = pq.read_table(work / "reconstruction_trip_facts.parquet").to_pylist()
        expected = pq.read_table(work / "reconstruction_expected_stop_events.parquet").to_pylist()

    assert trips[0]["service_observation_class"] == "matching_failure"
    assert [row["stop_service_class"] for row in expected] == ["regular", "request"]
    assert [row["observation_status"] for row in expected] == ["uncertain", "uncertain"]
    assert all(row["actual_arrival_time"] is None and row["delay_seconds"] is None for row in expected)
    assert all(row["uncertainty_evidence"] == ["unreliable_trip_assignment"] for row in expected)
    assert all(row["source_gps_date"] is None for row in expected)


def test_unsettled_passenger_boundaries_keep_operational_execution_but_suppress_passenger_facts(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        work = run._work()
        semantics_path = work / "stop_semantics.parquet"
        semantic_table = pq.read_table(semantics_path)
        semantics = semantic_table.to_pylist()
        for row in semantics:
            if row["trip_id"] == "today":
                row["are_passenger_boundaries_settled"] = False
                row["is_passenger_stop"] = False
        pq.write_table(pa.Table.from_pylist(semantics, schema=semantic_table.schema), semantics_path)
        execution_path = work / "duty_execution.parquet"
        executions = pq.read_table(execution_path).to_pylist()
        executions[0]["are_passenger_boundaries_settled"] = False
        pq.write_table(pa.Table.from_pylist(executions, schema=DUTY_EXECUTION_SCHEMA), execution_path)

        run.align_stops()
        counts = run.build_facts()
        operational = pq.read_table(work / "operational_stop_crossings.parquet").to_pylist()
        passenger = pq.read_table(work / "passenger_stop_arrivals.parquet").to_pylist()
        trips = pq.read_table(work / "reconstruction_trip_facts.parquet").to_pylist()
        stop_arrivals = pq.read_table(work / "reconstruction_stop_arrivals.parquet").to_pylist()
        expected = pq.read_table(work / "reconstruction_expected_stop_events.parquet").to_pylist()

    assert operational
    assert passenger == []
    assert counts == {
        "reconstruction_trip_facts": 1,
        "reconstruction_stop_arrivals": 0,
        "reconstruction_expected_stop_events": 0,
    }
    assert trips[0]["trip_quality"] == "broken"
    assert trips[0]["service_observation_class"] == "matching_failure"
    assert "unsettled_passenger_boundaries" in trips[0]["quality_flags"]
    assert "unsettled_passenger_boundaries" in trips[0]["service_observation_flags"]
    assert stop_arrivals == []
    assert expected == []


@pytest.mark.parametrize(
    ("execution_status", "confidence", "duty_chain_source"),
    [("executed", "high", "line_brigade"), ("vehicle_change_signal", "low", "block_id")],
)
def test_fallback_and_ambiguous_ownership_cannot_create_facts(
    tmp_path: Path, execution_status: str, confidence: str, duty_chain_source: str
) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        execution_path = run._work() / "duty_execution.parquet"
        execution = pq.read_table(execution_path).to_pylist()
        execution[0]["execution_status"] = execution_status
        execution[0]["confidence"] = confidence
        execution[0]["duty_chain_source"] = duty_chain_source
        pq.write_table(pa.Table.from_pylist(execution, schema=DUTY_EXECUTION_SCHEMA), execution_path)
        run.align_stops()
        counts = run.build_facts()

    assert counts == {
        "reconstruction_trip_facts": 0,
        "reconstruction_stop_arrivals": 0,
        "reconstruction_expected_stop_events": 0,
    }


def test_duplicate_accepted_trip_grain_rejects_fact_construction(tmp_path: Path) -> None:
    zip_path, output = tmp_path / "snapshot.zip", tmp_path / "output"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        zip_path,
        output,
        output / "metrics.json",
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        _write_stop_alignment_fixture(run, ("1",))
        execution_path = run._work() / "duty_execution.parquet"
        execution = pq.read_table(execution_path)
        pq.write_table(pa.concat_tables([execution, execution]), execution_path)
        with pytest.raises(MatcherError, match="duplicate accepted trip grain"):
            run.build_facts()


def test_rejects_required_prior_service_and_bounded_group(tmp_path: Path) -> None:
    root, zip_path = tmp_path / "gps", tmp_path / "snapshot.zip"
    _gps(root, [_row(), _row(Time=datetime(2026, 1, 15, 0, 0, tzinfo=UTC))])
    _gtfs(zip_path, prior=False)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        tmp_path / "failed",
        tmp_path / "metrics.json",
        max_vehicle_rows=1,
        allow_missing_hours=True,
    )
    with pytest.raises(MatcherError, match="snapshot_mismatch"):
        with ReconstructionRun(config) as run:
            run.prepare()


def test_rejects_schema_drift(tmp_path: Path) -> None:
    root = tmp_path / "gps"
    path = root / "vehicle_type=bus" / "date=2026-01-15" / "hour=01"
    path.mkdir(parents=True)
    (path / "bad.parquet").write_bytes(b"not parquet")
    zip_path = tmp_path / "snapshot.zip"
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        tmp_path / "output",
        tmp_path / "metrics.json",
        allow_missing_hours=True,
    )
    with pytest.raises(MatcherError, match="invalid_data"):
        with ReconstructionRun(config) as run:
            run.prepare()


def test_rejects_a_vehicle_group_over_its_bound(tmp_path: Path) -> None:
    root, zip_path = tmp_path / "gps", tmp_path / "snapshot.zip"
    _gps(root, [_row(), _row(Time=datetime(2026, 1, 15, 0, 0, tzinfo=UTC))])
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        tmp_path / "output",
        tmp_path / "metrics.json",
        max_vehicle_rows=1,
        allow_missing_hours=True,
    )
    with pytest.raises(MatcherError, match="resource_limit"):
        with ReconstructionRun(config) as run:
            run.prepare()


def test_cli_uses_stable_missing_input_exit(tmp_path: Path) -> None:
    assert (
        main(
            [
                "prepare",
                "--processing-date",
                "2026-01-15",
                "--snapshot-id",
                "synthetic",
                "--gps-root",
                str(tmp_path / "missing"),
                "--gtfs-zip",
                str(tmp_path / "missing.zip"),
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )
        == 10
    )


def test_missing_hours_are_recorded_without_blocking_reconstruction(tmp_path: Path) -> None:
    root, zip_path = tmp_path / "gps", tmp_path / "snapshot.zip"
    _gps(root, [_row()])
    _gtfs(zip_path)
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        root,
        zip_path,
        tmp_path / "output",
        tmp_path / "metrics.json",
    )
    with ReconstructionRun(config) as run:
        result = run.prepare()
    assert result["manifest"]["missing_hours"]["tram"] == list(range(24))


def test_rejects_nonpositive_stop_alignment_workers(tmp_path: Path) -> None:
    config = RunConfig(
        date(2026, 1, 15),
        "synthetic",
        tmp_path / "gps",
        tmp_path / "snapshot.zip",
        tmp_path / "output",
        tmp_path / "metrics.json",
        alignment_workers=0,
    )
    with pytest.raises(MatcherError, match="alignment_workers must be positive"):
        with ReconstructionRun(config):
            pass


def test_rejects_noncanonical_snapshot_identity(tmp_path: Path) -> None:
    zip_path = tmp_path / "snapshot.zip"
    _gtfs(zip_path)
    with pytest.raises(MatcherError, match="canonical timestamp_hash"):
        load(zip_path, "wrong-snapshot")
