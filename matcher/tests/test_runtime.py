from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ztm_matcher.runtime as runtime
from ztm_matcher import ReconstructionRun, RunConfig
from ztm_matcher.cli import main
from ztm_matcher.errors import MatcherError
from ztm_matcher.gtfs import load
from ztm_matcher.runtime import STOP_ALIGNMENT_ROW_GROUP_ROWS, _flush_stop_alignment_rows
from ztm_matcher.schemas import (
    DUTY_EXECUTION_SCHEMA,
    NORMALIZED_GPS_SCHEMA,
    PASSENGER_STOP_ARRIVAL_SCHEMA,
    RAW_GPS_SCHEMA,
    STOP_CROSSING_SCHEMA,
)


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
    assert "vehicle_number in (select unnest(?))" in normalized_queries[0].lower()


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
        with pytest.raises(MatcherError, match="vehicle 1 exceeds max_vehicle_rows"):
            run.align_stops()


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


def test_missing_hours_fail_without_explicit_partial_day_opt_in(tmp_path: Path) -> None:
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
    with pytest.raises(MatcherError, match="missing hourly partitions"):
        with ReconstructionRun(config) as run:
            run.prepare()


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
