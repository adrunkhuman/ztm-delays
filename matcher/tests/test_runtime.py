from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ztm_matcher import ReconstructionRun, RunConfig
from ztm_matcher.cli import main
from ztm_matcher.errors import MatcherError
from ztm_matcher.gtfs import load
from ztm_matcher.schemas import DUTY_EXECUTION_SCHEMA, RAW_GPS_SCHEMA


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
    assert result["metrics"]["duty_execution_rows"] == 2
    execution = pq.read_table(output / "duty_execution.parquet")
    assert execution.schema == DUTY_EXECUTION_SCHEMA
    assert execution.num_rows == 2
    assert all(
        row["ownership_interval_start_time"] is None
        or row["ownership_interval_start_time"] <= row["ownership_interval_end_time"]
        for row in execution.to_pylist()
    )
    semantics = pq.read_table(output / "stop_semantics.parquet").to_pylist()
    assert any(row["stop_execution_class"] == "technical_suffix" for row in semantics)


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


def test_rejects_noncanonical_snapshot_identity(tmp_path: Path) -> None:
    zip_path = tmp_path / "snapshot.zip"
    _gtfs(zip_path)
    with pytest.raises(MatcherError, match="canonical timestamp_hash"):
        load(zip_path, "wrong-snapshot")
