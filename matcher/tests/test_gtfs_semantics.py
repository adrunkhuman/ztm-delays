from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ztm_matcher.gtfs import Snapshot, StopTime, Trip, load, select
from ztm_matcher.semantics import _is_technical_trip, duties, stop_semantics


def _zip(path: Path, block: str = "block-a", *, include_zone_id: bool = True, handoff_stop_lat: str = "52.202") -> None:
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
            ["one", "r", "svc", "One", "0", block, "0007", "s"],
            ["two", "r", "svc", "Two", "0", block, "0007", "s"],
        ],
        "stop_times.txt": [
            ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "pickup_type", "drop_off_type"],
            ["one", "200001", "1", "01:00:00", "01:00:00", "0", "0"],
            ["one", "200002", "2", "01:05:00", "01:05:00", "0", "0"],
            ["two", "200002", "1", "01:05:00", "01:05:00", "0", "0"],
            ["two", "200003", "2", "01:10:00", "01:10:00", "0", "0"],
        ],
        "stops.txt": [
            ["stop_id", "stop_name", "stop_code", "stop_lat", "stop_lon", "zone_id", "stop_name_stem", "town_name"],
            ["200001", "A", "1", "52.20", "21.00", "1", "A", "Warszawa"],
            ["200002", "B", "2", "52.201", "21.001", "1", "B", "Warszawa"],
            ["200003", "C", "3", handoff_stop_lat, "21.002", "1", "C", "Warszawa"],
        ],
        "shapes.txt": [["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"], ["s", "52.2", "21", "1"]],
        "routes.txt": [["route_id", "route_short_name", "route_type"], ["r", "1", "3"]],
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


def test_exact_block_handoff_is_technical_and_fallback_is_unknown(tmp_path: Path) -> None:
    path = tmp_path / "schedule.zip"
    _zip(path)
    snapshot = load(path, "synthetic")
    duty_rows = duties(select(snapshot, date(2026, 1, 15)), snapshot)
    rows = stop_semantics(duty_rows, snapshot)
    assert (
        next(row for row in rows if row["trip_id"] == "two" and row["stop_sequence"] == 1)["stop_execution_class"]
        == "technical_prefix"
    )
    _zip(path, block="")
    snapshot = load(path, "synthetic")
    fallback_rows = stop_semantics(duties(select(snapshot, date(2026, 1, 15)), snapshot), snapshot)
    boundary = next(row for row in fallback_rows if row["trip_id"] == "two" and row["stop_sequence"] == 1)
    assert boundary["stop_execution_class"] == "unknown"
    assert not boundary["is_passenger_stop"]


def test_same_group_terminal_handoff_is_not_limited_by_distance(tmp_path: Path) -> None:
    path = tmp_path / "schedule.zip"
    _zip(path, handoff_stop_lat="52.212")
    snapshot = load(path, "synthetic")
    duty_rows = duties(select(snapshot, date(2026, 1, 15)), snapshot)

    boundary = next(
        row for row in stop_semantics(duty_rows, snapshot) if row["trip_id"] == "two" and row["stop_sequence"] == 1
    )
    assert boundary["stop_execution_class"] == "technical_prefix"


def test_stop_times_are_compact_records_indexed_by_trip(tmp_path: Path) -> None:
    path = tmp_path / "schedule.zip"
    _zip(path)
    snapshot = load(path, "synthetic")

    assert set(snapshot.stop_times) == {"one", "two"}
    assert all(isinstance(row, StopTime) for rows in snapshot.stop_times.values() for row in rows)
    assert not hasattr(snapshot.stop_times["one"][0], "__dict__")
    assert snapshot.stop_times["one"][0].stop_id == "200001"


def test_missing_stop_zone_is_retained_as_unknown_for_conservative_classification(tmp_path: Path) -> None:
    path = tmp_path / "schedule.zip"
    _zip(path, include_zone_id=False)

    snapshot = load(path, "synthetic")
    rows = stop_semantics(duties(select(snapshot, date(2026, 1, 15)), snapshot), snapshot)

    assert snapshot.stops["200001"].zone_id is None
    assert snapshot.stops["200001"].effective_zone_id is None
    assert all(row["effective_zone_id"] is None for row in rows)


def test_same_trip_ids_remain_distinct_across_service_dates(tmp_path: Path) -> None:
    path = tmp_path / "schedule.zip"
    _zip(path)
    snapshot = load(path, "synthetic")
    duty_rows = duties(select(snapshot, date(2026, 1, 15)), snapshot)
    duty_rows.extend(
        {
            **row,
            "service_date": row["service_date"] - timedelta(days=1),
            "duty_chain_id": f"prior-{row['duty_chain_id']}",
        }
        for row in list(duty_rows)
    )
    rows = stop_semantics(duty_rows, snapshot)
    keys = {(row["service_date"], row["trip_id"], row["stop_sequence"]) for row in rows}
    assert len(keys) == len(rows)
    assert {row["service_date"] for row in rows} == {date(2026, 1, 14), date(2026, 1, 15)}


def test_select_uses_shared_warsaw_wall_clock_policy_across_dst() -> None:
    trip = Trip("dst", "r", "svc", "DST", 0, "block", "1", "1", "s")
    stop_times = {
        "dst": [
            StopTime("a", 1, 2 * 3600 + 30 * 60, 2 * 3600 + 30 * 60, 0, 0, "regular"),
            StopTime("b", 2, 25 * 3600, 25 * 3600, 0, 0, "regular"),
        ]
    }
    for processing_date, expected_start, expected_end in (
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
    ):
        snapshot = Snapshot(
            "synthetic",
            "hash",
            [trip],
            stop_times,
            {},
            {"r": {"route_short_name": "1", "route_type": 3}},
            {("svc", processing_date - timedelta(days=1)), ("svc", processing_date)},
        )

        selected = select(snapshot, processing_date)

        current = next(row for row in selected if row["service_date"] == processing_date)
        assert current["scheduled_start_time"] == expected_start
        assert current["scheduled_end_time"] == expected_end


def test_only_trips_without_passenger_eligible_stops_are_technical() -> None:
    assert _is_technical_trip(0)
    assert not _is_technical_trip(1)
    assert not _is_technical_trip(25)
