from __future__ import annotations

import csv
import io
import zipfile
from datetime import date, timedelta
from pathlib import Path

from ztm_matcher.gtfs import load, select
from ztm_matcher.semantics import _depot, duties, stop_semantics


def _zip(path: Path, block: str = "block-a") -> None:
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
            ["200003", "C", "3", "52.202", "21.002", "1", "C", "Warszawa"],
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


def test_depot_name_parity_is_case_insensitive() -> None:
    assert _depot("r-4 zajezdnia Żoliborz")
    assert _depot("Metro Zajezdnia")
    assert not _depot("Zajezdniowa")
