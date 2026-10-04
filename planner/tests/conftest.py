"""Synthetic network: a bus line and a tram line with known peak slowdowns, plus metro and SKM on their timetable.

Metro platforms sit next to the bus and tram stops of the same group (as in the Warsaw feed); the metro runs from
one frequency template.
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ztm_planner.bundle import STOP_EPS_COLUMNS, STOP_SLOT_COLUMNS
from ztm_planner.settings import STOP_EPS_GRID

TRAIN_START, TRAIN_END = date(2026, 8, 3), date(2026, 9, 6)  # five full weeks
SCORE_START = date(2026, 9, 7)
STOPS = {  # stop_id: (name, lat, lon)
    "100101": ("Alpha", 52.20, 21.00),
    "100201": ("Bravo", 52.21, 21.01),
    "100301": ("Charlie", 52.22, 21.02),
    "100401": ("Delta", 52.23, 21.03),
    "200101": ("Echo", 52.24, 21.04),
    "200201": ("Foxtrot", 52.25, 21.05),
    "200301": ("Golf", 52.26, 21.06),
    "999901": ("Depot", 52.27, 21.07),
    "1001M:P1": ("Alpha", 52.2005, 21.0005),  # ~65 m from Alpha's bus post
    "2001M:P1": ("Echo", 52.2405, 21.0405),
    "4900": ("Bravo PKP", 52.2105, 21.0105),
    "4901": ("Golf PKP", 52.2605, 21.0605),
}
ROUTES = {  # route_id: (route_type, stops in order, scheduled minutes between stops)
    "110": ("3", ["100101", "100201", "100301", "100401"], 4),
    "17": ("0", ["200101", "200201", "200301"], 3),
    "M1": ("1", ["1001M:P1", "2001M:P1"], 5),
    "S1": ("2", ["4900", "4901"], 9),
}
METRO_FREQUENCIES = [("06:00:00", "09:00:00", 600), ("09:00:00", "10:00:00", 900)]  # 18 + 4 runs a day
METRO_RUNS = 22


def departures() -> list[int]:
    """Seconds after midnight: every 30 min from 06:00 to 22:00."""
    return [6 * 3600 + 1800 * i for i in range(33)]


def peak_factor(mode: str, hour: int) -> float:
    """True slowdown used to generate observations: buses at peak are 40% slower."""
    return 1.4 if mode == "bus" and hour in {7, 8, 15, 16, 17} else 1.0


def _hms(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def write_gtfs(
    path: Path, start: date, days: int, stop_types: dict[str, tuple[str, str]] | None = None
) -> None:
    files: dict[str, list[list[str]]] = {
        "routes.txt": [["route_id", "route_short_name", "route_type"]],
        "trips.txt": [["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"]],
        "stop_times.txt": [
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type"]
        ],
        "stops.txt": [["stop_id", "stop_name", "stop_lat", "stop_lon"]],
        "calendar_dates.txt": [["service_id", "date", "exception_type"]],
        "frequencies.txt": [["trip_id", "start_time", "end_time", "headway_secs", "exact_times"]],
    }
    for stop_id, (name, lat, lon) in STOPS.items():
        files["stops.txt"].append([stop_id, name, str(lat), str(lon)])
    for d in range(days):
        files["calendar_dates.txt"].append(["S", (start + timedelta(days=d)).strftime("%Y%m%d"), "1"])
    for route, (route_type, stops, minutes) in ROUTES.items():
        files["routes.txt"].append([route, route, route_type])
        if route_type == "1":
            files["trips.txt"].append([route, "S", "M1:T", STOPS[stops[-1]][0], "0"])
            for seq, stop in enumerate(stops):
                t = seq * minutes * 60
                pickup, dropoff = (stop_types or {}).get(stop, ("0", "0"))
                files["stop_times.txt"].append(["M1:T", _hms(t), _hms(t), stop, str(seq), pickup, dropoff])
            for start_time, end_time, headway in METRO_FREQUENCIES:
                files["frequencies.txt"].append(["M1:T", start_time, end_time, str(headway), "0"])
            continue
        for dep in departures():
            trip = f"{route}:{dep}"
            files["trips.txt"].append([route, "S", trip, STOPS[stops[-1]][0], "0"])
            for seq, stop in enumerate(stops):
                t = dep + seq * minutes * 60
                pickup, dropoff = (stop_types or {}).get(stop, ("3" if stop == "100201" else "0", "0"))
                files["stop_times.txt"].append([trip, _hms(t), _hms(t), stop, str(seq + 1), pickup, dropoff])
            # technical run to the depot: not in passenger service, must be skipped
            t = dep + len(stops) * minutes * 60
            files["stop_times.txt"].append([trip, _hms(t), _hms(t), "999901", str(len(stops) + 1), "1", "1"])
    with zipfile.ZipFile(path, "w") as archive:
        for name, rows in files.items():
            buffer = io.StringIO()
            buffer.write("\n".join(",".join(row) for row in rows) + "\n")
            archive.writestr(name, buffer.getvalue())


def write_segments(path: Path, start: date, end: date, seed: int = 0) -> None:
    """Observed segments with the SEGMENT_COLUMNS layout plus actual_s."""
    rng = np.random.default_rng(seed)
    cols: dict[str, list] = {k: [] for k in (
        "service_date", "trip_key", "mode", "line", "direction_id", "a_stop", "b_stop", "b_seq", "pos",
        "a_request", "b_request", "dist_m", "a_sched_sod", "sched_s", "actual_s",
    )}  # fmt: skip
    day = start
    while day <= end:
        for route, (route_type, stops, minutes) in ROUTES.items():
            if route_type in {"1", "2"}:
                continue
            mode = "tram" if route_type == "0" else "bus"
            for dep in departures():
                key = hash((day, route, dep)) & 0x7FFFFFFFFFFFFFFF
                for seq in range(1, len(stops)):
                    a_sod = dep + (seq - 1) * minutes * 60
                    sched = minutes * 60
                    actual = sched * peak_factor(mode, a_sod // 3600) + rng.normal(0, 8)
                    for name, value in zip(cols, (
                        day, key, mode, route, 0, stops[seq - 1], stops[seq], seq + 1, seq, stops[seq - 1] == "100201",
                        stops[seq] == "100201", 1400.0, a_sod, sched, round(max(actual, 30)),
                    ), strict=True):  # fmt: skip
                        cols[name].append(value)
        day += timedelta(days=1)
    pq.write_table(pa.table(cols), path)


def write_weather(path: Path, start: date, end: date) -> None:
    hours = (
        int(
            (datetime.combine(end, datetime.min.time()) - datetime.combine(start, datetime.min.time())).total_seconds()
            // 3600
        )
        + 48
    )
    times = [datetime.combine(start, datetime.min.time()) + timedelta(hours=h) for h in range(hours)]
    path.write_text(json.dumps({"hourly": {
        "time": [t.strftime("%Y-%m-%dT%H:%M") for t in times],
        "precipitation": [1.2 if t.hour == 14 else 0.0 for t in times],
        "snowfall": [0.0] * hours,
        "temperature_2m": [12.0] * hours,
        "wind_speed_10m": [9.0] * hours,
    }}))  # fmt: skip


def write_stop_tables(slots: Path, eps: Path) -> None:
    rows = []

    def row(level: str, **keys) -> dict:
        base = {c: None for c in STOP_SLOT_COLUMNS}
        base.update(
            level=level,
            n=500,
            dq10=-20.0,
            dq50=40.0,
            dq90=150.0,
            **{f"e{i}": -90.0 + 8 * i for i in range(len(STOP_EPS_GRID))},
        )
        base.update(keys)
        return base

    for is_tram in (True, False):
        for is_origin in (True, False):
            for rel_b in range(10):
                for daytype in range(3):
                    for hb in range(5):
                        rows.append(
                            row("generic", is_tram=is_tram, is_origin=is_origin, rel_b=rel_b, daytype=daytype, hb=hb)
                        )
    # A line-specific slot for Charlie on the 110 overrides the generic delays.
    rows.append(row("line_stop", line="110", direction_id=0, stop_id="100301", dq50=200.0, dq90=420.0))
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([
        ("level", pa.string()), ("line", pa.string()), ("direction_id", pa.int64()), ("stop_id", pa.string()),
        ("daytype", pa.int64()), ("hr", pa.int64()), ("hb", pa.int64()), ("is_tram", pa.bool_()),
        ("is_origin", pa.bool_()), ("rel_b", pa.int64()), ("n", pa.int64()), ("dq10", pa.float64()),
        ("dq50", pa.float64()), ("dq90", pa.float64()), *((f"e{i}", pa.float64()) for i in range(len(STOP_EPS_GRID))),
    ])), slots)  # fmt: skip
    eps_rows = [{"is_tram": t, "band": b, "eps_index": 5} for t in (True, False) for b in ("night", "peak", "other")]
    pq.write_table(pa.Table.from_pylist(eps_rows), eps)
    assert set(STOP_EPS_COLUMNS) <= set(eps_rows[0])


@pytest.fixture(scope="session")
def world(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("world")
    paths = {
        "gtfs": root / "gtfs.zip",
        "gtfs_latest": root / "gtfs_latest.zip",
        "segments": root / "segments.parquet",
        "weather_archive": root / "archive.json",
        "weather_forecast": root / "forecast.json",
        "stop_slots": root / "stop_slots.parquet",
        "stop_eps": root / "stop_eps.parquet",
        "recent": root / "recent.parquet",
    }
    write_gtfs(paths["gtfs"], SCORE_START - timedelta(days=1), 9)
    write_gtfs(paths["gtfs_latest"], SCORE_START, 8)  # taken today: no longer lists yesterday
    write_segments(paths["segments"], TRAIN_START, TRAIN_END)
    write_weather(paths["weather_archive"], TRAIN_START, TRAIN_END)
    write_weather(paths["weather_forecast"], SCORE_START - timedelta(days=2), SCORE_START + timedelta(days=8))
    write_stop_tables(paths["stop_slots"], paths["stop_eps"])
    pq.write_table(pa.table({"seg": ["100101>100201"], "hb": pa.array([1], pa.int8()), "service_date": [TRAIN_END],
                             "s": [300.0], "n": [1]}), paths["recent"])  # fmt: skip
    return paths
