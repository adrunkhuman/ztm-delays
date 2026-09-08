"""Dated loading must equal full-feed selection without allocating excluded stops."""

import csv
import io
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pytest
from test_gtfs_semantics import _zip

from ztm_matcher import gtfs
from ztm_matcher.errors import MatcherError
from ztm_matcher.semantics import duties, stop_semantics


def _feed(path: Path, day: date, schedules: list[tuple[str, str, str, str]]) -> None:
    _zip(path)
    with zipfile.ZipFile(path) as archive:
        tables = {name: list(csv.reader(io.StringIO(archive.read(name).decode()))) for name in archive.namelist()}
    tables["trips.txt"] = tables["trips.txt"][:1]
    tables["stop_times.txt"] = tables["stop_times.txt"][:1]
    tables["calendar_dates.txt"] = [
        tables["calendar_dates.txt"][0],
        ["prior", str(day - timedelta(days=1)), "1"],
        ["current", str(day), "1"],
        ["both", str(day - timedelta(days=1)), "1"],
        ["both", str(day), "1"],
    ]
    for trip, service, arrival, departure in schedules:
        tables["trips.txt"].append([trip, "r", service, trip, "0", "block", "7", "s"])
        # Reverse sequence order and put extrema in different time fields.
        tables["stop_times.txt"].extend(
            [
                [trip, "200002", "2", departure, arrival, "0", "0"],
                [trip, "200001", "1", arrival, arrival, "0", "0"],
            ]
        )
    with zipfile.ZipFile(path, "w") as archive:
        for name, rows in tables.items():
            stream = io.StringIO()
            csv.writer(stream).writerows(rows)
            archive.writestr(name, stream.getvalue())


@pytest.mark.parametrize("day", [date(2026, 1, 15), date(2026, 3, 29), date(2026, 10, 25)])
def test_dated_load_matches_full_selection_and_semantics(tmp_path: Path, day: date) -> None:
    path = tmp_path / "feed.zip"
    _feed(
        path,
        day,
        [
            ("prior-out", "prior", "23:59:59", "23:59:59"),
            ("touch-start", "prior", "23:59:59", "24:00:00"),
            ("current-start", "current", "00:00:00", "00:00:00"),
            ("touch-end", "current", "24:00:00", "24:00:00"),
            ("both", "both", "02:30:00", "25:00:00"),
            ("long", "prior", "48:00:00", "49:00:00"),
        ],
    )
    full = gtfs.load(path, "synthetic")
    bounded = gtfs.load(path, "synthetic", day)
    expected = gtfs.select(full, day)
    assert gtfs.select(bounded, day) == expected
    assert set(bounded.stop_times) == {"touch-start", "current-start", "both"}
    assert sum(row["trip_id"] == "both" for row in expected) == 2
    assert bounded.stop_times == {key: full.stop_times[key] for key in bounded.stop_times}
    assert stop_semantics(duties(expected, bounded), bounded) == stop_semantics(duties(expected, full), full)


def test_cap_counts_only_materialized_rows_and_is_inclusive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "feed.zip"
    day = date(2026, 1, 15)
    _feed(
        path,
        day,
        [
            ("out", "prior", "01:00:00", "02:00:00"),
            ("a", "current", "01:00:00", "02:00:00"),
            ("b", "current", "03:00:00", "04:00:00"),
        ],
    )
    monkeypatch.setattr(gtfs, "MAX_GTFS_ROWS", 4)
    original = gtfs.StopTime
    allocations = []

    def counted(**kwargs):
        allocations.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(gtfs, "StopTime", counted)
    assert sum(map(len, gtfs.load(path, "synthetic", day).stop_times.values())) == 4
    assert len(allocations) == 4
    with pytest.raises(MatcherError, match="resource_limit.*stop rows"):
        gtfs.load(path, "synthetic")
    _feed(path, day, [(str(i), "current", "01:00:00", "02:00:00") for i in range(3)])
    with pytest.raises(MatcherError, match="resource_limit.*stop rows"):
        gtfs.load(path, "synthetic", day)


@pytest.mark.parametrize(
    "column,value",
    [
        ("arrival_time", "bad"),
        ("departure_time", "00:60:00"),
        ("stop_sequence", "bad"),
        ("pickup_type", "bad"),
        ("drop_off_type", "bad"),
    ],
)
def test_excluded_active_rows_still_validate(tmp_path: Path, column: str, value: str) -> None:
    path = tmp_path / "feed.zip"
    day = date(2026, 1, 15)
    _feed(path, day, [("out", "prior", "01:00:00", "02:00:00")])
    with zipfile.ZipFile(path) as archive:
        members = {name: archive.read(name) for name in archive.namelist()}
    rows = list(csv.reader(io.StringIO(members["stop_times.txt"].decode())))
    rows[1][rows[0].index(column)] = value
    stream = io.StringIO()
    csv.writer(stream).writerows(rows)
    members["stop_times.txt"] = stream.getvalue().encode()
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    with pytest.raises(MatcherError, match="invalid_data"):
        gtfs.load(path, "synthetic", day)


def test_no_overlap_is_not_invalid_empty_feed(tmp_path: Path) -> None:
    path = tmp_path / "feed.zip"
    day = date(2026, 1, 15)
    _feed(path, day, [("out", "prior", "01:00:00", "02:00:00")])
    assert gtfs.select(gtfs.load(path, "synthetic", day), day) == []
    _feed(path, day, [])
    with pytest.raises(MatcherError, match="invalid_data"):
        gtfs.load(path, "synthetic", day)


def test_archive_guard_precedes_streaming(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "feed.zip"
    _zip(path)
    monkeypatch.setattr(gtfs, "MAX_GTFS_UNCOMPRESSED_BYTES", 1)
    with pytest.raises(MatcherError, match="resource_limit.*uncompressed"):
        gtfs.load(path, "synthetic", date(2026, 1, 15))


@pytest.mark.parametrize("missing_member", [False, True])
def test_stop_times_input_gates(tmp_path: Path, missing_member: bool) -> None:
    path = tmp_path / "feed.zip"
    _zip(path)
    with zipfile.ZipFile(path) as archive:
        members = {name: archive.read(name) for name in archive.namelist() if name != "stop_times.txt"}
    if not missing_member:
        members["stop_times.txt"] = b"trip_id,stop_id\none,200001\n"
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    with pytest.raises(MatcherError, match="missing_input" if missing_member else "schema_drift"):
        gtfs.load(path, "synthetic", date(2026, 1, 15))


def test_missing_prior_service_still_fails_selection(tmp_path: Path) -> None:
    path = tmp_path / "feed.zip"
    _zip(path)
    day = date(2026, 1, 14)
    snapshot = gtfs.load(path, "synthetic", day)
    with pytest.raises(MatcherError, match="snapshot_mismatch.*2026-01-13"):
        gtfs.select(snapshot, day)
