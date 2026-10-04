from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest
from conftest import SCORE_START, TRAIN_END, TRAIN_START, peak_factor

from ztm_planner import artifact, calendar, features, gtfs, lookup, weather
from ztm_planner.cli import main
from ztm_planner.db import one
from ztm_planner.settings import SHRINK, Resources

CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "planner_artifact_v1.json"


def _resources(tmp_path: Path) -> Resources:
    return Resources(threads=2, memory_limit="1GB", temp_dir=str(tmp_path / "spill"))


def test_calendar_matches_the_warehouse_rules() -> None:
    assert calendar.easter_sunday(2026) == date(2026, 4, 5)
    assert calendar.easter_sunday(2024) == date(2024, 3, 31)
    assert {date(2026, 5, 3), date(2026, 6, 4), date(2026, 4, 6)} <= calendar.holidays(
        2026
    )  # Corpus Christi, Easter Monday
    assert calendar.day_type(date(2026, 9, 26), holiday=False) == calendar.SATURDAY
    assert calendar.day_type(date(2026, 9, 23), holiday=True) == calendar.SUNDAY_HOLIDAY
    assert [calendar.time_band(h, weekday=True) for h in (2, 8, 12, 25)] == ["night", "peak", "other", "night"]
    assert calendar.time_band(8, weekday=False) == "other"


def test_lookup_shrinks_sparse_cells_and_new_posts_inherit_their_group(tmp_path: Path) -> None:
    con = duckdb.connect()
    features.register_holidays(con, date(2026, 9, 1), date(2026, 9, 30))
    con.execute(
        """create table raw as select * from (values
            (date '2026-09-21', 1::bigint, 'bus', '1', 0, '100101', '100201', 2, 1, false, false, 500., 28800, 60, 100),
            (date '2026-09-21', 2::bigint, 'bus', '1', 0, '100101', '100201', 2, 1, false, false, 500., 28800, 60, 80)
        ) t(service_date, trip_key, mode, line, direction_id, a_stop, b_stop, b_seq, pos, a_request, b_request, dist_m,
            a_sched_sod, sched_s, actual_s)"""
    )
    features.add_features(con, "raw", "seg")
    lookup.fit(con, "seg", "t_")
    # A new post (100102) of the same stop group, never observed.
    con.execute("create table target as select * replace ('100102' as a_stop, '100102>100201' as seg) from seg limit 1")
    lookup.apply(con, "target", "out", "t_")
    p0, p1, p2 = one(con, "select p0, p1, p2 from out")
    assert p0 == 90  # base mean
    assert p1 == p0  # group level adds the residual mean (0) shrunk by SHRINK
    assert p2 == p1  # unseen segment: no segment-level adjustment
    con.execute("create table seen as select * from seg limit 1")
    lookup.apply(con, "seen", "seen_out", "t_")
    assert one(con, "select n2 from seen_out")[0] == 2
    assert SHRINK > 0


def test_weather_features_roll_over_hours(tmp_path: Path) -> None:
    path = tmp_path / "w.json"
    times = [f"2026-09-01T{h:02d}:00" for h in range(6)]
    path.write_text(json.dumps({"hourly": {
        "time": times, "precipitation": [0, 0.5, 0, 0, 0, 0], "snowfall": [0] * 6,
        "temperature_2m": [0.5] * 6, "wind_speed_10m": [5] * 6,
    }}))  # fmt: skip
    con = duckdb.connect()
    weather.load(con, path)
    rows = con.execute("select precip_3h, hours_since_rain, freeze_risk from weather order by wx_ts").fetchall()
    assert [r[0] for r in rows] == [0, 0.5, 0.5, 0.5, 0, 0]
    assert [r[1] for r in rows] == [168, 0, 1, 2, 3, 4]
    assert all(r[2] == 1 for r in rows[1:])  # wet within 6 h and at most 1 °C


def test_gtfs_keeps_passenger_bus_and_tram_stops(tmp_path: Path, world: dict[str, Path]) -> None:
    con = duckdb.connect()
    gtfs.load_schedule(con, world["gtfs"], tmp_path, SCORE_START, SCORE_START)
    modes = {r[0] for r in con.execute("select distinct mode from sched_stop").fetchall()}
    assert modes == {"bus", "tram"}  # metro has no observations to learn from
    assert one(con, "select count(*) from sched_stop where stop_id = '999901'")[0] == 0
    assert one(con, "select bool_and(request) from sched_stop where stop_id = '100201'")[0]
    gtfs.segments(con)
    trips, segs = one(con, "select count(distinct trip_key), count(*) from sched_seg")
    assert (trips, segs) == (66, 33 * 3 + 33 * 2)
    assert one(con, "select min(sched_s), max(sched_s) from sched_seg where mode = 'bus'") == (240, 240)


def test_artifact_contract_matches_the_repository_contract() -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    expected = {
        table: tuple((f["name"], f["duckdb_type"], f["nullable"]) for f in fields)
        for table, fields in contract["tables"].items()
    }
    assert expected == artifact.CONTRACT
    assert artifact.search_key("  Łomianki  Kampinoska ") == "lomianki kampinoska"
    assert artifact.search_key("Żółkiewskiego") == "zolkiewskiego"


def test_artifact_refuses_nulls_where_the_frontend_expects_values(tmp_path: Path) -> None:
    con = duckdb.connect()
    sources = {
        table: "select " + ", ".join(f"null as {c}" for c, _, _ in cols) for table, cols in artifact.CONTRACT.items()
    }
    with pytest.raises(ValueError, match="nulls"):
        artifact.write(con, tmp_path / "planner.duckdb", sources)
    assert not (tmp_path / "planner.duckdb").exists()


@pytest.fixture(scope="module")
def trained(tmp_path_factory: pytest.TempPathFactory, world: dict[str, Path]) -> Path:
    root = tmp_path_factory.mktemp("train")
    main([
        "--workdir", str(root / "work"), "--threads", "2", "--memory-limit", "1GB", "--nice", "0", "train",
        "--segments", str(world["segments"]), "--weather-json", str(world["weather_archive"]),
        "--stop-slots", str(world["stop_slots"]), "--stop-eps", str(world["stop_eps"]),
        "--start", TRAIN_START.isoformat(), "--end", TRAIN_END.isoformat(), "--version", "test-1",
        "--bundle-out", str(root / "bundle"),
    ])  # fmt: skip
    return root / "bundle"


def test_training_writes_a_complete_bundle_that_beats_the_timetable(trained: Path) -> None:
    names = {p.name for p in trained.iterdir()}
    assert {"gbm.txt", "meta.json", "line_map.parquet", "ride_range.parquet", "stop_slots.parquet"} <= names
    assert {f"lookup_{t}.parquet" for t in lookup.TABLES} <= names
    meta = json.loads((trained / "meta.json").read_text())
    mae = meta["metrics"]["segment_mae_s"]
    assert mae["model"] < mae["timetable"]
    con = duckdb.connect()
    ranges = one(con, f"select count(*), bool_and(low_ratio < high_ratio) from '{trained / 'ride_range.parquet'}'")
    assert ranges == (2 * 2 * 24 * 8, True)  # complete mode x weekday x hour x bucket grid


def test_scoring_publishes_a_contract_artifact_with_learned_times(
    tmp_path: Path, world: dict[str, Path], trained: Path
) -> None:
    output = tmp_path / "serving" / "planner" / "planner.duckdb"
    main([
        "--workdir", str(tmp_path / "work"), "--threads", "2", "--memory-limit", "1GB", "--nice", "0", "score",
        "--bundle", str(trained), "--gtfs-zip", str(world["gtfs_latest"]), "--previous-gtfs-zip", str(world["gtfs"]),
        "--recent-daily", str(world["recent"]),
        "--weather-json", str(world["weather_forecast"]), "--start", SCORE_START.isoformat(), "--days", "7",
        "--output", str(output), "--build-id", "b-test",
    ])  # fmt: skip
    con = duckdb.connect(str(output), read_only=True)
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    for table, fields in contract["tables"].items():
        assert [r[0] for r in con.execute(f"describe {table}").fetchall()] == [f["name"] for f in fields]
    meta = one(con, "select first_date, last_date, model_version from planner_metadata")
    assert meta == (SCORE_START, SCORE_START + timedelta(days=6), "test-1")
    # The previous service date comes from the previous snapshot, for night trips after midnight.
    assert one(con, "select min(service_date) from planner_trip")[0] == SCORE_START - timedelta(days=1)
    assert one(con, "select count(*) from planner_stop where leave_by_offset_s > 0")[0] == 0
    last_stops = one(
        con,
        "select count(*), count(leave_by_offset_s) from planner_stop s "
        "where stop_sequence = (select max(stop_sequence) from planner_stop x where x.trip_key = s.trip_key)",
    )
    assert last_stops[0] > 0 and last_stops[1] == 0  # no boarding at the last stop
    # The line-specific slot wins over the generic one.
    assert con.execute("select distinct usual_delay_s from planner_stop where stop_id = '100301'").fetchall() == [
        (200,)
    ]
    groups = dict(con.execute("select stop_group_id, search_key from planner_stop_group").fetchall())
    assert groups["1001"] == "alpha"
    # Bus rides at peak take longer than off-peak, as in the observations (40% slower).
    peak, calm = (
        one(
            con,
            "select avg(ride) from (select s.trip_key, max(s.ride_from_start_s) as ride from planner_stop s "
            "join planner_trip t using (trip_key) where t.mode = 'bus' and isodow(t.service_date) <= 5 "
            f"group by s.trip_key having min(s.scheduled_sod) // 3600 {cond})",
        )[0]
        for cond in ("in (7, 15, 16)", "in (11, 12, 13)")
    )
    assert peak > calm * (1 + (peak_factor("bus", 8) - 1) / 2)
