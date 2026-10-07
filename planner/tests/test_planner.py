from __future__ import annotations

import json
import math
import zipfile
from datetime import date, timedelta
from itertools import pairwise
from pathlib import Path

import conftest
import duckdb
import osmium
import osmium.osm.mutable
import pytest
from conftest import METRO_RUNS, SCORE_START, STOPS, TRAIN_END, TRAIN_START, peak_factor, write_gtfs

from ztm_planner import artifact, calendar, features, footpaths, gtfs, lookup, score, weather
from ztm_planner.cli import main
from ztm_planner.db import one
from ztm_planner.settings import RAIL_LATE_S, SHRINK, STATION_ACCESS_S, WALK_SPEED_MPS, Resources

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


def test_gtfs_splits_modelled_and_timetable_modes(tmp_path: Path, world: dict[str, Path]) -> None:
    con = duckdb.connect()
    gtfs.load_schedule(con, world["gtfs"], tmp_path, SCORE_START, SCORE_START)
    modes = {r[0] for r in con.execute("select distinct mode from sched_stop").fetchall()}
    assert modes == {"bus", "tram"}  # metro and SKM have no observations to learn from
    fixed = dict(con.execute("select mode, count(distinct trip_key) from sched_fixed group by mode").fetchall())
    assert fixed == {"metro": METRO_RUNS, "rail": 33}
    # Frequency runs: the template's times shifted to each start, every headway until end_time.
    starts = [r[0] for r in con.execute(
        "select scheduled_sod from sched_fixed where mode = 'metro' and stop_id = '1001M:P1' order by 1"
    ).fetchall()]  # fmt: skip
    assert starts[:2] == [6 * 3600, 6 * 3600 + 600] and starts[-1] == 9 * 3600 + 45 * 60
    assert one(con, "select max(scheduled_sod) from sched_fixed where stop_id = '2001M:P1'")[0] == 9 * 3600 + 50 * 60
    assert one(con, "select count(*) from sched_stop where stop_id = '999901'")[0] == 0
    assert one(con, "select bool_and(request) from sched_stop where stop_id = '100201'")[0]
    gtfs.segments(con)
    trips, segs = one(con, "select count(distinct trip_key), count(*) from sched_seg")
    assert (trips, segs) == (66, 33 * 3 + 33 * 2)
    assert one(con, "select min(sched_s), max(sched_s) from sched_seg where mode = 'bus'") == (240, 240)


def test_footpaths_follow_osm_paths_and_mark_covered_posts(tmp_path: Path, world: dict[str, Path]) -> None:
    # An L-shaped footway from Alpha's bus post via a corner to its metro platform; a motorway (not walkable)
    # cuts the corner. Other posts are too far from any path to snap.
    pbf = tmp_path / "paths.osm.pbf"
    (alpha_lat, alpha_lon), (platform_lat, platform_lon) = STOPS["100101"][1:], STOPS["1001M:P1"][1:]
    with osmium.SimpleWriter(str(pbf)) as writer:
        for node_id, (lat, lon) in enumerate(
            [(alpha_lat, alpha_lon), (platform_lat, alpha_lon), (platform_lat, platform_lon)], start=1
        ):
            writer.add_node(osmium.osm.mutable.Node(id=node_id, location=(lon, lat), version=1))
        writer.add_way(osmium.osm.mutable.Way(id=1, nodes=[1, 2, 3], tags={"highway": "footway"}, version=1))
        writer.add_way(osmium.osm.mutable.Way(id=2, nodes=[1, 3], tags={"highway": "motorway"}, version=1))
    output = tmp_path / "footpaths.parquet"
    footpaths.build(pbf, world["gtfs"], output, min_component_nodes=1)
    rows = {(a, b): d for a, b, d in duckdb.sql(f"select * from '{output}'").fetchall()}
    leg_lat, leg_lon = 0.0005 * 111_195, 0.0005 * 111_195 * math.cos(math.radians(alpha_lat))
    assert rows[("100101", "1001M:P1")] == rows[("1001M:P1", "100101")] == round(leg_lat + leg_lon)
    assert rows[("100101", "100101")] == 0  # covered marker
    assert {a for a, _ in rows} == {"100101", "1001M:P1"}


def test_artifact_contract_matches_the_repository_contract() -> None:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    expected = {
        table: tuple((f["name"], f["duckdb_type"], f["nullable"]) for f in fields)
        for table, fields in contract["tables"].items()
    }
    assert expected == artifact.CONTRACT
    assert {table: tuple(key) for table, key in contract["keys"].items()} == artifact.KEYS
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


def test_artifact_refuses_a_trip_key_shared_by_two_trips(tmp_path: Path) -> None:
    literal = {"VARCHAR[]": "['x']", "VARCHAR": "'x'", "TIMESTAMP": "now()", "DATE": "current_date", "BOOLEAN": "true"}
    sources = {
        table: "select " + ", ".join(f"{literal.get(kind, '1')} as {c}" for c, kind, _ in cols)
        for table, cols in artifact.CONTRACT.items()
    }
    sources["planner_trip"] += " union all " + sources["planner_trip"]
    with pytest.raises(ValueError, match="duplicated"):
        artifact.write(duckdb.connect(), tmp_path / "planner.duckdb", sources)
    assert not (tmp_path / "planner.duckdb").exists()


def test_gtfs_trip_keys_tell_apart_trips_whose_ids_differ_in_two_digits(tmp_path: Path) -> None:
    # DuckDB's hash() gave these two trips of the 2026-10-05 Warsaw feed the same key.
    ids = ["2026-10-06:10:PcS:017:2319", "2026-10-06:19:PcS:018:2319"]
    files = {
        "routes.txt": "route_id,route_type\n10,0\n19,0\n",
        "trips.txt": "route_id,service_id,trip_id,trip_headsign,direction_id\n"
        + "".join(f"{i[11:13]},PcS,{i},B,0\n" for i in ids),
        "stop_times.txt": "trip_id,arrival_time,stop_id,stop_sequence,pickup_type,drop_off_type\n"
        + "".join(f"{i},23:19:00,A,0,0,0\n{i},23:21:00,B,1,0,0\n" for i in ids),
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nA,A,52.2,21.0\nB,B,52.21,21.0\n",
        "calendar_dates.txt": "service_id,date,exception_type\nPcS,20261006,1\n",
    }
    gtfs_zip = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(gtfs_zip, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    con = duckdb.connect()
    gtfs.load_schedule(con, gtfs_zip, tmp_path, date(2026, 10, 6), date(2026, 10, 6))
    assert one(con, "select count(distinct trip_key), count(distinct line) from sched_stop") == (2, 2)


def test_gtfs_keeps_duties_brigades_and_shapes_for_live_positions(tmp_path: Path) -> None:
    files = {
        "routes.txt": "route_id,route_type\n175,3\n",
        "trips.txt": "route_id,service_id,trip_id,trip_headsign,direction_id,block_id,block_short_name,shape_id\n"
        "175,S,T1,B,0,D9,007,SH\n175,S,T2,A,1,D9,007,\n",
        "stop_times.txt": "trip_id,arrival_time,stop_id,stop_sequence,pickup_type,drop_off_type,shape_dist_traveled\n"
        "T1,10:00:00,A,0,0,0,0\nT1,10:02:00,B,1,0,0,0.75\nT2,10:10:00,B,0,0,0,\nT2,10:12:00,A,1,0,0,\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nA,A,52.2,21.0\nB,B,52.21,21.0\n",
        "calendar_dates.txt": "service_id,date,exception_type\nS,20261006,1\n",
        "shapes.txt": "shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,shape_dist_traveled\n"
        "SH,1,52.205,21.001,0.4\nSH,0,52.2,21.0,0\nSH,2,52.21,21.0,0.75\nUNUSED,0,50.0,20.0,0\n",
    }
    gtfs_zip = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(gtfs_zip, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    con = duckdb.connect()
    gtfs.load_schedule(con, gtfs_zip, tmp_path, date(2026, 10, 6), date(2026, 10, 6), shape_prefix="p:")

    rows = con.execute(
        "select duty_id, brigade, shape_id, shape_dist_m from sched_stop order by scheduled_sod"
    ).fetchall()
    # The GPS feed's brigade drops leading zeros, as in the matcher.
    assert rows == [
        ("D9", "7", "p:SH", 0),
        ("D9", "7", "p:SH", 750),
        ("D9", "7", None, None),
        ("D9", "7", None, None),
    ]
    assert con.execute("select * from sched_shape").fetchall() == [
        ("p:SH", [52.2, 52.205, 52.21], [21.0, 21.001, 21.0], [0, 400, 750])
    ]


def test_yesterdays_trips_reuse_identical_shapes_that_today_numbers_differently() -> None:
    con = duckdb.connect()
    con.execute(
        "create table sched_shape as select * from (values ('p:1', [1.0], [2.0], [0]), ('p:2', [5.0], [5.0], [0]),"
        " ('9', [1.0], [2.0], [0]), ('1', [3.0], [3.0], [0])) t(shape_id, lat, lon, dist_m)"
    )
    con.execute("create table sched_stop as select * from (values ('p:1'), ('p:2'), ('9')) t(shape_id)")
    con.execute("create table sched_fixed as select * from sched_stop where false")

    score._merge_previous_shapes(con)

    assert con.execute("select shape_id from sched_stop order by all").fetchall() == [("9",), ("9",), ("p:2",)]
    assert con.execute("select shape_id from sched_shape order by all").fetchall() == [("1",), ("9",), ("p:2",)]


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


def test_gtfs_alighting_restrictions_survive_scoring(
    tmp_path: Path, world: dict[str, Path], trained: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The last platform is near the origin, so the synthetic artifact also has a valid footpath.
    stops = ["100101", "100201", "100301", "100401", "200101", "200201", "1001M:P1"]
    monkeypatch.setattr(
        conftest,
        "ROUTES",
        {line: (kind, stops, 4) for line, kind in (("110", "3"), ("17", "0"), ("M1", "1"), ("S1", "2"))},
    )
    timetable = tmp_path / "restricted.zip"
    write_gtfs(
        timetable,
        SCORE_START,
        1,
        stop_types={
            "100201": ("0", "1"),  # pickup-only intermediate stop: keep boarding and through-riding
            "100301": ("1", "0"),  # drop-off-only stop: keep alighting
            "100401": ("2", "2"),  # coordination remains supported
            "200101": ("3", "3"),  # request stop remains supported
            "200201": ("", ""),  # blanks mean normal passenger service
        },
    )
    with duckdb.connect() as con:
        gtfs.load_schedule(con, timetable, tmp_path / "schedule", SCORE_START, SCORE_START)
        for table, modes in (("sched_stop", {"bus", "tram"}), ("sched_fixed", {"metro", "rail"})):
            assert {r[0] for r in con.execute(f"select distinct mode from {table}").fetchall()} == modes
            assert one(con, f"select bool_and(no_dropoff and not no_pickup) from {table} where stop_id = '100201'")[0]
            assert one(con, f"select count(*) from {table} where stop_id <> '100201' and no_dropoff")[0] == 0
            assert one(con, f"select count(*) from {table} where stop_id = '999901'")[0] == 0
        gtfs.segments(con)
        assert one(con, "select count(distinct b_seq) from sched_seg")[0] == len(stops) - 1

    output = tmp_path / "planner.duckdb"
    main([
        "--workdir", str(tmp_path / "work"), "--threads", "2", "--memory-limit", "1GB", "--nice", "0", "score",
        "--bundle", str(trained), "--gtfs-zip", str(timetable), "--recent-daily", str(world["recent"]),
        "--weather-json", str(world["weather_forecast"]), "--start", SCORE_START.isoformat(), "--days", "1",
        "--output", str(output), "--build-id", "restricted",
    ])  # fmt: skip
    with duckdb.connect(str(output), read_only=True) as con:
        columns = {row[0]: row[1] for row in con.execute("describe planner_stop").fetchall()}
        assert columns["can_alight"] == "BOOLEAN"
        assert one(con, "select count(*) from planner_stop where can_alight is null")[0] == 0
        for mode in ("bus", "tram", "metro", "rail"):
            rows = con.execute(
                "select s.stop_id, s.can_alight, s.leave_by_offset_s, s.ride_from_start_s "
                "from planner_stop s join planner_trip t using (trip_key) "
                "where t.mode = ? and t.trip_key = (select min(trip_key) from planner_trip where mode = ?) "
                "order by s.stop_sequence",
                [mode, mode],
            ).fetchall()
            assert [r[0] for r in rows] == stops
            assert [r[1] for r in rows] == [True, False, True, True, True, True, True]
            assert rows[1][2] is not None  # pickup-only passengers can still board here
            assert rows[2][2] is None  # no pickup at the drop-off-only stop
            assert all(r[2] is not None for r in (rows[3], rows[4], rows[5]))
            assert rows[-1][2] is None  # no boarding at the terminus
            assert all(b[3] > a[3] for a, b in pairwise(rows))
            if mode in {"metro", "rail"}:
                assert [r[3] for r in rows] == [i * 240 for i in range(len(stops))]


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


def test_expected_times_average_the_trip_start_each_boardable_stop_implies() -> None:
    con = duckdb.connect()
    con.execute(
        """
        create table out_stop as select * from (values
            -- starts implied: 1000 + 60 - 0 = 1060, then 1300 + 0 - 200 = 1100, then 1600 + 120 - 600 = 1120
            (1, 0, 1000, 60, -30, 0.0), (1, 1, 1300, 0, -20, 200.0), (1, 2, 1600, 120, 0, 600.0),
            (1, 3, 1900, 900, null, 760.0),  -- alighting only: ride from the mean start, 1093 + 760
            -- starts 1000, 1500, 1200: the third lowers the mean enough to turn the trip back, so it holds 1350
            (2, 0, 1000, 0, 0, 0.0), (2, 1, 1300, 300, 0, 100.0), (2, 2, 1310, 0, 0, 110.0),
            (2, 3, 1400, 0, null, 200.0),
            -- no boarding before the stop: its own usual delay
            (3, 0, 1000, 30, null, 0.0)
        ) t(trip_key, stop_sequence, scheduled_sod, usual_delay_s, leave_by_offset_s, ride_from_start_s)
        """
    )
    score._expected_times(con)
    rows = con.execute("select trip_key, expected_sod from out_stop order by trip_key, stop_sequence").fetchall()
    assert rows == [(1, 1060), (1, 1280), (1, 1693), (1, 1853), (2, 1000), (2, 1350), (2, 1350), (2, 1433), (3, 1030)]


def test_scoring_publishes_a_contract_artifact_with_learned_times(
    tmp_path: Path, world: dict[str, Path], trained: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "serving" / "planner" / "planner.duckdb"
    # OSM covers Alpha's bus post and platform (a 200 m walk around a building); the rest is estimated.
    osm = tmp_path / "footpaths.parquet"
    pairs = [
        ("100101", "100101", 0),
        ("1001M:P1", "1001M:P1", 0),
        ("100101", "1001M:P1", 200),
        ("1001M:P1", "100101", 200),
    ]
    duckdb.sql(
        f"copy (select * from (values {', '.join(map(str, pairs))}) t(from_stop_id, to_stop_id, distance_m)) to '{osm}'"
    )
    monkeypatch.setattr("ztm_planner.score.INTERCHANGES", {("100301", "1001M:P1"): 90, ("100301", "absent"): 90})
    # The weekly live calibration's rows, as planner_queries.live_persistence and live_turnaround return them.
    live = tmp_path / "live.json"
    live.write_text(json.dumps({
        "version": 1,
        "persistence": [{"is_tram": False, "horizon_min": 5, "n": 9, "alpha": 1.0, "low_s": -100, "mid_s": 0,
                         "high_s": 50}],
        "turnaround": [{"is_tram": True, "n": 9, "low_s": -60, "mid_s": 180, "high_s": 240}],
    }))
    main([
        "--workdir", str(tmp_path / "work"), "--threads", "2", "--memory-limit", "1GB", "--nice", "0", "score",
        "--bundle", str(trained), "--gtfs-zip", str(world["gtfs_latest"]), "--previous-gtfs-zip", str(world["gtfs"]),
        "--recent-daily", str(world["recent"]),
        "--weather-json", str(world["weather_forecast"]), "--start", SCORE_START.isoformat(), "--days", "7",
        "--output", str(output), "--build-id", "b-test", "--footpaths", str(osm), "--live-calibration", str(live),
    ])  # fmt: skip
    con = duckdb.connect(str(output), read_only=True)
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    for table, fields in contract["tables"].items():
        assert [r[0] for r in con.execute(f"describe {table}").fetchall()] == [f["name"] for f in fields]
    meta = one(con, "select first_date, last_date, model_version from planner_metadata")
    assert meta == (SCORE_START, SCORE_START + timedelta(days=6), "test-1")
    assert con.execute("select * from planner_live_persistence").fetchall() == [(False, 5, 1.0, -100.0, 0.0, 50.0)]
    assert con.execute("select * from planner_live_turnaround").fetchall() == [(True, -60.0, 180.0, 240.0)]
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
    assert "M1" in one(con, "select lines from planner_stop_group where stop_group_id = '1001'")[0]
    # Metro and SKM keep their timetable: no delay, board on time, ride = timetable difference.
    fixed = one(
        con,
        "select count(*), max(abs(usual_delay_s)), max(abs(leave_by_offset_s)), max(late_delay_s), "
        "max(ride_from_start_s) from planner_stop s join planner_trip t using (trip_key) where t.mode = 'rail'",
    )
    assert fixed[0] > 0 and fixed[1:] == (0, 0, RAIL_LATE_S, 9 * 60)
    timetabled = "select count(*) from planner_stop s join planner_trip t using (trip_key) where t.mode in"
    assert one(con, f"{timetabled} ('metro', 'rail') and expected_sod <> scheduled_sod")[0] == 0
    backwards = (
        "select count(*) from (select expected_sod < lag(expected_sod) over "
        "(partition by trip_key order by stop_sequence) as back from planner_stop) where back"
    )
    assert one(con, backwards)[0] == 0
    # OSM distances where covered, straight-line estimates elsewhere; platforms add the station access time.
    walks = dict(con.execute("select from_stop_id || '>' || to_stop_id, walk_s from planner_footpath").fetchall())
    assert walks["100101>1001M:P1"] == walks["1001M:P1>100101"] == math.ceil(200 / WALK_SPEED_MPS) + STATION_ACCESS_S
    assert STATION_ACCESS_S + 60 < walks["100201>4900"] < STATION_ACCESS_S + 90  # ~65 m x detour at walking pace
    assert len([k for k in walks if k.startswith("100101>")]) == 1
    # An interchange replaces the walk with its fixed time, both ways, and has no distance.
    interchange = "select distance_m, walk_s from planner_footpath where from_stop_id = ? and to_stop_id = ?"
    assert con.execute(interchange, ["100301", "1001M:P1"]).fetchall() == [(None, 90)]
    assert con.execute(interchange, ["1001M:P1", "100301"]).fetchall() == [(None, 90)]
    assert "100301>absent" not in walks
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
