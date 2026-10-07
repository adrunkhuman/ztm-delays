"""Nightly scoring: the timetable for the coming days + model bundle -> planner artifact.

Every scheduled bus and tram segment gets a predicted ride time; every stop gets its usual and late delay
and the 'be at the stop' margin from the stop tables. Metro and SKM keep their timetable. The previous service
date is included so early-morning searches still find night trips that started before midnight. Walks between
nearby posts come from the weekly OSM footpaths, estimated from straight lines for posts they don't cover.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa

from ztm_planner import artifact, assemble, bundle, features, gtfs, model, weather
from ztm_planner.bundle import STOP_LEVELS
from ztm_planner.db import connect, one
from ztm_planner.settings import (
    INTERCHANGES,
    RAIL_LATE_S,
    STATION_ACCESS_S,
    STOP_EPS_GRID,
    STOP_TOLERANCE_S,
    WALK_DETOUR,
    WALK_MAX_M,
    WALK_MIN_S,
    WALK_SPEED_MPS,
    Resources,
)

log = logging.getLogger(__name__)
TRIP_COLUMNS = "trip_key, service_date, mode, line, headsign, duty_id, brigade, shape_id"
STOP_LEVEL_KEYS = {
    "line_stop_hour": ("line", "direction_id", "stop_id", "daytype", "hr"),
    "line_stop_band": ("line", "direction_id", "stop_id", "daytype", "hb"),
    "line_stop": ("line", "direction_id", "stop_id"),
    "generic": ("is_tram", "is_origin", "rel_b", "daytype", "hb"),
}


def score(
    bundle_dir: Path,
    gtfs_zip: Path,
    recent_daily: Path,
    weather_json: Path,
    start: date,
    days: int,
    workdir: Path,
    output: Path,
    build_id: str,
    resources: Resources,
    previous_gtfs_zip: Path | None = None,
    footpaths: Path | None = None,
    live_calibration: Path | None = None,
) -> dict:
    """Build the artifact for service dates [start, start + days) and publish it to ``output``.

    A snapshot taken today no longer lists yesterday's service date, whose night trips still run after
    midnight; ``previous_gtfs_zip`` (the last snapshot before today) supplies that date when given.
    Without ``footpaths`` (the weekly OSM table), every walk is a straight-line estimate. Without
    ``live_calibration`` (the weekly JSON of how live delays carry on), the live tables are empty and the frontend
    does not adjust trips to live positions.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "score.duckdb"
    db_path.unlink(missing_ok=True)
    con = connect(resources, str(db_path))
    booster, meta = bundle.load(con, bundle_dir)
    first, last = start - timedelta(days=1), start + timedelta(days=days - 1)
    features.register_holidays(con, first, last)
    if previous_gtfs_zip is not None:
        gtfs.load_schedule(con, previous_gtfs_zip, workdir / "previous", first, first, shape_prefix="p:")
        gtfs.load_schedule(con, gtfs_zip, workdir, start, last, append=True)
        _merge_previous_shapes(con)
    else:
        gtfs.load_schedule(con, gtfs_zip, workdir, first, last)
    gtfs.segments(con)
    features.add_features(con, "sched_seg", "seg_feat")
    weather.load(con, weather_json)
    con.execute(f"create or replace table recent_daily as select * from read_parquet('{recent_daily}')")
    # Recent conditions end on the last published day, so a late warehouse run cannot leave an empty day in the window.
    (latest,) = con.execute("select max(service_date) from recent_daily").fetchone() or (None,)
    asof = min(start - timedelta(days=1), latest or start)

    con.execute("create or replace table seg_pred (trip_key bigint, b_seq integer, pred double)")
    for (day,) in con.execute("select distinct service_date from seg_feat order by 1").fetchall():
        con.execute(f"create or replace table chunk as select * from seg_feat where service_date = date '{day}'")
        assemble.model_rows(con, "chunk", "chunk_model", "m_", "recent_daily", asof)
        pred = model.predict(booster, con, "chunk_model", resources.threads)
        keys = con.execute("select trip_key, b_seq from chunk_model order by row_id").fetchnumpy()
        con.register("chunk_pred", pa.table({**keys, "pred": np.maximum(pred, 0.0)}))
        con.execute("insert into seg_pred select trip_key, b_seq, pred from chunk_pred")
        con.unregister("chunk_pred")
    _stop_rows(con)
    _fixed_stop_rows(con)
    _expected_times(con)
    _stop_groups(con)
    _footpaths(con, footpaths)
    _live_calibration(con, live_calibration)
    artifact.write(
        con,
        output,
        {
            "planner_metadata": f"select '{build_id}' as build_id, now()::timestamp as built_at, "
            f"'{meta['version']}' as model_version, date '{start}' as first_date, date '{last}' as last_date",
            "planner_stop_group": "select * from out_group",
            "planner_trip": f"select distinct {TRIP_COLUMNS} from sched_stop "
            f"union all select distinct {TRIP_COLUMNS} from sched_fixed",
            "planner_stop": "select * from out_stop",
            "planner_range": "select * from ride_range",
            "planner_footpath": "select * from out_footpath",
            "planner_stop_post": "select stop_id, lat, lon from post",
            "planner_shape": "select * from sched_shape",
            "planner_live_persistence": "select * from live_persistence",
            "planner_live_turnaround": "select * from live_turnaround",
        },
    )
    summary = one(con, "select count(distinct trip_key), count(*) from out_stop")
    con.close()
    db_path.unlink(missing_ok=True)
    result = {"build_id": build_id, "model_version": meta["version"], "trips": summary[0], "stops": summary[1]}
    log.info("published %s: %s", output, result)
    return result


def _live_calibration(con: duckdb.DuckDBPyConnection, path: Path | None) -> None:
    """Copy the weekly live calibration (planner_queries.live_persistence / live_turnaround rows) as given."""
    loaded = json.loads(path.read_text()) if path is not None else {}
    con.execute(
        "create or replace table live_persistence (is_tram boolean, horizon_min integer, alpha double, low_s double, "
        "mid_s double, high_s double)"
    )
    con.execute("create or replace table live_turnaround (is_tram boolean, low_s double, mid_s double, high_s double)")
    for table, columns in (
        ("live_persistence", ("is_tram", "horizon_min", "alpha", "low_s", "mid_s", "high_s")),
        ("live_turnaround", ("is_tram", "low_s", "mid_s", "high_s")),
    ):
        rows = [[row[c] for c in columns] for row in loaded.get(table.removeprefix("live_"), [])]
        if rows:
            con.executemany(f"insert into {table} values ({', '.join('?' * len(columns))})", rows)


def _merge_previous_shapes(con: duckdb.DuckDBPyConnection) -> None:
    """Point yesterday's trips at an identical shape of today's snapshot, which may number it differently."""
    con.execute(
        """
        create or replace temp table same_shape as
        with drawn as (
            select shape_id, md5(lat::varchar || lon::varchar || dist_m::varchar) as geometry from sched_shape
        )
        select p.shape_id as old_id, any_value(c.shape_id) as new_id
        from drawn p join drawn c on p.geometry = c.geometry and c.shape_id not like 'p:%'
        where p.shape_id like 'p:%'
        group by p.shape_id
        """
    )
    for table in ("sched_stop", "sched_fixed"):
        con.execute(f"update {table} set shape_id = m.new_id from same_shape m where {table}.shape_id = m.old_id")
    con.execute("delete from sched_shape where shape_id in (select old_id from same_shape)")
    con.execute("drop table same_shape")


def _stop_rows(con: duckdb.DuckDBPyConnection) -> None:
    """Contract planner_stop rows: cumulative predicted ride and stop delays from the most specific slot."""
    con.execute(
        """
        create or replace table stop_ctx as
        select s.*, left(s.stop_id, 4) as stop_group_id,
            coalesce(sum(p.pred) over (partition by s.trip_key order by s.stop_sequence
                rows between unbounded preceding and current row), 0) as ride_from_start_s,
            (case when h.service_date is not null or isodow(s.service_date) = 7 then 2
                  when isodow(s.service_date) = 6 then 1 else 0 end) as daytype,
            s.scheduled_sod // 3600 as hr,
            """
        + features.hour_band_sql("(s.scheduled_sod // 3600)")
        + """ as hb,
            s.mode = 'tram' as is_tram,
            row_number() over w = 1 as is_origin,
            row_number() over (partition by s.trip_key order by s.stop_sequence desc) = 1 as is_last,
            least(9, floor(10 * (row_number() over w - 1) / greatest(count(*) over (partition by s.trip_key) - 1, 1)))
                as rel_b,
            case when (s.scheduled_sod // 3600) % 24 >= 23 or (s.scheduled_sod // 3600) % 24 < 5 then 'night'
                 when isodow(s.service_date) <= 5 and (s.scheduled_sod // 3600) % 24 in (7, 8, 15, 16, 17) then 'peak'
                 else 'other' end as band
        from sched_stop s
        left join seg_pred p on p.trip_key = s.trip_key and p.b_seq = s.stop_sequence
        left join holiday h on h.service_date = s.service_date
        window w as (partition by s.trip_key order by s.stop_sequence)
        """
    )
    joins, picks = [], {"dq50": [], "dq90": [], "eps": []}
    # A mode x band without calibration falls back to the most conservative quantile, not to no margin.
    eps_case = " ".join(f"when coalesce(e.eps_index, 0) = {i} then {{t}}.e{i}" for i in range(len(STOP_EPS_GRID)))
    for i, level in enumerate(STOP_LEVELS):
        on = " and ".join(f"l{i}.{key} = c.{key}" for key in STOP_LEVEL_KEYS[level])
        joins.append(f"left join (select * from stop_slots where level = '{level}') l{i} on {on}")
        picks["dq50"].append(f"l{i}.dq50")
        picks["dq90"].append(f"l{i}.dq90")
        picks["eps"].append(f"case {eps_case.format(t=f'l{i}')} end")
    con.execute(
        f"""
        create or replace table out_stop as
        select c.trip_key, c.stop_sequence, c.stop_id, c.stop_group_id, coalesce(c.stop_name, c.stop_id) as stop_name,
            c.scheduled_sod,
            round(coalesce({", ".join(picks["dq50"])}, 0)) as usual_delay_s,
            round(greatest(coalesce({", ".join(picks["dq90"])}, 0), coalesce({", ".join(picks["dq50"])}, 0)))
                as late_delay_s,
            case when c.is_last or c.no_pickup then null
                 else floor(least(coalesce({", ".join(picks["eps"])}, 0) + {STOP_TOLERANCE_S}, 0)) end
                as leave_by_offset_s,
            c.ride_from_start_s, not c.no_dropoff as can_alight, c.shape_dist_m
        from stop_ctx c
        left join stop_eps e on e.is_tram = c.is_tram and e.band = c.band
        {" ".join(joins)}
        """
    )


def _fixed_stop_rows(con: duckdb.DuckDBPyConnection) -> None:
    """Metro and SKM rows of planner_stop: the timetable as the expectation, a fixed late margin for SKM."""
    con.execute(
        f"""
        insert into out_stop
        select trip_key, stop_sequence, stop_id, left(stop_id, 4) as stop_group_id,
            coalesce(stop_name, stop_id) as stop_name, scheduled_sod, 0 as usual_delay_s,
            case when mode = 'rail' then {RAIL_LATE_S} else 0 end as late_delay_s,
            case when no_pickup or stop_sequence = max(stop_sequence) over (partition by trip_key) then null
                 else 0 end as leave_by_offset_s,
            scheduled_sod - min(scheduled_sod) over (partition by trip_key) as ride_from_start_s,
            not no_dropoff as can_alight, shape_dist_m
        from sched_fixed
        """
    )


def _expected_times(con: duckdb.DuckDBPyConnection) -> None:
    """One expected time per stop of a trip, whichever stop the passenger boards at.

    Each boardable stop implies a trip start: its timetable time plus usual delay (never before the boarding
    deadline), less the predicted ride to it. A stop's expected time is the mean start implied by the boardable
    stops up to it, plus its predicted ride; a running maximum keeps it from going backwards. Averaging smooths
    the noise of single stop tables yet follows delay that builds up along the route: on held-out data it beat
    both the stop's own usual delay and anchoring at the boarding stop. Metro and SKM keep their timetable.
    """
    con.execute(
        """
        create or replace table out_stop as
        with started as (
            select *,
                case when leave_by_offset_s is not null
                     then scheduled_sod + greatest(usual_delay_s, leave_by_offset_s) - ride_from_start_s end as start_s
            from out_stop
        ),
        averaged as (
            select *,
                avg(start_s) over w as mean_start_s,
                -- a trip that cannot be boarded before this stop: the stop's own delay
                scheduled_sod + usual_delay_s as own_s
            from started
            window w as (partition by trip_key order by stop_sequence rows unbounded preceding)
        )
        select * exclude (start_s, mean_start_s, own_s),
            round(max(coalesce(mean_start_s + ride_from_start_s, own_s)) over (
                partition by trip_key order by stop_sequence rows unbounded preceding
            ))::integer as expected_sod
        from averaged
        """
    )


def _footpaths(con: duckdb.DuckDBPyConnection, footpaths: Path | None) -> None:
    """planner_footpath: walks between the artifact's posts, both directions.

    OSM distances where the weekly table covers both posts (it marks a covered post with a row to itself);
    otherwise straight line x WALK_DETOUR. Metro and rail platforms add the station access time.
    INTERCHANGES replace the walk between their posts with a fixed time and no distance.
    """
    con.execute(
        """
        create or replace table post as
        select stop_id, any_value(lat) as lat, any_value(lon) as lon, bool_or(mode in ('metro', 'rail')) as station
        from (select stop_id, lat, lon, mode from sched_stop union all select stop_id, lat, lon, mode from sched_fixed)
        group by stop_id
        """
    )
    if footpaths is None:
        con.execute("create or replace table osm_walk (from_stop_id varchar, to_stop_id varchar, distance_m integer)")
    else:
        con.execute(
            "create or replace table osm_walk as select f.* from read_parquet(?) f "
            "semi join post a on a.stop_id = f.from_stop_id semi join post b on b.stop_id = f.to_stop_id",
            [str(footpaths)],
        )
    reach_deg = WALK_MAX_M / WALK_DETOUR / 111_000
    con.execute(
        f"""
        create or replace table out_footpath as
        with covered as (select from_stop_id as stop_id from osm_walk where from_stop_id = to_stop_id),
        estimated as (
            select a.stop_id as from_stop_id, b.stop_id as to_stop_id,
                {WALK_DETOUR} * 2 * 6371000 * asin(sqrt(power(sin(radians(b.lat - a.lat) / 2), 2)
                    + cos(radians(a.lat)) * cos(radians(b.lat)) * power(sin(radians(b.lon - a.lon) / 2), 2)))
                    as distance_m
            from post a join post b
                on b.lat between a.lat - {reach_deg} and a.lat + {reach_deg}
                and b.lon between a.lon - {2 * reach_deg} and a.lon + {2 * reach_deg}
                and a.stop_id <> b.stop_id
            where a.stop_id not in (select stop_id from covered) or b.stop_id not in (select stop_id from covered)
        ),
        walk as (
            select from_stop_id, to_stop_id, distance_m from osm_walk where from_stop_id <> to_stop_id
            union all
            select from_stop_id, to_stop_id, distance_m from estimated where distance_m <= {WALK_MAX_M}
        )
        select w.from_stop_id, w.to_stop_id, round(w.distance_m)::integer as distance_m,
            (greatest({WALK_MIN_S}, ceil(w.distance_m / {WALK_SPEED_MPS}))
                + case when a.station or b.station then {STATION_ACCESS_S} else 0 end)::integer as walk_s
        from walk w join post a on a.stop_id = w.from_stop_id join post b on b.stop_id = w.to_stop_id
        """
    )
    rows = [(a, b, s) for (x, y), s in INTERCHANGES.items() for a, b in ((x, y), (y, x))]
    con.execute("create or replace temp table interchange (from_stop_id varchar, to_stop_id varchar, walk_s integer)")
    con.executemany("insert into interchange values (?, ?, ?)", rows)
    con.execute(
        """
        delete from out_footpath f using interchange i
        where f.from_stop_id = i.from_stop_id and f.to_stop_id = i.to_stop_id;
        insert into out_footpath
        select from_stop_id, to_stop_id, null, walk_s from interchange i
        where from_stop_id in (select stop_id from post) and to_stop_id in (select stop_id from post)
        """
    )


def _stop_groups(con: duckdb.DuckDBPyConnection) -> None:
    """One search entry per stop group, named by its most common post name."""
    con.execute(
        """
        create or replace table out_group_base as
        select o.stop_group_id, mode(o.stop_name) as name, list(distinct s.line order by s.line) as lines,
            count(*)::integer as visits
        from out_stop o
        join (select trip_key, stop_sequence, line from sched_stop
              union all select trip_key, stop_sequence, line from sched_fixed) s using (trip_key, stop_sequence)
        group by o.stop_group_id
        """
    )
    names = con.execute("select stop_group_id, name from out_group_base").fetchall()
    con.register(
        "group_keys",
        pa.table({"stop_group_id": [g for g, _ in names], "search_key": [artifact.search_key(n) for _, n in names]}),
    )
    con.execute(
        "create or replace table out_group as select b.stop_group_id, b.name, k.search_key, b.lines, b.visits "
        "from out_group_base b join group_keys k using (stop_group_id)"
    )
    con.unregister("group_keys")


def build_id_now() -> str:
    """UTC timestamp id for a scoring run."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
