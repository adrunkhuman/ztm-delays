"""Nightly scoring: the timetable for the coming days + model bundle -> planner artifact.

Every scheduled bus and tram segment gets a predicted ride time; every stop gets its usual and late delay
and the 'be at the stop' margin from the stop tables. The previous service date is included so early-morning
searches still find night trips that started before midnight.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa

from ztm_planner import artifact, assemble, bundle, features, gtfs, model, weather
from ztm_planner.bundle import STOP_LEVELS
from ztm_planner.db import connect, one
from ztm_planner.settings import STOP_EPS_GRID, STOP_TOLERANCE_S, Resources

log = logging.getLogger(__name__)
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
) -> dict:
    """Build the artifact for service dates [start, start + days) and publish it to ``output``.

    A snapshot taken today no longer lists yesterday's service date, whose night trips still run after
    midnight; ``previous_gtfs_zip`` (the last snapshot before today) supplies that date when given.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "score.duckdb"
    db_path.unlink(missing_ok=True)
    con = connect(resources, str(db_path))
    booster, meta = bundle.load(con, bundle_dir)
    first, last = start - timedelta(days=1), start + timedelta(days=days - 1)
    features.register_holidays(con, first, last)
    if previous_gtfs_zip is not None:
        gtfs.load_schedule(con, previous_gtfs_zip, workdir / "previous", first, first)
        gtfs.load_schedule(con, gtfs_zip, workdir, start, last, append=True)
    else:
        gtfs.load_schedule(con, gtfs_zip, workdir, first, last)
    gtfs.segments(con)
    features.add_features(con, "sched_seg", "seg_feat")
    weather.load(con, weather_json)
    con.execute(f"create or replace table recent_daily as select * from read_parquet('{recent_daily}')")
    asof = start - timedelta(days=1)

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
    _stop_groups(con)
    artifact.write(
        con,
        output,
        {
            "planner_metadata": f"select '{build_id}' as build_id, now()::timestamp as built_at, "
            f"'{meta['version']}' as model_version, date '{start}' as first_date, date '{last}' as last_date",
            "planner_stop_group": "select * from out_group",
            "planner_trip": "select distinct trip_key, service_date, mode, line, headsign from sched_stop",
            "planner_stop": "select * from out_stop",
            "planner_range": "select * from ride_range",
        },
    )
    summary = one(con, "select count(distinct trip_key), count(*) from out_stop")
    con.close()
    db_path.unlink(missing_ok=True)
    result = {"build_id": build_id, "model_version": meta["version"], "trips": summary[0], "stops": summary[1]}
    log.info("published %s: %s", output, result)
    return result


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
    eps_case = " ".join(f"when e.eps_index = {i} then {{t}}.e{i}" for i in range(len(STOP_EPS_GRID)))
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
            case when c.is_last then null
                 else floor(least(coalesce({", ".join(picks["eps"])}, 0) + {STOP_TOLERANCE_S}, 0)) end
                as leave_by_offset_s,
            c.ride_from_start_s
        from stop_ctx c
        left join stop_eps e on e.is_tram = c.is_tram and e.band = c.band
        {" ".join(joins)}
        """
    )


def _stop_groups(con: duckdb.DuckDBPyConnection) -> None:
    """One search entry per stop group, named by its most common post name."""
    con.execute(
        """
        create or replace table out_group_base as
        select o.stop_group_id, mode(o.stop_name) as name, list(distinct s.line order by s.line) as lines,
            count(*)::integer as visits
        from out_stop o join sched_stop s using (trip_key, stop_sequence)
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
