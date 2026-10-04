"""Ride-time ranges: how far actual A->B rides fall from predictions, per mode x hour x predicted length.

Pairs mirror what riders ask: per contiguous run of observed segments in a trip, the whole run plus one
random sub-run. Hours with fewer than MIN_RANGE_PAIRS pairs use their time band; the output grid is complete.
"""

from __future__ import annotations

from itertools import pairwise
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa

from ztm_planner.settings import MIN_RANGE_PAIRS, RANGE_QUANTILES, RIDE_BUCKETS

if TYPE_CHECKING:
    import duckdb

MIN_EDGE, MAX_EDGE = -1.0, 1e12


def bucket_edges() -> list[tuple[float, float]]:
    """(min_ride_s, max_ride_s] intervals covering every ride."""
    edges = [MIN_EDGE, *map(float, RIDE_BUCKETS), MAX_EDGE]
    return list(pairwise(edges))


def pairs(con: duckdb.DuckDBPyConnection, predicted: str, seed: int = 1) -> pa.Table:
    """A->B pairs from ``predicted(trip_key, b_seq, service_date, is_tram, a_sched_sod, actual_s, pred)``."""
    cols = con.execute(
        "select trip_key, b_seq, isodow(service_date) <= 5 as weekday, is_tram, a_sched_sod, "
        f"actual_s::double as actual_s, pred from {predicted} "
        "order by trip_key, b_seq"
    ).fetchnumpy()
    trip, seq = cols["trip_key"], cols["b_seq"]
    if len(trip) == 0:
        return pa.table({"is_tram": [], "weekday": [], "hour": [], "actual": [], "pred": []})
    brk = np.ones(len(trip), dtype=bool)
    brk[1:] = (trip[1:] != trip[:-1]) | (seq[1:] != seq[:-1] + 1)
    starts = np.flatnonzero(brk)
    lens = np.diff(np.append(starts, len(trip)))
    i, j = np.floor(np.random.default_rng(seed).random((2, len(starts))) * lens).astype(int)
    first = np.concatenate([starts, starts + np.minimum(i, j)])
    last = np.concatenate([starts + lens - 1, starts + np.maximum(i, j)])
    sums = {}
    for name in ("actual_s", "pred"):
        cumulative = np.concatenate([[0.0], np.cumsum(cols[name], dtype=np.float64)])
        sums[name] = cumulative[last + 1] - cumulative[first]
    return pa.table(
        {
            "is_tram": cols["is_tram"][first],
            "weekday": cols["weekday"][first],
            "hour": (cols["a_sched_sod"][first] // 3600) % 24,
            "actual": sums["actual_s"],
            "pred": sums["pred"],
        }
    )


def ride_ranges(con: duckdb.DuckDBPyConnection, pair_table: pa.Table, target: str = "ride_range") -> None:
    """Complete ``target`` grid (contract columns of planner_range) from calibration pairs."""
    con.register("pairs_src", pair_table)
    edges = pa.table({"min_ride_s": [e[0] for e in bucket_edges()], "max_ride_s": [e[1] for e in bucket_edges()]})
    con.register("edges_src", edges)
    lo, hi = RANGE_QUANTILES
    band = """case when hour >= 23 or hour < 5 then 'night'
                   when weekday and hour in (7, 8, 15, 16, 17) then 'peak' else 'other' end"""
    con.execute(
        f"""
        create or replace table {target} as
        with p as (
            select p.*, e.min_ride_s, e.max_ride_s, {band} as band, actual / pred as r
            from pairs_src p join edges_src e on p.pred > e.min_ride_s and p.pred <= e.max_ride_s
            where p.pred > 0
        ),
        by_hour as (
            select is_tram, weekday, hour, min_ride_s, quantile_cont(r, {lo}) as lo, quantile_cont(r, {hi}) as hi
            from p group by all having count(*) >= {MIN_RANGE_PAIRS}
        ),
        by_band as (
            select is_tram, band, min_ride_s, quantile_cont(r, {lo}) as lo, quantile_cont(r, {hi}) as hi
            from p group by all
        ),
        by_mode as (
            select is_tram, min_ride_s, quantile_cont(r, {lo}) as lo, quantile_cont(r, {hi}) as hi from p group by all
        ),
        grid as (
            select m.is_tram, w.weekday, h.hour::integer as hour, e.min_ride_s, e.max_ride_s,
                case when h.hour >= 23 or h.hour < 5 then 'night'
                     when w.weekday and h.hour in (7, 8, 15, 16, 17) then 'peak' else 'other' end as band
            from (values (true), (false)) m(is_tram), (values (true), (false)) w(weekday),
                range(24) h(hour), edges_src e
        )
        select g.is_tram, g.weekday, g.hour, g.min_ride_s, g.max_ride_s,
            coalesce(bh.lo, bb.lo, bm.lo, 0.85) as low_ratio, coalesce(bh.hi, bb.hi, bm.hi, 1.15) as high_ratio
        from grid g
        left join by_hour bh using (is_tram, weekday, hour, min_ride_s)
        left join by_band bb using (is_tram, band, min_ride_s)
        left join by_mode bm using (is_tram, min_ride_s)
        order by all
        """
    )
    con.unregister("pairs_src")
    con.unregister("edges_src")
