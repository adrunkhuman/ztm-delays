"""Segment features shared by training (observed segments) and scoring (timetable segments).

A segment is the ride between two consecutive scheduled stops of one trip. Input tables carry
``SEGMENT_COLUMNS`` (training adds ``actual_s``); ``add_features`` derives the model keys and features.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

from ztm_planner.calendar import HOUR_BAND_EDGES, holiday_dates
from ztm_planner.settings import RECENT_DAYS, RECENT_GAP_DAYS

if TYPE_CHECKING:
    import duckdb

SEGMENT_COLUMNS = (
    "service_date", "trip_key", "mode", "line", "direction_id", "a_stop", "b_stop", "b_seq", "pos",
    "a_request", "b_request", "dist_m", "a_sched_sod", "sched_s",
)  # fmt: skip
WEEK_ORIGIN = date(2000, 1, 3)  # a Monday: OOF folds are calendar weeks


def hour_band_sql(hour: str) -> str:
    """SQL for the 0..4 hour band of an hour expression (hours past 24 stay in the last band)."""
    cases = " ".join(f"when {hour} < {edge} then {band}" for band, edge in enumerate(HOUR_BAND_EDGES))
    return f"(case {cases} else {len(HOUR_BAND_EDGES)} end)::tinyint"


def register_holidays(con: duckdb.DuckDBPyConnection, start: date, end: date) -> None:
    """Holiday table for [start, end] (with a margin for night trips)."""
    days = holiday_dates(start - timedelta(days=1), end + timedelta(days=1))
    con.execute("create or replace table holiday (service_date date)")
    if days:
        con.executemany("insert into holiday values (?)", [[d] for d in days])


def add_features(con: duckdb.DuckDBPyConnection, source: str, target: str) -> None:
    """Create ``target`` from segment table ``source`` with keys, calendar and time features."""
    con.execute(
        f"""
        create or replace table {target} as
        select s.*,
            s.a_stop || '>' || s.b_stop as seg,
            left(s.a_stop, 4) || '>' || left(s.b_stop, 4) as grp,
            h.service_date is not null as is_holiday,
            isodow(s.service_date)::tinyint as dow,
            (case when h.service_date is not null or isodow(s.service_date) = 7 then 2
                  when isodow(s.service_date) = 6 then 1 else 0 end)::tinyint as daytype,
            (s.a_sched_sod // 3600)::smallint as hr,
            {hour_band_sql("(s.a_sched_sod // 3600)")} as hb,
            (least(greatest(s.sched_s, 0), 900) // 30)::smallint as sched_b,
            s.mode = 'tram' as is_tram,
            s.service_date::timestamp + to_hours((s.a_sched_sod // 3600)::integer) as wx_ts,
            s.a_sched_sod / 3600.0 as sod_h,
            (((s.service_date - date '{WEEK_ORIGIN}') // 7) % 4)::tinyint as fold
        from {source} s
        left join holiday h using (service_date)
        """
    )


def build_daily(con: duckdb.DuckDBPyConnection, observed: str, target: str) -> None:
    """Daily observed sums per segment and per segment x hour band, for recent-conditions features."""
    con.execute(
        f"""
        create or replace table {target} as
        select seg, hb, service_date, sum(actual_s)::double as s, count(*) as n
        from {observed} group by all
        """
    )


def add_recent(con: duckdb.DuckDBPyConnection, rows: str, daily: str, target: str, asof: date | None) -> None:
    """Recent mean segment time minus its long-run mean (``lr_seg``/``lr_seghb`` must be on ``rows``).

    Scoring uses the window ending ``asof``; training rows (``asof`` None) use the window ending
    RECENT_GAP_DAYS before their date, roughly the middle of the scoring horizon.
    """
    end = f"date '{asof.isoformat()}'" if asof else f"r.service_date - {RECENT_GAP_DAYS}"
    con.execute(
        f"""
        create or replace table {target} as
        with ends as (select distinct {end} as win_end from {rows} r),
        win as (
            select e.win_end, d.seg, d.hb, d.s, d.n
            from ends e join {daily} d
              on d.service_date > e.win_end - {RECENT_DAYS} and d.service_date <= e.win_end
        ),
        by_seg as (select win_end, seg, sum(s) / sum(n) as recent_seg, sum(n) as recent_n_seg from win group by all),
        by_seghb as (
            select win_end, seg, hb, sum(s) / sum(n) as recent_seghb, sum(n) as recent_n_seghb from win group by all
        )
        select r.*,
            a.recent_seg - r.lr_seg as shift_seg, b.recent_seghb - r.lr_seghb as shift_seghb,
            coalesce(a.recent_n_seg, 0) as recent_n_seg, coalesce(b.recent_n_seghb, 0) as recent_n_seghb
        from {rows} r
        left join by_seg a on a.win_end = {end} and a.seg = r.seg
        left join by_seghb b on b.win_end = {end} and b.seg = r.seg and b.hb = r.hb
        """
    )
