"""Scheduled stops for upcoming service dates, read from a GTFS ZIP.

Same mapping as the warehouse staging models: line = route_id, mode from route_type (0 tram, 1 metro, 2 rail,
3 bus); stops that are not in passenger service (pickup and drop-off both 1) are skipped. Buses and trams go to
``sched_stop`` for the travel-time model; metro and SKM rail have no observations and keep their timetable in
``sched_fixed``. Metro trips are templates repeated every headway (``frequencies.txt``).
"""

from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

MEMBERS = ("stop_times.txt", "trips.txt", "routes.txt", "stops.txt", "calendar_dates.txt")
OPTIONAL_MEMBERS = ("frequencies.txt",)
MODE_SQL = "case r.route_type when '0' then 'tram' when '1' then 'metro' when '2' then 'rail' else 'bus' end"


def _sod(column: str) -> str:
    return (
        f"split_part({column}, ':', 1)::integer * 3600 + split_part({column}, ':', 2)::integer * 60"
        f" + split_part({column}, ':', 3)::integer"
    )


def load_schedule(
    con: duckdb.DuckDBPyConnection, gtfs_zip: Path, workdir: Path, start: date, end: date, append: bool = False
) -> None:
    """Create (or with ``append``, extend) ``sched_stop`` and ``sched_fixed``: one row per scheduled passenger stop
    in [start, end]."""
    extract = workdir / "gtfs"
    extract.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(gtfs_zip) as archive:
        names = set(archive.namelist())
        missing = [m for m in MEMBERS if m not in names]
        if missing:
            raise ValueError(f"GTFS ZIP lacks {missing}")
        members = [*MEMBERS, *(m for m in OPTIONAL_MEMBERS if m in names)]
        for member in members:
            archive.extract(member, extract)
    for member in members:
        table = "gtfs_" + member.removesuffix(".txt")
        con.execute(
            f"create or replace table {table} as select * from read_csv(?, all_varchar = true, header = true)",
            [str(extract / member)],
        )
    if "frequencies.txt" not in members:
        con.execute(
            "create or replace table gtfs_frequencies (trip_id varchar, start_time varchar, end_time varchar, "
            "headway_secs varchar)"
        )
    con.execute(
        f"""
        create or replace temp table gtfs_trip_stop as
        with svc as (
            select service_id, strptime(date, '%Y%m%d')::date as service_date
            from gtfs_calendar_dates where exception_type = '1'
              and strptime(date, '%Y%m%d')::date between ?::date and ?::date
        ),
        st as (
            select trip_id, stop_id, stop_sequence::integer as stop_sequence, {_sod("arrival_time")} as scheduled_sod,
                coalesce(nullif(pickup_type, ''), '0') as pickup_type,
                coalesce(nullif(drop_off_type, ''), '0') as drop_off_type
            from gtfs_stop_times
        )
        select svc.service_date, t.trip_id, r.route_type, {MODE_SQL} as mode,
            t.route_id as line, coalesce(nullif(t.direction_id, '')::integer, 0) as direction_id,
            coalesce(nullif(t.trip_headsign, ''), '') as headsign,
            st.stop_id, st.stop_sequence, st.scheduled_sod, st.pickup_type = '3' as request,
            s.stop_name, s.stop_lat::double as lat, s.stop_lon::double as lon, st.pickup_type = '1' as no_pickup,
            st.drop_off_type = '1' as no_dropoff
        from gtfs_trips t
        join svc using (service_id)
        join gtfs_routes r using (route_id)
        join st using (trip_id)
        join gtfs_stops s using (stop_id)
        where r.route_type in ('0', '1', '2', '3') and not (st.pickup_type = '1' and st.drop_off_type = '1')
        """,
        [start, end],
    )
    columns = (
        "service_date, trip_key, mode, line, direction_id, headsign, stop_id, stop_sequence, scheduled_sod, request, "
        "stop_name, lat, lon, no_pickup, no_dropoff"
    )
    trip_key = "(hash(service_date::varchar || ':' || {id}) >> 1)::bigint as trip_key"
    con.execute(
        f"""
        {"insert into sched_stop" if append else "create or replace table sched_stop as"}
        select {columns.replace("trip_key", trip_key.format(id="trip_id"))}
        from gtfs_trip_stop where route_type in ('0', '3')
        """
    )
    # A frequency template's times are relative to its first stop; each run starts every headway in
    # [start_time, end_time). Trips without frequencies run once, as listed.
    con.execute(
        f"""
        {"insert into sched_fixed" if append else "create or replace table sched_fixed as"}
        with runs as (
            select trip_id, s + n * h as run_start
            from (select trip_id, {_sod("start_time")} as s, {_sod("end_time")} as e, headway_secs::integer as h
                  from gtfs_frequencies where headway_secs::integer > 0)
            cross join lateral (select unnest(range(0, (e - s + h - 1) // h)) as n)
        ),
        fixed as (
            select ts.* exclude (scheduled_sod), r.run_start,
                ts.scheduled_sod - min(ts.scheduled_sod) over (partition by ts.service_date, ts.trip_id)
                    + coalesce(r.run_start, min(ts.scheduled_sod) over (partition by ts.service_date, ts.trip_id))
                    as scheduled_sod
            from gtfs_trip_stop ts left join runs r using (trip_id)
            where ts.route_type in ('1', '2')
        )
        select {columns.replace("trip_key", trip_key.format(id="trip_id || ':' || coalesce(run_start, -1)"))}
        from fixed
        """
    )
    for member in (*MEMBERS, *OPTIONAL_MEMBERS):
        con.execute(f"drop table if exists gtfs_{member.removesuffix('.txt')}")
    con.execute("drop table gtfs_trip_stop")


def segments(con: duckdb.DuckDBPyConnection, target: str = "sched_seg") -> None:
    """Consecutive scheduled stops of each trip as segments with SEGMENT_COLUMNS (no actual_s)."""
    con.execute(
        f"""
        create or replace table {target} as
        with ordered as (
            select *,
                lag(stop_id) over w as a_stop, lag(scheduled_sod) over w as a_sched_sod,
                lag(request) over w as a_request, lag(lat) over w as a_lat, lag(lon) over w as a_lon,
                (row_number() over w - 1)::integer as pos
            from sched_stop
            window w as (partition by trip_key order by stop_sequence)
        )
        select service_date, trip_key, mode, line, direction_id, a_stop, stop_id as b_stop, stop_sequence as b_seq,
            pos, a_request, request as b_request,
            -- haversine metres; the warehouse used BigQuery ST_DISTANCE, equal within GPS precision
            2 * 6371000 * asin(sqrt(power(sin(radians(lat - a_lat) / 2), 2)
                + cos(radians(a_lat)) * cos(radians(lat)) * power(sin(radians(lon - a_lon) / 2), 2))) as dist_m,
            a_sched_sod, scheduled_sod - a_sched_sod as sched_s
        from ordered where a_stop is not null
        """
    )
