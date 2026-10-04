"""Scheduled bus and tram stops for upcoming service dates, read from a GTFS ZIP.

Same mapping as the warehouse staging models: line = route_id, mode from route_type (0 tram, 3 bus);
stops that are not in passenger service (pickup and drop-off both 1) are skipped.
"""

from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

MEMBERS = ("stop_times.txt", "trips.txt", "routes.txt", "stops.txt", "calendar_dates.txt")


def load_schedule(
    con: duckdb.DuckDBPyConnection, gtfs_zip: Path, workdir: Path, start: date, end: date, append: bool = False
) -> None:
    """Create (or with ``append``, extend) ``sched_stop``: one row per scheduled passenger stop in [start, end]."""
    extract = workdir / "gtfs"
    extract.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(gtfs_zip) as archive:
        names = set(archive.namelist())
        missing = [m for m in MEMBERS if m not in names]
        if missing:
            raise ValueError(f"GTFS ZIP lacks {missing}")
        for member in MEMBERS:
            archive.extract(member, extract)
    for member in MEMBERS:
        table = "gtfs_" + member.removesuffix(".txt")
        con.execute(
            f"create or replace table {table} as select * from read_csv(?, all_varchar = true, header = true)",
            [str(extract / member)],
        )
    con.execute(
        f"""
        {"insert into sched_stop" if append else "create or replace table sched_stop as"}
        with svc as (
            select service_id, strptime(date, '%Y%m%d')::date as service_date
            from gtfs_calendar_dates where exception_type = '1'
              and strptime(date, '%Y%m%d')::date between ?::date and ?::date
        ),
        st as (
            select trip_id, stop_id, stop_sequence::integer as stop_sequence,
                split_part(arrival_time, ':', 1)::integer * 3600 + split_part(arrival_time, ':', 2)::integer * 60
                    + split_part(arrival_time, ':', 3)::integer as scheduled_sod,
                coalesce(nullif(pickup_type, ''), '0') as pickup_type,
                coalesce(nullif(drop_off_type, ''), '0') as drop_off_type
            from gtfs_stop_times
        )
        select svc.service_date,
            (hash(svc.service_date::varchar || ':' || t.trip_id) >> 1)::bigint as trip_key,
            case r.route_type when '0' then 'tram' else 'bus' end as mode,
            t.route_id as line, coalesce(nullif(t.direction_id, '')::integer, 0) as direction_id,
            coalesce(nullif(t.trip_headsign, ''), '') as headsign,
            st.stop_id, st.stop_sequence, st.scheduled_sod, st.pickup_type = '3' as request,
            s.stop_name, s.stop_lat::double as lat, s.stop_lon::double as lon
        from gtfs_trips t
        join svc using (service_id)
        join gtfs_routes r using (route_id)
        join st using (trip_id)
        join gtfs_stops s using (stop_id)
        where r.route_type in ('0', '3') and not (st.pickup_type = '1' and st.drop_off_type = '1')
        """,
        [start, end],
    )
    for member in MEMBERS:
        con.execute(f"drop table gtfs_{member.removesuffix('.txt')}")


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
