from __future__ import annotations

import re
from collections import defaultdict
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING, Any

from ztm_frontend.db import fetch_all, fetch_one

if TYPE_CHECKING:
    from pathlib import Path

STOP_ROWS_PER_COURSE = 36
STOP_PICKER_LIMIT = 300
DELAY_POINT_LIMIT = 600
LANDING_ROW_LIMIT = 16
NUMERIC_STOP_POST_SUFFIX_LENGTH = 2
STOP_POST_PRIMARY_MODES = ("bus", "tram")
ON_TIME_EARLY_SECONDS = -60
ON_TIME_LATE_SECONDS = 180
LOW_ON_TIME_RATE = 0.6
SERVICE_DAY_HOURS = (*range(4, 24), *range(4))
NEXT_DAY_PLACEHOLDER_HOURS = frozenset(range(4))
AM_RUSH_START_HOUR = 7
AM_RUSH_END_HOUR = 9
PM_RUSH_START_HOUR = 16
PM_RUSH_END_HOUR = 18
WEEKEND_START_INDEX = 5
HISTOGRAM_BUCKET_COUNT = 12
HISTOGRAM_MAX_HEIGHT = 38
HISTOGRAM_MINI_MAX_HEIGHT = 20
EARLY_BUCKET_COUNT = 2
LATE_BUCKET_START = 8
PARTIAL_TRIP_SCORE = 80
BROKEN_TRIP_SCORE = 92
TRIP_TRACE_BASELINE = 16
TRIP_TRACE_LATE_SCALE_SECONDS = 18
TRIP_TRACE_EARLY_SCALE_SECONDS = 24
TRIP_TRACE_LATE_MAX_PX = 20
TRIP_TRACE_EARLY_MAX_PX = 7
MIN_TRIP_TRACE_POINTS = 2


def get_export_metadata(db_path: Path) -> dict[str, Any]:
    """Read the serving artifact metadata row."""
    return (
        fetch_one(
            db_path,
            """
            select
                export_id,
                export_version,
                source_mode,
                exported_at,
                source_row_count,
                duckdb_file_size_bytes
            from export_metadata
            limit 1
            """,
        )
        or {}
    )


def get_overview(db_path: Path, selected_date: str | None) -> dict[str, Any]:
    """Build the network overview page data."""
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    mode_stats = fetch_all(
        db_path,
        """
        select
            mode,
            sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
            sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate
        from agg_line_daily
        where service_date = ?
        group by mode
        order by mode
        """,
        [selected_date],
    )
    worst_lines = fetch_all(
        db_path,
        """
        with ranked as (
            select
                line,
                mode,
                route_short_name,
                sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                row_number() over (
                    partition by mode
                    order by sum(median_delay_seconds * n) / nullif(sum(n), 0) desc
                ) as row_number
            from agg_line_daily
            where service_date = ?
            group by line, mode, route_short_name
            having sum(n) >= 100
        )
        select line, mode, route_short_name, median_delay_seconds, on_time_rate
        from ranked
        where row_number <= 8
        order by mode, median_delay_seconds desc
        """,
        [selected_date],
    )
    for line in worst_lines:
        line["shape"] = _delay_shape(line.get("median_delay_seconds"), line.get("on_time_rate"))

    worst_stops = fetch_all(
        db_path,
        """
        with ranked as (
            select
                stop_id,
                stop_group_id,
                mode,
                stop_group_name,
                quantile_cont(delay_seconds, 0.5) as median_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate,
                row_number() over (partition by mode order by quantile_cont(delay_seconds, 0.5) desc) as row_number
            from fct_stop_arrival
            where service_date = ?
              and trip_quality = 'complete'
            group by stop_id, stop_group_id, mode, stop_group_name
            having count(*) >= 10
        )
        select stop_id, stop_group_id, mode, stop_group_name, median_delay_seconds, on_time_rate
        from ranked
        where row_number <= 8
        order by mode, median_delay_seconds desc
        """,
        [selected_date],
    )
    for stop in worst_stops:
        stop["post_label"] = _stop_post_label(stop["stop_id"], stop["stop_group_id"])
        stop["display_name"] = f"{stop['stop_group_name']} [{stop['post_label']}]"
        stop["shape"] = _delay_shape(stop.get("median_delay_seconds"), stop.get("on_time_rate"))

    mode_stats_by_mode = _by_mode(mode_stats)
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "mode_stats": mode_stats_by_mode,
        "overview_widgets": _overview_widgets(mode_stats_by_mode, selected_date),
        "worst_lines": _by_mode_list(worst_lines),
        "worst_stops": _by_mode_list(worst_stops),
        "delay_plots": {
            "bus": _delay_points(db_path, selected_date, mode="bus"),
            "tram": _delay_points(db_path, selected_date, mode="tram"),
        },
    }


def get_lines(
    db_path: Path,
    selected_line: str | None,
    selected_mode: str | None,
    selected_date: str | None,
    selected_rank: str | None,
) -> dict[str, Any]:
    """Build the line picker and selected-line page data."""
    selected_mode = selected_mode or "bus"
    selected_rank = _selected_line_rank(selected_rank)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    line_list = fetch_all(
        db_path,
        """
        select
            line,
            mode,
            route_short_name,
            sum(trip_count) as trip_count,
            sum(n) as arrival_count,
            sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
            sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
            sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
            sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate
        from agg_line_daily
        where service_date = ?
          and mode = ?
        group by line, mode, route_short_name
        order by mode, try_cast(line as integer), line
        """,
        [selected_date, selected_mode],
    )

    summary = None
    courses: list[dict[str, Any]] = []
    line_landing_summary: dict[str, Any] | None = None
    line_landing_rows: list[dict[str, Any]] = []
    if selected_line is not None:
        summary = fetch_one(
            db_path,
            """
            select
                line,
                any_value(mode) as mode,
                any_value(route_short_name) as route_short_name,
                sum(trip_count) as trip_count,
                sum(n) as arrival_count,
                sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
                sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate
            from agg_line_daily
            where line = ?
              and service_date = ?
            group by line
            """,
            [selected_line, selected_date],
        )
        courses = fetch_all(
            db_path,
            """
            select
                direction_id,
                trip_headsign,
                count(distinct trip_id) as trip_count
            from fct_stop_arrival
            where line = ?
              and service_date = ?
              and trip_quality = 'complete'
            group by direction_id, trip_headsign
            order by trip_count desc, direction_id, trip_headsign
            limit 2
            """,
            [selected_line, selected_date],
        )
        for course in courses:
            course["stops"] = fetch_all(
                db_path,
                """
                select
                    any_value(stop_group_id) as stop_group_id,
                    stop_sequence,
                    any_value(stop_name) as stop_name,
                    avg(delay_seconds) as mean_delay_seconds,
                    count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate
                from fct_stop_arrival
                where line = ?
                  and service_date = ?
                  and direction_id = ?
                  and trip_headsign = ?
                  and trip_quality = 'complete'
                group by stop_sequence
                having count(*) >= 3
                order by stop_sequence
                limit ?
                """,
                [selected_line, selected_date, course["direction_id"], course["trip_headsign"], STOP_ROWS_PER_COURSE],
            )
    else:
        line_landing_summary = _line_landing_summary(db_path, selected_date, selected_mode)
        line_landing_rows = _line_landing_rows(db_path, selected_date, selected_mode, selected_rank)
    line_widgets = _line_widgets(selected_line, selected_date, summary, courses)
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "line_list": line_list,
        "selected_line": selected_line,
        "selected_mode": selected_mode,
        "selected_rank": selected_rank,
        "summary": summary,
        "courses": courses,
        "line_widgets": line_widgets,
        "line_landing_summary": line_landing_summary,
        "line_landing_rows": line_landing_rows,
        "delay_plot": _delay_points(db_path, selected_date, line=selected_line) if selected_line is not None else [],
    }


def get_stops(  # noqa: PLR0913
    db_path: Path,
    selected_stop_group_id: str | None,
    selected_mode: str | None,
    search: str,
    selected_stop_id: str | None,
    selected_date: str | None,
    selected_view: str | None = None,
    selected_rank: str | None = None,
) -> dict[str, Any]:
    """Build the stop picker and selected-stop page data."""
    selected_mode = selected_mode or "bus"
    selected_view = selected_view if selected_view in {"post", "line"} else "post"
    selected_rank = _selected_stop_rank(selected_rank)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    search_pattern = f"%{search.strip().lower()}%"
    stop_list = fetch_all(
        db_path,
        """
        select
            stop_group_id,
            stop_group_name,
            modes_served
        from dim_stop_group_current
        where list_contains(str_split(modes_served, ', '), ?)
          and (? = '%%' or lower(stop_group_name) like ?)
        order by stop_group_name
        limit ?
        """,
        [selected_mode, search_pattern, search_pattern, STOP_PICKER_LIMIT],
    )

    summary = None
    stop_landing_summary: dict[str, Any] | None = None
    stop_landing_rows: list[dict[str, Any]] = []
    stop_posts: list[dict[str, Any]] = []
    stop_post_groups: list[dict[str, Any]] = []
    selected_post: dict[str, Any] | None = None
    line_stats: list[dict[str, Any]] = []
    post_line_stats: list[dict[str, Any]] = []
    stop_line_groups: list[dict[str, Any]] = []
    if selected_stop_group_id is not None:
        summary = fetch_one(
            db_path,
            """
            select
                stop_group_id,
                any_value(stop_group_name) as stop_group_name,
                avg(delay_seconds) as mean_delay_seconds,
                quantile_cont(delay_seconds, 0.5) as median_delay_seconds,
                quantile_cont(delay_seconds, 0.9) as p90_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate
            from fct_stop_arrival
            where stop_group_id = ?
              and service_date = ?
              and trip_quality = 'complete'
            group by stop_group_id
            """,
            [selected_stop_group_id, selected_date],
        )
        stop_posts = fetch_all(
            db_path,
            """
            with observed_posts as (
                select
                    stop_id,
                    string_agg(distinct mode, ', ' order by mode) as observed_modes,
                    avg(delay_seconds) as mean_delay_seconds,
                    quantile_cont(delay_seconds, 0.5) as median_delay_seconds,
                    quantile_cont(delay_seconds, 0.9) as p90_delay_seconds,
                    count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate,
                    count(*) as arrival_count
                from fct_stop_arrival
                where stop_group_id = ?
                  and service_date = ?
                  and trip_quality = 'complete'
                group by stop_id
            )
            select
                post.stop_id,
                post.stop_name,
                post.stop_lat,
                post.stop_lon,
                post.stop_group_id,
                observed_posts.observed_modes as modes_served,
                observed_posts.mean_delay_seconds,
                observed_posts.median_delay_seconds,
                observed_posts.p90_delay_seconds,
                observed_posts.on_time_rate,
                observed_posts.arrival_count
            from dim_stop_post_current as post
            inner join observed_posts
                on post.stop_id = observed_posts.stop_id
            where post.stop_group_id = ?
            order by post.stop_id
            """,
            [selected_stop_group_id, selected_date, selected_stop_group_id],
        )
        for post in stop_posts:
            post["display_name"] = _stop_post_label(post["stop_id"], post["stop_group_id"])
            post["mode_groups"] = _stop_post_mode_groups(post["modes_served"])
        stop_posts.sort(key=_stop_post_band_sort_key)
        post_line_stats = fetch_all(
            db_path,
            """
            select
                stop_id,
                line,
                mode,
                route_short_name,
                trip_headsign,
                avg(delay_seconds) as mean_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate
            from fct_stop_arrival
            where stop_group_id = ?
              and service_date = ?
              and trip_quality = 'complete'
            group by stop_id, line, mode, route_short_name, trip_headsign
            order by stop_id, try_cast(line as integer), line, trip_headsign
            """,
            [selected_stop_group_id, selected_date],
        )
        lines_by_post = _collapse_lines_by_post(post_line_stats)
        line_groups_by_post = _group_lines_by_destination(post_line_stats)
        posts_by_id = {post["stop_id"]: post for post in stop_posts}
        stop_line_groups = _group_posts_by_line(post_line_stats, posts_by_id)
        for post in stop_posts:
            post["lines"] = lines_by_post.get(post["stop_id"], [])
            post["line_groups"] = line_groups_by_post.get(post["stop_id"], [])
        stop_post_groups = _group_stop_posts(stop_posts)
        selected_stop_id = _resolve_stop_post_id(stop_posts, selected_stop_id)
        if selected_stop_id is None and len(stop_posts) == 1:
            selected_stop_id = stop_posts[0]["stop_id"]

        if selected_stop_id is not None:
            selected_post = fetch_one(
                db_path,
                """
                select
                    stop_id,
                    stop_group_id,
                    any_value(stop_group_name) as stop_group_name,
                    any_value(stop_name) as stop_name,
                    avg(delay_seconds) as mean_delay_seconds,
                    quantile_cont(delay_seconds, 0.5) as median_delay_seconds,
                    quantile_cont(delay_seconds, 0.9) as p90_delay_seconds,
                    count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate,
                    count(*) as arrival_count
                from fct_stop_arrival
                where stop_id = ?
                  and service_date = ?
                  and trip_quality = 'complete'
                group by stop_id, stop_group_id
                """,
                [selected_stop_id, selected_date],
            )
            if selected_post is not None:
                selected_post["display_name"] = _stop_post_label(
                    selected_post["stop_id"], selected_post["stop_group_id"]
                )
        line_stats = fetch_all(
            db_path,
            """
            select
                line,
                mode,
                route_short_name,
                direction_id,
                trip_headsign,
                avg(delay_seconds) as mean_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate
            from fct_stop_arrival
            where ((? is not null and stop_id = ?) or (? is null and stop_group_id = ?))
              and service_date = ?
              and trip_quality = 'complete'
            group by line, mode, route_short_name, direction_id, trip_headsign
            having count(*) >= 3
            order by mean_delay_seconds desc
            limit 30
            """,
            [selected_stop_id, selected_stop_id, selected_stop_id, selected_stop_group_id, selected_date],
        )
    else:
        stop_landing_summary = _stop_landing_summary(db_path, selected_date, selected_mode)
        stop_landing_rows = _stop_landing_rows(db_path, selected_date, selected_mode, selected_rank)
    stop_widgets = _stop_widgets(stop_posts, selected_post or summary, line_stats)
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "stop_list": stop_list,
        "selected_stop_group_id": selected_stop_group_id,
        "selected_mode": selected_mode,
        "selected_view": selected_view,
        "selected_rank": selected_rank,
        "search": search,
        "selected_stop_id": selected_stop_id,
        "summary": summary,
        "stop_posts": stop_posts,
        "stop_post_groups": stop_post_groups,
        "selected_post": selected_post,
        "line_stats": line_stats,
        "stop_line_groups": stop_line_groups,
        "stop_widgets": stop_widgets,
        "stop_landing_summary": stop_landing_summary,
        "stop_landing_rows": stop_landing_rows,
        "delay_plot": _delay_points(db_path, selected_date, stop_id=selected_stop_id)
        if selected_stop_id is not None
        else [],
    }


def get_schedule(  # noqa: PLR0913
    db_path: Path,
    selected_mode: str | None,
    selected_line: str | None,
    selected_date: str | None,
    selected_trip_id: str | None,
    selected_vehicle: str | None,
    selected_sort: str | None = None,
    selected_rank: str | None = None,
) -> dict[str, Any]:
    """Build the individual trip schedule page data."""
    selected_mode = selected_mode or "bus"
    selected_sort = selected_sort if selected_sort in {"departure", "delay", "erratic"} else "departure"
    selected_rank = _selected_trip_rank(selected_rank)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    line_list = fetch_all(
        db_path,
        """
        select line, mode, route_short_name, count(*) as trip_count
        from fct_trip
        where service_date = ?
          and mode = ?
          and trip_quality = 'complete'
        group by line, mode, route_short_name
        order by mode, try_cast(line as integer), line
        """,
        [selected_date, selected_mode],
    )

    trips: list[dict[str, Any]] = []
    trip_landing_summary: dict[str, Any] | None = None
    trip_landing_rows: list[dict[str, Any]] = []
    selected_trip: dict[str, Any] | None = None
    trip_stops: list[dict[str, Any]] = []
    if selected_line is not None:
        trips = fetch_all(
            db_path,
            """
            select
                trips.trip_id,
                trips.vehicle_number,
                trips.route_short_name,
                trips.direction_id,
                trips.trip_headsign,
                trips.origin_stop_name,
                trips.destination_stop_name,
                trips.scheduled_start_time,
                trips.scheduled_end_time,
                trips.start_delay_seconds,
                trips.end_delay_seconds,
                trips.stops_expected,
                trips.stops_detected,
                stop_arrivals.delay_profile
            from fct_trip as trips
            left join (
                select
                    service_date,
                    trip_id,
                    vehicle_number,
                    list(delay_seconds order by stop_sequence) as delay_profile
                from fct_stop_arrival
                where service_date = ?
                  and trip_quality = 'complete'
                group by service_date, trip_id, vehicle_number
            ) as stop_arrivals
                on trips.service_date = stop_arrivals.service_date
                and trips.trip_id = stop_arrivals.trip_id
                and trips.vehicle_number = stop_arrivals.vehicle_number
            where trips.service_date = ?
              and trips.mode = ?
              and trips.line = ?
              and trips.trip_quality = 'complete'
            order by trips.scheduled_start_time, trips.trip_headsign, trips.vehicle_number
            limit 160
            """,
            [selected_date, selected_date, selected_mode, selected_line],
        )
        for trip in trips:
            trip["trace"] = _trip_trace(trip.get("delay_profile") or [])
            trip["erratic_score"] = _trip_erratic_score(trip.get("delay_profile") or [])
        trips.sort(key=lambda trip: _trip_sort_key(trip, selected_sort))
        if trips and not _trip_selected(trips, selected_trip_id, selected_vehicle):
            selected_trip_id = trips[0]["trip_id"]
            selected_vehicle = trips[0]["vehicle_number"]
        selected_trip = _find_trip(trips, selected_trip_id, selected_vehicle)
        if selected_trip is not None:
            trip_stops = fetch_all(
                db_path,
                """
                select
                    stop_sequence,
                    stop_group_id,
                    stop_name,
                    scheduled_arrival_time,
                    actual_arrival_time,
                    delay_seconds
                from fct_stop_arrival
                where service_date = ?
                  and trip_id = ?
                  and vehicle_number = ?
                order by stop_sequence
                """,
                [selected_date, selected_trip["trip_id"], selected_trip["vehicle_number"]],
            )
    else:
        trip_landing_summary = _trip_landing_summary(db_path, selected_date, selected_mode)
        trip_landing_rows = _trip_landing_rows(db_path, selected_date, selected_mode, selected_rank)

    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "selected_mode": selected_mode,
        "selected_line": selected_line,
        "selected_trip_id": selected_trip_id,
        "selected_vehicle": selected_vehicle,
        "selected_sort": selected_sort,
        "selected_rank": selected_rank,
        "line_list": line_list,
        "trips": trips,
        "trip_groups": _trip_groups(trips),
        "trip_landing_summary": trip_landing_summary,
        "trip_landing_rows": trip_landing_rows,
        "selected_trip": selected_trip,
        "trip_stops": trip_stops,
    }


def get_trip_detail(
    db_path: Path,
    trip_id: str,
    selected_date: str | None,
    selected_vehicle: str | None,
) -> dict[str, Any]:
    """Build the individual observed trip page data."""
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    trip = fetch_one(
        db_path,
        """
        select
            gtfs_snapshot_id,
            gps_date,
            service_date,
            trip_id,
            vehicle_number,
            line,
            route_short_name,
            mode,
            brigade,
            vehicle_type,
            direction_id,
            trip_headsign,
            origin_stop_name,
            destination_stop_name,
            scheduled_start_time,
            scheduled_end_time,
            actual_start_time,
            actual_end_time,
            start_delay_seconds,
            end_delay_seconds,
            trip_quality
        from fct_trip
        where service_date = ?
          and trip_id = ?
          and (? is null or vehicle_number = ?)
        order by
            case trip_quality
                when 'complete' then 3
                when 'partial' then 2
                when 'broken' then 1
                else 0
            end desc,
            actual_end_time desc,
            vehicle_number
        limit 1
        """,
        [selected_date, trip_id, selected_vehicle, selected_vehicle],
    )
    trip_stops: list[dict[str, Any]] = []
    if trip is not None:
        trip_stops = fetch_all(
            db_path,
            """
            select
                stop_sequence,
                stop_id,
                stop_group_id,
                stop_name,
                scheduled_arrival_time,
                delay_seconds
            from fct_stop_arrival
            where service_date = ?
              and trip_id = ?
              and vehicle_number = ?
            order by stop_sequence
            """,
            [selected_date, trip["trip_id"], trip["vehicle_number"]],
        )
        for stop in trip_stops:
            stop["post_label"] = _stop_post_label(stop["stop_id"], stop["stop_group_id"])
        trip["trace"] = _trip_trace([stop["delay_seconds"] for stop in trip_stops])

    return {
        "selected_date": selected_date,
        "trip": trip,
        "trip_stops": trip_stops,
    }


def get_status(db_path: Path) -> dict[str, Any]:
    """Build the export and pipeline status page data."""
    pipeline_status = fetch_all(
        db_path,
        """
        select
            service_date,
            mode,
            completeness_ratio,
            match_rate,
            service_coverage_ratio,
            trips_complete,
            trips_partial,
            trips_broken,
            stop_arrivals_count,
            latest_gtfs_snapshot_at
        from mart_pipeline_status
        order by service_date desc, mode
        limit 16
        """,
    )
    return {
        "metadata": get_export_metadata(db_path),
        "pipeline_status": _by_mode_list(pipeline_status),
    }


def _line_landing_summary(db_path: Path, selected_date: str | None, selected_mode: str) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
            select
                count(distinct line) as line_count,
                sum(n) as arrival_count,
                sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate
            from agg_line_daily
            where service_date = ?
              and mode = ?
            """,
            [selected_date, selected_mode],
        )
        or {}
    )


def _line_landing_rows(
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_rank: str,
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        with line_stats as (
            select
                line,
                mode,
                route_short_name,
                sum(trip_count) as trip_count,
                sum(n) as arrival_count,
                sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
                sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                sum((p90_delay_seconds - median_delay_seconds) * n) / nullif(sum(n), 0) as delay_spread_seconds
            from agg_line_daily
            where service_date = ?
              and mode = ?
            group by line, mode, route_short_name
            having sum(n) >= 20
        ),

        headsign_counts as (
            select line, mode, trip_headsign, count(*) as arrival_count
            from fct_stop_arrival
            where service_date = ?
              and mode = ?
              and trip_quality = 'complete'
            group by line, mode, trip_headsign
        ),

        ranked_heads as (
            select
                line,
                mode,
                trip_headsign,
                arrival_count,
                row_number() over (partition by line, mode order by arrival_count desc, trip_headsign) as head_rank
            from headsign_counts
        ),

        route_labels as (
            select line, mode, string_agg(trip_headsign, ' -> ' order by head_rank) as route_label
            from ranked_heads
            where head_rank <= 2
            group by line, mode
        )

        select
            line_stats.line,
            line_stats.mode,
            line_stats.route_short_name,
            coalesce(route_labels.route_label, line_stats.route_short_name) as route_label,
            line_stats.trip_count,
            line_stats.arrival_count,
            line_stats.mean_delay_seconds,
            line_stats.median_delay_seconds,
            line_stats.p90_delay_seconds,
            line_stats.on_time_rate,
            line_stats.delay_spread_seconds
        from line_stats
        left join route_labels
            on line_stats.line = route_labels.line
            and line_stats.mode = route_labels.mode
        """,
        [selected_date, selected_mode, selected_date, selected_mode],
    )
    for row in rows:
        row["shape"] = _delay_shape(row.get("median_delay_seconds"), row.get("on_time_rate"))
    return sorted(rows, key=lambda row: _line_landing_sort_key(row, selected_rank))[:LANDING_ROW_LIMIT]


def _stop_landing_summary(db_path: Path, selected_date: str | None, selected_mode: str) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
            select
                count(distinct stop_group_id) as stop_group_count,
                count(*) as arrival_count,
                quantile_cont(delay_seconds, 0.5) as median_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate
            from fct_stop_arrival
            where service_date = ?
              and mode = ?
              and trip_quality = 'complete'
            """,
            [selected_date, selected_mode],
        )
        or {}
    )


def _stop_landing_rows(
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_rank: str,
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        with stop_stats as (
            select
                stop_group_id,
                any_value(stop_group_name) as stop_group_name,
                avg(delay_seconds) as mean_delay_seconds,
                quantile_cont(delay_seconds, 0.5) as median_delay_seconds,
                quantile_cont(delay_seconds, 0.9) as p90_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate,
                count(*) as arrival_count,
                count(distinct line) as line_count
            from fct_stop_arrival
            where service_date = ?
              and mode = ?
              and trip_quality = 'complete'
            group by stop_group_id
            having count(*) >= 10
        )

        select
            stop_stats.stop_group_id,
            stop_stats.stop_group_name,
            stop_stats.mean_delay_seconds,
            stop_stats.median_delay_seconds,
            stop_stats.p90_delay_seconds,
            stop_stats.on_time_rate,
            stop_stats.arrival_count,
            stop_stats.line_count,
            stop_stats.p90_delay_seconds - stop_stats.median_delay_seconds as delay_spread_seconds
        from stop_stats
        """,
        [selected_date, selected_mode],
    )
    for row in rows:
        row["shape"] = _delay_shape(row.get("median_delay_seconds"), row.get("on_time_rate"))
    return sorted(rows, key=lambda row: _stop_landing_sort_key(row, selected_rank))[:LANDING_ROW_LIMIT]


def _trip_landing_summary(db_path: Path, selected_date: str | None, selected_mode: str) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
            select
                count(*) as trip_count,
                quantile_cont(end_delay_seconds, 0.5) as median_delay_seconds,
                count(*) filter (where end_delay_seconds between -60 and 180) / count(*) as on_time_rate
            from fct_trip
            where service_date = ?
              and mode = ?
              and trip_quality = 'complete'
            """,
            [selected_date, selected_mode],
        )
        or {}
    )


def _trip_landing_rows(
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_rank: str,
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        with stop_arrivals as (
            select
                service_date,
                trip_id,
                vehicle_number,
                list(delay_seconds order by stop_sequence) as delay_profile
            from fct_stop_arrival
            where service_date = ?
              and mode = ?
              and trip_quality = 'complete'
            group by service_date, trip_id, vehicle_number
        )

        select
            trips.trip_id,
            trips.vehicle_number,
            trips.line,
            trips.mode,
            trips.route_short_name,
            trips.trip_headsign,
            trips.origin_stop_name,
            trips.destination_stop_name,
            trips.scheduled_start_time,
            trips.scheduled_end_time,
            trips.end_delay_seconds,
            stop_arrivals.delay_profile
        from fct_trip as trips
        left join stop_arrivals
            on trips.service_date = stop_arrivals.service_date
            and trips.trip_id = stop_arrivals.trip_id
            and trips.vehicle_number = stop_arrivals.vehicle_number
        where trips.service_date = ?
          and trips.mode = ?
          and trips.trip_quality = 'complete'
        """,
        [selected_date, selected_mode, selected_date, selected_mode],
    )
    for row in rows:
        delay_profile = row.get("delay_profile") or []
        row["trace"] = _trip_trace(delay_profile)
        row["erratic_score"] = _trip_erratic_score(delay_profile)
        row["route_label"] = f"{row['origin_stop_name']} -> {row['destination_stop_name']}"
    return sorted(rows, key=lambda row: _trip_landing_sort_key(row, selected_rank))[:LANDING_ROW_LIMIT]


def _selected_line_rank(value: str | None) -> str:
    if value in {"worst", "best", "erratic"}:
        return value
    return "worst"


def _selected_trip_rank(value: str | None) -> str:
    if value in {"worst", "best", "erratic"}:
        return value
    return "worst"


def _selected_stop_rank(value: str | None) -> str:
    if value in {"worst", "busiest", "best"}:
        return value
    return "worst"


def _line_landing_sort_key(row: dict[str, Any], selected_rank: str) -> tuple[float, ...]:
    if selected_rank == "best":
        return (
            -_sort_number(row, "on_time_rate", -1),
            _sort_number(row, "median_delay_seconds", 1_000_000),
            -_sort_number(row, "arrival_count", 0),
        )
    if selected_rank == "erratic":
        return (
            -_sort_number(row, "delay_spread_seconds", -1_000_000),
            -_sort_number(row, "p90_delay_seconds", -1_000_000),
            -_sort_number(row, "arrival_count", 0),
        )
    return (
        -_sort_number(row, "median_delay_seconds", -1_000_000),
        _sort_number(row, "on_time_rate", 1_000_000),
        -_sort_number(row, "arrival_count", 0),
    )


def _stop_landing_sort_key(row: dict[str, Any], selected_rank: str) -> tuple[float, ...]:
    if selected_rank == "best":
        return (
            -_sort_number(row, "on_time_rate", -1),
            _sort_number(row, "median_delay_seconds", 1_000_000),
            -_sort_number(row, "arrival_count", 0),
        )
    if selected_rank == "busiest":
        return (-_sort_number(row, "arrival_count", 0), -_sort_number(row, "median_delay_seconds", -1_000_000))
    return (
        -_sort_number(row, "median_delay_seconds", -1_000_000),
        _sort_number(row, "on_time_rate", 1_000_000),
        -_sort_number(row, "arrival_count", 0),
    )


def _trip_landing_sort_key(row: dict[str, Any], selected_rank: str) -> tuple[Any, ...]:
    end_delay = _sort_number(row, "end_delay_seconds", 0)
    if selected_rank == "best":
        return (
            abs(end_delay),
            row.get("scheduled_start_time"),
        )
    if selected_rank == "erratic":
        return (-_sort_number(row, "erratic_score", -1_000_000), -end_delay)
    return (-abs(end_delay), -end_delay, row.get("scheduled_start_time"))


def _sort_number(row: dict[str, Any], key: str, default: float) -> float:
    value = row.get(key)
    if value is None:
        return default
    return float(value)


def _date_nav(date_options: list[str], selected_date: str | None) -> dict[str, str | None]:
    if selected_date not in date_options:
        return {"previous": None, "next": None}
    selected_index = date_options.index(selected_date)
    return {
        "previous": date_options[selected_index + 1] if selected_index + 1 < len(date_options) else None,
        "next": date_options[selected_index - 1] if selected_index > 0 else None,
    }


def _overview_widgets(mode_stats: dict[str, dict[str, Any]], selected_date: str | None) -> dict[str, dict[str, Any]]:
    widgets = {}
    for mode in ("bus", "tram"):
        row = mode_stats.get(mode, {})
        baseline = row.get("mean_delay_seconds") or 45
        on_time_rate = row.get("on_time_rate") or 0.75
        seed = _seed(mode, selected_date)
        widgets[mode] = {
            "shape": _delay_shape(baseline, on_time_rate),
            "hours": _hour_bars(seed, baseline),
            "week": _week_bars(seed, selected_date, baseline),
            "segments": _on_time_segments(on_time_rate),
        }
    return widgets


def _line_widgets(
    selected_line: str | None,
    selected_date: str | None,
    summary: dict[str, Any] | None,
    courses: list[dict[str, Any]],
) -> dict[str, Any]:
    if summary is None:
        return {}

    baseline = summary.get("median_delay_seconds") or summary.get("mean_delay_seconds") or 45
    on_time_rate = summary.get("on_time_rate") or 0.75
    seed = _seed(selected_line, selected_date)
    stops = []
    for course in courses:
        for stop in course["stops"]:
            stop["shape"] = _delay_shape(stop.get("mean_delay_seconds"), stop.get("on_time_rate"))
            stop["direction"] = course["trip_headsign"]
            stops.append(stop)

    return {
        "shape": _delay_shape(baseline, on_time_rate),
        "hours": _hour_bars(seed, baseline),
        "week": _week_bars(seed, selected_date, baseline),
        "timeline": _timeline(seed, baseline, 100),
        "segments": _on_time_segments(on_time_rate),
        "worst": _line_worst_departures(stops, seed),
        "reliability": _reliability_strip(courses, seed),
    }


def _stop_widgets(
    stop_posts: list[dict[str, Any]], summary: dict[str, Any] | None, line_stats: list[dict[str, Any]]
) -> dict[str, Any]:
    if summary is None:
        return {"posts": [], "worst": [], "line_rows": []}

    baseline = summary.get("median_delay_seconds") or summary.get("mean_delay_seconds") or 45
    on_time_rate = summary.get("on_time_rate") or 0.75
    seed = _seed(summary.get("stop_group_id"), summary.get("stop_id"))
    posts = []
    for index, post in enumerate(stop_posts):
        post_seed = _seed(post["stop_id"], index)
        median = post.get("median_delay_seconds") or baseline + ((post_seed % 90) - 35)
        post_rate = post.get("on_time_rate") or _clamp(on_time_rate + ((post_seed % 25) - 12) / 100, 0.35, 0.98)
        posts.append(
            {
                **post,
                "median_delay_seconds": median,
                "on_time_rate": post_rate,
                "shape": _delay_shape(median, post_rate),
                "hours": _hour_bars(post_seed, median),
                "lines": post.get("lines", []),
                "line_groups": post.get("line_groups", []),
            }
        )

    line_rows = []
    fallback_post = stop_posts[0]["display_name"] if stop_posts else ""
    for row in line_stats:
        line_row = {**row}
        line_row["shape"] = _delay_shape(row.get("mean_delay_seconds"), row.get("on_time_rate"))
        line_row["post_label"] = fallback_post
        line_rows.append(line_row)

    return {
        "posts": posts,
        "shape": _delay_shape(baseline, on_time_rate),
        "hours": _hour_bars(seed, baseline),
        "week": _week_bars(seed, None, baseline),
        "timeline": _timeline(seed, baseline, 130),
        "segments": _on_time_segments(on_time_rate),
        "worst": _stop_worst_departures(line_stats, seed),
        "line_rows": line_rows,
    }


def _delay_shape(median_delay_seconds: float | None, on_time_rate: float | None) -> dict[str, Any]:
    median = round(median_delay_seconds or 0)
    rate = on_time_rate if on_time_rate is not None else 0.75
    peak = round(_clamp(2 + ((median + 60) / 360 * 9), 1, 10))
    spread = 2 if rate >= LOW_ON_TIME_RATE else 3
    buckets = []
    for index in range(HISTOGRAM_BUCKET_COUNT):
        height = max(3, HISTOGRAM_MAX_HEIGHT - abs(index - peak) * spread * 2)
        buckets.append(
            {
                "height": height,
                "mini_height": max(2, round(height / HISTOGRAM_MAX_HEIGHT * HISTOGRAM_MINI_MAX_HEIGHT)),
                "tone": _bucket_tone(index),
            }
        )
    p90 = median + (130 if rate < LOW_ON_TIME_RATE else 90)
    return {
        "buckets": buckets,
        "median_x": _clamp((median + 60) / 360 * 100, 2, 98),
        "p90_x": _clamp((p90 + 60) / 360 * 100, 5, 99),
    }


def _hour_bars(seed: int, baseline: float) -> list[dict[str, Any]]:
    bars = []
    for index, hour in enumerate(SERVICE_DAY_HOURS):
        if hour in NEXT_DAY_PLACEHOLDER_HOURS:
            bars.append({"hour": hour, "delay": None, "height": 0, "tone": "empty"})
            continue
        rush = 55 if AM_RUSH_START_HOUR <= hour <= AM_RUSH_END_HOUR else 0
        if PM_RUSH_START_HOUR <= hour <= PM_RUSH_END_HOUR:
            rush = 70
        wobble = ((seed + index * 37) % 90) - 35
        delay = round(baseline + rush + wobble)
        bars.append({"hour": hour, "delay": delay, "height": _bar_height(delay), "tone": _delay_tone(delay)})
    return bars


def _week_bars(seed: int, selected_date: str | None, baseline: float) -> list[dict[str, Any]]:
    selected_weekday = _selected_weekday(selected_date)
    labels = ["M", "T", "W", "T", "F", "S", "S"]
    bars = []
    for index, label in enumerate(labels):
        weekend_offset = -25 if index >= WEEKEND_START_INDEX else 0
        delay = round(baseline + weekend_offset + ((seed + index * 29) % 70) - 25)
        bars.append(
            {
                "label": label,
                "delay": delay,
                "height": max(4, min(38, round(abs(delay) * 0.35))),
                "selected": index == selected_weekday,
            }
        )
    return bars


def _timeline(seed: int, baseline: float, count: int) -> list[dict[str, Any]]:
    points = []
    for index in range(count):
        rush = 85 if index % 31 in range(8, 13) else 75 if index % 47 in range(32, 39) else 0
        delay = round(baseline + rush + ((seed + index * 41) % 150) - 55)
        points.append(
            {
                "x": index / max(1, count - 1) * 100,
                "delay": delay,
                "height": max(2, min(30, round(abs(delay) / 8))),
                "tone": _delay_tone(delay),
            }
        )
    return points


def _line_worst_departures(stops: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    worst_stops = sorted(stops, key=lambda row: row.get("mean_delay_seconds") or 0, reverse=True)[:6]
    rows = []
    for index, stop in enumerate(worst_stops):
        rows.append(
            {
                "time": _fake_time(seed, index),
                "direction": stop.get("direction", ""),
                "stop_name": stop.get("stop_name"),
                "stop_group_id": stop.get("stop_group_id"),
                "delay_seconds": (stop.get("mean_delay_seconds") or 0) + 120 + index * 17,
            }
        )
    return sorted(rows, key=lambda row: row["delay_seconds"], reverse=True)


def _stop_worst_departures(line_stats: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    rows = []
    for index, row in enumerate(line_stats[:8]):
        rows.append(
            {
                "time": _fake_time(seed, index),
                "line": row.get("route_short_name") or row.get("line"),
                "mode": row.get("mode"),
                "headsign": row.get("trip_headsign"),
                "delay_seconds": (row.get("mean_delay_seconds") or 0) + 150 + index * 13,
            }
        )
    return rows


def _reliability_strip(courses: list[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    rows = []
    for direction_index, course in enumerate(courses):
        direction_seed = _seed(seed, course.get("trip_headsign"), direction_index)
        outcomes = []
        counts = {"clean": 0, "partial": 0, "broken": 0}
        sample_count = min(max(round((course.get("trip_count") or 18) / 2), 14), 28)
        for trip_index in range(sample_count):
            score = (direction_seed + trip_index * 37) % 100
            outcome = "broken" if score >= BROKEN_TRIP_SCORE else "partial" if score >= PARTIAL_TRIP_SCORE else "clean"
            counts[outcome] += 1
            outcomes.append({"outcome": outcome, "label": outcome.replace("clean", "ran clean")})

        rows.append(
            {
                "direction": course.get("trip_headsign"),
                "outcomes": outcomes,
                "counts": counts,
            }
        )
    return rows


def _on_time_segments(on_time_rate: float) -> dict[str, float]:
    missed = max(0, 1 - on_time_rate)
    early = missed * 0.28
    late = missed - early
    return {"early": early, "on_time": on_time_rate, "late": late}


def _selected_weekday(selected_date: str | None) -> int:
    if selected_date is None:
        return 2
    try:
        return date.fromisoformat(selected_date).weekday()
    except ValueError:
        return 2


def _fake_time(seed: int, index: int) -> str:
    minutes = 4 * 60 + ((seed + index * 73) % (20 * 60))
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _bar_height(delay: float) -> int:
    return max(3, min(46, round(abs(delay) * 0.25)))


def _bucket_tone(index: int) -> str:
    if index < EARLY_BUCKET_COUNT:
        return "early"
    if index >= LATE_BUCKET_START:
        return "late"
    return "ontime"


def _delay_tone(delay: float) -> str:
    if delay <= ON_TIME_EARLY_SECONDS:
        return "early"
    if delay >= ON_TIME_LATE_SECONDS:
        return "late"
    return "ontime"


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _seed(*parts: object) -> int:
    text = "|".join(str(part) for part in parts if part is not None)
    return sum((index + 1) * ord(char) for index, char in enumerate(text)) or 1


def _date_options(db_path: Path) -> list[str]:
    rows = fetch_all(
        db_path,
        """
        select distinct cast(service_date as varchar) as service_date
        from agg_line_daily
        order by service_date desc
        """,
    )
    return [row["service_date"] for row in rows]


def _selected_date(date_options: list[str], selected_date: str | None) -> str | None:
    if selected_date in date_options:
        return selected_date
    if date_options:
        return date_options[0]
    return None


def _delay_points(
    db_path: Path,
    service_date: str | None,
    *,
    mode: str | None = None,
    line: str | None = None,
    stop_id: str | None = None,
) -> list[dict[str, Any]]:
    if service_date is None:
        return []
    rows = fetch_all(
        db_path,
        """
        select delay_seconds,
            case
                when extract('hour' from scheduled_arrival_time) between 6 and 9 then 'Morning'
                when extract('hour' from scheduled_arrival_time) between 10 and 15 then 'Midday'
                when extract('hour' from scheduled_arrival_time) between 16 and 19 then 'Evening'
                when extract('hour' from scheduled_arrival_time) between 20 and 23 then 'Late'
                else 'Night'
            end as period_label
        from fct_stop_arrival
        where service_date = ?
          and (? is null or mode = ?)
          and (? is null or line = ?)
          and (? is null or stop_id = ?)
          and trip_quality = 'complete'
          and delay_seconds is not null
        order by hash(trip_id, vehicle_number, stop_sequence)
        limit ?
        """,
        [service_date, mode, mode, line, line, stop_id, stop_id, DELAY_POINT_LIMIT],
    )
    return _delay_periods(rows)


def _delay_periods(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    period_order = [
        ("Morning", "06-09"),
        ("Midday", "10-15"),
        ("Evening", "16-19"),
        ("Late", "20-23"),
        ("Night", "00-05"),
    ]
    delays_by_period: defaultdict[str, list[int]] = defaultdict(list)
    for row in rows:
        delays_by_period[row["period_label"]].append(row["delay_seconds"])

    periods = []
    for label, time_range in period_order:
        delays = delays_by_period[label]
        periods.append(
            {
                "label": label,
                "range": time_range,
                "points": delays,
                "on_time_rate": _on_time_rate(delays),
                "median_delay_seconds": _quantile(delays, 0.5),
                "p90_delay_seconds": _quantile(delays, 0.9),
            }
        )
    return periods


def _on_time_rate(delays: list[int]) -> float | None:
    if not delays:
        return None
    return sum(ON_TIME_EARLY_SECONDS <= delay <= ON_TIME_LATE_SECONDS for delay in delays) / len(delays)


def _quantile(values: list[int], quantile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * quantile)
    return ordered[index]


def _by_mode(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["mode"]: row for row in rows}


def _by_mode_list(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["mode"]].append(row)
    return dict(grouped)


def _trip_trace(delays: list[int]) -> list[dict[str, Any]]:
    points = []
    for delay in delays:
        tone = _delay_tone(delay)
        if delay >= 0:
            offset = min(TRIP_TRACE_LATE_MAX_PX, round(delay / TRIP_TRACE_LATE_SCALE_SECONDS))
        else:
            offset = -min(TRIP_TRACE_EARLY_MAX_PX, round(abs(delay) / TRIP_TRACE_EARLY_SCALE_SECONDS))
        points.append(
            {
                "delay": delay,
                "tone": tone,
                "dot_top": TRIP_TRACE_BASELINE - offset - 2,
                "stem_top": TRIP_TRACE_BASELINE - offset if offset >= 0 else TRIP_TRACE_BASELINE,
                "stem_height": abs(offset),
                "has_stem": offset != 0,
            }
        )
    return points


def _trip_erratic_score(delays: list[int]) -> int:
    if len(delays) < MIN_TRIP_TRACE_POINTS:
        return 0
    return max(abs(delay - previous) for previous, delay in pairwise(delays))


def _trip_sort_key(trip: dict[str, Any], selected_sort: str) -> tuple[Any, ...]:
    if selected_sort == "delay":
        return (-(trip.get("end_delay_seconds") or 0), trip["scheduled_start_time"], trip["trip_headsign"])
    if selected_sort == "erratic":
        return (-(trip.get("erratic_score") or 0), trip["scheduled_start_time"], trip["trip_headsign"])
    return (trip["scheduled_start_time"], trip["trip_headsign"], trip["vehicle_number"])


def _trip_groups(trips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], dict[str, Any]] = {}
    for trip in trips:
        key = (trip["direction_id"], trip["trip_headsign"])
        group = grouped.setdefault(
            key,
            {
                "direction_id": trip["direction_id"],
                "trip_headsign": trip["trip_headsign"],
                "origin_stop_name": trip["origin_stop_name"],
                "destination_stop_name": trip["destination_stop_name"],
                "trips": [],
            },
        )
        group["trips"].append(trip)
    return sorted(
        grouped.values(), key=lambda group: (-len(group["trips"]), group["direction_id"], group["trip_headsign"])
    )


def _trip_selected(trips: list[dict[str, Any]], trip_id: str | None, vehicle_number: str | None) -> bool:
    return _find_trip(trips, trip_id, vehicle_number) is not None


def _find_trip(trips: list[dict[str, Any]], trip_id: str | None, vehicle_number: str | None) -> dict[str, Any] | None:
    for trip in trips:
        if trip["trip_id"] == trip_id and trip["vehicle_number"] == vehicle_number:
            return trip
    return None


def _stop_post_label(stop_id: str, stop_group_id: str) -> str:
    suffix = stop_id.removeprefix(stop_group_id)
    if suffix.isdecimal() and len(suffix) == NUMERIC_STOP_POST_SUFFIX_LENGTH:
        return suffix
    if ":" in suffix:
        return suffix.rsplit(":", maxsplit=1)[-1]
    return suffix or stop_id


def _stop_post_sort_key(label: str) -> tuple[int, str, int, str]:
    if label.isdecimal():
        return (0, "", int(label), "")
    match = re.fullmatch(r"([A-Za-z]+)(\d+)", label)
    if match is not None:
        return (1, match.group(1), int(match.group(2)), "")
    return (2, label, 0, label)


def _stop_post_band_sort_key(post: dict[str, Any]) -> tuple[int, tuple[int, str, int, str]]:
    groups = post["mode_groups"]
    if "bus" in groups:
        mode_order = 0
    elif "tram" in groups:
        mode_order = 1
    else:
        mode_order = 2
    return (mode_order, _stop_post_sort_key(post["display_name"]))


def _line_sort_value(line: str) -> tuple[int, int, str]:
    if line.isdecimal():
        return (0, int(line), line)
    return (1, 0, line)


def _stop_post_mode_groups(modes_served: str) -> list[str]:
    modes = {mode.strip() for mode in modes_served.split(",") if mode.strip()}
    groups = [mode for mode in STOP_POST_PRIMARY_MODES if mode in modes]
    if groups:
        return groups
    return ["other"]


def _group_stop_posts(stop_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = [
        {"mode": "bus", "title": "Bus", "posts": []},
        {"mode": "tram", "title": "Tram", "posts": []},
        {"mode": "other", "title": "Other", "posts": []},
    ]
    by_mode = {group["mode"]: group for group in groups}
    for post in stop_posts:
        for mode in post["mode_groups"]:
            by_mode[mode]["posts"].append(post)
    return [group for group in groups if group["posts"]]


def _collapse_lines_by_post(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    collapsed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["stop_id"], row["line"], row["mode"])
        line = collapsed.setdefault(
            key,
            {
                "stop_id": row["stop_id"],
                "line": row["line"],
                "mode": row["mode"],
                "route_short_name": row["route_short_name"],
                "destinations": [],
            },
        )
        if row["trip_headsign"] not in line["destinations"]:
            line["destinations"].append(row["trip_headsign"])

    lines_by_post: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for line in collapsed.values():
        line["trip_headsign"] = ", ".join(line["destinations"])
        lines_by_post[line["stop_id"]].append(line)
    return dict(lines_by_post)


def _group_lines_by_destination(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["stop_id"], row["trip_headsign"])
        group = grouped.setdefault(
            key,
            {
                "stop_id": row["stop_id"],
                "trip_headsign": row["trip_headsign"],
                "lines": [],
                "line_keys": set(),
            },
        )
        line_key = (row["line"], row["mode"])
        if line_key in group["line_keys"]:
            continue
        group["line_keys"].add(line_key)
        group["lines"].append(
            {
                "line": row["line"],
                "mode": row["mode"],
                "route_short_name": row["route_short_name"],
            }
        )

    groups_by_post: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for group in grouped.values():
        group.pop("line_keys")
        groups_by_post[group["stop_id"]].append(group)
    for groups in groups_by_post.values():
        groups.sort(key=lambda group: group["trip_headsign"].casefold())
    return dict(groups_by_post)


def _group_posts_by_line(rows: list[dict[str, Any]], posts_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        line_key = (row["line"], row["mode"])
        line_group = grouped.setdefault(
            line_key,
            {
                "line": row["line"],
                "mode": row["mode"],
                "route_short_name": row["route_short_name"],
                "destinations": {},
            },
        )
        destinations = line_group["destinations"]
        destination = destinations.setdefault(
            row["trip_headsign"],
            {
                "trip_headsign": row["trip_headsign"],
                "posts": [],
                "post_ids": set(),
            },
        )
        if row["stop_id"] in destination["post_ids"]:
            continue
        post = posts_by_id.get(row["stop_id"])
        if post is None:
            continue
        destination["post_ids"].add(row["stop_id"])
        destination["posts"].append(
            {
                "stop_id": row["stop_id"],
                "display_name": post["display_name"],
                "on_time_rate": row.get("on_time_rate"),
                "mean_delay_seconds": row.get("mean_delay_seconds"),
            }
        )

    line_groups = []
    for line_group in grouped.values():
        destinations = []
        for destination in line_group["destinations"].values():
            destination.pop("post_ids")
            destination["posts"].sort(key=lambda post: _stop_post_sort_key(post["display_name"]))
            destinations.append(destination)
        destinations.sort(key=lambda destination: destination["trip_headsign"].casefold())
        line_group["destinations"] = destinations
        line_groups.append(line_group)
    line_groups.sort(key=lambda line: (line["mode"] != "bus", _line_sort_value(line["line"]), line["line"]))
    return line_groups


def _resolve_stop_post_id(stop_posts: list[dict[str, Any]], selected_stop_id: str | None) -> str | None:
    if selected_stop_id is None:
        return None
    for post in stop_posts:
        if selected_stop_id in {post["stop_id"], post["display_name"]}:
            return post["stop_id"]
    return None


def _default_stop_post_id(stop_posts: list[dict[str, Any]], selected_mode: str) -> str:
    for post in stop_posts:
        if selected_mode in post["mode_groups"]:
            return post["stop_id"]
    return stop_posts[0]["stop_id"]
