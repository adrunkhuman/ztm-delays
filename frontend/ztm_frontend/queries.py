from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date
from itertools import pairwise
from pathlib import Path
from typing import Any

from ztm_frontend.db import fetch_all, fetch_one

STOP_ROWS_PER_COURSE = 36
STOP_PICKER_LIMIT = 300
DELAY_POINT_LIMIT = 600
LANDING_ROW_LIMIT = 16
NUMERIC_STOP_POST_SUFFIX_LENGTH = 2
STOP_POST_PRIMARY_MODES = ("bus", "tram")
ON_TIME_EARLY_SECONDS = -60
ON_TIME_LATE_SECONDS = 180
LOW_ON_TIME_RATE = 0.6
GOOD_STATUS_HEALTH_RATIO = 0.9
USABLE_STATUS_HEALTH_RATIO = 0.7
SERVICE_DAY_HOURS = (*range(4, 24), *range(4))
HISTOGRAM_BUCKET_COUNT = 12
HISTOGRAM_MAX_HEIGHT = 38
HISTOGRAM_MINI_MAX_HEIGHT = 20
EARLY_BUCKET_COUNT = 2
LATE_BUCKET_START = 8
HISTOGRAM_BUCKET_LABELS = (
    "early_over_5m",
    "early_2_to_5m",
    "early_1_to_2m",
    "on_time_early_30_60s",
    "on_time_early_0_30s",
    "on_time_late_0_30s",
    "on_time_late_30_60s",
    "on_time_late_1_to_3m",
    "late_3_to_5m",
    "late_5_to_10m",
    "late_10_to_20m",
    "late_over_20m",
)
TRIP_TRACE_BASELINE = 16
TRIP_TRACE_LATE_SCALE_SECONDS = 18
TRIP_TRACE_EARLY_SCALE_SECONDS = 24
TRIP_TRACE_LATE_MAX_PX = 20
TRIP_TRACE_EARLY_MAX_PX = 7
MIN_TRIP_TRACE_POINTS = 2
TIMELINE_MIN_GAP_PERCENT = 0.55
MIN_HOUR_SAMPLE_SIZE = 3
EXPECTED_STOP_EVENT_TABLE = "fct_expected_stop_event"
LEGACY_STOP_EVENT_TABLE = "fct_scheduled_stop_event"


def get_export_metadata(db_path: Path) -> dict[str, Any]:
    """Read the serving artifact metadata row."""
    metadata = (
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
    sidecar_metadata = _export_metadata_sidecar(db_path)
    if sidecar_metadata:
        metadata["last_export_at"] = sidecar_metadata.get("last_export_at") or sidecar_metadata.get("exported_at")
        metadata["poller_status"] = _poller_status_metadata(sidecar_metadata.get("poller_status"))
    return metadata


def _stop_event_table(db_path: Path) -> str:
    row = fetch_one(
        db_path,
        """
        select table_name
        from information_schema.tables
        where table_schema = 'main'
          and table_name in (?, ?)
        order by case table_name when ? then 0 else 1 end
        limit 1
        """,
        [EXPECTED_STOP_EVENT_TABLE, LEGACY_STOP_EVENT_TABLE, EXPECTED_STOP_EVENT_TABLE],
    )
    if row is None:
        return EXPECTED_STOP_EVENT_TABLE
    return str(row["table_name"])


def _with_stop_event_table(sql: str, table_name: str) -> str:
    if table_name not in {EXPECTED_STOP_EVENT_TABLE, LEGACY_STOP_EVENT_TABLE}:
        raise ValueError("Unexpected stop-event table")
    return sql.replace("__STOP_EVENT_TABLE__", table_name)


def _export_metadata_sidecar(db_path: Path) -> dict[str, Any]:
    metadata_path = Path(f"{db_path}.meta.json")
    if not metadata_path.exists():
        return {}
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _poller_status_metadata(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"status": "unknown", "vehicle_types": {}}
    status = (
        value.get("status")
        if value.get("status") in {"ok", "starting", "degraded", "down", "stale", "unknown"}
        else "unknown"
    )
    vehicle_types = value.get("vehicle_types") if isinstance(value.get("vehicle_types"), dict) else {}
    return {
        "status": status,
        "updated_at": value.get("updated_at") if isinstance(value.get("updated_at"), str) else None,
        "last_success_at": value.get("last_success_at") if isinstance(value.get("last_success_at"), str) else None,
        "stale_after_seconds": value.get("stale_after_seconds")
        if isinstance(value.get("stale_after_seconds"), int)
        else None,
        "vehicle_types": vehicle_types,
    }


def get_overview(db_path: Path, selected_date: str | None) -> dict[str, Any]:
    """Build the network overview page data."""
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    mode_stats = fetch_all(
        db_path,
        """
        select
            mode,
            mean_delay_seconds,
            median_delay_seconds,
            p90_delay_seconds,
            on_time_rate,
            early_count,
            on_time_count,
            late_count,
            delay_histogram
        from agg_mode_daily
        where service_date = ?
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
                sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                flatten(list(delay_histogram)) as delay_histogram,
                row_number() over (
                    partition by mode
                    order by sum(median_delay_seconds * n) / nullif(sum(n), 0) desc
                ) as row_number
            from agg_line_daily
            where service_date = ?
            group by line, mode, route_short_name
            having sum(n) >= 100
        )
        select line, mode, route_short_name, median_delay_seconds, p90_delay_seconds, on_time_rate, delay_histogram
        from ranked
        where row_number <= 8
        order by mode, median_delay_seconds desc
        """,
        [selected_date],
    )
    for line in worst_lines:
        line["shape"] = _delay_shape(
            line.get("median_delay_seconds"),
            line.get("on_time_rate"),
            line.get("delay_histogram"),
            line.get("p90_delay_seconds"),
        )

    worst_stops = fetch_all(
        db_path,
        """
        with ranked as (
            select
                stop_group_id,
                mode,
                stop_group_name,
                stop_id,
                median_delay_seconds,
                p90_delay_seconds,
                on_time_rate,
                delay_histogram,
                row_number() over (partition by mode order by median_delay_seconds desc) as row_number
            from agg_stop_post_daily
            where service_date = ?
              and n >= 10
        )
        select stop_id, stop_group_id, mode, stop_group_name, median_delay_seconds, p90_delay_seconds, on_time_rate, delay_histogram
        from ranked
        where row_number <= 8
        order by mode, median_delay_seconds desc
        """,
        [selected_date],
    )
    for stop in worst_stops:
        stop["post_label"] = _stop_post_label(stop["stop_id"], stop["stop_group_id"])
        stop["display_name"] = f"{stop['stop_group_name']} [{stop['post_label']}]"
        stop["shape"] = _delay_shape(
            stop.get("median_delay_seconds"),
            stop.get("on_time_rate"),
            stop.get("delay_histogram"),
            stop.get("p90_delay_seconds"),
        )

    mode_stats_by_mode = _by_mode(mode_stats)
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "mode_stats": mode_stats_by_mode,
        "overview_widgets": _overview_widgets(db_path, mode_stats_by_mode, selected_date),
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
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                flatten(list(delay_histogram)) as delay_histogram
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
                sum(trip_count) as trip_count
            from agg_line_stop_daily
            where line = ?
              and service_date = ?
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
                    min_stop_sequence as stop_sequence,
                    any_value(stop_name) as stop_name,
                    sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
                    sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                    sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
                    sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                    flatten(list(delay_histogram)) as delay_histogram
                from agg_line_stop_daily
                where line = ?
                  and service_date = ?
                  and direction_id = ?
                  and trip_headsign = ?
                group by min_stop_sequence
                having sum(n) >= 3
                order by min_stop_sequence
                limit ?
                """,
                [selected_line, selected_date, course["direction_id"], course["trip_headsign"], STOP_ROWS_PER_COURSE],
            )
    else:
        line_landing_summary = _line_landing_summary(db_path, selected_date, selected_mode)
        line_landing_rows = _line_landing_rows(db_path, selected_date, selected_mode, selected_rank)
    line_widgets = _line_widgets(db_path, selected_line, selected_date, summary, courses)
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
                sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
                sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                sum(n) as arrival_count,
                sum(early_count) as early_count,
                sum(on_time_count) as on_time_count,
                sum(late_count) as late_count,
                flatten(list(delay_histogram)) as delay_histogram
            from agg_stop_group_daily
            where stop_group_id = ?
              and service_date = ?
              and mode = ?
            group by stop_group_id
            """,
            [selected_stop_group_id, selected_date, selected_mode],
        )
        stop_posts = fetch_all(
            db_path,
            """
            select
                post.stop_id,
                post.stop_name,
                post.stop_lat,
                post.stop_lon,
                post.stop_group_id,
                agg.mode as modes_served,
                agg.mean_delay_seconds,
                agg.median_delay_seconds,
                agg.p90_delay_seconds,
                agg.on_time_rate,
                agg.n as arrival_count,
                agg.early_count,
                agg.on_time_count,
                agg.late_count,
                agg.delay_histogram
            from dim_stop_post_current as post
            inner join agg_stop_post_daily as agg
                on post.stop_id = agg.stop_id
            where post.stop_group_id = ?
              and agg.service_date = ?
              and agg.mode = ?
            order by post.stop_id
            """,
            [selected_stop_group_id, selected_date, selected_mode],
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
                mean_delay_seconds,
                median_delay_seconds,
                p90_delay_seconds,
                on_time_rate,
                delay_histogram,
                n as arrival_count
            from agg_stop_line_daily
            where stop_group_id = ?
              and service_date = ?
              and mode = ?
            order by stop_id, try_cast(line as integer), line, trip_headsign
            """,
            [selected_stop_group_id, selected_date, selected_mode],
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
                    sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
                    sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                    sum(p90_delay_seconds * n) / nullif(sum(n), 0) as p90_delay_seconds,
                    sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                    sum(n) as arrival_count,
                    sum(early_count) as early_count,
                    sum(on_time_count) as on_time_count,
                    sum(late_count) as late_count,
                    flatten(list(delay_histogram)) as delay_histogram
                from agg_stop_post_daily
                where stop_id = ?
                  and service_date = ?
                  and mode = ?
                group by stop_id, stop_group_id
                """,
                [selected_stop_id, selected_date, selected_mode],
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
                mean_delay_seconds,
                median_delay_seconds,
                p90_delay_seconds,
                on_time_rate,
                delay_histogram,
                n as arrival_count
            from agg_stop_line_daily
            where ((? is not null and stop_id = ?) or (? is null and stop_group_id = ?))
              and service_date = ?
              and mode = ?
              and n >= 3
            order by mean_delay_seconds desc
            limit 30
            """,
            [
                selected_stop_id,
                selected_stop_id,
                selected_stop_id,
                selected_stop_group_id,
                selected_date,
                selected_mode,
            ],
        )
    else:
        stop_landing_summary = _stop_landing_summary(db_path, selected_date, selected_mode)
        stop_landing_rows = _stop_landing_rows(db_path, selected_date, selected_mode, selected_rank)
    stop_widgets = _stop_widgets(
        db_path,
        stop_posts,
        selected_post or summary,
        line_stats,
        {"selected_date": selected_date, "selected_mode": selected_mode},
    )
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
    stop_event_table = _stop_event_table(db_path)
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
            _with_stop_event_table(
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
                    list(delay_seconds order by stop_sequence)
                        filter (where observation_status = 'observed' and delay_seconds is not null) as delay_profile
                from __STOP_EVENT_TABLE__
                where service_date = ?
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
                stop_event_table,
            ),
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
                _with_stop_event_table(
                    """
                select
                    stop_sequence,
                    stop_id,
                    stop_group_id,
                    stop_name,
                    scheduled_arrival_time,
                    actual_arrival_time,
                    delay_seconds,
                    observation_status
                from __STOP_EVENT_TABLE__
                where service_date = ?
                  and trip_id = ?
                  and vehicle_number = ?
                order by stop_sequence
                """,
                    stop_event_table,
                ),
                [selected_date, selected_trip["trip_id"], selected_trip["vehicle_number"]],
            )
            for stop in trip_stops:
                stop["post_label"] = _stop_post_label(stop["stop_id"], stop["stop_group_id"])
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
    stop_event_table = _stop_event_table(db_path)
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
            _with_stop_event_table(
                """
            select
                stop_sequence,
                stop_id,
                stop_group_id,
                stop_name,
                scheduled_arrival_time,
                actual_arrival_time,
                delay_seconds,
                observation_status
            from __STOP_EVENT_TABLE__
            where service_date = ?
              and trip_id = ?
              and vehicle_number = ?
            order by stop_sequence
            """,
                stop_event_table,
            ),
            [selected_date, trip["trip_id"], trip["vehicle_number"]],
        )
        for stop in trip_stops:
            stop["post_label"] = _stop_post_label(stop["stop_id"], stop["stop_group_id"])
        trip["trace"] = _trip_trace(
            [
                stop["delay_seconds"]
                for stop in trip_stops
                if stop["observation_status"] == "observed" and stop["delay_seconds"] is not None
            ],
        )

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
    for row in pipeline_status:
        row["health_ratio"] = _status_health_ratio(row)
        row["health_label"] = _status_health_label(row["health_ratio"])
    pipeline_by_mode = _by_mode_list(pipeline_status)
    return {
        "metadata": get_export_metadata(db_path),
        "pipeline_status": pipeline_by_mode,
        "latest_status": {mode: rows[0] for mode, rows in pipeline_by_mode.items() if rows},
        "status_summary": _status_summary_by_mode(pipeline_by_mode),
        "status_days": _status_days(pipeline_status),
    }


def _status_summary_by_mode(pipeline_by_mode: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    summaries = {}
    for mode, rows in pipeline_by_mode.items():
        recent_rows = rows[:8]
        if not recent_rows:
            continue
        total_trips = sum(row.get("trips_complete") or 0 for row in recent_rows)
        total_issues = sum(row.get("trips_broken") or 0 for row in recent_rows)
        summary = {
            "mode": mode,
            "day_count": len(recent_rows),
            "first_date": recent_rows[-1]["service_date"],
            "last_date": recent_rows[0]["service_date"],
            "completeness_ratio": _weighted_status_ratio(recent_rows, "completeness_ratio", "stop_arrivals_count"),
            "match_rate": _weighted_status_ratio(recent_rows, "match_rate", "stop_arrivals_count"),
            "service_coverage_ratio": _weighted_status_ratio(recent_rows, "service_coverage_ratio", "trips_complete"),
            "trips_complete": total_trips,
            "trips_broken": total_issues,
        }
        summary["health_ratio"] = _status_health_ratio(summary)
        summary["health_label"] = _status_health_label(summary["health_ratio"])
        summaries[mode] = summary
    return summaries


def _weighted_status_ratio(rows: list[dict[str, Any]], ratio_key: str, weight_key: str) -> float | None:
    weighted_values = [
        (float(row[ratio_key]), row.get(weight_key) or 0) for row in rows if row.get(ratio_key) is not None
    ]
    total_weight = sum(weight for _, weight in weighted_values)
    if total_weight == 0:
        values = [value for value, _ in weighted_values]
        return sum(values) / len(values) if values else None
    return sum(value * weight for value, weight in weighted_values) / total_weight


def _status_days(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    days: dict[Any, dict[str, Any]] = {}
    for row in rows:
        day = days.setdefault(row["service_date"], {"service_date": row["service_date"]})
        day[row["mode"]] = row
    return sorted(days.values(), key=lambda day: day["service_date"], reverse=True)


def _status_health_ratio(row: dict[str, Any]) -> float | None:
    ratios = [row.get("completeness_ratio"), row.get("service_coverage_ratio"), row.get("match_rate")]
    known_ratios = [float(ratio) for ratio in ratios if ratio is not None]
    if not known_ratios:
        return None
    return min(known_ratios)


def _status_health_label(health_ratio: float | None) -> str:
    if health_ratio is None:
        return "no data"
    if health_ratio >= GOOD_STATUS_HEALTH_RATIO:
        return "good"
    if health_ratio >= USABLE_STATUS_HEALTH_RATIO:
        return "usable"
    if health_ratio > 0:
        return "patchy"
    return "missing"


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
                flatten(list(delay_histogram)) as delay_histogram,
                sum((p90_delay_seconds - median_delay_seconds) * n) / nullif(sum(n), 0) as delay_spread_seconds
            from agg_line_daily
            where service_date = ?
              and mode = ?
            group by line, mode, route_short_name
            having sum(n) >= 20
        ),

        headsign_counts as (
            select line, mode, trip_headsign, sum(n) as arrival_count
            from agg_line_stop_daily
            where service_date = ?
              and mode = ?
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
            select line, mode, any_value(trip_headsign) filter (where head_rank = 1) as route_label
            from ranked_heads
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
            line_stats.delay_histogram,
            line_stats.delay_spread_seconds
        from line_stats
        left join route_labels
            on line_stats.line = route_labels.line
            and line_stats.mode = route_labels.mode
        """,
        [selected_date, selected_mode, selected_date, selected_mode],
    )
    for row in rows:
        row["shape"] = _delay_shape(
            row.get("median_delay_seconds"),
            row.get("on_time_rate"),
            row.get("delay_histogram"),
            row.get("p90_delay_seconds"),
        )
    return sorted(rows, key=lambda row: _line_landing_sort_key(row, selected_rank))[:LANDING_ROW_LIMIT]


def _stop_landing_summary(db_path: Path, selected_date: str | None, selected_mode: str) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
            select
                count(distinct stop_group_id) as stop_group_count,
                sum(n) as arrival_count,
                sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate
            from agg_stop_group_daily
            where service_date = ?
              and mode = ?
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
                stop_group_name,
                mean_delay_seconds,
                median_delay_seconds,
                p90_delay_seconds,
                on_time_rate,
                n as arrival_count,
                line_count,
                delay_histogram
            from agg_stop_group_daily
            where service_date = ?
              and mode = ?
              and n >= 10
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
            stop_stats.delay_histogram,
            stop_stats.p90_delay_seconds - stop_stats.median_delay_seconds as delay_spread_seconds
        from stop_stats
        """,
        [selected_date, selected_mode],
    )
    for row in rows:
        row["shape"] = _delay_shape(
            row.get("median_delay_seconds"),
            row.get("on_time_rate"),
            row.get("delay_histogram"),
            row.get("p90_delay_seconds"),
        )
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
    stop_event_table = _stop_event_table(db_path)
    rows = fetch_all(
        db_path,
        _with_stop_event_table(
            """
        with stop_arrivals as (
            select
                service_date,
                trip_id,
                vehicle_number,
                list(delay_seconds order by stop_sequence)
                    filter (where observation_status = 'observed' and delay_seconds is not null) as delay_profile
            from __STOP_EVENT_TABLE__
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
            stop_event_table,
        ),
        [selected_date, selected_mode, selected_date, selected_mode],
    )
    for row in rows:
        delay_profile = row.get("delay_profile") or []
        row["trace"] = _trip_trace(delay_profile)
        row["erratic_score"] = _trip_erratic_score(delay_profile)
        row["route_label"] = row.get("trip_headsign") or row.get("destination_stop_name")
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


def _overview_widgets(
    db_path: Path, mode_stats: dict[str, dict[str, Any]], selected_date: str | None
) -> dict[str, dict[str, Any]]:
    widgets = {}
    for mode in ("bus", "tram"):
        row = mode_stats.get(mode, {})
        baseline = row.get("mean_delay_seconds") or 45
        on_time_rate = row.get("on_time_rate") or 0.75
        hour_rows = fetch_all(
            db_path,
            """
            select local_hour, median_delay_seconds, n
            from agg_mode_hour_daily
            where service_date = ?
              and mode = ?
            order by local_hour
            """,
            [selected_date, mode],
        )
        week_rows = fetch_all(
            db_path,
            """
            select cast(service_date as varchar) as service_date, median_delay_seconds
            from agg_mode_daily
            where mode = ?
            order by service_date
            """,
            [mode],
        )
        widgets[mode] = {
            "shape": _delay_shape(
                row.get("median_delay_seconds") or baseline,
                on_time_rate,
                row.get("delay_histogram"),
                row.get("p90_delay_seconds"),
            ),
            "hours": _hour_bars_from_rows(hour_rows),
            "week": _week_bars_from_rows(week_rows, selected_date),
            "segments": _on_time_segments(
                on_time_rate, row.get("early_count"), row.get("on_time_count"), row.get("late_count")
            ),
        }
    return widgets


def _line_widgets(
    db_path: Path,
    selected_line: str | None,
    selected_date: str | None,
    summary: dict[str, Any] | None,
    courses: list[dict[str, Any]],
) -> dict[str, Any]:
    if summary is None:
        return {}

    baseline = summary.get("median_delay_seconds") or summary.get("mean_delay_seconds") or 45
    on_time_rate = summary.get("on_time_rate") or 0.75
    stops = []
    for course in courses:
        for stop in course["stops"]:
            stop["shape"] = _delay_shape(
                stop.get("median_delay_seconds") or stop.get("mean_delay_seconds"),
                stop.get("on_time_rate"),
                stop.get("delay_histogram"),
                stop.get("p90_delay_seconds"),
            )
            stop["direction"] = course["trip_headsign"]
            stops.append(stop)

    hour_rows = fetch_all(
        db_path,
        """
        select local_hour, median_delay_seconds, n
        from agg_line_hour_daily
        where service_date = ?
          and line = ?
        order by local_hour
        """,
        [selected_date, selected_line],
    )
    week_rows = fetch_all(
        db_path,
        """
        select
            cast(service_date as varchar) as service_date,
            sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds
        from agg_line_daily
        where line = ?
        group by service_date
        order by service_date
        """,
        [selected_line],
    )
    timeline_rows = fetch_all(
        db_path,
        """
        with trip_delays as (
            select
                trip_id,
                vehicle_number,
                quantile_cont(delay_seconds, 0.5) as delay_seconds,
                service_date,
                (
                    epoch_ms(min(scheduled_arrival_time))
                    + epoch_ms(max(scheduled_arrival_time))
                ) / 2 as middle_ms
            from fct_stop_arrival
            where service_date = ?
              and line = ?
              and trip_quality = 'complete'
            group by service_date, trip_id, vehicle_number
        ),

        positioned as (
            select
                delay_seconds,
                middle_ms,
                epoch_ms(service_date::timestamp + interval '4 hours') as service_day_start_ms,
                epoch_ms(service_date::timestamp + interval '28 hours') as service_day_end_ms
            from trip_delays
        )

        select
            delay_seconds,
            (middle_ms - service_day_start_ms) / (service_day_end_ms - service_day_start_ms) * 100 as x
        from positioned
        order by middle_ms
        limit 140
        """,
        [selected_date, selected_line],
    )
    worst_rows = fetch_all(
        db_path,
        """
        select
            strftime(scheduled_arrival_time + interval '2 hours', '%H:%M') as time,
            trip_headsign as direction,
            stop_name,
            stop_group_id,
            delay_seconds
        from mart_delay_events
        where service_date = ?
          and line = ?
          and line_delay_rank <= 6
        order by line_delay_rank
        """,
        [selected_date, selected_line],
    )
    reliability_rows = fetch_all(
        db_path,
        """
        select direction_id, trip_headsign, trip_quality
        from mart_trip_reliability
        where service_date = ?
          and line = ?
        order by direction_id, trip_headsign, trip_order
        """,
        [selected_date, selected_line],
    )

    return {
        "shape": _delay_shape(
            summary.get("median_delay_seconds") or baseline,
            on_time_rate,
            summary.get("delay_histogram"),
            summary.get("p90_delay_seconds"),
        ),
        "hours": _hour_bars_from_rows(hour_rows),
        "week": _week_bars_from_rows(week_rows, selected_date),
        "timeline": _timeline_from_rows(timeline_rows),
        "segments": _on_time_segments(on_time_rate, delay_histogram=summary.get("delay_histogram")),
        "worst": worst_rows,
        "reliability": _reliability_strip_from_rows(reliability_rows),
    }


def _stop_widgets(
    db_path: Path,
    stop_posts: list[dict[str, Any]],
    summary: dict[str, Any] | None,
    line_stats: list[dict[str, Any]],
    context: dict[str, str | None],
) -> dict[str, Any]:
    if summary is None:
        return {"posts": [], "worst": [], "line_rows": []}

    selected_date = context["selected_date"]
    selected_mode = context["selected_mode"] or "bus"
    baseline = summary.get("median_delay_seconds") or summary.get("mean_delay_seconds") or 45
    on_time_rate = summary.get("on_time_rate") or 0.75
    posts = []
    for post in stop_posts:
        median = post.get("median_delay_seconds") or baseline
        post_rate = post.get("on_time_rate") or on_time_rate
        post_hour_rows = fetch_all(
            db_path,
            """
            select local_hour, median_delay_seconds, n
            from agg_stop_hour_daily
            where service_date = ?
              and mode = ?
              and stop_id = ?
            order by local_hour
            """,
            [selected_date, selected_mode, post["stop_id"]],
        )
        posts.append(
            {
                **post,
                "median_delay_seconds": median,
                "on_time_rate": post_rate,
                "shape": _delay_shape(median, post_rate, post.get("delay_histogram"), post.get("p90_delay_seconds")),
                "hours": _hour_bars_from_rows(post_hour_rows),
                "lines": post.get("lines", []),
                "line_groups": post.get("line_groups", []),
            }
        )

    line_rows = []
    fallback_post = stop_posts[0]["display_name"] if stop_posts else ""
    for row in line_stats:
        line_row = {**row}
        line_row["shape"] = _delay_shape(
            row.get("median_delay_seconds") or row.get("mean_delay_seconds"),
            row.get("on_time_rate"),
            row.get("delay_histogram"),
            row.get("p90_delay_seconds"),
        )
        line_row["post_label"] = fallback_post
        line_rows.append(line_row)

    selected_stop_id = summary.get("stop_id")
    selected_stop_group_id = summary.get("stop_group_id")
    hour_rows = fetch_all(
        db_path,
        """
        select local_hour, median_delay_seconds, n
        from agg_stop_hour_daily
        where service_date = ?
          and mode = ?
          and ((? is not null and stop_id = ?) or (? is null and stop_group_id = ?))
        order by local_hour
        """,
        [selected_date, selected_mode, selected_stop_id, selected_stop_id, selected_stop_id, selected_stop_group_id],
    )
    week_rows = fetch_all(
        db_path,
        """
        select
            cast(service_date as varchar) as service_date,
            sum(median_delay_seconds * n) / nullif(sum(n), 0) as median_delay_seconds
        from agg_stop_post_daily
        where mode = ?
          and ((? is not null and stop_id = ?) or (? is null and stop_group_id = ?))
        group by service_date
        order by service_date
        """,
        [selected_mode, selected_stop_id, selected_stop_id, selected_stop_id, selected_stop_group_id],
    )
    event_value = selected_stop_id if selected_stop_id is not None else selected_stop_group_id
    event_scope = "stop_id" if selected_stop_id is not None else "stop_group_id"
    timeline_rows = fetch_all(
        db_path,
        """
        with arrivals as (
            select
                delay_seconds,
                service_date,
                epoch_ms(scheduled_arrival_time) as scheduled_ms
            from fct_stop_arrival
            where service_date = ?
              and mode = ?
              and case when ? = 'stop_id' then stop_id else stop_group_id end = ?
              and trip_quality = 'complete'
        ),

        positioned as (
            select
                delay_seconds,
                scheduled_ms,
                epoch_ms(service_date::timestamp + interval '4 hours') as service_day_start_ms,
                epoch_ms(service_date::timestamp + interval '28 hours') as service_day_end_ms
            from arrivals
        )

        select
            delay_seconds,
            (scheduled_ms - service_day_start_ms) / (service_day_end_ms - service_day_start_ms) * 100 as x
        from positioned
        order by scheduled_ms
        limit 140
        """,
        [selected_date, selected_mode, event_scope, event_value],
    )
    worst_rows = fetch_all(
        db_path,
        """
        select
            strftime(scheduled_arrival_time + interval '2 hours', '%H:%M') as time,
            line,
            mode,
            trip_headsign as headsign,
            delay_seconds
        from mart_delay_events
        where service_date = ?
          and mode = ?
          and case when ? = 'stop_id' then stop_id else stop_group_id end = ?
          and case when ? = 'stop_id' then stop_post_delay_rank else stop_group_delay_rank end <= 8
        order by case when ? = 'stop_id' then stop_post_delay_rank else stop_group_delay_rank end
        """,
        [selected_date, selected_mode, event_scope, event_value, event_scope, event_scope],
    )

    return {
        "posts": posts,
        "shape": _delay_shape(baseline, on_time_rate, summary.get("delay_histogram"), summary.get("p90_delay_seconds")),
        "hours": _hour_bars_from_rows(hour_rows),
        "week": _week_bars_from_rows(week_rows, selected_date),
        "timeline": _timeline_from_rows(timeline_rows),
        "segments": _on_time_segments(
            on_time_rate, summary.get("early_count"), summary.get("on_time_count"), summary.get("late_count")
        ),
        "worst": worst_rows,
        "line_rows": line_rows,
    }


def _delay_shape(
    median_delay_seconds: float | None,
    _on_time_rate: float | None,
    delay_histogram: list[dict[str, Any]] | None = None,
    p90_delay_seconds: float | None = None,
) -> dict[str, Any]:
    median = round(median_delay_seconds or 0)
    p90 = round(p90_delay_seconds if p90_delay_seconds is not None else median)
    return {
        "buckets": _histogram_buckets(delay_histogram or []),
        "median_x": _delay_axis_x(median),
        "p90_x": _delay_axis_x(p90),
    }


def _histogram_buckets(delay_histogram: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts_by_label = {str(bucket.get("bucket_label") or ""): int(bucket.get("n") or 0) for bucket in delay_histogram}
    visual_counts = [counts_by_label.get(label, 0) for label in HISTOGRAM_BUCKET_LABELS]
    max_count = max(visual_counts, default=0)
    if max_count <= 0:
        return [{"height": 3, "mini_height": 2, "tone": _bucket_tone(index)} for index in range(HISTOGRAM_BUCKET_COUNT)]
    buckets = []
    for index, count in enumerate(visual_counts):
        height = max(3, round(count / max_count * HISTOGRAM_MAX_HEIGHT)) if count else 3
        buckets.append(
            {
                "height": height,
                "mini_height": max(2, round(height / HISTOGRAM_MAX_HEIGHT * HISTOGRAM_MINI_MAX_HEIGHT)),
                "tone": _bucket_tone(index),
            }
        )
    return buckets


def _delay_axis_x(delay_seconds: float) -> float:
    return _clamp((delay_seconds + 60) / 360 * 100, 2, 98)


def _hour_bars_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    delays_by_hour = {
        int(row["local_hour"]): row.get("median_delay_seconds")
        for row in rows
        if int(row.get("n") or 0) >= MIN_HOUR_SAMPLE_SIZE
    }
    bars = []
    for hour in SERVICE_DAY_HOURS:
        delay = delays_by_hour.get(hour)
        if delay is None:
            bars.append({"hour": hour, "delay": None, "height": 0, "tone": "empty"})
            continue
        delay = round(delay)
        bars.append({"hour": hour, "delay": delay, "height": _bar_height(delay), "tone": _delay_tone(delay)})
    return bars


def _week_bars_from_rows(rows: list[dict[str, Any]], selected_date: str | None) -> list[dict[str, Any]]:
    if not rows:
        return []
    bars = []
    for row in rows[-7:]:
        service_date = str(row["service_date"])
        delay = row.get("median_delay_seconds")
        height = 0 if delay is None else max(4, min(38, round(abs(delay) * 0.35)))
        bars.append(
            {
                "label": date.fromisoformat(service_date).strftime("%a")[:1],
                "delay": delay,
                "height": height,
                "selected": service_date == selected_date,
            }
        )
    return bars


def _timeline_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    delay_rows = [row for row in rows if row.get("delay_seconds") is not None]
    if not delay_rows:
        return []
    points = []
    for index, row in enumerate(delay_rows):
        raw_delay = float(row["delay_seconds"])
        delay = round(raw_delay)
        x = float(row["x"]) if row.get("x") is not None else index / max(1, len(delay_rows) - 1) * 100
        points.append(
            {
                "x": _clamp(x, 0, 100),
                "delay": delay,
                "height": max(2, min(30, round(abs(delay) / 8))),
                "tone": _delay_tone(delay),
            }
        )
    return _spread_timeline_points(points)


def _spread_timeline_points(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(points) <= 1:
        return points

    gap = min(TIMELINE_MIN_GAP_PERCENT, 100 / max(1, len(points) - 1))
    packed = [{**point} for point in points]
    for index in range(1, len(packed)):
        packed[index]["x"] = max(float(packed[index]["x"]), float(packed[index - 1]["x"]) + gap)

    overflow = float(packed[-1]["x"]) - 100
    if overflow > 0:
        for point in packed:
            point["x"] = float(point["x"]) - overflow

    for index in range(len(packed) - 2, -1, -1):
        packed[index]["x"] = min(float(packed[index]["x"]), float(packed[index + 1]["x"]) - gap)

    underflow = -float(packed[0]["x"])
    if underflow > 0:
        for point in packed:
            point["x"] = float(point["x"]) + underflow

    for point in packed:
        point["x"] = _clamp(float(point["x"]), 0, 100)
    return packed


def _reliability_strip_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    grouped: dict[tuple[int | None, str | None], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("direction_id"), row.get("trip_headsign"))
        group = grouped.setdefault(
            key,
            {"direction": row.get("trip_headsign"), "outcomes": [], "counts": {"clean": 0, "partial": 0, "broken": 0}},
        )
        outcome = _trip_quality_outcome(row.get("trip_quality"))
        group["counts"][outcome] += 1
        group["outcomes"].append({"outcome": outcome, "label": outcome.replace("clean", "ran clean")})
    return list(grouped.values())


def _trip_quality_outcome(trip_quality: str | None) -> str:
    if trip_quality == "complete":
        return "clean"
    if trip_quality == "broken":
        return "broken"
    return "partial"


def _on_time_segments(
    on_time_rate: float,
    early_count: int | None = None,
    on_time_count: int | None = None,
    late_count: int | None = None,
    delay_histogram: list[dict[str, Any]] | None = None,
) -> dict[str, float]:
    if early_count is not None and on_time_count is not None and late_count is not None:
        total = early_count + on_time_count + late_count
        if total > 0:
            return {"early": early_count / total, "on_time": on_time_count / total, "late": late_count / total}

    if delay_histogram:
        early = sum(
            int(bucket.get("n") or 0)
            for bucket in delay_histogram
            if str(bucket.get("bucket_label") or "").startswith("early")
        )
        on_time = sum(
            int(bucket.get("n") or 0)
            for bucket in delay_histogram
            if str(bucket.get("bucket_label") or "").startswith("on_time")
        )
        late = sum(
            int(bucket.get("n") or 0)
            for bucket in delay_histogram
            if str(bucket.get("bucket_label") or "").startswith("late")
        )
        total = early + on_time + late
        if total > 0:
            return {"early": early / total, "on_time": on_time / total, "late": late / total}

    return {"early": 0, "on_time": on_time_rate, "late": 0}


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
