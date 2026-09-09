from __future__ import annotations

# ruff: noqa: S608
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ztm_frontend.db import fetch_all, fetch_one

LANDING_PAGE_SIZE = 20
STOP_PICKER_PAGE_SIZE = 12
MAX_PAGE = (2**63 - 1) // LANDING_PAGE_SIZE
ON_TIME_EARLY_SECONDS = -60
ON_TIME_LATE_SECONDS = 180
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
TIMELINE_MIN_GAP_PERCENT = 0.55
EXPECTED_STOP_EVENT_TABLE = "fct_expected_stop_event"
NUMERIC_STOP_POST_SUFFIX_LENGTH = 2
WINDOW_TYPES = ("day", "weekdays", "weekend", "month")
MAX_TREND_LABELS = 6
MAX_RELIABILITY_OUTCOMES = 160


def get_export_metadata(db_path: Path) -> dict[str, Any]:
    """Read serving export metadata and sanitized sidecar status."""
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
    if sidecar_metadata and sidecar_metadata.get("export_id") == metadata.get("export_id"):
        metadata["last_export_at"] = sidecar_metadata.get("last_export_at") or sidecar_metadata.get("exported_at")
        metadata["poller_status"] = _poller_status_metadata(sidecar_metadata.get("poller_status"))
    return metadata


def get_overview(db_path: Path, selected_date: str | None, selected_window: str | None = None) -> dict[str, Any]:
    """Build the network overview page data."""
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    selected_window = _available_window(db_path, selected_window)
    window_key = _window_key(selected_date, selected_window)
    mode_stats = _mode_stats(db_path, window_key, selected_window, selected_date)
    worst_lines = _ranked_entities(
        db_path,
        window_key,
        "line",
        "median_delay_seconds",
        window_type=selected_window,
        source_end_date=selected_date,
        limit=8,
    )
    worst_stops = _ranked_entities(
        db_path,
        window_key,
        "stop_post",
        "median_delay_seconds",
        window_type=selected_window,
        source_end_date=selected_date,
        limit=8,
    )
    for row in worst_lines:
        _attach_shape(row)
    for row in worst_stops:
        row["post_label"] = row.get("stop_post_code") or _stop_post_label(row["stop_id"], row["stop_group_id"])
        row["display_name"] = f"{row['stop_group_name']} [{row['stop_post_code']}]"
        _attach_shape(row)
    mode_stats_by_mode = _by_mode(mode_stats)
    window_context = _window_context(db_path, selected_window, selected_date, next(iter(mode_stats), None))
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "selected_window": selected_window,
        "date_nav": _window_date_nav(date_options, selected_date, selected_window),
        "window_context": window_context,
        "mode_stats": mode_stats_by_mode,
        "overview_widgets": _overview_widgets(db_path, mode_stats_by_mode, selected_date, selected_window, window_key),
        "worst_lines": _by_mode_list(worst_lines),
        "worst_stops": _by_mode_list(worst_stops),
        "delay_plots": {"bus": [], "tram": []},
    }


def get_lines(  # noqa: PLR0913
    db_path: Path,
    selected_line: str | None,
    selected_mode: str | None,
    selected_date: str | None,
    selected_rank: str | None,
    selected_page: str | None = None,
    selected_window: str | None = None,
) -> dict[str, Any]:
    """Build the line landing or selected-line page data."""
    selected_mode = selected_mode or "bus"
    selected_rank = _selected_line_rank(selected_rank)
    page = _selected_page(selected_page)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    selected_window = _available_window(db_path, selected_window)
    window_key = _window_key(selected_date, selected_window)
    line_list = fetch_all(
        db_path,
        """
        select line, mode, route_short_name, trip_count, arrival_count
        from mart_line_window_summary
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and mode = ?
          and universe_type = 'all_observed'
        order by mode, try_cast(line as integer), line
        """,
        [selected_window, window_key, selected_date, selected_mode],
    )
    summary = None
    courses: list[dict[str, Any]] = []
    line_landing_summary = None
    line_landing_rows: list[dict[str, Any]] = []
    pagination = None
    if selected_line is None:
        line_landing_summary = _line_landing_summary(db_path, window_key, selected_mode, selected_window, selected_date)
        line_landing_rows, pagination = _line_landing_rows(
            db_path, window_key, selected_mode, selected_rank, page, selected_window, selected_date
        )
    else:
        summary = fetch_one(
            db_path,
            """
            select *
            from mart_line_window_summary
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
              and mode = ?
              and line = ?
              and universe_type = 'all_observed'
            limit 1
            """,
            [selected_window, window_key, selected_date, selected_mode, selected_line],
        )
        courses = fetch_all(
            db_path,
            """
            select direction_id, trip_headsign, trip_count
            from mart_line_course_window
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
              and mode = ?
              and line = ?
            order by course_rank
            """,
            [selected_window, window_key, selected_date, selected_mode, selected_line],
        )
        stops = fetch_all(
            db_path,
            """
            select *
            from mart_line_course_stop_window
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
              and mode = ?
              and line = ?
            order by direction_id, trip_headsign, display_rank, stop_group_id, stop_id
            """,
            [selected_window, window_key, selected_date, selected_mode, selected_line],
        )
        stops_by_course = _course_rows(stops)
        for course in courses:
            course["stops"] = stops_by_course.get((course["direction_id"], course["trip_headsign"]), [])
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "selected_window": selected_window,
        "date_nav": _window_date_nav(date_options, selected_date, selected_window),
        "window_context": _window_context(
            db_path,
            selected_window,
            selected_date,
            summary or line_landing_summary,
            prefer_summary=summary is not None,
        ),
        "line_list": line_list,
        "line_groups": _line_rail_groups(line_list),
        "selected_line": selected_line,
        "selected_mode": selected_mode,
        "selected_rank": selected_rank,
        "summary": summary,
        "courses": courses,
        "line_widgets": _line_widgets(
            db_path, selected_line, selected_date, summary, courses, selected_window, window_key
        ),
        "line_landing_summary": line_landing_summary,
        "line_landing_rows": line_landing_rows,
        "pagination": pagination,
        "delay_plot": [],
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
    selected_page: str | None = None,
    selected_picker_page: str | None = None,
    selected_window: str | None = None,
) -> dict[str, Any]:
    """Build the stop landing, group, or selected-post page data."""
    selected_mode = selected_mode or "bus"
    selected_view = selected_view if selected_view in {"post", "line"} else "post"
    requested_stop_id = selected_stop_id
    selected_rank = _selected_stop_rank(selected_rank)
    page = _selected_page(selected_page)
    picker_page = _selected_page(selected_picker_page)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    selected_window = _available_window(db_path, selected_window)
    window_key = _window_key(selected_date, selected_window)
    search_pattern = f"%{search.strip().lower()}%"
    stop_list = fetch_all(
        db_path,
        """
        select stop_group_id, stop_group_name, modes_served
        from dim_stop_group_current
        where list_contains(str_split(modes_served, ', '), ?)
          and (? = '%%' or lower(stop_group_name) like ?)
        order by stop_group_name, stop_group_id
        limit ? offset ?
        """,
        [
            selected_mode,
            search_pattern,
            search_pattern,
            STOP_PICKER_PAGE_SIZE + 1,
            (picker_page - 1) * STOP_PICKER_PAGE_SIZE,
        ],
    )
    stop_list, picker_pagination = _page_result(stop_list, picker_page, STOP_PICKER_PAGE_SIZE)
    summary = None
    stop_landing_summary = None
    stop_landing_rows: list[dict[str, Any]] = []
    pagination = None
    stop_posts: list[dict[str, Any]] = []
    stop_post_groups: list[dict[str, Any]] = []
    selected_post = None
    line_stats: list[dict[str, Any]] = []
    stop_line_groups: list[dict[str, Any]] = []
    if selected_stop_group_id is None:
        stop_landing_summary = _stop_landing_summary(db_path, window_key, selected_mode, selected_window, selected_date)
        stop_landing_rows, pagination = _stop_landing_rows(
            db_path, window_key, selected_mode, selected_rank, page, selected_window, selected_date
        )
    else:
        summary = fetch_one(
            db_path,
            """
            select *
            from mart_stop_group_window_summary
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
              and mode = ?
              and stop_group_id = ?
              and universe_type = 'all_observed'
            limit 1
            """,
            [selected_window, window_key, selected_date, selected_mode, selected_stop_group_id],
        )
        stop_posts = fetch_all(
            db_path,
            """
            select *
            from mart_stop_post_window_summary
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
              and mode = ?
              and stop_group_id = ?
              and universe_type = 'all_observed'
            order by stop_id
            """,
            [selected_window, window_key, selected_date, selected_mode, selected_stop_group_id],
        )
        line_groups_by_post = _line_groups_by_post(
            db_path, window_key, selected_date, selected_mode, selected_stop_group_id, selected_window
        )
        lines_by_post = _lines_by_post(line_groups_by_post)
        for post in stop_posts:
            post["display_name"] = post.get("stop_post_code") or post["stop_id"]
            post["modes_served"] = post["mode"]
            post["mode_groups"] = _stop_post_mode_groups(post["mode"])
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
                select *
                from mart_stop_post_window_summary
                where window_type = ?
                  and window_key = ?
                  and source_end_date = cast(? as date)
                  and mode = ?
                  and stop_id = ?
                  and universe_type = 'all_observed'
                limit 1
                """,
                [selected_window, window_key, selected_date, selected_mode, selected_stop_id],
            )
            if selected_post is not None:
                selected_post["display_name"] = selected_post.get("stop_post_code") or selected_post["stop_id"]
        if selected_stop_id is not None:
            line_stats, pagination = _stop_line_rows(
                db_path,
                selected_date,
                selected_mode,
                selected_stop_id,
                page,
                window_type=selected_window,
                window_key=window_key,
            )
        stop_line_groups = _stop_group_line_groups(
            db_path, window_key, selected_date, selected_mode, selected_stop_group_id, selected_window
        )
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "selected_window": selected_window,
        "date_nav": _window_date_nav(date_options, selected_date, selected_window),
        "window_context": _window_context(
            db_path, selected_window, selected_date, selected_post or summary or stop_landing_summary
        ),
        "stop_list": stop_list,
        "picker_pagination": picker_pagination,
        "selected_stop_group_id": selected_stop_group_id,
        "selected_mode": selected_mode,
        "selected_view": selected_view,
        "selected_rank": selected_rank,
        "search": search,
        "selected_stop_id": selected_stop_id,
        "requested_stop_id": requested_stop_id,
        "summary": summary,
        "stop_posts": stop_posts,
        "stop_post_groups": stop_post_groups,
        "selected_post": selected_post,
        "line_stats": line_stats,
        "stop_line_groups": stop_line_groups,
        "stop_widgets": _stop_widgets(
            db_path,
            stop_posts,
            selected_post or summary,
            line_stats,
            {
                "selected_date": selected_date,
                "selected_mode": selected_mode,
                "selected_window": selected_window,
                "window_key": window_key,
            },
        ),
        "stop_landing_summary": stop_landing_summary,
        "stop_landing_rows": stop_landing_rows,
        "pagination": pagination,
        "delay_plot": [],
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
    selected_page: str | None = None,
    selected_window: str | None = None,
) -> dict[str, Any]:
    """Build the trip landing or selected-line trip page data."""
    selected_mode = selected_mode or "bus"
    selected_sort = selected_sort if selected_sort in {"departure", "delay", "erratic"} else "departure"
    selected_rank = _selected_trip_rank(selected_rank)
    page = _selected_page(selected_page)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    selected_window = _available_window(db_path, selected_window)
    window_key = _window_key(selected_date, selected_window)
    mode_summary = next(
        (
            row
            for row in _mode_stats(db_path, window_key, selected_window, selected_date)
            if row["mode"] == selected_mode
        ),
        {},
    )
    scope_params = _window_scope_params(mode_summary, selected_date)
    line_list = fetch_all(
        db_path,
        f"""
        select line, mode, any_value(route_short_name) as route_short_name, count(*) as trip_count
        from mart_trip_daily
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(selected_window)}
          {_active_schedule_version_filter(selected_window, "mart_trip_daily.schedule_version_id")}
          and mode = ?
          and trip_quality = 'complete'
        group by line, mode
        order by try_cast(line as integer), line
        """,
        [*scope_params, *_active_schedule_version_params(selected_window, selected_date), selected_mode],
    )
    line_scope_summary = None
    if selected_line is not None:
        line_scope_summary = fetch_one(
            db_path,
            """
            select source_start_date, source_end_date, source_day_count
            from mart_line_window_summary
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
              and mode = ?
              and line = ?
              and universe_type = 'all_observed'
            limit 1
            """,
            [selected_window, window_key, selected_date, selected_mode, selected_line],
        )
    trips: list[dict[str, Any]] = []
    trip_landing_summary = None
    trip_landing_rows: list[dict[str, Any]] = []
    pagination = None
    selected_trip = None
    trip_stops: list[dict[str, Any]] = []
    if selected_line is None:
        trip_landing_summary = (
            fetch_one(
                db_path,
                f"""
            select
                count(*) as trip_count,
                avg(case when end_delay_seconds > -60 and end_delay_seconds < 180 then 1.0 else 0.0 end) as on_time_rate,
                median(end_delay_seconds) as median_delay_seconds
            from mart_trip_daily
            where service_date between cast(? as date) and cast(? as date)
              {_schedule_day_filter(selected_window)}
              {_active_schedule_version_filter(selected_window, "mart_trip_daily.schedule_version_id")}
              and mode = ?
              and trip_quality = 'complete'
            """,
                [*scope_params, *_active_schedule_version_params(selected_window, selected_date), selected_mode],
            )
            or {}
        )
        trip_landing_rows, pagination = _trip_landing_rows(
            db_path, selected_date, selected_mode, selected_rank, page, selected_window, mode_summary
        )
    else:
        order_by = {
            "departure": "service_date desc, scheduled_start_time, trip_id, vehicle_number",
            "delay": "end_delay_seconds desc, service_date desc, scheduled_start_time",
            "erratic": "erratic_score desc, service_date desc, scheduled_start_time",
        }[selected_sort]
        trips, pagination = _selected_line_trip_rows(
            db_path, selected_date, selected_mode, selected_line, order_by, page, selected_window, mode_summary
        )
        for trip in trips:
            trip["trace"] = _trip_trace(trip.get("delay_profile") or [])
        if trips and not _trip_selected(trips, selected_trip_id, selected_vehicle):
            selected_trip_id = trips[0]["trip_id"]
            selected_vehicle = trips[0]["vehicle_number"]
        selected_trip = _find_trip(trips, selected_trip_id, selected_vehicle)
        if selected_trip is not None:
            trip_stops = _trip_stops(
                db_path,
                str(selected_trip["service_date"]),
                selected_trip.get("gtfs_snapshot_id"),
                selected_trip["trip_id"],
                selected_trip["vehicle_number"],
            )
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "selected_window": selected_window,
        "date_nav": _window_date_nav(date_options, selected_date, selected_window),
        "window_context": _window_context(
            db_path,
            selected_window,
            selected_date,
            line_scope_summary or mode_summary,
            prefer_summary=line_scope_summary is not None,
        ),
        "selected_mode": selected_mode,
        "selected_line": selected_line,
        "selected_trip_id": selected_trip_id,
        "selected_vehicle": selected_vehicle,
        "selected_sort": selected_sort,
        "selected_rank": selected_rank,
        "line_list": line_list,
        "line_groups": _line_rail_groups(line_list),
        "trips": trips,
        "trip_groups": _trip_groups(
            db_path, selected_date, selected_mode, selected_line, trips, selected_window, mode_summary
        ),
        "trip_landing_summary": trip_landing_summary,
        "trip_landing_rows": trip_landing_rows,
        "pagination": pagination,
        "selected_trip": selected_trip,
        "trip_stops": trip_stops,
    }


def get_trip_detail(  # noqa: PLR0913
    db_path: Path,
    trip_id: str,
    selected_date: str | None,
    selected_vehicle: str | None,
    return_window: str | None = None,
    return_date: str | None = None,
) -> dict[str, Any]:
    """Build an individual trip detail page."""
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    trip = fetch_one(
        db_path,
        """
        select *
        from mart_trip_daily
        where service_date = ?
          and trip_id = ?
          and (? is null or vehicle_number = ?)
        order by trip_quality = 'complete' desc, actual_end_time desc, vehicle_number
        limit 1
        """,
        [selected_date, trip_id, selected_vehicle, selected_vehicle],
    )
    trip_stops = []
    if trip is not None:
        trip["trace"] = _trip_trace(trip.get("delay_profile") or [])
        trip_stops = _trip_stops(
            db_path,
            selected_date,
            trip.get("gtfs_snapshot_id"),
            trip["trip_id"],
            trip["vehicle_number"],
        )
    return {
        "selected_date": selected_date,
        "return_window": normalize_window(return_window),
        "return_date": return_date or selected_date,
        "trip": trip,
        "trip_stops": trip_stops,
    }


def get_status(db_path: Path) -> dict[str, Any]:
    """Build the pipeline/export status page data."""
    pipeline_status = fetch_all(
        db_path,
        """
        select *
        from mart_pipeline_status
        qualify row_number() over (partition by mode order by service_date desc) <= 8
        order by service_date desc, mode
        """,
    )
    pipeline_by_mode = _by_mode_list(pipeline_status)
    return {
        "metadata": get_export_metadata(db_path),
        "pipeline_status": pipeline_by_mode,
        "latest_status": {mode: rows[0] for mode, rows in pipeline_by_mode.items() if rows},
        "status_summary": _status_summary_by_mode(db_path),
        "status_days": _status_days(pipeline_status),
    }


def _line_landing_summary(
    db_path: Path,
    window_key: str | None,
    selected_mode: str,
    window_type: str,
    source_end_date: str | None,
) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
        select line_count, arrival_count, median_delay_seconds, on_time_rate
        from mart_mode_window_summary
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and mode = ?
        limit 1
        """,
            [window_type, window_key, source_end_date, selected_mode],
        )
        or {}
    )


def _line_landing_rows(  # noqa: PLR0913
    db_path: Path,
    window_key: str | None,
    selected_mode: str,
    selected_rank: str,
    page: int,
    window_type: str,
    source_end_date: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metric = {"worst": "median_delay_seconds", "best": "on_time_rate", "erratic": "delay_spread_seconds"}[selected_rank]
    rows = _ranked_entities(
        db_path,
        window_key,
        "line",
        metric,
        selected_mode,
        window_type=window_type,
        source_end_date=source_end_date,
        limit=LANDING_PAGE_SIZE + 1,
        offset=(page - 1) * LANDING_PAGE_SIZE,
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        _attach_shape(row)
    return rows, pagination


def _stop_landing_summary(
    db_path: Path,
    window_key: str | None,
    selected_mode: str,
    window_type: str,
    source_end_date: str | None,
) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
        select stop_group_count, arrival_count, median_delay_seconds, on_time_rate
        from mart_mode_window_summary
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and mode = ?
        limit 1
        """,
            [window_type, window_key, source_end_date, selected_mode],
        )
        or {}
    )


def _stop_landing_rows(  # noqa: PLR0913
    db_path: Path,
    window_key: str | None,
    selected_mode: str,
    selected_rank: str,
    page: int,
    window_type: str,
    source_end_date: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metric = {"worst": "median_delay_seconds", "best": "on_time_rate", "busiest": "arrival_count"}[selected_rank]
    rows = _ranked_entities(
        db_path,
        window_key,
        "stop_group",
        metric,
        selected_mode,
        window_type=window_type,
        source_end_date=source_end_date,
        limit=LANDING_PAGE_SIZE + 1,
        offset=(page - 1) * LANDING_PAGE_SIZE,
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        _attach_shape(row)
    return rows, pagination


def _trip_landing_rows(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_rank: str,
    page: int,
    window_type: str = "day",
    summary: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    summary = summary or {}
    order_by = {
        "worst": "abs(end_delay_seconds) desc, end_delay_seconds desc, service_date desc",
        "best": "abs(end_delay_seconds), service_date desc, scheduled_start_time",
        "erratic": "erratic_score desc, end_delay_seconds desc, service_date desc",
    }[selected_rank]
    rows = fetch_all(
        db_path,
        f"""
        select *
        from mart_trip_daily
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(window_type)}
          {_active_schedule_version_filter(window_type, "mart_trip_daily.schedule_version_id")}
          and mode = ?
          and trip_quality = 'complete'
        order by {order_by}
        limit ? offset ?
        """,
        [
            *_window_scope_params(summary, selected_date),
            *_active_schedule_version_params(window_type, selected_date),
            selected_mode,
            LANDING_PAGE_SIZE + 1,
            (page - 1) * LANDING_PAGE_SIZE,
        ],
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        row["trace"] = _trip_trace(row.get("delay_profile") or [])
        row["display_date"] = _trip_display_date(row, window_type)
    return rows, pagination


def _selected_line_trip_rows(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_line: str,
    order_by: str,
    page: int,
    window_type: str = "day",
    summary: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    summary = summary or {}
    rows = fetch_all(
        db_path,
        f"""
        select *
        from mart_trip_daily
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(window_type)}
          {_active_schedule_version_filter(window_type, "mart_trip_daily.schedule_version_id")}
          and mode = ?
          and line = ?
          and trip_quality = 'complete'
        order by {order_by}
        limit ? offset ?
        """,
        [
            *_window_scope_params(summary, selected_date),
            *_active_schedule_version_params(window_type, selected_date),
            selected_mode,
            selected_line,
            LANDING_PAGE_SIZE + 1,
            (page - 1) * LANDING_PAGE_SIZE,
        ],
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        row["display_date"] = _trip_display_date(row, window_type)
    return rows, pagination


def _stop_line_rows(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_stop_id: str,
    page: int,
    window_type: str = "day",
    window_key: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    window_key = window_key or selected_date
    rows = fetch_all(
        db_path,
        """
        select *
        from mart_stop_line_window_summary
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and mode = ?
          and entity_type = 'stop_post'
          and entity_id = ?
        order by display_rank
        limit ? offset ?
        """,
        [
            window_type,
            window_key,
            selected_date,
            selected_mode,
            selected_stop_id,
            LANDING_PAGE_SIZE + 1,
            (page - 1) * LANDING_PAGE_SIZE,
        ],
    )
    return _page_result(rows, page)


def _ranked_entities(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    entity_type: str,
    metric: str,
    selected_mode: str | None = None,
    *,
    window_type: str = "day",
    source_end_date: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict[str, Any]]:
    summary_table = {
        "line": "mart_line_window_summary",
        "stop_group": "mart_stop_group_window_summary",
        "stop_post": "mart_stop_post_window_summary",
    }[entity_type]
    entity_column = {"line": "line", "stop_group": "stop_group_id", "stop_post": "stop_id"}[entity_type]
    limit_sql = "" if limit is None else "and rankings.rank > ? and rankings.rank <= ?"
    source_end_date = source_end_date or selected_date
    params: list[Any] = [
        entity_type,
        metric,
        window_type,
        selected_date,
        source_end_date,
        window_type,
        selected_date,
        source_end_date,
        selected_mode,
        selected_mode,
    ]
    if limit is not None:
        params.extend((offset, offset + limit))
    return fetch_all(
        db_path,
        f"""
        select summaries.*, rankings.rank, rankings.n_entities, rankings.value as ranking_value
        from mart_entity_rankings as rankings
        inner join {summary_table} as summaries
            on rankings.entity_id = summaries.{entity_column}
            and rankings.mode = summaries.mode
            and rankings.window_type = summaries.window_type
            and rankings.window_key = summaries.window_key
            and summaries.universe_type = 'zone1_public'
        where rankings.entity_type = ?
          and rankings.metric = ?
          and rankings.window_type = ?
          and rankings.window_key = ?
          and rankings.source_end_date = cast(? as date)
          and summaries.window_type = ?
          and summaries.window_key = ?
          and summaries.source_end_date = cast(? as date)
          and (? is null or rankings.mode = ?)
        {limit_sql}
        order by rankings.mode, rankings.rank
        """,
        params,
    )


def _mode_stats(
    db_path: Path, window_key: str | None, window_type: str, source_end_date: str | None
) -> list[dict[str, Any]]:
    return fetch_all(
        db_path,
        """
        select *
        from mart_mode_window_summary
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
        order by mode
        """,
        [window_type, window_key, source_end_date],
    )


def _overview_widgets(
    db_path: Path,
    mode_stats: dict[str, dict[str, Any]],
    selected_date: str | None,
    window_type: str,
    window_key: str | None,
) -> dict[str, dict[str, Any]]:
    widgets = {}
    for mode in ("bus", "tram"):
        row = mode_stats.get(mode, {})
        widgets[mode] = {
            "shape": _delay_shape(
                row.get("median_delay_seconds"),
                row.get("on_time_rate"),
                row.get("delay_histogram"),
                row.get("p90_delay_seconds"),
            ),
            "hours": _hour_bars(db_path, "mode", mode, mode, window_key, window_type, selected_date),
            "comparison": _comparison_bars(db_path, "mode", mode, mode, selected_date, window_type),
            "comparison_label": _comparison_label(window_type),
            "segments": _on_time_segments(
                row.get("on_time_rate") or 0, row.get("early_count"), row.get("on_time_count"), row.get("late_count")
            ),
        }
    return widgets


def _line_widgets(  # noqa: PLR0913
    db_path: Path,
    selected_line: str | None,
    selected_date: str | None,
    summary: dict[str, Any] | None,
    courses: list[dict[str, Any]],
    window_type: str,
    window_key: str | None,
) -> dict[str, Any]:
    if summary is None or selected_line is None:
        return {}
    for course in courses:
        for stop in course.get("stops", []):
            _attach_shape(stop)
            stop["direction"] = course["trip_headsign"]
    scope_params = _window_scope_params(summary, selected_date)
    worst_rows = fetch_all(
        db_path,
        f"""
        select
            case when ? = 'day' then time_label else strftime(service_date, '%d %b') || ' · ' || time_label end as time,
            service_date, trip_id, vehicle_number,
            trip_headsign as direction,
            stop_name,
            stop_group_id,
            delay_seconds
        from mart_worst_delay_event
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(window_type)}
          {_active_schedule_version_filter(window_type, "mart_worst_delay_event.schedule_version_id")}
          and mode = ?
          and scope_type = 'line'
          and scope_id = ?
        order by delay_seconds desc, service_date desc, scheduled_arrival_time
        limit 6
        """,
        [
            window_type,
            *scope_params,
            *_active_schedule_version_params(window_type, selected_date),
            summary.get("mode"),
            selected_line,
        ],
    )
    reliability_rows = fetch_all(
        db_path,
        f"""
        select service_date, direction_id, trip_headsign, clean_count, partial_count, broken_count, outcomes
        from mart_line_reliability_daily
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(window_type)}
          {_active_schedule_version_filter(window_type, "mart_line_reliability_daily.schedule_version_id")}
          and mode = ?
          and line = ?
        order by service_date, display_rank
        """,
        [
            *scope_params,
            *_active_schedule_version_params(window_type, selected_date),
            summary.get("mode"),
            selected_line,
        ],
    )
    for row in reliability_rows:
        row["outcomes"] = [{**trip, "service_date": row["service_date"]} for trip in row.get("outcomes") or []]
    return {
        "shape": _delay_shape(
            summary.get("median_delay_seconds"),
            summary.get("on_time_rate"),
            summary.get("delay_histogram"),
            summary.get("p90_delay_seconds"),
        ),
        "hours": _hour_bars(
            db_path, "line", selected_line, summary.get("mode"), window_key, window_type, selected_date
        ),
        "comparison": _comparison_bars(db_path, "line", selected_line, summary.get("mode"), selected_date, window_type),
        "comparison_label": _comparison_label(window_type),
        "daily": _period_daily_bars(db_path, "line", selected_line, summary.get("mode"), selected_date, window_type),
        "timeline": _timeline(db_path, "line", selected_line, summary.get("mode"), selected_date)
        if window_type == "day"
        else [],
        "segments": _on_time_segments(
            summary.get("on_time_rate") or 0,
            summary.get("early_count"),
            summary.get("on_time_count"),
            summary.get("late_count"),
        ),
        "worst": worst_rows,
        "reliability": _period_reliability(reliability_rows),
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
    window_type = context["selected_window"] or "day"
    window_key = context["window_key"]
    posts = []
    for post in stop_posts:
        _attach_shape(post)
        post["hours"] = _hour_bars(
            db_path, "stop_post", post["stop_id"], selected_mode, window_key, window_type, selected_date
        )
        posts.append(post)
    line_rows = []
    for row in line_stats:
        _attach_shape(row)
        row["post_label"] = summary.get("display_name") or summary.get("stop_post_code") or ""
        line_rows.append(row)
    selected_stop_id = summary.get("stop_id")
    entity_type = "stop_post" if selected_stop_id is not None else "stop_group"
    entity_id = selected_stop_id or summary.get("stop_group_id")
    scope_params = _window_scope_params(summary, selected_date)
    worst_rows = fetch_all(
        db_path,
        f"""
        select
            case when ? = 'day' then time_label else strftime(service_date, '%d %b') || ' · ' || time_label end as time,
            line,
            mode,
            trip_headsign as headsign,
            service_date, trip_id, vehicle_number,
            delay_seconds
        from mart_worst_delay_event
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(window_type)}
          and mode = ?
          and scope_type = ?
          and scope_id = ?
        order by delay_seconds desc, service_date desc, scheduled_arrival_time
        limit 8
        """,
        [window_type, *scope_params, selected_mode, entity_type, entity_id],
    )
    return {
        "posts": posts,
        "shape": _delay_shape(
            summary.get("median_delay_seconds"),
            summary.get("on_time_rate"),
            summary.get("delay_histogram"),
            summary.get("p90_delay_seconds"),
        ),
        "hours": _hour_bars(db_path, entity_type, entity_id, selected_mode, window_key, window_type, selected_date),
        "comparison": _comparison_bars(db_path, entity_type, entity_id, selected_mode, selected_date, window_type),
        "comparison_label": _comparison_label(window_type),
        "daily": _period_daily_bars(db_path, entity_type, entity_id, selected_mode, selected_date, window_type),
        "timeline": _timeline(db_path, entity_type, entity_id, selected_mode, selected_date)
        if window_type == "day"
        else [],
        "segments": _on_time_segments(
            summary.get("on_time_rate") or 0,
            summary.get("early_count"),
            summary.get("on_time_count"),
            summary.get("late_count"),
        ),
        "worst": worst_rows,
        "line_rows": line_rows,
    }


def _hour_bars(  # noqa: PLR0913
    db_path: Path,
    entity_type: str,
    entity_id: str | None,
    mode: str | None,
    window_key: str | None,
    window_type: str = "day",
    source_end_date: str | None = None,
) -> list[dict[str, Any]]:
    source_end_date = source_end_date or window_key
    rows = fetch_all(
        db_path,
        """
        select local_hour, median_delay_seconds, has_min_sample
        from mart_hour_window_summary
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
        order by service_hour_index
        """,
        [window_type, window_key, source_end_date, entity_type, entity_id, mode, mode],
    )
    delays_by_hour = {
        int(row["local_hour"]): row.get("median_delay_seconds") for row in rows if row.get("has_min_sample")
    }
    bars = []
    for hour in SERVICE_DAY_HOURS:
        delay = delays_by_hour.get(hour)
        if delay is None:
            bars.append({"hour": hour, "delay": None, "height": 0, "tone": "empty"})
            continue
        rounded = round(delay)
        bars.append({"hour": hour, "delay": rounded, "height": _bar_height(rounded), "tone": _delay_tone(rounded)})
    return bars


def _week_bars(
    db_path: Path, entity_type: str, entity_id: str | None, mode: str | None, selected_date: str | None
) -> list[dict[str, Any]]:
    if selected_date is None:
        return []
    selected_day = date.fromisoformat(selected_date)
    week_start = selected_day - timedelta(days=selected_day.weekday())
    week_end = week_start + timedelta(days=6)
    rows = fetch_all(
        db_path,
        """
        select cast(service_date as varchar) as service_date, median_delay_seconds
        from mart_entity_daily_summary
        where entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
          and service_date between cast(? as date) and cast(? as date)
        order by service_date
        """,
        [entity_type, entity_id, mode, mode, week_start, week_end],
    )
    delays_by_date = {str(row["service_date"]): row.get("median_delay_seconds") for row in rows}
    bars = []
    for day_offset in range(7):
        service_day = week_start + timedelta(days=day_offset)
        service_date = service_day.isoformat()
        delay = delays_by_date.get(service_date)
        height = 0 if delay is None else max(4, min(38, round(abs(float(delay)) * 0.35)))
        bars.append(
            {
                "service_date": service_date,
                "label": service_day.strftime("%a")[:1],
                "delay": delay,
                "height": height,
                "selected": service_date == selected_date,
            }
        )
    return bars


def _comparison_bars(  # noqa: PLR0913
    db_path: Path,
    entity_type: str,
    entity_id: str | None,
    mode: str | None,
    selected_date: str | None,
    window_type: str,
) -> list[dict[str, Any]]:
    if window_type == "day":
        return _week_bars(db_path, entity_type, entity_id, mode, selected_date)
    if selected_date is None:
        return []
    rows = fetch_all(
        db_path,
        """
        select service_date as bucket_date, median_delay_seconds as delay
        from mart_entity_window_daily_summary
        where entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
          and window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
        order by service_date desc
        limit 12
        """,
        [
            entity_type,
            entity_id,
            mode,
            mode,
            window_type,
            _window_key(selected_date, window_type),
            selected_date,
        ],
    )
    rows.reverse()
    return _trend_bars(rows, "%d %b")


def _period_daily_bars(  # noqa: PLR0913
    db_path: Path,
    entity_type: str,
    entity_id: str | None,
    mode: str | None,
    selected_date: str | None,
    window_type: str,
) -> list[dict[str, Any]]:
    if window_type == "day":
        return []
    rows = fetch_all(
        db_path,
        """
        select service_date as bucket_date, median_delay_seconds as delay
        from mart_entity_window_daily_summary
        where entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
          and window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
        order by service_date
        """,
        [
            entity_type,
            entity_id,
            mode,
            mode,
            window_type,
            _window_key(selected_date, window_type),
            selected_date,
        ],
    )
    return _trend_bars(rows, "%d %b")


def _trend_bars(rows: list[dict[str, Any]], label_format: str) -> list[dict[str, Any]]:
    bars = []
    scale = max((abs(float(row["delay"])) for row in rows if row.get("delay") is not None), default=1) or 1
    for index, row in enumerate(rows):
        bucket_date = row["bucket_date"]
        delay = row.get("delay")
        bars.append(
            {
                "service_date": str(bucket_date),
                "label": bucket_date.strftime(label_format)
                if index in {0, len(rows) - 1}
                or (index % max(3, len(rows) // MAX_TREND_LABELS) == 0 and index < len(rows) - 3)
                else "",
                "delay": delay,
                "height": 0 if delay is None else max(1, round(abs(float(delay)) / scale * 45)),
                "tone": "missing" if delay is None else ("early" if delay < 0 else "late" if delay > 0 else "zero"),
                "daily": True,
                "scale": scale,
                "selected": index == len(rows) - 1,
            }
        )
    return bars


def _comparison_label(window_type: str) -> str:
    return {
        "day": "This week · mean",
        "weekdays": "12 service days · median",
        "weekend": "12 service days · median",
        "month": "12 service days · median",
    }[window_type]


def _window_scope_params(summary: dict[str, Any], selected_date: str | None) -> list[Any]:
    source_start_date = summary.get("source_start_date") or selected_date
    source_end_date = summary.get("source_end_date") or selected_date
    return [source_start_date, source_end_date]


def _schedule_day_filter(window_type: str) -> str:
    if window_type == "weekdays":
        return "and schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')"
    if window_type == "weekend":
        return "and schedule_day_type in ('saturday', 'sunday_holiday')"
    return ""


def _active_schedule_version_filter(window_type: str, schedule_version_column: str) -> str:
    if window_type not in {"weekdays", "weekend"}:
        return ""
    return f"""and exists (
        select 1
        from dim_schedule_version as active_version
        where active_version.schedule_version_id = {schedule_version_column}
          and cast(? as date) between active_version.valid_from_date
              and coalesce(active_version.valid_to_date, date '9999-12-31')
    )"""


def _active_schedule_version_params(window_type: str, selected_date: str | None) -> list[Any]:
    return [selected_date] if window_type in {"weekdays", "weekend"} else []


def _timeline(
    db_path: Path, entity_type: str, entity_id: str | None, mode: str | None, selected_date: str | None
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        select x_percent as x, delay_seconds
        from mart_entity_timeline_daily
        where service_date = ?
          and entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
        order by point_rank
        """,
        [selected_date, entity_type, entity_id, mode, mode],
    )
    return _timeline_from_rows(rows)


def _trip_groups(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_line: str | None,
    trips: list[dict[str, Any]],
    window_type: str,
    summary: dict[str, Any],
) -> list[dict[str, Any]]:
    if selected_line is None:
        return []
    groups = fetch_all(
        db_path,
        f"""
        select
            direction_id,
            trip_headsign,
            count(*) as trip_count
        from mart_trip_daily
        where service_date between cast(? as date) and cast(? as date)
          {_schedule_day_filter(window_type)}
          {_active_schedule_version_filter(window_type, "mart_trip_daily.schedule_version_id")}
          and mode = ?
          and line = ?
          and trip_quality = 'complete'
        group by direction_id, trip_headsign
        order by trip_count desc, direction_id, trip_headsign
        """,
        [
            *_window_scope_params(summary, selected_date),
            *_active_schedule_version_params(window_type, selected_date),
            selected_mode,
            selected_line,
        ],
    )
    trips_by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for trip in trips:
        trips_by_key.setdefault((trip["direction_id"], trip["trip_headsign"]), []).append(trip)
    for group in groups:
        group["trips"] = trips_by_key.get((group["direction_id"], group["trip_headsign"]), [])
    return [group for group in groups if group["trips"]]


def _trip_display_date(row: dict[str, Any], window_type: str) -> str:
    if window_type == "day":
        return ""
    service_date = _as_date(row.get("service_date"))
    return service_date.strftime("%d %b · ") if service_date else ""


def _trip_stops(
    db_path: Path,
    selected_date: str | None,
    gtfs_snapshot_id: str | None,
    trip_id: str,
    vehicle_number: str,
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        select stop_sequence, stop_id, stop_group_id, stop_post_code, stop_name, scheduled_arrival_time, actual_arrival_time, delay_seconds, observation_status
        from fct_expected_stop_event
        where service_date = ?
          and (? is null or gtfs_snapshot_id = ?)
          and trip_id = ?
          and vehicle_number = ?
          and observation_status != 'not_in_passenger_service'
        order by stop_sequence
        """,
        [selected_date, gtfs_snapshot_id, gtfs_snapshot_id, trip_id, vehicle_number],
    )
    for row in rows:
        row["post_label"] = row.get("stop_post_code") or _stop_post_label(row["stop_id"], row["stop_group_id"])
    return rows


def _line_groups_by_post(  # noqa: PLR0913
    db_path: Path,
    window_key: str | None,
    selected_date: str | None,
    selected_mode: str,
    stop_group_id: str,
    window_type: str,
) -> dict[str, list[dict[str, Any]]]:
    rows = fetch_all(
        db_path,
        """
        select stop_id, trip_headsign, lines
        from mart_stop_post_line_group_window
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and mode = ?
          and stop_group_id = ?
        order by stop_id, display_rank
        """,
        [window_type, window_key, selected_date, selected_mode, stop_group_id],
    )
    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(row["stop_id"], []).append(
            {"trip_headsign": row["trip_headsign"], "lines": row.get("lines") or []}
        )
    return result


def _lines_by_post(line_groups_by_post: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for stop_id, groups in line_groups_by_post.items():
        lines: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for group in groups:
            for line in group["lines"]:
                key = (line["line"], line["mode"])
                if key in seen:
                    continue
                seen.add(key)
                lines.append({**line, "trip_headsign": group["trip_headsign"]})
        result[stop_id] = lines
    return result


def _stop_group_line_groups(  # noqa: PLR0913
    db_path: Path,
    window_key: str | None,
    selected_date: str | None,
    selected_mode: str,
    stop_group_id: str,
    window_type: str,
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        select line, mode, route_short_name, trip_headsign, posts
        from mart_stop_group_line_group_window
        where window_type = ?
          and window_key = ?
          and source_end_date = cast(? as date)
          and mode = ?
          and stop_group_id = ?
        order by line_display_rank, destination_display_rank
        """,
        [window_type, window_key, selected_date, selected_mode, stop_group_id],
    )
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        line_group = grouped.setdefault(
            (row["line"], row["mode"]),
            {"line": row["line"], "mode": row["mode"], "route_short_name": row["route_short_name"], "destinations": []},
        )
        line_group["destinations"].append(
            {
                "trip_headsign": row["trip_headsign"],
                "posts": _stop_line_group_posts(stop_group_id, row.get("posts") or []),
            }
        )
    return list(grouped.values())


def _stop_line_group_posts(stop_group_id: str, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **post,
            "display_name": post.get("stop_post_code") or _stop_post_label(post["stop_id"], stop_group_id),
        }
        for post in posts
    ]


def _course_rows(rows: list[dict[str, Any]]) -> dict[tuple[int, str], list[dict[str, Any]]]:
    result: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in rows:
        _attach_shape(row)
        result.setdefault((row["direction_id"], row["trip_headsign"]), []).append(row)
    return result


def _status_summary_by_mode(db_path: Path) -> dict[str, dict[str, Any]]:
    rows = fetch_all(db_path, "select * from mart_pipeline_status_recent_summary order by mode")
    return _by_mode(rows)


def _status_days(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    days: dict[Any, dict[str, Any]] = {}
    for row in rows:
        day = days.setdefault(row["service_date"], {"service_date": row["service_date"]})
        day[row["mode"]] = row
    return list(days.values())


def _attach_shape(row: dict[str, Any]) -> None:
    row["shape"] = _delay_shape(
        row.get("median_delay_seconds"),
        row.get("on_time_rate"),
        row.get("delay_histogram"),
        row.get("p90_delay_seconds"),
    )


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


def _timeline_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points = []
    for row in rows:
        if row.get("delay_seconds") is None:
            continue
        delay = round(float(row["delay_seconds"]))
        points.append(
            {
                "x": _clamp(float(row.get("x") or 0), 0, 100),
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
    return [
        {
            "direction": row.get("trip_headsign"),
            "counts": {
                "clean": row.get("clean_count") or 0,
                "partial": row.get("partial_count") or 0,
                "broken": row.get("broken_count") or 0,
            },
            "outcomes": row.get("outcomes") or [],
        }
        for row in rows
    ]


def _period_reliability(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("direction_id"), row.get("trip_headsign"))
        aggregate = grouped.setdefault(
            key,
            {
                "trip_headsign": row.get("trip_headsign"),
                "clean_count": 0,
                "partial_count": 0,
                "broken_count": 0,
                "outcomes": [],
            },
        )
        for outcome in ("clean", "partial", "broken"):
            aggregate[f"{outcome}_count"] += row.get(f"{outcome}_count") or 0
        if len(aggregate["outcomes"]) < MAX_RELIABILITY_OUTCOMES:
            aggregate["outcomes"].extend(
                (row.get("outcomes") or [])[: MAX_RELIABILITY_OUTCOMES - len(aggregate["outcomes"])]
            )
    aggregates = sorted(
        grouped.values(),
        key=lambda row: -(row["clean_count"] + row["partial_count"] + row["broken_count"]),
    )
    return _reliability_strip_from_rows(aggregates)


def _on_time_segments(
    on_time_rate: float, early_count: int | None = None, on_time_count: int | None = None, late_count: int | None = None
) -> dict[str, float]:
    if early_count is not None and on_time_count is not None and late_count is not None:
        total = early_count + on_time_count + late_count
        if total > 0:
            return {"early": early_count / total, "on_time": on_time_count / total, "late": late_count / total}
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
        db_path, "select service_date_key as service_date from dim_serving_date order by service_date desc"
    )
    return [row["service_date"] for row in rows]


def normalize_window(value: str | None) -> str:
    """Return a supported aggregate window, defaulting invalid input to day."""
    return value if value in WINDOW_TYPES else "day"


def _available_window(db_path: Path, value: str | None) -> str:
    window_type = normalize_window(value)
    if window_type == "day":
        return window_type
    return window_type if grouped_windows_available(db_path) else "day"


def grouped_windows_available(db_path: Path) -> bool:
    """Return whether the artifact contains the complete grouped-window contract."""
    capability = fetch_one(
        db_path,
        """
        select 1 as found
        from information_schema.tables
        where table_name in ('dim_serving_window_date', 'dim_schedule_version', 'mart_entity_window_daily_summary')
        having count(distinct table_name) = 3
        """,
    )
    return capability is not None


def _window_context(
    db_path: Path,
    window_type: str,
    selected_date: str | None,
    summary: dict[str, Any] | None,
    *,
    prefer_summary: bool = False,
) -> dict[str, Any]:
    summary = summary or {}
    membership = None
    has_membership = fetch_one(
        db_path,
        "select 1 as found from information_schema.tables where table_name = 'dim_serving_window_date' limit 1",
    )
    if has_membership and selected_date:
        membership = fetch_one(
            db_path,
            """
            select min(service_date) as source_start_date,
                   max(service_date) as source_observed_end_date,
                   count(*) as source_day_count
            from dim_serving_window_date
            where window_type = ?
              and window_key = ?
              and source_end_date = cast(? as date)
            """,
            [window_type, _window_key(selected_date, window_type), selected_date],
        )
    source_start = (
        (summary.get("source_start_date") if prefer_summary else (membership or {}).get("source_start_date"))
        or summary.get("source_start_date")
        or selected_date
    )
    source_end = (membership or {}).get("source_observed_end_date") or summary.get("source_end_date") or selected_date
    source_day_count = (
        (summary.get("source_day_count") if prefer_summary else (membership or {}).get("source_day_count"))
        or summary.get("source_day_count")
        or (1 if selected_date and window_type == "day" else 0)
    )
    start_day = _as_date(source_start)
    end_day = _as_date(source_end)
    if end_day is None:
        return {"label": "n/a", "source_day_count": 0}
    if window_type == "day":
        label = end_day.strftime("%d %b %Y")
    elif window_type == "month":
        label = end_day.strftime("%b %Y")
    else:
        date_range = f"{start_day:%d %b}–{end_day:%d %b}" if start_day else f"through {end_day:%d %b}"  # noqa: RUF001
        label = f"{date_range} · {source_day_count}d"
    return {
        "label": label,
        "source_start_date": source_start,
        "source_end_date": source_end,
        "source_day_count": source_day_count,
    }


def _window_date_nav(date_options: list[str], selected_date: str | None, window_type: str) -> dict[str, str | None]:
    if window_type == "day" or selected_date is None:
        return _date_nav(date_options, selected_date)
    selected_day = date.fromisoformat(selected_date)
    available_days = sorted(date.fromisoformat(value) for value in date_options)
    if window_type in {"weekdays", "weekend"}:
        previous_target = selected_day - timedelta(days=7)
        next_target = selected_day + timedelta(days=7)
        previous = max((day for day in available_days if day <= previous_target), default=None)
        next_day = min((day for day in available_days if day >= next_target), default=None)
        return {
            "previous": previous.isoformat() if previous else None,
            "next": next_day.isoformat() if next_day else None,
        }
    months = sorted({value[:7] for value in date_options})
    selected_month = selected_date[:7]
    if selected_month not in months:
        return {"previous": None, "next": None}
    month_index = months.index(selected_month)

    def latest_in_month(month: str) -> str | None:
        return max((value for value in date_options if value.startswith(month)), default=None)

    return {
        "previous": latest_in_month(months[month_index - 1]) if month_index > 0 else None,
        "next": latest_in_month(months[month_index + 1]) if month_index + 1 < len(months) else None,
    }


def _as_date(value: date | str | None) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    return date.fromisoformat(str(value))


def _window_key(selected_date: str | None, window_type: str) -> str | None:
    if selected_date is None:
        return None
    return selected_date[:7] if window_type == "month" else selected_date


def _selected_date(date_options: list[str], selected_date: str | None) -> str | None:
    if selected_date in date_options:
        return selected_date
    if date_options:
        return date_options[0]
    return None


def _date_nav(date_options: list[str], selected_date: str | None) -> dict[str, str | None]:
    if selected_date not in date_options:
        return {"previous": None, "next": None}
    selected_index = date_options.index(selected_date)
    return {
        "previous": date_options[selected_index + 1] if selected_index + 1 < len(date_options) else None,
        "next": date_options[selected_index - 1] if selected_index > 0 else None,
    }


def _by_mode(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["mode"]: row for row in rows}


def _by_mode_list(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["mode"], []).append(row)
    return grouped


def _line_rail_groups(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    regular: list[dict[str, Any]] = []
    replacement: list[dict[str, Any]] = []
    night: list[dict[str, Any]] = []
    for row in rows:
        line = str(row["line"]).upper()
        if line.startswith("Z"):
            replacement.append(row)
        elif line.startswith("N"):
            night.append(row)
        else:
            regular.append(row)

    for group in (regular, replacement, night):
        group.sort(key=_line_rail_sort_key)
    return [group for group in (regular, replacement, night) if group]


def _line_rail_sort_key(row: dict[str, Any]) -> tuple[int, int, str]:
    line = str(row["line"]).upper()
    if line.isdigit():
        return 0, int(line), line
    number = "".join(character for character in line if character.isdigit())
    return 1, int(number or 0), line


def _trip_trace(delays: list[int]) -> list[dict[str, Any]]:
    points = []
    for delay in delays:
        tone = _delay_tone(delay)
        offset = (
            min(TRIP_TRACE_LATE_MAX_PX, round(delay / TRIP_TRACE_LATE_SCALE_SECONDS))
            if delay >= 0
            else -min(TRIP_TRACE_EARLY_MAX_PX, round(abs(delay) / TRIP_TRACE_EARLY_SCALE_SECONDS))
        )
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
    mode_order = 0 if "bus" in groups else 1 if "tram" in groups else 2
    return (mode_order, _stop_post_sort_key(post["display_name"]))


def _stop_post_mode_groups(modes_served: str) -> list[str]:
    modes = {mode.strip() for mode in modes_served.split(",") if mode.strip()}
    groups = [mode for mode in ("bus", "tram") if mode in modes]
    return groups or ["other"]


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


def _resolve_stop_post_id(stop_posts: list[dict[str, Any]], selected_stop_id: str | None) -> str | None:
    if selected_stop_id is None:
        return None
    for post in stop_posts:
        if selected_stop_id in {post["stop_id"], post["display_name"]}:
            return post["stop_id"]
    return None


def _selected_line_rank(value: str | None) -> str:
    return value if value in {"worst", "best", "erratic"} else "worst"


def _selected_trip_rank(value: str | None) -> str:
    return value if value in {"worst", "best", "erratic"} else "worst"


def _selected_stop_rank(value: str | None) -> str:
    return value if value in {"worst", "busiest", "best"} else "worst"


def _selected_page(value: str | None) -> int:
    try:
        return min(MAX_PAGE, max(1, int(value or 1)))
    except ValueError:
        return 1


def _page_result(
    rows: list[dict[str, Any]], page: int, page_size: int = LANDING_PAGE_SIZE
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return rows[:page_size], {
        "page": page,
        "first_item": (page - 1) * page_size + 1,
        "has_previous": page > 1,
        "has_next": len(rows) > page_size,
    }


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
