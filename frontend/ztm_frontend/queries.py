from __future__ import annotations

# ruff: noqa: S608
import json
import re
from datetime import date
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


def get_overview(db_path: Path, selected_date: str | None) -> dict[str, Any]:
    """Build the network overview page data."""
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    mode_stats = _mode_stats(db_path, selected_date)
    worst_lines = _ranked_entities(db_path, selected_date, "line", "median_delay_seconds", limit=8)
    worst_stops = _ranked_entities(db_path, selected_date, "stop_post", "median_delay_seconds", limit=8)
    for row in worst_lines:
        _attach_shape(row)
    for row in worst_stops:
        row["post_label"] = row.get("stop_post_code") or _stop_post_label(row["stop_id"], row["stop_group_id"])
        row["display_name"] = f"{row['stop_group_name']} [{row['stop_post_code']}]"
        _attach_shape(row)
    mode_stats_by_mode = _by_mode(mode_stats)
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "mode_stats": mode_stats_by_mode,
        "overview_widgets": _overview_widgets(db_path, mode_stats_by_mode, selected_date),
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
) -> dict[str, Any]:
    """Build the line landing or selected-line page data."""
    selected_mode = selected_mode or "bus"
    selected_rank = _selected_line_rank(selected_rank)
    page = _selected_page(selected_page)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    line_list = fetch_all(
        db_path,
        """
        select line, mode, route_short_name, trip_count, arrival_count
        from mart_line_window_summary
        where window_type = 'day'
          and window_key = ?
          and mode = ?
          and universe_type = 'all_observed'
        order by mode, try_cast(line as integer), line
        """,
        [selected_date, selected_mode],
    )
    summary = None
    courses: list[dict[str, Any]] = []
    line_landing_summary = None
    line_landing_rows: list[dict[str, Any]] = []
    pagination = None
    if selected_line is None:
        line_landing_summary = _line_landing_summary(db_path, selected_date, selected_mode)
        line_landing_rows, pagination = _line_landing_rows(db_path, selected_date, selected_mode, selected_rank, page)
    else:
        summary = fetch_one(
            db_path,
            """
            select *
            from mart_line_window_summary
            where window_type = 'day'
              and window_key = ?
              and mode = ?
              and line = ?
              and universe_type = 'all_observed'
            limit 1
            """,
            [selected_date, selected_mode, selected_line],
        )
        courses = fetch_all(
            db_path,
            """
            select direction_id, trip_headsign, trip_count
            from mart_line_course_window
            where window_type = 'day'
              and window_key = ?
              and mode = ?
              and line = ?
            order by course_rank
            """,
            [selected_date, selected_mode, selected_line],
        )
        stops = fetch_all(
            db_path,
            """
            select *
            from mart_line_course_stop_window
            where window_type = 'day'
              and window_key = ?
              and mode = ?
              and line = ?
            order by direction_id, trip_headsign, display_rank, stop_group_id, stop_id
            """,
            [selected_date, selected_mode, selected_line],
        )
        stops_by_course = _course_rows(stops)
        for course in courses:
            course["stops"] = stops_by_course.get((course["direction_id"], course["trip_headsign"]), [])
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
        "line_widgets": _line_widgets(db_path, selected_line, selected_date, summary, courses),
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
) -> dict[str, Any]:
    """Build the stop landing, group, or selected-post page data."""
    selected_mode = selected_mode or "bus"
    selected_view = selected_view if selected_view in {"post", "line"} else "post"
    selected_rank = _selected_stop_rank(selected_rank)
    page = _selected_page(selected_page)
    picker_page = _selected_page(selected_picker_page)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
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
        stop_landing_summary = _stop_landing_summary(db_path, selected_date, selected_mode)
        stop_landing_rows, pagination = _stop_landing_rows(db_path, selected_date, selected_mode, selected_rank, page)
    else:
        summary = fetch_one(
            db_path,
            """
            select *
            from mart_stop_group_window_summary
            where window_type = 'day'
              and window_key = ?
              and mode = ?
              and stop_group_id = ?
              and universe_type = 'all_observed'
            limit 1
            """,
            [selected_date, selected_mode, selected_stop_group_id],
        )
        stop_posts = fetch_all(
            db_path,
            """
            select *
            from mart_stop_post_window_summary
            where window_type = 'day'
              and window_key = ?
              and mode = ?
              and stop_group_id = ?
              and universe_type = 'all_observed'
            order by stop_id
            """,
            [selected_date, selected_mode, selected_stop_group_id],
        )
        line_groups_by_post = _line_groups_by_post(db_path, selected_date, selected_mode, selected_stop_group_id)
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
                where window_type = 'day'
                  and window_key = ?
                  and mode = ?
                  and stop_id = ?
                  and universe_type = 'all_observed'
                limit 1
                """,
                [selected_date, selected_mode, selected_stop_id],
            )
            if selected_post is not None:
                selected_post["display_name"] = selected_post.get("stop_post_code") or selected_post["stop_id"]
        if selected_stop_id is not None:
            line_stats, pagination = _stop_line_rows(db_path, selected_date, selected_mode, selected_stop_id, page)
        stop_line_groups = _stop_group_line_groups(db_path, selected_date, selected_mode, selected_stop_group_id)
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "date_nav": _date_nav(date_options, selected_date),
        "stop_list": stop_list,
        "picker_pagination": picker_pagination,
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
        "stop_widgets": _stop_widgets(
            db_path,
            stop_posts,
            selected_post or summary,
            line_stats,
            {"selected_date": selected_date, "selected_mode": selected_mode},
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
) -> dict[str, Any]:
    """Build the trip landing or selected-line trip page data."""
    selected_mode = selected_mode or "bus"
    selected_sort = selected_sort if selected_sort in {"departure", "delay", "erratic"} else "departure"
    selected_rank = _selected_trip_rank(selected_rank)
    page = _selected_page(selected_page)
    date_options = _date_options(db_path)
    selected_date = _selected_date(date_options, selected_date)
    line_list = fetch_all(
        db_path,
        """
        select line, mode, route_short_name, trip_count
        from mart_trip_line_daily
        where service_date = ?
          and mode = ?
        order by line_display_rank
        """,
        [selected_date, selected_mode],
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
                """
            select *
            from mart_trip_mode_daily_summary
            where service_date = ?
              and mode = ?
            limit 1
            """,
                [selected_date, selected_mode],
            )
            or {}
        )
        trip_landing_rows, pagination = _trip_landing_rows(db_path, selected_date, selected_mode, selected_rank, page)
    else:
        rank_column = {"departure": "departure_rank", "delay": "line_end_delay_rank", "erratic": "line_erratic_rank"}[
            selected_sort
        ]
        trips, pagination = _selected_line_trip_rows(
            db_path, selected_date, selected_mode, selected_line, rank_column, page
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
                selected_date,
                selected_trip.get("gtfs_snapshot_id"),
                selected_trip["trip_id"],
                selected_trip["vehicle_number"],
            )
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
        "trip_groups": _trip_groups(db_path, selected_date, selected_mode, selected_line, trips),
        "trip_landing_summary": trip_landing_summary,
        "trip_landing_rows": trip_landing_rows,
        "pagination": pagination,
        "selected_trip": selected_trip,
        "trip_stops": trip_stops,
    }


def get_trip_detail(
    db_path: Path, trip_id: str, selected_date: str | None, selected_vehicle: str | None
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
    return {"selected_date": selected_date, "trip": trip, "trip_stops": trip_stops}


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


def _line_landing_summary(db_path: Path, selected_date: str | None, selected_mode: str) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
        select line_count, arrival_count, median_delay_seconds, on_time_rate
        from mart_mode_window_summary
        where window_type = 'day'
          and window_key = ?
          and mode = ?
        limit 1
        """,
            [selected_date, selected_mode],
        )
        or {}
    )


def _line_landing_rows(
    db_path: Path, selected_date: str | None, selected_mode: str, selected_rank: str, page: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metric = {"worst": "median_delay_seconds", "best": "on_time_rate", "erratic": "delay_spread_seconds"}[selected_rank]
    rows = _ranked_entities(
        db_path,
        selected_date,
        "line",
        metric,
        selected_mode,
        limit=LANDING_PAGE_SIZE + 1,
        offset=(page - 1) * LANDING_PAGE_SIZE,
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        _attach_shape(row)
    return rows, pagination


def _stop_landing_summary(db_path: Path, selected_date: str | None, selected_mode: str) -> dict[str, Any]:
    return (
        fetch_one(
            db_path,
            """
        select stop_group_count, arrival_count, median_delay_seconds, on_time_rate
        from mart_mode_window_summary
        where window_type = 'day'
          and window_key = ?
          and mode = ?
        limit 1
        """,
            [selected_date, selected_mode],
        )
        or {}
    )


def _stop_landing_rows(
    db_path: Path, selected_date: str | None, selected_mode: str, selected_rank: str, page: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metric = {"worst": "median_delay_seconds", "best": "on_time_rate", "busiest": "arrival_count"}[selected_rank]
    rows = _ranked_entities(
        db_path,
        selected_date,
        "stop_group",
        metric,
        selected_mode,
        limit=LANDING_PAGE_SIZE + 1,
        offset=(page - 1) * LANDING_PAGE_SIZE,
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        _attach_shape(row)
    return rows, pagination


def _trip_landing_rows(
    db_path: Path, selected_date: str | None, selected_mode: str, selected_rank: str, page: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rank_column = {"worst": "landing_worst_rank", "best": "landing_best_rank", "erratic": "landing_erratic_rank"}[
        selected_rank
    ]
    rows = fetch_all(
        db_path,
        f"""
        select *
        from mart_trip_daily
        where service_date = ?
          and mode = ?
          and trip_quality = 'complete'
          and {rank_column} is not null
        order by {rank_column}
        limit ? offset ?
        """,
        [selected_date, selected_mode, LANDING_PAGE_SIZE + 1, (page - 1) * LANDING_PAGE_SIZE],
    )
    rows, pagination = _page_result(rows, page)
    for row in rows:
        row["trace"] = _trip_trace(row.get("delay_profile") or [])
    return rows, pagination


def _selected_line_trip_rows(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    selected_mode: str,
    selected_line: str,
    rank_column: str,
    page: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = fetch_all(
        db_path,
        f"""
        select *
        from mart_trip_daily
        where service_date = ?
          and mode = ?
          and line = ?
          and trip_quality = 'complete'
        order by {rank_column}
        limit ? offset ?
        """,
        [selected_date, selected_mode, selected_line, LANDING_PAGE_SIZE + 1, (page - 1) * LANDING_PAGE_SIZE],
    )
    return _page_result(rows, page)


def _stop_line_rows(
    db_path: Path, selected_date: str | None, selected_mode: str, selected_stop_id: str, page: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        select *
        from mart_stop_line_window_summary
        where window_type = 'day'
          and window_key = ?
          and mode = ?
          and entity_type = 'stop_post'
          and entity_id = ?
        order by display_rank
        limit ? offset ?
        """,
        [selected_date, selected_mode, selected_stop_id, LANDING_PAGE_SIZE + 1, (page - 1) * LANDING_PAGE_SIZE],
    )
    return _page_result(rows, page)


def _ranked_entities(  # noqa: PLR0913
    db_path: Path,
    selected_date: str | None,
    entity_type: str,
    metric: str,
    selected_mode: str | None = None,
    *,
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
    params: list[Any] = [entity_type, metric, selected_date, selected_date, selected_mode, selected_mode]
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
          and rankings.window_type = 'day'
          and rankings.window_key = ?
          and summaries.window_type = 'day'
          and summaries.window_key = ?
          and (? is null or rankings.mode = ?)
        {limit_sql}
        order by rankings.mode, rankings.rank
        """,
        params,
    )


def _mode_stats(db_path: Path, selected_date: str | None) -> list[dict[str, Any]]:
    return fetch_all(
        db_path,
        """
        select *
        from mart_mode_window_summary
        where window_type = 'day'
          and window_key = ?
        order by mode
        """,
        [selected_date],
    )


def _overview_widgets(
    db_path: Path, mode_stats: dict[str, dict[str, Any]], selected_date: str | None
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
            "hours": _hour_bars(db_path, "mode", mode, mode, selected_date),
            "week": _week_bars(db_path, "mode", mode, mode, selected_date),
            "segments": _on_time_segments(
                row.get("on_time_rate") or 0, row.get("early_count"), row.get("on_time_count"), row.get("late_count")
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
    if summary is None or selected_line is None:
        return {}
    for course in courses:
        for stop in course.get("stops", []):
            _attach_shape(stop)
            stop["direction"] = course["trip_headsign"]
    worst_rows = fetch_all(
        db_path,
        """
        select time_label as time, trip_headsign as direction, stop_name, stop_group_id, delay_seconds
        from mart_worst_delay_event
        where service_date = ?
          and mode = ?
          and scope_type = 'line'
          and scope_id = ?
          and delay_rank <= 6
        order by delay_rank
        """,
        [selected_date, summary.get("mode"), selected_line],
    )
    reliability_rows = fetch_all(
        db_path,
        """
        select direction_id, trip_headsign, clean_count, partial_count, broken_count, outcomes
        from mart_line_reliability_daily
        where service_date = ?
          and mode = ?
          and line = ?
        order by display_rank
        """,
        [selected_date, summary.get("mode"), selected_line],
    )
    return {
        "shape": _delay_shape(
            summary.get("median_delay_seconds"),
            summary.get("on_time_rate"),
            summary.get("delay_histogram"),
            summary.get("p90_delay_seconds"),
        ),
        "hours": _hour_bars(db_path, "line", selected_line, summary.get("mode"), selected_date),
        "week": _week_bars(db_path, "line", selected_line, summary.get("mode"), selected_date),
        "timeline": _timeline(db_path, "line", selected_line, summary.get("mode"), selected_date),
        "segments": _on_time_segments(
            summary.get("on_time_rate") or 0,
            summary.get("early_count"),
            summary.get("on_time_count"),
            summary.get("late_count"),
        ),
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
    posts = []
    for post in stop_posts:
        _attach_shape(post)
        post["hours"] = _hour_bars(db_path, "stop_post", post["stop_id"], selected_mode, selected_date)
        posts.append(post)
    line_rows = []
    for row in line_stats:
        _attach_shape(row)
        row["post_label"] = summary.get("display_name") or summary.get("stop_post_code") or ""
        line_rows.append(row)
    selected_stop_id = summary.get("stop_id")
    entity_type = "stop_post" if selected_stop_id is not None else "stop_group"
    entity_id = selected_stop_id or summary.get("stop_group_id")
    worst_rows = fetch_all(
        db_path,
        """
        select time_label as time, line, mode, trip_headsign as headsign, delay_seconds
        from mart_worst_delay_event
        where service_date = ?
          and mode = ?
          and scope_type = ?
          and scope_id = ?
          and delay_rank <= 8
        order by delay_rank
        """,
        [selected_date, selected_mode, entity_type, entity_id],
    )
    return {
        "posts": posts,
        "shape": _delay_shape(
            summary.get("median_delay_seconds"),
            summary.get("on_time_rate"),
            summary.get("delay_histogram"),
            summary.get("p90_delay_seconds"),
        ),
        "hours": _hour_bars(db_path, entity_type, entity_id, selected_mode, selected_date),
        "week": _week_bars(db_path, entity_type, entity_id, selected_mode, selected_date),
        "timeline": _timeline(db_path, entity_type, entity_id, selected_mode, selected_date),
        "segments": _on_time_segments(
            summary.get("on_time_rate") or 0,
            summary.get("early_count"),
            summary.get("on_time_count"),
            summary.get("late_count"),
        ),
        "worst": worst_rows,
        "line_rows": line_rows,
    }


def _hour_bars(
    db_path: Path, entity_type: str, entity_id: str | None, mode: str | None, selected_date: str | None
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        select local_hour, median_delay_seconds, has_min_sample
        from mart_hour_window_summary
        where window_type = 'day'
          and window_key = ?
          and entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
        order by service_hour_index
        """,
        [selected_date, entity_type, entity_id, mode, mode],
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
    rows = fetch_all(
        db_path,
        """
        select cast(service_date as varchar) as service_date, median_delay_seconds
        from mart_entity_daily_summary
        where entity_type = ?
          and entity_id = ?
          and (? is null or mode = ?)
          and service_date between cast(? as date) - interval 6 day and cast(? as date)
        order by service_date
        """,
        [entity_type, entity_id, mode, mode, selected_date, selected_date],
    )
    bars = []
    for row in rows:
        service_date = str(row["service_date"])
        delay = row.get("median_delay_seconds")
        height = 0 if delay is None else max(4, min(38, round(abs(float(delay)) * 0.35)))
        bars.append(
            {
                "label": date.fromisoformat(service_date).strftime("%a")[:1],
                "delay": delay,
                "height": height,
                "selected": service_date == selected_date,
            }
        )
    return bars


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


def _trip_groups(
    db_path: Path, selected_date: str | None, selected_mode: str, selected_line: str | None, trips: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if selected_line is None:
        return []
    groups = fetch_all(
        db_path,
        """
        select *
        from mart_line_trip_group_daily
        where service_date = ?
          and mode = ?
          and line = ?
        order by display_rank
        """,
        [selected_date, selected_mode, selected_line],
    )
    trips_by_key: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for trip in trips:
        trips_by_key.setdefault((trip["direction_id"], trip["trip_headsign"]), []).append(trip)
    for group in groups:
        group["trips"] = trips_by_key.get((group["direction_id"], group["trip_headsign"]), [])
    return [group for group in groups if group["trips"]]


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
        order by stop_sequence
        """,
        [selected_date, gtfs_snapshot_id, gtfs_snapshot_id, trip_id, vehicle_number],
    )
    for row in rows:
        row["post_label"] = row.get("stop_post_code") or _stop_post_label(row["stop_id"], row["stop_group_id"])
    return rows


def _line_groups_by_post(
    db_path: Path, selected_date: str | None, selected_mode: str, stop_group_id: str
) -> dict[str, list[dict[str, Any]]]:
    rows = fetch_all(
        db_path,
        """
        select stop_id, trip_headsign, lines
        from mart_stop_post_line_group_window
        where window_type = 'day'
          and window_key = ?
          and mode = ?
          and stop_group_id = ?
        order by stop_id, display_rank
        """,
        [selected_date, selected_mode, stop_group_id],
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


def _stop_group_line_groups(
    db_path: Path, selected_date: str | None, selected_mode: str, stop_group_id: str
) -> list[dict[str, Any]]:
    rows = fetch_all(
        db_path,
        """
        select line, mode, route_short_name, trip_headsign, posts
        from mart_stop_group_line_group_window
        where window_type = 'day'
          and window_key = ?
          and mode = ?
          and stop_group_id = ?
        order by line_display_rank, destination_display_rank
        """,
        [selected_date, selected_mode, stop_group_id],
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
