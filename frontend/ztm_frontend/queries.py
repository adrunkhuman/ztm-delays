from __future__ import annotations

import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from ztm_frontend.db import fetch_all, fetch_one

if TYPE_CHECKING:
    from pathlib import Path

STOP_ROWS_PER_COURSE = 36
STOP_PICKER_LIMIT = 300
DELAY_POINT_LIMIT = 600
NUMERIC_STOP_POST_SUFFIX_LENGTH = 2
STOP_POST_PRIMARY_MODES = ("bus", "tram")
ON_TIME_EARLY_SECONDS = -60
ON_TIME_LATE_SECONDS = 180


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
                sum(mean_delay_seconds * n) / nullif(sum(n), 0) as mean_delay_seconds,
                sum(on_time_rate * n) / nullif(sum(n), 0) as on_time_rate,
                row_number() over (
                    partition by mode
                    order by sum(mean_delay_seconds * n) / nullif(sum(n), 0) desc
                ) as row_number
            from agg_line_daily
            where service_date = ?
            group by line, mode, route_short_name
            having sum(n) >= 100
        )
        select line, mode, route_short_name, mean_delay_seconds, on_time_rate
        from ranked
        where row_number <= 8
        order by mode, mean_delay_seconds desc
        """,
        [selected_date],
    )
    worst_stops = fetch_all(
        db_path,
        """
        with ranked as (
            select
                stop_id,
                stop_group_id,
                mode,
                stop_group_name,
                avg(delay_seconds) as mean_delay_seconds,
                count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate,
                row_number() over (partition by mode order by avg(delay_seconds) desc) as row_number
            from fct_stop_arrival
            where service_date = ?
              and trip_quality = 'complete'
            group by stop_id, stop_group_id, mode, stop_group_name
            having count(*) >= 10
        )
        select stop_id, stop_group_id, mode, stop_group_name, mean_delay_seconds, on_time_rate
        from ranked
        where row_number <= 8
        order by mode, mean_delay_seconds desc
        """,
        [selected_date],
    )
    for stop in worst_stops:
        stop["display_name"] = f"{stop['stop_group_name']} [{_stop_post_label(stop['stop_id'], stop['stop_group_id'])}]"

    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "mode_stats": _by_mode(mode_stats),
        "worst_lines": _by_mode_list(worst_lines),
        "worst_stops": _by_mode_list(worst_stops),
        "delay_plots": {
            "bus": _delay_points(db_path, selected_date, mode="bus"),
            "tram": _delay_points(db_path, selected_date, mode="tram"),
        },
    }


def get_lines(
    db_path: Path, selected_line: str | None, selected_mode: str | None, selected_date: str | None
) -> dict[str, Any]:
    """Build the line picker and selected-line page data."""
    selected_mode = selected_mode or "bus"
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
    if selected_line is None and line_list:
        selected_line = max(line_list, key=lambda row: row["trip_count"] or 0)["line"]

    summary = None
    courses: list[dict[str, Any]] = []
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
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "line_list": line_list,
        "selected_line": selected_line,
        "selected_mode": selected_mode,
        "summary": summary,
        "courses": courses,
        "delay_plot": _delay_points(db_path, selected_date, line=selected_line) if selected_line is not None else [],
    }


def get_stops(  # noqa: PLR0913
    db_path: Path,
    selected_stop_group_id: str | None,
    selected_mode: str | None,
    search: str,
    selected_stop_id: str | None,
    selected_date: str | None,
) -> dict[str, Any]:
    """Build the stop picker and selected-stop page data."""
    selected_mode = selected_mode or "bus"
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
    if selected_stop_group_id is None and stop_list:
        selected_stop_group_id = stop_list[0]["stop_group_id"]

    summary = None
    stop_posts: list[dict[str, Any]] = []
    stop_post_groups: list[dict[str, Any]] = []
    selected_post: dict[str, Any] | None = None
    line_stats: list[dict[str, Any]] = []
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
                    string_agg(distinct mode, ', ' order by mode) as observed_modes
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
                observed_posts.observed_modes as modes_served
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
        stop_posts.sort(key=lambda post: _stop_post_sort_key(post["display_name"]))
        stop_post_groups = _group_stop_posts(stop_posts)
        if stop_posts and not any(post["stop_id"] == selected_stop_id for post in stop_posts):
            selected_stop_id = _default_stop_post_id(stop_posts, selected_mode)

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
                    count(*) filter (where delay_seconds between -60 and 180) / count(*) as on_time_rate
                from fct_stop_arrival
                where stop_id = ?
                  and service_date = ?
                  and trip_quality = 'complete'
                group by stop_id, stop_group_id
                """,
                [selected_stop_id, selected_date],
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
    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "stop_list": stop_list,
        "selected_stop_group_id": selected_stop_group_id,
        "selected_mode": selected_mode,
        "search": search,
        "selected_stop_id": selected_stop_id,
        "summary": summary,
        "stop_posts": stop_posts,
        "stop_post_groups": stop_post_groups,
        "selected_post": selected_post,
        "line_stats": line_stats,
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
) -> dict[str, Any]:
    """Build the individual trip schedule page data."""
    selected_mode = selected_mode or "bus"
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
    if selected_line is None and line_list:
        selected_line = max(line_list, key=lambda row: row["trip_count"] or 0)["line"]

    trips: list[dict[str, Any]] = []
    selected_trip: dict[str, Any] | None = None
    trip_stops: list[dict[str, Any]] = []
    if selected_line is not None:
        trips = fetch_all(
            db_path,
            """
            select
                trip_id,
                vehicle_number,
                route_short_name,
                trip_headsign,
                origin_stop_name,
                destination_stop_name,
                scheduled_start_time,
                scheduled_end_time,
                start_delay_seconds,
                end_delay_seconds,
                stops_expected,
                stops_detected
            from fct_trip
            where service_date = ?
              and mode = ?
              and line = ?
              and trip_quality = 'complete'
            order by scheduled_start_time, trip_headsign, vehicle_number
            limit 160
            """,
            [selected_date, selected_mode, selected_line],
        )
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

    return {
        "date_options": date_options,
        "selected_date": selected_date,
        "selected_mode": selected_mode,
        "selected_line": selected_line,
        "selected_trip_id": selected_trip_id,
        "selected_vehicle": selected_vehicle,
        "line_list": line_list,
        "trips": trips,
        "selected_trip": selected_trip,
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


def _stop_post_mode_groups(modes_served: str) -> list[str]:
    modes = {mode.strip() for mode in modes_served.split(",") if mode.strip()}
    groups = [mode for mode in STOP_POST_PRIMARY_MODES if mode in modes]
    if groups:
        return groups
    return ["other"]


def _group_stop_posts(stop_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = [
        {"mode": "bus", "title": "Bus", "posts": []},
        {"mode": "tram", "title": "Tram", "posts": []},
        {"mode": "other", "title": "Other", "posts": []},
    ]
    by_mode = {group["mode"]: group for group in groups}
    for post in stop_posts:
        for mode in post["mode_groups"]:
            by_mode[mode]["posts"].append(post)
    return [group for group in groups if group["posts"]]


def _default_stop_post_id(stop_posts: list[dict[str, Any]], selected_mode: str) -> str:
    for post in stop_posts:
        if selected_mode in post["mode_groups"]:
            return post["stop_id"]
    return stop_posts[0]["stop_id"]
