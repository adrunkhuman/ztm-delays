"""Direct-trip planner over the published planner artifact.

The artifact (``planner/planner.duckdb`` beside the serving export) is rebuilt nightly by the pipeline for the
coming days and is read-only here. Its tables:

- ``planner_metadata``: ``build_id``, ``built_at``, ``model_version``, ``first_date``, ``last_date``.
- ``planner_stop_group``: one search entry per stop group (all posts of a stop): ``stop_group_id``, ``name``,
  ``search_key`` (lowercase, accents removed, ``ł`` -> ``l``), ``lines`` (list), ``visits``.
- ``planner_trip``: ``trip_key``, ``service_date``, ``mode``, ``line``, ``headsign``.
- ``planner_stop``: per scheduled stop of a trip: ``trip_key``, ``stop_sequence``, ``stop_id``, ``stop_group_id``,
  ``stop_name``, ``scheduled_sod`` (seconds after service-date midnight, may exceed 24 h), ``usual_delay_s`` (median
  delay there), ``late_delay_s`` (90th percentile), ``leave_by_offset_s`` (<= 0: be at the stop this long before
  the timetable; null at the last stop), ``ride_from_start_s`` (predicted ride from the trip's first stop).
- ``planner_range``: calibrated ride-time spread, complete for every ``is_tram`` x ``weekday`` x ``hour`` x ride
  bucket ``(min_ride_s, max_ride_s]``: ``low_ratio``/``high_ratio`` are the 10th/90th percentile of actual/predicted.
"""

from __future__ import annotations

import math
import unicodedata
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from ztm_frontend.db import fetch_all, fetch_one

if TYPE_CHECKING:
    from pathlib import Path

RESULTS = 6
SUGGESTIONS = 8
MIN_QUERY_LENGTH = 2
DAY_SECONDS = 86_400
EARLIER_STEP_SECONDS = 30 * 60
DEFAULT_HIGH_RATIO = 1.1
TIMETABLE_FLAG_SECONDS = 120  # expected times at least this far from the timetable get a marker


def search_key(text: str) -> str:
    """Lowercase, accent-free form used for stop search; ``ł`` has no Unicode decomposition, so map it explicitly."""
    decomposed = unicodedata.normalize("NFKD", text.lower().replace("ł", "l"))
    return " ".join("".join(c for c in decomposed if not unicodedata.combining(c)).split())


def available_dates(path: Path) -> list[date]:
    """Service dates the artifact covers, oldest first."""
    meta = fetch_one(path, "select first_date, last_date from planner_metadata limit 1")
    if not meta:
        return []
    first, last = meta["first_date"], meta["last_date"]
    return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]


def suggest(path: Path, query: str) -> list[dict[str, Any]]:
    """Stop groups matching typed text: exact name, then name prefix, then word prefix, then busier stops."""
    key = search_key(query)
    if len(key) < MIN_QUERY_LENGTH:
        return []
    return fetch_all(
        path,
        """
        select stop_group_id, name, lines
        from planner_stop_group
        where contains(search_key, ?)
        order by search_key = ? desc, starts_with(search_key, ?) desc, contains(search_key, ' ' || ?) desc, visits desc, name
        limit ?
        """,
        [key, key, key, key, SUGGESTIONS],
    )


def resolve_stop_group(path: Path, stop_group_id: str | None, typed: str | None) -> dict[str, Any] | None:
    """The chosen group, or the best match for typed text that no longer names the chosen group."""
    chosen = None
    if stop_group_id:
        chosen = fetch_one(
            path, "select stop_group_id, name, lines from planner_stop_group where stop_group_id = ?", [stop_group_id]
        )
    if typed and (chosen is None or search_key(typed) != search_key(chosen["name"])):
        matches = suggest(path, typed)
        chosen = matches[0] if matches else None
    return chosen


def departures(path: Path, origin: str, destination: str, day: date, after_sod: int) -> list[dict[str, Any]]:
    """Next direct trips from any post of ``origin`` to a later post of ``destination``.

    Trips expected to leave at or after ``after_sod`` on ``day``; trips of the previous service date running
    past midnight count too.
    """
    rows = fetch_all(
        path,
        """
        with board as (select * from planner_stop where stop_group_id = ?),
        alight as (select * from planner_stop where stop_group_id = ?),
        pairs as (
            select
                t.trip_key, t.service_date, t.mode, t.line, t.headsign,
                board.stop_sequence as board_sequence, alight.stop_sequence as alight_sequence,
                board.stop_id as board_stop_id, board.stop_name as board_name, alight.stop_name as alight_name,
                board.scheduled_sod + ? * (t.service_date - ?::date) as board_scheduled,
                alight.scheduled_sod + ? * (t.service_date - ?::date) as alight_scheduled,
                board.usual_delay_s, board.late_delay_s, coalesce(board.leave_by_offset_s, 0) as leave_by_offset_s,
                (alight.ride_from_start_s - board.ride_from_start_s)::double as ride_s,
                alight.stop_sequence - board.stop_sequence as stop_count,
                (board.scheduled_sod // 3600) % 24 as board_hour,
                -- weekday/weekend by service date, as the ranges were calibrated
                isodow(t.service_date) <= 5 as weekday
            from board
            join alight using (trip_key)
            join planner_trip t using (trip_key)
            where alight.stop_sequence > board.stop_sequence and t.service_date in (?::date, ?::date - 1)
            qualify row_number() over (partition by t.trip_key order by alight.stop_sequence - board.stop_sequence) = 1
        )
        select pairs.*, coalesce(r.high_ratio, ?)::double as high_ratio
        from pairs
        left join planner_range r
            on r.is_tram = (pairs.mode = 'tram') and r.weekday = pairs.weekday and r.hour = pairs.board_hour
            and pairs.ride_s > r.min_ride_s and pairs.ride_s <= r.max_ride_s
        where board_scheduled + usual_delay_s >= ?
        order by board_scheduled + usual_delay_s, ride_s
        limit ?
        """,
        [origin, destination, DAY_SECONDS, day, DAY_SECONDS, day, day, day, DEFAULT_HIGH_RATIO, after_sod, RESULTS],
    )
    return [_departure(row) for row in rows]


def _departure(row: dict[str, Any]) -> dict[str, Any]:
    depart = row["board_scheduled"] + row["usual_delay_s"]
    arrive = depart + row["ride_s"]
    # Late-side spread of the departure plus that of the ride; summed because combining them as independent
    # errors under-covered on held-out data (~86% instead of 90%).
    arrive_by = arrive + (row["late_delay_s"] - row["usual_delay_s"]) + row["ride_s"] * (row["high_ratio"] - 1)
    return {
        **row,
        "depart": _round_minute(depart),
        "depart_differs": differs_from_timetable(depart, row["board_scheduled"]),
        "leave_by": _floor_minute(row["board_scheduled"] + row["leave_by_offset_s"]),
        "arrive": _round_minute(arrive),
        "arrive_differs": differs_from_timetable(arrive, row["alight_scheduled"]),
        "arrive_by": _ceil_minute(arrive_by),
        "ride_minutes": max(1, round(row["ride_s"] / 60)),
        "timetable_minutes": max(1, round((row["alight_scheduled"] - row["board_scheduled"]) / 60)),
    }


def trip_stops(path: Path, trip_key: int, board_sequence: int, alight_sequence: int, day: date) -> list[dict[str, Any]]:
    """Stops from boarding to alighting with expected times, given the boarding stop's usual delay."""
    rows = fetch_all(
        path,
        """
        select s.stop_sequence, s.stop_name, s.stop_id,
            s.scheduled_sod + ? * (t.service_date - ?::date) as scheduled,
            s.ride_from_start_s::double as ride_from_start_s, s.usual_delay_s
        from planner_stop s join planner_trip t using (trip_key)
        where s.trip_key = ? and s.stop_sequence between ? and ?
        order by s.stop_sequence
        """,
        [DAY_SECONDS, day, trip_key, board_sequence, alight_sequence],
    )
    if not rows:
        return []
    start = rows[0]["scheduled"] + rows[0]["usual_delay_s"] - rows[0]["ride_from_start_s"]
    stops = []
    for row in rows:
        expected = start + row["ride_from_start_s"]
        stops.append(
            {**row, "expected": _round_minute(expected), "differs": differs_from_timetable(expected, row["scheduled"])}
        )
    return stops


def differs_from_timetable(expected: float, scheduled: float) -> bool:
    """Whether an expected time is far enough from the timetable to point out."""
    return abs(expected - scheduled) >= TIMETABLE_FLAG_SECONDS


def earlier_after(after_sod: int) -> int:
    """Search start for the 'earlier' link."""
    return max(0, after_sod - EARLIER_STEP_SECONDS)


def clock(sod: float | None) -> str:
    """HH:MM for seconds after midnight; times past midnight wrap."""
    if sod is None:
        return ""
    minutes = round(sod) // 60
    return f"{minutes // 60 % 24:02d}:{minutes % 60:02d}"


def parse_clock(text: str | None, default: int) -> int:
    """Seconds after midnight for ``HH:MM``; ``default`` for anything else."""
    try:
        hours, minutes = (int(part) for part in (text or "").split(":")[:2])
    except ValueError:
        return default
    if not (0 <= hours < 24 and 0 <= minutes < 60):  # noqa: PLR2004
        return default
    return hours * 3600 + minutes * 60


def _round_minute(seconds: float) -> int:
    return round(seconds / 60) * 60


def _floor_minute(seconds: float) -> int:
    return math.floor(seconds / 60) * 60


def _ceil_minute(seconds: float) -> int:
    return math.ceil(seconds / 60) * 60


def get_page(path: Path, args: dict[str, str], today: date, now_sod: int) -> dict[str, Any]:
    """Template context for the planner: the search form state and, with both stops chosen, departures."""
    dates = available_dates(path)
    requested = parse_date(args.get("date"))
    # Default to today; outside the published window fall back to its first day.
    day = requested if requested in dates else today if today in dates else (dates[0] if dates else today)
    after = parse_clock(args.get("time"), now_sod)
    origin = resolve_stop_group(path, args.get("from"), args.get("q_from"))
    destination = resolve_stop_group(path, args.get("to"), args.get("q_to"))
    results: list[dict[str, Any]] = []
    if origin and destination and origin["stop_group_id"] != destination["stop_group_id"]:
        results = departures(path, origin["stop_group_id"], destination["stop_group_id"], day, after)
    later = results[-1]["depart"] + 60 if results else None
    return {
        "dates": dates,
        "day": day,
        "time": clock(after),
        "origin": origin,
        "destination": destination,
        "results": results,
        "earlier_time": clock(earlier_after(after)) if after > 0 and results else None,
        "later_time": clock(later) if later is not None and later < DAY_SECONDS else None,
        "searched": bool(args.get("from") or args.get("q_from") or args.get("to") or args.get("q_to")),
    }


def parse_date(text: str | None) -> date | None:
    """ISO date, or None for missing or malformed input."""
    try:
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None
