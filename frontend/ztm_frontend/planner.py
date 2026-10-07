"""Journey cards over the published planner artifact.

The artifact (``planner/planner.duckdb`` beside the serving export) is rebuilt nightly by the pipeline for the
coming days and is read-only here. Its tables:

- ``planner_metadata``: ``build_id``, ``built_at``, ``model_version``, ``first_date``, ``last_date``.
- ``planner_stop_group``: one search entry per stop group (all posts of a stop): ``stop_group_id``, ``name``,
  ``search_key`` (lowercase, accents removed, ``ł`` -> ``l``), ``lines`` (list), ``visits``.
- ``planner_trip``: ``trip_key``, ``service_date``, ``mode``, ``line``, ``headsign``, ``duty_id`` (GTFS block),
  ``brigade`` (as in the GPS feed), ``shape_id``; the last three are null where the feed has none.
- ``planner_stop``: per scheduled stop of a trip: ``trip_key``, ``stop_sequence``, ``stop_id``, ``stop_group_id``,
  ``stop_name``, ``scheduled_sod`` (seconds after service-date midnight, may exceed 24 h), ``usual_delay_s`` (median
  delay there), ``late_delay_s`` (90th percentile), ``leave_by_offset_s`` (<= 0: be at the stop this long before
  the timetable; null where boarding is prohibited), ``ride_from_start_s`` (predicted ride from the trip's first
  stop), ``expected_sod`` (the trip's expected time there, whichever stop it is boarded at), ``can_alight`` (false
  at pickup-only stops), ``shape_dist_m`` (metres along the trip's shape).
- ``planner_range``: calibrated ride-time spread, complete for every ``is_tram`` x ``weekday`` x ``hour`` x ride
  bucket ``(min_ride_s, max_ride_s]``: ``low_ratio``/``high_ratio`` are the 10th/90th percentile of actual/predicted.
- ``planner_footpath``: directed walks between posts, with ``distance_m`` and ``walk_s``.
- ``planner_stop_post``: ``stop_id``, ``lat``, ``lon``.
- ``planner_shape``: ``shape_id`` and its polyline as parallel lists ``lat``, ``lon``, ``dist_m``.
"""

from __future__ import annotations

import math
import unicodedata
from bisect import bisect_right
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

from ztm_frontend import journey, live_times
from ztm_frontend.db import fetch_all, fetch_one

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

RESULTS = 6
EXTRA_CANDIDATES = 2  # unbeaten journeys beyond RESULTS: a later one can still beat a card on usual times
SUGGESTIONS = 8
MIN_QUERY_LENGTH = 2
DAY_SECONDS = 86_400
EARLIER_STEP_SECONDS = 30 * 60
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


def connections(path: Path, origin: str, destination: str, day: date, after_sod: int) -> list[dict[str, Any]]:
    """Up to RESULTS journey cards, by departure."""
    return search(path, origin, destination, day, after_sod)[0]


def search(  # noqa: PLR0913
    path: Path, origin: str, destination: str, day: date, after_sod: int, now: datetime | None = None
) -> tuple[list[dict[str, Any]], int | None]:
    """Journey cards and where the next page's search starts (None without cards).

    With now, today's trips follow the live positions when there are any (live_times.view).

    The router keeps journeys that win on the late-case arrival; a journey is also dropped when another leaves
    no earlier, with no more changes, and is expected to arrive no later (e.g. a change to the metro whose worst
    case is better but whose usual arrival is later than staying on the bus).
    """
    net = journey.network(path, day)
    current = live_times.view(path, net, day, now) if now is not None else None
    if current is not None:
        net = current.net
    results = journey.plan(net, origin, destination, after_sod, RESULTS + EXTRA_CANDIDATES, useful=_unbeaten)
    shown, hidden = results[:RESULTS], results[RESULTS:]
    later = None
    if shown:
        # The form searches whole minutes: advance past the last raw boarding deadline, unless that would skip
        # an unshown journey leaving in the same minute (the page then starts there, repeating that minute's cards).
        last = _floor_minute(shown[-1].depart)
        later = last if hidden and _floor_minute(hidden[0].depart) == last > after_sod else last + 60
    return [_connection(path, result, day, current) for result in shown], later


def _unbeaten(found: list[journey.Journey]) -> list[journey.Journey]:
    """Journeys no other beats on the card's leave-by minute, changes and usual arrival minute.

    The router's front holds every late-case winner, often a change that saves a minute only in the late case.
    """
    keys = [_usual(result) for result in found]
    return [result for result, key in zip(found, keys, strict=True) if not any(_beats(o, key) for o in keys)]


def _usual(result: journey.Journey) -> tuple[int, int, int]:
    expected = float(result.depart)
    for leg in result.legs:
        expected = expected + leg.walk_s if isinstance(leg, journey.Walk) else float(leg.arrive or 0)
    return _floor_minute(result.depart), result.vehicles - 1, _round_minute(expected)


def _beats(key: tuple[int, int, int], other: tuple[int, int, int]) -> bool:
    """Leaves no earlier, with no more changes, usually arriving no later; equal keys beat neither."""
    return key != other and key[0] >= other[0] and key[1] <= other[1] and key[2] <= other[2]


def _ride_details(path: Path, leg: journey.Ride, day: date) -> dict[str, Any]:
    row = fetch_one(
        path,
        """
        select t.trip_key, t.mode, t.line, t.headsign,
            b.stop_sequence as board_sequence, a.stop_sequence as alight_sequence,
            b.stop_id as board_stop_id, a.stop_id as alight_stop_id,
            b.stop_name as board_name, a.stop_name as alight_name,
            b.scheduled_sod + ? * (t.service_date - ?::date) as board_scheduled,
            a.scheduled_sod + ? * (t.service_date - ?::date) as alight_scheduled,
            a.stop_sequence - b.stop_sequence as stop_count
        from planner_trip t
        join planner_stop b using (trip_key)
        join planner_stop a using (trip_key)
        where t.trip_key = ? and b.stop_sequence = ? and a.stop_sequence = ?
        """,
        [DAY_SECONDS, day, DAY_SECONDS, day, leg.trip_key, leg.board_sequence, leg.alight_sequence],
    )
    if row is None or leg.depart is None or leg.arrive is None or leg.arrive_late is None:
        raise ValueError("journey leg missing from the planner artifact")
    return row


def _walk_details(path: Path, leg: journey.Walk) -> dict[str, Any]:
    row = fetch_one(
        path,
        """
        select (select any_value(stop_name) from planner_stop where stop_id = ?) as from_name,
            (select any_value(stop_name) from planner_stop where stop_id = ?) as to_name,
            (select min(distance_m) from planner_footpath where from_stop_id = ? and to_stop_id = ?) as distance_m
        """,
        [leg.from_stop, leg.to_stop, leg.from_stop, leg.to_stop],
    )
    if row is None or row["from_name"] is None or row["to_name"] is None:
        raise ValueError("journey walk missing from the planner artifact")
    return row


def post_label(stop_id: str) -> str:
    """Which post of a stop: ZTM bus and tram posts are the stop group plus a two-digit number."""
    if "M:" in stop_id:
        return "metro"
    if len(stop_id) == 4:  # noqa: PLR2004 - SKM stations are bare four-digit groups
        return "train"
    return stop_id[4:]


def _stop_node(stop_id: str, name: str) -> dict[str, Any]:
    return {"kind": "stop", "stop_id": stop_id, "name": name, "post": post_label(stop_id)}


def _connection(
    path: Path, result: journey.Journey, day: date, current: live_times.LiveView | None = None
) -> dict[str, Any]:
    """Card model: summary chips and an itinerary of stop nodes joined by ride or walk segments.

    Stop times shown are expected times; a boarding node also shows when to be there (the never-early margin).
    A change at the same post is one node with both an arrival and a departure.
    """
    nodes: list[dict[str, Any]] = []  # nodes[i] and nodes[i + 1] are joined by segments[i]
    segments: list[dict[str, Any]] = []
    chips: list[dict[str, Any]] = []
    expected: float = result.depart
    for leg in result.legs:
        if isinstance(leg, journey.Walk):
            walk = _walk_details(path, leg)
            if not nodes:
                nodes.append({**_stop_node(leg.from_stop, walk["from_name"]), "depart": _floor_minute(result.depart)})
            minutes = max(1, math.ceil(leg.walk_s / 60))
            segments.append({"kind": "walk", "minutes": minutes, "distance_m": walk["distance_m"]})
            chips.append({"kind": "walk", "minutes": minutes})
            expected += leg.walk_s
            nodes.append({**_stop_node(leg.to_stop, walk["to_name"]), "arrive": _round_minute(expected)})
            continue
        ride = _ride_details(path, leg, day)
        depart, arrive = float(leg.depart or 0), float(leg.arrive or 0)
        if not nodes or nodes[-1]["stop_id"] != ride["board_stop_id"]:
            nodes.append(_stop_node(ride["board_stop_id"], ride["board_name"]))
        board = nodes[-1]
        board.update(
            depart=_round_minute(depart),
            depart_differs=differs_from_timetable(depart, ride["board_scheduled"]),
            board_scheduled=ride["board_scheduled"],
            be_by=_floor_minute(leg.board_by),
        )
        if any(seg["kind"] == "ride" for seg in segments):
            # A change; the router already guarantees it holds when the previous vehicle runs late.
            board["wait_minutes"] = max(0, round((depart - expected) / 60))
        segments.append({
            "kind": "ride",
            "trip_key": ride["trip_key"],
            "mode": ride["mode"],
            "line": ride["line"],
            "headsign": ride["headsign"],
            "board_sequence": ride["board_sequence"],
            "alight_sequence": ride["alight_sequence"],
            "stop_count": ride["stop_count"],
            "minutes": max(1, round((arrive - depart) / 60)),
            "live": _live_ride(path, current, leg, ride["board_stop_id"]) if current is not None else None,
        })  # fmt: skip
        chips.append(
            {"kind": "ride", "mode": ride["mode"], "line": ride["line"], "live": segments[-1]["live"] is not None}
        )
        expected = arrive
        nodes.append({
            **_stop_node(ride["alight_stop_id"], ride["alight_name"]),
            "arrive": _round_minute(arrive),
            "arrive_differs": differs_from_timetable(arrive, ride["alight_scheduled"]),
            "alight_scheduled": ride["alight_scheduled"],
        })  # fmt: skip
    first_board = next(n for n in nodes if "be_by" in n)
    return {
        "depart": result.depart,  # seconds, for pagination; the card shows leave_by
        "leave_by": _floor_minute(result.depart),
        "initial_walk": isinstance(result.legs[0], journey.Walk),
        "from_name": first_board["name"],
        "from_post": first_board["post"],
        "arrive": _round_minute(expected),
        "arrive_by": _ceil_minute(result.arrive_late),
        "duration_minutes": max(1, math.ceil((expected - result.depart) / 60)),
        "changes": result.vehicles - 1,
        "chips": chips,
        "timeline": [item for pair in zip(nodes, [*segments, None], strict=True) for item in pair if item],
        "live": any(chip.get("live") for chip in chips),
        "key": next(f"{seg['trip_key']}-{seg['board_sequence']}" for seg in segments if seg["kind"] == "ride"),
    }


def _live_ride(path: Path, current: live_times.LiveView, leg: journey.Ride, board_stop: str) -> dict[str, Any] | None:
    """Where the ride's vehicle is now: on this trip, waiting at its first stop, or still on its previous trip.

    status picks the wording, late the minutes beyond the timetable there; the map joins the vehicle to the
    boarding stop along the shapes, and is None once the vehicle is at the stop or past it.
    """
    matcher = current.matcher
    net = matcher.net
    row, length = net.trip_index[leg.trip_key]
    trip = bisect_right(net.trip_rows, row) - 1
    board = row + list(net.seqs[row : row + length]).index(leg.board_sequence)
    fix, before = current.fixes.get(trip), None
    if fix is None:
        before = matcher.previous_trip.get(trip)
        fix = current.fixes.get(before) if before is not None else None
        if fix is None or before is None:
            return None
    stop = fetch_one(path, "select lon, lat from planner_stop_post where stop_id = ?", [board_stop])
    if stop is None:
        return None
    line: list[list[float]] = []
    shapes = matcher.shapes
    if before is not None:
        shape = shapes.get(net.trip_meta[before][4] or "")
        if shape is not None:
            line += shape.path(fix.dist_m, net.shape_dist[matcher.rows(before).stop - 1])
    shape = shapes.get(net.trip_meta[trip][4] or "")
    if shape is not None:
        start = fix.dist_m if before is None else net.shape_dist[row]
        line += shape.path(start, net.shape_dist[board])
    late = round(fix.delay_s / 60)
    status = "previous" if before is not None else "waiting" if fix.waiting else "running"
    at_stop = before is None and fix.dist_m >= net.shape_dist[board] - live_times.PASSED_M
    shown = {
        "vehicle": [round(fix.lon, 6), round(fix.lat, 6)],
        "stop": [float(stop["lon"]), float(stop["lat"])],
        "path": line,
    }
    return {"status": status, "late": late, "map": None if at_stop else shown}


def trip_stops(  # noqa: PLR0913
    path: Path, trip_key: int, board_sequence: int, alight_sequence: int, day: date, now: datetime | None = None
) -> list[dict[str, Any]]:
    """Stops from boarding to alighting with the trip's expected times, as the router uses them (live with now)."""
    rows = fetch_all(
        path,
        """
        select s.stop_sequence, s.stop_name, s.stop_id,
            s.scheduled_sod + ? * (t.service_date - ?::date) as scheduled,
            s.expected_sod + ? * (t.service_date - ?::date) as expected_sod, s.leave_by_offset_s
        from planner_stop s join planner_trip t using (trip_key)
        where s.trip_key = ? and s.stop_sequence between ? and ?
        order by s.stop_sequence
        """,
        [DAY_SECONDS, day, DAY_SECONDS, day, trip_key, board_sequence, alight_sequence],
    )
    if not rows:
        return []
    board = rows[0]
    depart = board["expected_sod"]
    if board["leave_by_offset_s"] is not None:
        depart = max(depart, board["scheduled"] + board["leave_by_offset_s"])
    current = live_times.view(path, journey.network(path, day), day, now) if now is not None else None
    if current is not None and trip_key in current.net.trip_index:
        net = current.net
        start, length = net.trip_index[trip_key]
        by_sequence = {net.seqs[row]: row for row in range(start, start + length)}
        for row in rows:
            if row["stop_sequence"] in by_sequence:
                row["expected_sod"] = net.expected[by_sequence[row["stop_sequence"]]]
        if board["stop_sequence"] in by_sequence:
            depart = net.depart[by_sequence[board["stop_sequence"]]]
    stops = []
    for row in rows:
        expected = max(row["expected_sod"], depart)
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


def get_page(
    path: Path, args: dict[str, str], today: date, now_sod: int, now: datetime | None = None
) -> dict[str, Any]:
    """Template context for the search form and, with both stops chosen, modelled journeys (live with now)."""
    dates = available_dates(path)
    requested = parse_date(args.get("date"))
    # Default to today; outside the published window fall back to its first day.
    day = requested if requested in dates else today if today in dates else (dates[0] if dates else today)
    after = parse_clock(args.get("time"), now_sod)
    origin = resolve_stop_group(path, args.get("from"), args.get("q_from"))
    destination = resolve_stop_group(path, args.get("to"), args.get("q_to"))
    results: list[dict[str, Any]] = []
    later = None
    if origin and destination and origin["stop_group_id"] != destination["stop_group_id"]:
        results, later = search(path, origin["stop_group_id"], destination["stop_group_id"], day, after, now)
    return {
        "dates": dates,
        "today": today,
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
