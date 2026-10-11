"""Bounded, boarding-dependent earliest-arrival search over one artifact service day.

A round adds one vehicle (at most five). Patterns group stop sequences and alighting
permissions, not timetable order: trips can overtake, and every feasible trip at
every improved boarding post is examined.
Transfers use conservative arrival seconds, not the alighting stop's delay model.
Walks may precede/follow a ride, but cannot chain without an intervening ride.
plan() lists departures as a profile search (rRAPTOR: latest departure first, labels
reused), pruned by a no-waiting lower bound to the destination.
"""

from __future__ import annotations

import copy
import heapq
import math
import threading
from array import array
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import partial
from itertools import groupby
from operator import itemgetter
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

from ztm_frontend.db import read_connection
from ztm_frontend.routing import NativeState, PreparedNet, PreparedQuery

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import duckdb

DAY_SECONDS = 86_400
INF = 2**31 - 1
NO_BOARD = 2**30
MAX_VEHICLES = 5
MAX_JOURNEY_S = 3 * 3600  # longer trips are not offered; bounds each search to a time window
PROFILE_WINDOW_S = 30 * 60  # plan()'s first departure window and the least of any further one
WINDOW_EVENTS = 1500  # most departures a window runs; around a busy stop that can be under 30 min
WINDOW_SLACK = 1.25  # a further window covers the journeys still missing at the rate seen so far, plus this
PROFILE_SPAN_S = 8 * 3600  # plan() looks no further past the requested time
CACHED_DAYS = 2
WALK_PERMISSION_WORDS = 32_768  # bound cached permission masks by their 64-bit words (at least one per entry)
DEFAULT_HIGH_RATIO = 1.1
# Point walks use the pipeline's pace/detour model, but allow a longer endpoint walk.
# They approximate streets, not pedestrian routing: great-circle distance x detour,
# at walking pace, plus station stairs/corridors.
WALK_DETOUR = 1.3
WALK_SPEED_MPS = 1.2
WALK_MIN_S = 30
WALK_MAX_M = 1000
STATION_ACCESS_S = 60
ORIGIN_POINT = "@origin"
DESTINATION_POINT = "@destination"


@dataclass(frozen=True)
class Point:
    """A geographic endpoint; its name is display-only and never affects routing."""

    lat: float
    lon: float
    name: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        """Reject nonfinite or out-of-range coordinates, including direct API callers."""
        if not (
            math.isfinite(self.lat) and math.isfinite(self.lon) and -90 <= self.lat <= 90 and -180 <= self.lon <= 180  # noqa: PLR2004
        ):
            raise ValueError("invalid geographic coordinates")


@dataclass(frozen=True)
class Ride:
    """Ride timings in seconds from the queried day's midnight (not rounded minutes)."""

    trip_key: int
    board_sequence: int
    alight_sequence: int
    board_by: int
    depart: float | None = None
    arrive: float | None = None
    arrive_late: int | None = None


@dataclass(frozen=True)
class Walk:
    """Walk between posts or an endpoint; distance is supplied for approximate point walks."""

    from_stop: str
    to_stop: str
    walk_s: int
    distance_m: int | None = None


@dataclass(frozen=True)
class Journey:
    """A connection ranked by conservative arrival; depart includes the initial walk."""

    depart: int
    arrive_late: int
    legs: tuple[Ride | Walk, ...]

    @property
    def vehicles(self) -> int:
        """Number of rides."""
        return sum(isinstance(leg, Ride) for leg in self.legs)

    def dominates(self, other: Journey) -> bool:
        """Leaves no earlier, arrives no later, with no more vehicles, and improves at least one."""
        return (
            self.depart >= other.depart
            and self.arrive_late <= other.arrive_late
            and self.vehicles <= other.vehicles
            and (self.depart > other.depart or self.arrive_late < other.arrive_late or self.vehicles < other.vehicles)
        )


class _Envelope:
    """Monotone majorant of duration * high_ratio, including uncalibrated gaps.

    Buckets are (lower, upper]. At a downward ratio boundary we retain the largest
    preceding upper * ratio, so riding farther can never improve the late bound.
    """

    def __init__(self, buckets: list[tuple[float, float, float]]) -> None:
        self.upper: list[float] = []
        self.ratio: list[float] = []
        self.floor: list[float] = []
        end, high = 0.0, 0.0
        for lower, upper, raw_ratio in sorted(buckets):
            if lower > end:
                self.upper.append(lower)
                self.ratio.append(DEFAULT_HIGH_RATIO)
                self.floor.append(high)
                high = max(high, lower * DEFAULT_HIGH_RATIO)
            self.upper.append(upper)
            ratio = max(1.0, raw_ratio)
            self.ratio.append(ratio)
            self.floor.append(high)
            high = max(high, upper * ratio)
            end = upper
        self.upper.append(math.inf)
        self.ratio.append(DEFAULT_HIGH_RATIO)
        self.floor.append(high)

    def duration(self, seconds: float) -> float:
        i = bisect_left(self.upper, seconds)
        return max(self.floor[i], seconds * self.ratio[i])


@dataclass(frozen=True, slots=True)
class _Label:
    time: int
    previous: _Label | None = None
    leg: tuple[int, int, int] | Walk | None = None  # a ride is (trip, board row, alight row), built when shown


class Network:
    """Compact per-stop timing columns and per-pattern boarding indexes; no all-pairs matrix."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, day: date) -> None:
        """Load this service day and the previous day's trips still running after midnight."""
        build = connection.execute("select build_id from planner_metadata limit 1").fetchone()
        self.build_id = str(build[0]) if build else ""
        self.live: object | None = None  # live.Matcher, created on first use and dying with the network
        self.live_view: object | None = None  # live_times state for the latest feed object
        stops = connection.execute(
            """
            select distinct s.stop_id, s.stop_group_id
            from planner_stop s join planner_trip t using (trip_key)
            where t.service_date in (?::date, ?::date) order by s.stop_id
            """,
            [day, day - timedelta(days=1)],
        ).fetchall()
        self.stop_ids = [s for s, _ in stops]
        self.stop_index = {s: i for i, s in enumerate(self.stop_ids)}
        self.group_stops: dict[str, list[int]] = {}
        for i, (_, group) in enumerate(stops):
            self.group_stops.setdefault(group, []).append(i)
        self._load_ranges(connection)
        self._load_routes(connection, day)
        self._load_footpaths(connection)
        self.posts = [
            (self.stop_index[stop], lat, lon, station)
            for stop, lat, lon, station in self._post_rows(connection, day)
            if lat is not None and lon is not None
        ]

    @staticmethod
    def _post_rows(connection: duckdb.DuckDBPyConnection, day: date) -> list[tuple]:
        # Stop-only artifacts and minimal fixtures need no coordinates; they remain routable.
        if not connection.execute(
            "select 1 from information_schema.tables where table_name = 'planner_stop_post'"
        ).fetchone():
            return []
        return connection.execute(
            """
            select p.stop_id, p.lat, p.lon, bool_or(t.mode in ('metro', 'rail'))
            from planner_stop_post p join planner_stop s using (stop_id)
            join planner_trip t using (trip_key)
            where t.service_date in (?::date, ?::date)
            group by p.stop_id, p.lat, p.lon
            """,
            [day, day - timedelta(days=1)],
        ).fetchall()

    def _load_ranges(self, connection: duckdb.DuckDBPyConnection) -> None:
        cells: dict[tuple, list[tuple]] = {}
        for tram, weekday, hour, lower, upper, high in connection.execute(
            "select is_tram, weekday, hour, min_ride_s, max_ride_s, high_ratio from planner_range"
        ).fetchall():
            cells.setdefault((tram, weekday, hour), []).append((lower, upper, high))
        self.envelopes = [_Envelope([])]
        self.range_index: dict[tuple, int] = {}
        for cell, buckets in cells.items():
            self.range_index[cell] = len(self.envelopes)
            self.envelopes.append(_Envelope(buckets))

    def _load_routes(self, connection: duckdb.DuckDBPyConnection, day: date) -> None:
        # Read first: a new query on the connection would end the stream below.
        meta = _trip_meta(connection, day)
        cursor = connection.execute(
            f"""
            with ev as (
                select s.*, t.mode, isodow(t.service_date) <= 5 as weekday,
                    s.scheduled_sod + {DAY_SECONDS} * (t.service_date - ?::date) as sched
                from planner_stop s join planner_trip t using (trip_key)
                where t.service_date in (?::date, ?::date)
                qualify max(sched) over (partition by s.trip_key) >= 0
            )
            select trip_key, mode, weekday, stop_id, stop_sequence,
                coalesce(sched + leave_by_offset_s, {NO_BOARD}), sched - scheduled_sod + expected_sod,
                sched + late_delay_s,
                case when mode in ('bus', 'tram') then ride_from_start_s else sched end,
                (scheduled_sod // 3600) % 24, can_alight, sched, coalesce(shape_dist_m, -1)
            from ev order by trip_key, stop_sequence
            """,  # noqa: S608 - only integer constants; dates are bound parameters
            [day, day, day - timedelta(days=1)],
        )
        # Stream sorted rows: ordered list aggregates consume gigabytes on a full
        # day, while this retains Python copies of only one batch and one trip.
        rows = (row for batch in iter(lambda: cursor.fetchmany(8192), []) for row in batch)
        self.board, self.expected, self.depart, self.late_base = array("i"), array("i"), array("i"), array("i")
        self.cumulative, self.range_ids, self.seqs = array("d"), array("i"), array("i")
        self.trip_keys, self.trip_rows = array("q"), array("i")
        self.trip_index: dict[int, tuple[int, int]] = {}
        # For live positions: timetable seconds from this day's midnight and metres along the shape (-1: unknown)
        # per row; per trip, (mode, line, brigade, duty_id, shape_id) as in the artifact.
        self.sched, self.shape_dist = array("i"), array("i")
        self.trip_meta: list[tuple[str, str, str | None, str | None, str | None]] = []
        self.trip_pattern = array("i")
        self.patterns: list[list[int]] = []
        self.pattern_alights: list[tuple[int, ...]] = []
        pattern_trips: list[list[int]] = []
        pattern_index: dict[tuple[tuple[str, ...], tuple[bool, ...]], int] = {}
        for key, trip_rows in groupby(rows, key=itemgetter(0)):
            events = list(trip_rows)
            mode, weekday = events[0][1:3]
            stops, seqs, board, expected, late, cumulative, hours, can_alight, sched, shape_dist = zip(
                *(event[3:] for event in events), strict=True
            )
            pattern_key = (stops, can_alight)
            if pattern_key not in pattern_index:
                pattern_index[pattern_key] = len(self.patterns)
                self.patterns.append([self.stop_index[s] for s in stops])
                self.pattern_alights.append(tuple(pos for pos, allowed in enumerate(can_alight) if allowed))
                pattern_trips.append([])
            pattern_trips[pattern_index[pattern_key]].append(len(self.trip_keys))
            self.trip_pattern.append(pattern_index[pattern_key])
            self.trip_index[key] = (len(self.board), len(stops))
            self.trip_rows.append(len(self.board))
            self.trip_keys.append(key)
            self.trip_meta.append(meta[key])
            self.sched.extend(sched)
            self.shape_dist.extend(shape_dist)
            self.seqs.extend(seqs)
            self.board.extend(board)
            self.expected.extend(expected)
            # NO_BOARD is only a sentinel, never a real departure constraint.
            depart = [max(e, b) if b < NO_BOARD else e for e, b in zip(expected, board, strict=True)]
            self.depart.extend(depart)
            self.late_base.extend(
                max(late_time, d, b if b < NO_BOARD else d) for late_time, d, b in zip(late, depart, board, strict=True)
            )
            self.cumulative.extend(cumulative)
            self.range_ids.extend(
                self.range_index.get((mode == "tram", weekday, h), 0) if mode in {"bus", "tram"} else -1 for h in hours
            )
        self._index(pattern_trips)

    def _index(self, pattern_trips: list[list[int]]) -> None:
        """Per-pattern ride bounds and per-stop boarding indexes."""
        # Per pattern, the cumulative sum of each segment's shortest ride over its trips: any trip of the pattern
        # needs at least prefix[a] - prefix[p] seconds from position p to a, which bounds the trips worth scanning.
        self.pattern_prefix: list[array] = []
        for pattern, trips_in_pattern in enumerate(pattern_trips):
            prefix, total = array("d", [0.0]), 0.0
            for pos in range(len(self.patterns[pattern]) - 1):
                total += max(0.0, min(
                    self.cumulative[self.trip_rows[t] + pos + 1] - self.cumulative[self.trip_rows[t] + pos]
                    for t in trips_in_pattern
                ))  # fmt: skip
                prefix.append(total)
            self.pattern_prefix.append(prefix)
        # Each incidence has its own board_by order: no timetable or model FIFO assumption.
        self.pattern_trips = pattern_trips
        self.incidence: list[list[tuple[int, int, array, array]]] = [[] for _ in self.stop_ids]
        self.incidence_slot: dict[tuple[int, int], tuple[int, int]] = {}  # (pattern, pos) -> (stop, index)
        for pattern in range(len(pattern_trips)):
            alights = self.pattern_alights[pattern]
            for pos, stop in enumerate(self.patterns[pattern][:-1]):
                if not alights or pos >= alights[-1]:
                    continue
                self.incidence_slot[pattern, pos] = (stop, len(self.incidence[stop]))
                self.incidence[stop].append(self._boarding(pattern, pos))
        # First boardable position of each pattern at a stop: a walk must not lead to a trip the rider could
        # already board where the walk started.
        self.board_at: list[dict[int, int]] = [{} for _ in self.stop_ids]
        for stop, entries in enumerate(self.incidence):
            for pattern, pos, _, _ in entries:
                self.board_at[stop].setdefault(pattern, pos)

    def _boarding(self, pattern: int, pos: int) -> tuple[int, int, array, array]:
        """The pattern's trips boardable at pos, in board_by order."""
        ordered = sorted(
            (self.board[self.trip_rows[t] + pos], t)
            for t in self.pattern_trips[pattern]
            if self.board[self.trip_rows[t] + pos] < NO_BOARD
        )
        return pattern, pos, array("i", (b for b, _ in ordered)), array("i", (t for _, t in ordered))

    def patched(self, rows: dict[int, tuple[int, int, int]]) -> Network:
        """A copy with new (board_by, expected, late) times at some rows; this network is left as it was.

        board_by may be NO_BOARD (the trip has passed the stop). Times keep the invariants of the load: departure
        is the later of expected and board_by, the late base no earlier than either.
        """
        net = copy.copy(self)
        net.board, net.expected = array("i", self.board), array("i", self.expected)
        net.depart, net.late_base = array("i", self.depart), array("i", self.late_base)
        net.live = net.live_view = None
        slots = set()
        for row, (board, expected, late) in rows.items():
            depart = max(expected, board) if board < NO_BOARD else expected
            net.board[row], net.expected[row], net.depart[row] = board, expected, depart
            net.late_base[row] = max(late, depart, board if board < NO_BOARD else depart)
            trip = bisect_right(self.trip_rows, row) - 1
            slots.add((self.trip_pattern[trip], row - self.trip_rows[trip]))
        net.incidence = list(self.incidence)
        copied: set[int] = set()
        for slot in slots:
            if slot not in self.incidence_slot:
                continue  # no boarding there (the last alighting position or later)
            stop, index = self.incidence_slot[slot]
            if stop not in copied:
                net.incidence[stop] = list(net.incidence[stop])
                copied.add(stop)
            net.incidence[stop][index] = net._boarding(*slot)  # noqa: SLF001
        return net

    def _load_footpaths(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.footpaths: list[list[tuple[int, int]]] = [[] for _ in self.stop_ids]
        for a, b, seconds in connection.execute(
            "select from_stop_id, to_stop_id, walk_s from planner_footpath"
        ).fetchall():
            if a in self.stop_index and b in self.stop_index:
                self.footpaths[self.stop_index[a]].append((self.stop_index[b], seconds))
        # Reverse graph for lower bounds: each pattern segment at its shortest ride, and every walk.
        shortest: dict[tuple[int, int], float] = {}
        for pattern, stops in enumerate(self.patterns):
            prefix = self.pattern_prefix[pattern]
            for pos in range(len(stops) - 1):
                edge = (stops[pos + 1], stops[pos])
                shortest[edge] = min(prefix[pos + 1] - prefix[pos], shortest.get(edge, math.inf))
        for a, walks in enumerate(self.footpaths):
            for b, seconds in walks:
                shortest[b, a] = min(seconds, shortest.get((b, a), math.inf))
        self.reverse: list[list[tuple[int, float]]] = [[] for _ in self.stop_ids]
        for (b, a), seconds in shortest.items():
            self.reverse[b].append((a, seconds))

    def point_walks(self, point: Point, *, access: bool) -> dict[int, Walk]:
        """All served posts within 1,000 estimated metres, without changing the cached graph.

        The 1.3 detour factor cannot account for barriers or actual street paths. Distance
        excludes station access time; the minimum 30 seconds and station 60 seconds affect timing only.
        """
        walks = {}
        for stop, lat, lon, station in self.posts:
            if not (math.isfinite(lat) and math.isfinite(lon)):
                continue
            a, b = math.radians(point.lat), math.radians(lat)
            h = (
                math.sin((b - a) / 2) ** 2
                + math.cos(a) * math.cos(b) * math.sin(math.radians(lon - point.lon) / 2) ** 2
            )
            distance = WALK_DETOUR * 2 * 6_371_000 * math.asin(math.sqrt(min(1.0, max(0.0, h))))
            if distance > WALK_MAX_M:
                continue
            seconds = max(WALK_MIN_S, math.ceil(distance / WALK_SPEED_MPS)) + (STATION_ACCESS_S if station else 0)
            start, end = (ORIGIN_POINT, self.stop_ids[stop]) if access else (self.stop_ids[stop], DESTINATION_POINT)
            walks[stop] = Walk(start, end, seconds, math.ceil(distance))
        return walks

    def lower_bounds(self, targets: set[int] | dict[int, int]) -> list[float]:
        """Seconds from each stop to the nearest target riding without waiting at each segment's shortest ride.

        A late arrival is at least board_by plus the timetabled ride (every ratio is at least one), so no journey
        is faster. A target mapping seeds the graph with each post's egress time rather than zero.
        The reverse graph may chain walks: allowing extra paths only lowers this pruning bound.
        Stops farther than MAX_JOURNEY_S stay infinite.
        """
        bound = [math.inf] * len(self.stop_ids)
        heap = [(float(targets[t]) if isinstance(targets, dict) else 0.0, t) for t in targets]
        heapq.heapify(heap)
        for seconds, t in heap:
            bound[t] = seconds
        while heap:
            seconds, stop = heapq.heappop(heap)
            if seconds > bound[stop]:
                continue
            for previous, edge in self.reverse[stop]:
                total = seconds + edge
                if total < bound[previous] and total <= MAX_JOURNEY_S:
                    bound[previous] = total
                    heapq.heappush(heap, (total, previous))
        return bound

    def timing(self, trip_key: int, board_sequence: int, alight_sequence: int) -> Ride:
        """Return the exact search/display timing for artifact sequences (not array offsets).

        Expected times are the trip's, the same from every boarding stop. Late arrival is ceiled to an integer second,
        without an epsilon: even a fractional second past board_by misses a transfer.
        Metro/rail retain timetable duration and only their boarding delay spread.
        """
        row, length = self.trip_index[trip_key]
        board = self.seqs.index(board_sequence, row, row + length)
        alight = self.seqs.index(alight_sequence, board + 1, row + length)
        return self._ride(trip_key, board, alight, self._late(board, alight))

    def _late(self, board: int, alight: int) -> int:
        # Unlike expected times, the late bound grows with the ride from the boarding stop: per boarding, it held
        # its miss rate over every ride length, with less margin than a bound per vehicle and stop.
        duration = max(0.0, self.cumulative[alight] - self.cumulative[board])
        cell = self.range_ids[board]
        high = self.envelopes[cell].duration(duration) if cell >= 0 else duration
        return max(math.ceil(self.late_base[board] + high), self._arrive(board, alight))

    def _arrive(self, board: int, alight: int) -> int:
        return max(self.expected[alight], self.depart[board])

    def _ride(self, key: int, board: int, alight: int, late: int) -> Ride:
        depart, arrive = self.depart[board], self._arrive(board, alight)
        return Ride(key, self.seqs[board], self.seqs[alight], self.board[board], depart, arrive, late)

    def profile(self, origin: str | Point, destination: str | Point) -> _Profile:
        """Query-local endpoint walks and labels; cached network columns remain untouched."""
        origins = self.group_stops.get(origin, []) if isinstance(origin, str) else []
        access = self.point_walks(origin, access=True) if isinstance(origin, Point) else {}
        egress = self.point_walks(destination, access=False) if isinstance(destination, Point) else {}
        targets = set(egress) if isinstance(destination, Point) else set(self.group_stops.get(destination, []))
        return _Profile(self, origins, targets, access=access, egress=egress)

    def search(self, origin_group: str | Point, destination_group: str | Point, after: int) -> list[Journey]:
        """Earliest-arriving journeys that improve on all smaller vehicle counts, up to five."""
        profile = self.profile(origin_group, destination_group)
        if not (profile.origins or profile.access) or not profile.targets:
            return []
        return [_journey(self, label) for label in profile.run(after)]


def _trip_meta(connection: duckdb.DuckDBPyConnection, day: date) -> dict[int, tuple]:
    """Return (mode, line, brigade, duty_id, shape_id) by trip key.

    Read apart from the stop stream: repeated on every stop row, these columns would cost it hundreds of megabytes.
    """
    return {
        key: (mode, line, brigade, duty, shape)
        for key, mode, line, brigade, duty, shape in connection.execute(
            "select trip_key, mode, line, brigade, duty_id, shape_id from planner_trip "
            "where service_date in (?::date, ?::date)",
            [day, day - timedelta(days=1)],
        ).fetchall()
    }


def _journey(net: Network, label: _Label) -> Journey:
    arrival = label.time
    legs: list[Ride | Walk] = []
    while label.previous is not None:
        leg = label.leg
        if isinstance(leg, Walk):
            legs.append(leg)
        elif leg is not None:
            trip, board, alight = leg
            legs.append(net._ride(net.trip_keys[trip], board, alight, label.time))  # noqa: SLF001
        label = label.previous
    legs.reverse()
    first = next(leg for leg in legs if isinstance(leg, Ride))
    walk = legs[0].walk_s if isinstance(legs[0], Walk) else 0
    return Journey(first.board_by - walk, arrival, tuple(legs))


# Values own only native copies, never their weak Python keys. Live-patched copies
# have distinct identities and therefore cannot reuse stale timing metadata.
_prepared_networks: WeakKeyDictionary[Network, PreparedNet] = WeakKeyDictionary()
_prepared_lock = threading.Lock()


def _prepared_network(net: Network) -> PreparedNet:
    with _prepared_lock:
        prepared = _prepared_networks.get(net)
        if prepared is None:
            prepared = PreparedNet(net)
            _prepared_networks[net] = prepared
        return prepared


class _Profile:
    """Round labels of one origin and destination, kept while searching from later to earlier departures.

    A journey open to a rider at the origin later is open to one there earlier (they wait), so labels from a
    later departure remain valid bounds and an earlier departure explores only what it improves (rRAPTOR).
    Column k holds arrivals with at most k vehicles: one shared column would let a later departure's
    many-vehicle label prune an earlier departure's journey with fewer vehicles.
    """

    def __init__(  # noqa: PLR0913
        self,
        net: Network,
        origins: list[int],
        targets: set[int],
        to_target: list[float] | None = None,
        *,
        access: dict[int, Walk] | None = None,
        egress: dict[int, Walk] | None = None,
        query: PreparedQuery | None = None,
    ) -> None:
        self.net, self.origins, self.targets = net, origins, targets
        self.access, self.egress = access or {}, egress or {}
        # A label that cannot beat the target's even riding on without waiting leads nowhere; the target's
        # labels only fall, so the pruning holds for every later run.
        weighted = {stop: self.egress[stop].walk_s if stop in self.egress else 0 for stop in targets}
        self.to_target = net.lower_bounds(weighted) if to_target is None else to_target
        # Shared only across this search's windows. Native suffix scans use the
        # immutable linear pattern arrays directly, not a quadratic suffix cache.
        self.query = query
        self.state: NativeState | None = None
        # First boardable position of each pattern at any origin post, for the walking rule in run().
        self.origin_board: dict[int, int] = {}
        for stop in origins:
            for pattern, pos in net.board_at[stop].items():
                self.origin_board[pattern] = min(pos, self.origin_board.get(pattern, pos))
        # An earlier walk taking no longer to the same post makes a later candidate inert. Keep strict prefix
        # improvements, not just the shortest walk: the first improvement determines equal-time label order.
        # departures() must still enumerate every original walk; its events determine window boundaries.
        self.origin_walks: list[tuple[int, int, int]] = []
        shortest = {stop: walk.walk_s for stop, walk in self.access.items()}
        for stop in origins:
            for dest, seconds in net.footpaths[stop]:
                if seconds < shortest.get(dest, INF):
                    shortest[dest] = seconds
                    self.origin_walks.append((stop, dest, seconds))

    def run(self, after: int, boarding: set[int] | None = None) -> list[_Label]:
        """Run the required native kernel; departures must be latest first within a window.

        Each profile owns its mutable window state. Query permission masks are shared
        only with subsequent windows of the same search. Rust detaches the GIL during
        search, so independent requests can execute routing in parallel.
        """
        if self.state is None:
            if self.query is None:
                self.query = PreparedQuery(_prepared_network(self.net), self.to_target, WALK_PERMISSION_WORDS)
            self.state = NativeState(self.query, self)
        return self.state.run(self, after, boarding)

    def departures(self, start: int, end: int) -> list[tuple[int, set[int]]]:  # noqa: C901
        """Times in [start, end) a journey can leave the origin, latest first, with the posts boarded at them.

        A post is an origin post or a nearby one walked to.
        """
        net, events = self.net, dict[int, set[int]]()
        for stop, walk in self.access.items():
            for _, _, board_times, _ in net.incidence[stop]:
                first, last = bisect_left(board_times, start + walk.walk_s), bisect_left(board_times, end + walk.walk_s)
                for b in board_times[first:last]:
                    events.setdefault(b - walk.walk_s, set()).add(stop)
        for stop in self.origins:
            for _, _, board_times, _ in net.incidence[stop]:
                for b in board_times[bisect_left(board_times, start) : bisect_left(board_times, end)]:
                    events.setdefault(b, set()).add(stop)
            for dest, seconds in net.footpaths[stop]:
                for pattern, pos, board_times, _ in net.incidence[dest]:
                    if self.origin_board.get(pattern, pos) < pos:
                        continue  # run()'s walking rule
                    first, last = bisect_left(board_times, start + seconds), bisect_left(board_times, end + seconds)
                    for b in board_times[first:last]:
                        events.setdefault(b - seconds, set()).add(dest)
        return sorted(events.items(), reverse=True)


_cache: OrderedDict[tuple[str, str, date], Network] = OrderedDict()
_cache_lock = threading.Lock()


def network(path: Path, day: date) -> Network:
    """Cache day networks by the build in the request's DuckDB snapshot, not file metadata."""
    with read_connection(path) as connection:
        metadata = connection.execute("select build_id from planner_metadata limit 1").fetchone()
        if metadata is None:
            raise RuntimeError("planner artifact has no build metadata")
        # An atomic publication can replace the path while this request still
        # holds its old DuckDB instance. Identity and data must come from that
        # same connection, also used for the page's dates and trip details.
        key = (str(path), str(metadata[0]), day)
        with _cache_lock:
            if key in _cache:
                _cache.move_to_end(key)
                return _cache[key]
            built = Network(connection, day)
            _cache[key] = built
            while len(_cache) > CACHED_DAYS:
                _cache.popitem(last=False)
            return built


def plan(  # noqa: PLR0913
    net: Network,
    origin_group: str | Point,
    destination_group: str | Point,
    after: int,
    results: int,
    useful: Callable[[list[Journey]], list[Journey]] | None = None,
) -> list[Journey]:
    """Up to results nondominated journeys leaving no earlier than after, by departure.

    useful, if given, filters the journeys before they are counted and returned.
    Departure windows follow one another from after; each runs from its latest departure to its earliest,
    reusing labels (see _Profile), and keeps the journeys leaving within it: the complete front there.
    """
    profile = net.profile(origin_group, destination_group)
    if not (profile.origins or profile.access) or not profile.targets:
        return []
    found: list[Journey] = []
    start, width, stop = after, PROFILE_WINDOW_S, after + PROFILE_SPAN_S
    kept: list[Journey] = []
    while start < stop and len(kept) < results:
        end = min(start + width, stop)
        if start != after:
            profile = _Profile(
                net,
                profile.origins,
                profile.targets,
                profile.to_target,
                access=profile.access,
                egress=profile.egress,
                query=profile.query,
            )
        events = profile.departures(start, end)
        if len(events) > WINDOW_EVENTS:
            end, events = events[-WINDOW_EVENTS - 1][0], events[-WINDOW_EVENTS:]
        for n, (departure, boarding) in enumerate(events):
            labels = profile.run(departure, boarding if n else None)
            found.extend(j for j in map(partial(_journey, net), labels) if j.depart < end)
        # Windows label afresh, so a later window's journey can beat an earlier window's.
        found = sorted(
            (j for j in found if not any(o.dominates(j) for o in found)), key=lambda j: (j.depart, j.arrive_late)
        )
        kept = useful(found) if useful else found
        # Each window starts its labels afresh (a later window cannot reuse an earlier one's), and its every
        # departure costs a run, so size it to what is missing rather than doubling.
        covered, start = end - after, end
        missing = (results - len(kept)) * covered / len(kept) * WINDOW_SLACK if kept else 2 * covered
        width = max(PROFILE_WINDOW_S, math.ceil(missing))
    return kept[:results]
