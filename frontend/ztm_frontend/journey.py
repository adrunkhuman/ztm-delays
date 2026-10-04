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

import heapq
import math
import threading
from array import array
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, timedelta
from functools import partial
from itertools import groupby
from operator import itemgetter
from typing import TYPE_CHECKING

from ztm_frontend.db import read_connection

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
DEFAULT_HIGH_RATIO = 1.1


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
    """Walk between two posts."""

    from_stop: str
    to_stop: str
    walk_s: int


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
                coalesce(sched + leave_by_offset_s, {NO_BOARD}), sched + usual_delay_s, sched + late_delay_s,
                case when mode in ('bus', 'tram') then ride_from_start_s else sched end,
                (scheduled_sod // 3600) % 24, can_alight
            from ev order by trip_key, stop_sequence
            """,  # noqa: S608 - only integer constants; dates are bound parameters
            [day, day, day - timedelta(days=1)],
        )
        # Stream sorted rows: ordered list aggregates consume gigabytes on a full
        # day, while this retains Python copies of only one batch and one trip.
        rows = (row for batch in iter(lambda: cursor.fetchmany(8192), []) for row in batch)
        self.board, self.depart, self.late_base = array("i"), array("i"), array("i")
        self.cumulative, self.range_ids, self.seqs = array("d"), array("i"), array("i")
        self.trip_keys, self.trip_rows = array("q"), array("i")
        self.trip_index: dict[int, tuple[int, int]] = {}
        self.patterns: list[list[int]] = []
        self.pattern_alights: list[tuple[int, ...]] = []
        pattern_trips: list[list[int]] = []
        pattern_index: dict[tuple[tuple[str, ...], tuple[bool, ...]], int] = {}
        for key, trip_rows in groupby(rows, key=itemgetter(0)):
            events = list(trip_rows)
            mode, weekday = events[0][1:3]
            stops, seqs, board, depart, late, cumulative, hours, can_alight = zip(
                *(event[3:] for event in events), strict=True
            )
            pattern_key = (stops, can_alight)
            if pattern_key not in pattern_index:
                pattern_index[pattern_key] = len(self.patterns)
                self.patterns.append([self.stop_index[s] for s in stops])
                self.pattern_alights.append(tuple(pos for pos, allowed in enumerate(can_alight) if allowed))
                pattern_trips.append([])
            pattern_trips[pattern_index[pattern_key]].append(len(self.trip_keys))
            self.trip_index[key] = (len(self.board), len(stops))
            self.trip_rows.append(len(self.board))
            self.trip_keys.append(key)
            self.seqs.extend(seqs)
            self.board.extend(board)
            # NO_BOARD is only a sentinel, never a real departure constraint.
            self.depart.extend(max(d, b) if b < NO_BOARD else d for d, b in zip(depart, board, strict=True))
            self.late_base.extend(
                max(late_time, d, b if b < NO_BOARD else d) for late_time, d, b in zip(late, depart, board, strict=True)
            )
            self.cumulative.extend(cumulative)
            self.range_ids.extend(
                self.range_index.get((mode == "tram", weekday, h), 0) if mode in {"bus", "tram"} else -1 for h in hours
            )
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
        self.incidence: list[list[tuple[int, int, array, array]]] = [[] for _ in self.stop_ids]
        for pattern, trips_in_pattern in enumerate(pattern_trips):
            alights = self.pattern_alights[pattern]
            for pos, stop in enumerate(self.patterns[pattern][:-1]):
                if not alights or pos >= alights[-1]:
                    continue
                ordered = sorted(
                    (self.board[self.trip_rows[t] + pos], t)
                    for t in trips_in_pattern
                    if self.board[self.trip_rows[t] + pos] < NO_BOARD
                )
                self.incidence[stop].append(
                    (pattern, pos, array("i", (b for b, _ in ordered)), array("i", (t for _, t in ordered)))
                )
        # First boardable position of each pattern at a stop: a walk must not lead to a trip the rider could
        # already board where the walk started.
        self.board_at: list[dict[int, int]] = [{} for _ in self.stop_ids]
        for stop, entries in enumerate(self.incidence):
            for pattern, pos, _, _ in entries:
                self.board_at[stop].setdefault(pattern, pos)

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

    def lower_bounds(self, targets: set[int]) -> list[float]:
        """Seconds from each stop to the nearest target riding without waiting at each segment's shortest ride.

        A late arrival is at least board_by plus the timetabled ride (every ratio is at least one), so no journey
        is faster. Stops farther than MAX_JOURNEY_S stay infinite.
        """
        bound = [math.inf] * len(self.stop_ids)
        heap = [(0.0, t) for t in targets]
        for t in targets:
            bound[t] = 0.0
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

        Expected times remain floats. Late arrival is ceiled to an integer second,
        without an epsilon: even a fractional second past board_by misses a transfer.
        Metro/rail retain timetable duration and only their boarding delay spread.
        """
        row, length = self.trip_index[trip_key]
        board = self.seqs.index(board_sequence, row, row + length)
        alight = self.seqs.index(alight_sequence, board + 1, row + length)
        return self._ride(trip_key, board, alight, self._late(board, alight))

    def _late(self, board: int, alight: int) -> int:
        duration = max(0.0, self.cumulative[alight] - self.cumulative[board])
        cell = self.range_ids[board]
        high = self.envelopes[cell].duration(duration) if cell >= 0 else duration
        return math.ceil(self.late_base[board] + high)

    def _ride(self, key: int, board: int, alight: int, late: int) -> Ride:
        depart = self.depart[board]
        duration = max(0.0, self.cumulative[alight] - self.cumulative[board])
        return Ride(key, self.seqs[board], self.seqs[alight], self.board[board], depart, depart + duration, late)

    def search(self, origin_group: str, destination_group: str, after: int) -> list[Journey]:
        """Earliest-arriving journeys that improve on all smaller vehicle counts, up to five."""
        origins = self.group_stops.get(origin_group, [])
        targets = set(self.group_stops.get(destination_group, []))
        if not origins or not targets:
            return []
        return [_journey(self, label) for label in _Profile(self, origins, targets).run(after)]


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


def _lower(columns: list[list[int]], k: int, stop: int, value: int) -> None:
    """Lower stop's entry in column k and every later column (each is non-increasing in k)."""
    for column in columns[k:]:
        if value >= column[stop]:
            break
        column[stop] = value


def _lower_walk(columns: list[dict[tuple[int, int], int]], k: int, key: tuple[int, int], value: int) -> None:
    """Lower a sparse walking state's bound for this and every larger vehicle count."""
    for column in columns[k:]:
        if value >= column.get(key, INF):
            break
        column[key] = value


def _deadline(
    ride: list[int], downstream: list[int], shortest: list[float], reach: list[float], target_best: int
) -> float:
    return max(min(ride[d] - s, target_best - r) for d, s, r in zip(downstream, shortest, reach, strict=True))


class _Profile:
    """Round labels of one origin and destination, kept while searching from later to earlier departures.

    A journey open to a rider at the origin later is open to one there earlier (they wait), so labels from a
    later departure remain valid bounds and an earlier departure explores only what it improves (rRAPTOR).
    Column k holds arrivals with at most k vehicles: one shared column would let a later departure's
    many-vehicle label prune an earlier departure's journey with fewer vehicles.
    """

    def __init__(
        self, net: Network, origins: list[int], targets: set[int], to_target: list[float] | None = None
    ) -> None:
        n, columns = len(net.stop_ids), range(MAX_VEHICLES + 1)
        self.net, self.origins, self.targets = net, origins, targets
        # A label that cannot beat the target's even riding on without waiting leads nowhere; the target's
        # labels only fall, so the pruning holds for every later run.
        self.to_target = net.lower_bounds(targets) if to_target is None else to_target
        self.best = [[INF] * n for _ in columns]
        # Unrestricted labels can board every pattern and start a walk. A walked label can do neither,
        # so it must not hide an unrestricted label even when it arrives earlier.
        self.best_ride = [[INF] * n for _ in columns]
        # Walks from different posts forbid different boardings. Keep sparse bounds by (stop, walk origin);
        # -1 denotes the initial walk, whose restriction covers all origin posts.
        self.best_walk: list[dict[tuple[int, int], int]] = [{} for _ in columns]
        # Scan bounds must have the same restrictions as the labels that established them.
        self.scanned = [[INF] * n for _ in columns]
        self.scanned_walk: list[dict[tuple[int, int], int]] = [{} for _ in columns]
        # First boardable position of each pattern at any origin post, for the walking rule in run().
        self.origin_board: dict[int, int] = {}
        for stop in origins:
            for pattern, pos in net.board_at[stop].items():
                self.origin_board[pattern] = min(pos, self.origin_board.get(pattern, pos))

    def run(self, after: int, boarding: set[int] | None = None) -> list[_Label]:  # noqa: C901, PLR0912, PLR0915
        """Target labels from a departure no later than any previous run's, per improved vehicle count.

        boarding, after the first run, names the origin or nearby posts that have a departure exactly at after:
        the others have no trip between this label and the one their trips were last scanned from.
        """
        net, origins, targets = self.net, self.origins, self.targets
        late_base, cumulative, trip_rows = net.late_base, net.cumulative, net.trip_rows
        best, best_ride, scanned, to_target = self.best, self.best_ride, self.scanned, self.to_target
        best_walk, scanned_walk = self.best_walk, self.scanned_walk
        marked: dict[tuple[int, int | None], _Label] = {}
        origin = _Label(after)
        for stop in origins:
            if after < best_ride[0][stop]:
                _lower(best_ride, 0, stop, after)
                _lower(best, 0, stop, after)
                if boarding is None or stop in boarding:
                    marked[stop, None] = origin
        for stop in origins:
            for dest, seconds in net.footpaths[stop]:
                arrival = after + seconds
                key = (dest, -1)
                if arrival < best_walk[0].get(key, INF) and arrival < best_ride[0][dest]:
                    _lower_walk(best_walk, 0, key, arrival)
                    _lower(best, 0, dest, arrival)
                    if boarding is None or dest in boarding:
                        walk = Walk(net.stop_ids[stop], net.stop_ids[dest], seconds)
                        marked[key] = _Label(arrival, origin, walk)
        # Not a reachable arrival, only a horizon: without it, every pattern scans the rest of the day until the
        # destination is first reached.
        horizon = after + MAX_JOURNEY_S
        results = []
        for vehicles in range(1, MAX_VEHICLES + 1):
            if not marked:
                break
            best_k, ride_k, scanned_before = best[vehicles], best_ride[vehicles], scanned[vehicles - 1]
            target_best = min(horizon, *(best_k[t] for t in targets))
            rides: dict[int, _Label] = {}
            improved: dict[tuple[int, int | None], _Label] = {}
            # Earlier labels tend to establish tighter downstream bounds first.
            # This changes scan order only; it assumes nothing about trip overtaking.
            for (stop, walk_origin), previous in sorted(marked.items(), key=lambda item: item[1].time):
                remaining = target_best - to_target[stop]
                if previous.time >= remaining:
                    continue
                # Trips feasible at an earlier scan's (later) label were already considered, with no more
                # vehicles; their boarding-specific timings cannot improve the labels now.
                if walk_origin is None:
                    until = scanned_before[stop]
                    _lower(scanned, vehicles - 1, stop, previous.time)
                else:
                    key = (stop, walk_origin)
                    until = min(scanned_walk[vehicles - 1].get(key, INF), scanned_before[stop])
                    _lower_walk(scanned_walk, vehicles - 1, key, previous.time)
                # Reached on foot: skip trips that already stop, boardable, where the walk started (for a first
                # walk, any origin post). The same vehicle boarded later only looks better through the
                # boarding-dependent late bound; it would also let a rider "chase" a missed vehicle, which the
                # planner does not offer.
                walked_from = None
                if walk_origin is not None:
                    walked_from = self.origin_board if walk_origin == -1 else net.board_at[walk_origin]
                for pattern, pos, times, trips in net.incidence[stop]:
                    if walked_from is not None and walked_from.get(pattern, pos) < pos:
                        continue
                    begin = bisect_left(times, previous.time)
                    end = bisect_left(times, min(until, remaining))
                    if begin >= end:
                        continue
                    stops = net.patterns[pattern]
                    alights = net.pattern_alights[pattern]
                    alights = alights[bisect_right(alights, pos) :]
                    downstream = [stops[p] for p in alights]
                    prefix = net.pattern_prefix[pattern]
                    at = prefix[pos]
                    shortest = [prefix[p] - at for p in alights]
                    reach = [to_target[d] + ride for d, ride in zip(downstream, shortest, strict=True)]
                    # A trip boarding at b reaches downstream stop d no earlier than b + its shortest ride there
                    # (arrival >= board_by + ride, every ratio >= 1), and the target no earlier than b + reach;
                    # once b passes, for every d, its ride label or the target's less that, no later trip can
                    # improve one. Ride labels, not walked labels (which cannot start a walk).
                    deadline = _deadline(ride_k, downstream, shortest, reach, target_best)
                    for index in range(begin, end):
                        if times[index] >= deadline:
                            break
                        changed = False
                        trip = trips[index]
                        row = trip_rows[trip]
                        board = row + pos
                        base, start = late_base[board], cumulative[board]
                        for alight_pos in alights:
                            # Every calibrated ratio is at least one. This cheap lower
                            # bound skips envelope lookups, not feasible boardings.
                            ride_s = cumulative[row + alight_pos] - start
                            lower = base + ride_s if ride_s > 0 else base
                            if lower >= target_best:
                                break
                            dest = stops[alight_pos]
                            if lower >= ride_k[dest] or lower + to_target[dest] >= target_best:
                                continue
                            arrival = net._late(board, row + alight_pos)  # noqa: SLF001
                            if arrival >= target_best:
                                # Durations and the ratio envelope are monotone along a trip.
                                break
                            if arrival < ride_k[dest] and arrival + to_target[dest] < target_best:
                                _lower(best_ride, vehicles, dest, arrival)
                                changed = True
                                label = _Label(arrival, previous, (trip, board, row + alight_pos))
                                rides[dest] = label
                                improved[dest, None] = label
                                if arrival < best_k[dest]:
                                    _lower(best, vehicles, dest, arrival)
                                if dest in targets:
                                    target_best = arrival
                        if changed:
                            deadline = _deadline(ride_k, downstream, shortest, reach, target_best)
            for stop, previous in rides.items():
                for dest, seconds in net.footpaths[stop]:
                    arrival = previous.time + seconds
                    key = (dest, stop)
                    if (
                        arrival < best_walk[vehicles].get(key, INF)
                        and arrival < ride_k[dest]
                        and arrival + to_target[dest] < target_best
                    ):
                        _lower_walk(best_walk, vehicles, key, arrival)
                        _lower(best, vehicles, dest, arrival)
                        improved[key] = _Label(arrival, previous, Walk(net.stop_ids[stop], net.stop_ids[dest], seconds))
                        if dest in targets:
                            target_best = arrival
            reached = [label for (stop, _), label in improved.items() if stop in targets]
            if reached:
                results.append(min(reached, key=lambda label: label.time))
            marked = improved
        return results

    def departures(self, start: int, end: int) -> list[tuple[int, set[int]]]:
        """Times in [start, end) a journey can leave the origin, latest first, with the posts boarded at them.

        A post is an origin post or a nearby one walked to.
        """
        net, events = self.net, dict[int, set[int]]()
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
    origin_group: str,
    destination_group: str,
    after: int,
    results: int,
    useful: Callable[[list[Journey]], list[Journey]] | None = None,
) -> list[Journey]:
    """Up to results nondominated journeys leaving no earlier than after, by departure.

    useful, if given, filters the journeys before they are counted and returned.
    Departure windows follow one another from after; each runs from its latest departure to its earliest,
    reusing labels (see _Profile), and keeps the journeys leaving within it: the complete front there.
    """
    origins = net.group_stops.get(origin_group, [])
    targets = set(net.group_stops.get(destination_group, []))
    if not origins or not targets:
        return []
    found: list[Journey] = []
    to_target = net.lower_bounds(targets)
    start, width, stop = after, PROFILE_WINDOW_S, after + PROFILE_SPAN_S
    kept: list[Journey] = []
    while start < stop and len(kept) < results:
        end = min(start + width, stop)
        profile = _Profile(net, origins, targets, to_target)
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
