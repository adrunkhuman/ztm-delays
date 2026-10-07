"""Live vehicle positions matched to the planner's trips.

After every poll the poller replaces ``vehicles.json.gz`` under ``health/poller/public/`` in
``ZTM_STATUS_GCS_BUCKET`` (``contracts/live_vehicles_v1.json``): the latest ping of each vehicle with its line
and brigade. Line and brigade name the vehicle's duty for the day (GTFS ``block_short_name``); its position
along the shapes of that duty's trips says which trip it runs and how late it is there.

A vehicle is *running* a trip between its first and last stop, or *waiting* at the first stop of the duty's
next trip, including when it has just reached the end of the previous one. Ambiguity goes to the reading that
continues the vehicle's last fix, else to the delay closest to the trip's usual one. A vehicle off every
candidate shape, or implausibly early or late, gets no fix: its trips keep their usual times.
``ZTM_LIVE_VEHICLES_FILE`` reads a local copy instead of GCS, for development.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import zlib
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, date, datetime
from datetime import time as clock
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from google.api_core.exceptions import NotFound

from ztm_frontend import live_status
from ztm_frontend.db import read_connection

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    import duckdb

    from ztm_frontend.journey import Network

LOGGER = logging.getLogger(__name__)

FILE_ENV: Final = "ZTM_LIVE_VEHICLES_FILE"
FEED_OBJECT: Final = "health/poller/public/vehicles.json.gz"
FEED_TTL_S: Final = 10  # the poller replaces the object about this often
FEED_MAX_BYTES: Final = 1024 * 1024
FEED_MAX_JSON_BYTES: Final = 16 * 1024 * 1024
COLUMNS: Final = ("mode", "line", "brigade", "vehicle", "lat", "lon", "time")
MODES: Final = frozenset({"bus", "tram"})
STALE_FEED_S: Final = 120  # an older object means the poller or the feed has stopped
STALE_PING_S: Final = 120  # a vehicle silent this long says nothing about now

OFF_ROUTE_M: Final = 100  # GPS error plus road width; farther than this the vehicle is not on the shape
TERMINAL_M: Final = 60  # this close to the first or last stop along the shape, the vehicle is at the terminus
TERMINUS_REACH_M: Final = 300  # shapes may run on past a terminus (to a depot, a loop); that is not the terminus
EARLIEST_S, LATEST_S = -15 * 60, 60 * 60  # the matcher's own bounds on a plausible departure delay
BEFORE_TRIP_S, AFTER_TRIP_S = 45 * 60, 60 * 60  # a ping this far around a trip's timetable may belong to it
MAX_SPEED_MPS: Final = 25.0
CONTINUITY_SLACK_M: Final = 300.0
CONTINUES: Final = 1e6  # a reading that continues the last fix beats every other
# A vehicle still at the terminus this long past due, and this long after it arrived (or not seen arriving),
# has mostly handed its duty to another vehicle: on 6 Oct 2026 only 26% of them ran the trip, against ~88%
# of waiting vehicles overall (the rest mostly trips the batch matcher did not publish).
STUCK_S: Final = 10 * 60
MEMORY_S: Final = 30 * 60  # a vehicle's last fix still informs a ping this much later that gets none
CHUNK: Final = 32  # shape segments per bounding box
WARSAW_LAT, WARSAW_LON = 52.23, 21.01
M_PER_DEG_LAT: Final = 110_574.0
M_PER_DEG_LON: Final = 111_320.0 * math.cos(math.radians(WARSAW_LAT))


@dataclass(frozen=True, slots=True)
class Ping:
    """One vehicle in the feed; time in Unix seconds."""

    mode: str
    line: str
    brigade: str
    vehicle: str
    lat: float
    lon: float
    time: int


@dataclass(frozen=True, slots=True)
class Fix:
    """Where a vehicle is on a trip. Times are seconds from the network day's midnight.

    delay_s is the ping time less the timetable at dist_m; for a waiting vehicle, less the first stop's
    timetable, so it is negative while the vehicle waits ahead of time. arrived is when a waiting vehicle was
    first seen at the end of its previous trip, if it was.
    """

    trip: int  # index into the network's trips
    vehicle: str
    mode: str
    line: str
    lat: float
    lon: float
    seen: int
    dist_m: float
    delay_s: int
    waiting: bool
    arrived: int | None = None


@dataclass(frozen=True)
class Live:
    """The fixes of one feed object."""

    updated_at: datetime
    fixes: dict[int, Fix]  # by trip index; one vehicle per trip

    def age_s(self, now: datetime) -> float:
        """Seconds since the poller wrote this feed object."""
        return (now - self.updated_at).total_seconds()


# --- feed ---------------------------------------------------------------------------------------


@dataclass
class _Feed:
    lock: threading.Lock
    fetched_at: float | None = None
    value: tuple[datetime, list[Ping]] | None = None


_feed = _Feed(threading.Lock())


def enabled() -> bool:
    """Return whether a live feed is configured."""
    return bool(os.environ.get(FILE_ENV) or os.environ.get(live_status.BUCKET_ENV))


def _read_feed() -> bytes:
    local = os.environ.get(FILE_ENV)
    if local:
        data = Path(local).read_bytes()
        if len(data) > FEED_MAX_BYTES:
            raise ValueError(f"{local} exceeds {FEED_MAX_BYTES} bytes")
        return data
    return live_status.download(os.environ[live_status.BUCKET_ENV], FEED_OBJECT, FEED_MAX_BYTES)


def gunzip(data: bytes) -> bytes:
    """Decompress at most FEED_MAX_JSON_BYTES: a small object must not expand without bound."""
    inflater = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    out = inflater.decompress(data, FEED_MAX_JSON_BYTES)
    if inflater.unconsumed_tail:
        raise ValueError("live vehicles expand beyond the limit")
    return out


def parse_feed(payload: object) -> tuple[datetime, list[Ping]] | None:
    """Validate a decoded feed object; skip malformed rows, reject a malformed object."""
    if not isinstance(payload, dict) or payload.get("version") != 1 or payload.get("columns") != list(COLUMNS):
        return None
    stamp, rows = payload.get("updated_at"), payload.get("vehicles")
    if not isinstance(stamp, str) or not isinstance(rows, list):
        return None
    try:
        updated_at = datetime.fromisoformat(stamp).astimezone(UTC)
    except ValueError:
        return None
    return updated_at, [ping for row in rows if (ping := _ping(row)) is not None]


def _ping(row: object) -> Ping | None:
    if not isinstance(row, list) or len(row) != len(COLUMNS):
        return None
    mode, line, brigade, vehicle, lat, lon, seen = row
    texts = (mode, line, brigade, vehicle)
    numbers = (lat, lon, seen)
    if not all(isinstance(v, str) and v for v in texts) or mode not in MODES:
        return None
    if not all(isinstance(v, int | float) and not isinstance(v, bool) and math.isfinite(v) for v in numbers):
        return None
    mode, line, brigade, vehicle = (str(v) for v in texts)
    lat, lon, seen = (float(v) for v in numbers)  # ty: ignore[invalid-argument-type] - checked numeric above
    # The GPS feed may drop a brigade's leading zeros; the artifact strips them too.
    return Ping(mode, line, brigade.lstrip("0") or "0", vehicle, lat, lon, int(seen))


def feed() -> tuple[datetime, list[Ping]] | None:
    """The current feed, read at most once per FEED_TTL_S; None when unavailable."""
    if not enabled():
        return None
    with _feed.lock:
        now = time.monotonic()
        if _feed.fetched_at is None or now - _feed.fetched_at >= FEED_TTL_S:
            _feed.fetched_at = now
            try:
                _feed.value = parse_feed(json.loads(gunzip(_read_feed())))
            except NotFound:
                _feed.value = None
            except Exception:  # noqa: BLE001 - credentials, network, size, gzip, JSON: no live data
                LOGGER.warning("Could not read live vehicles", exc_info=True)
                _feed.value = None
        return _feed.value


def clear_cache() -> None:
    """Forget the feed and every matcher (tests)."""
    with _feed.lock:
        _feed.fetched_at, _feed.value = None, None


# --- shapes -------------------------------------------------------------------------------------


def to_metres(lat: float, lon: float) -> tuple[float, float]:
    """Local planar metres around Warsaw: east, north."""
    return (lon - WARSAW_LON) * M_PER_DEG_LON, (lat - WARSAW_LAT) * M_PER_DEG_LAT


class Shape:
    """A polyline in local metres, with distances along it and coarse bounding boxes for projection."""

    def __init__(self, lat: Sequence[float], lon: Sequence[float], dist_m: Sequence[int | None]) -> None:
        """Project to metres; distances along come from GTFS, or from the geometry when any is missing."""
        points = [to_metres(a, b) for a, b in zip(lat, lon, strict=True)]
        self.x = [p[0] for p in points]
        self.y = [p[1] for p in points]
        if any(d is None for d in dist_m):
            along, total = [0.0], 0.0
            for i in range(1, len(points)):
                total += math.dist(points[i - 1], points[i])
                along.append(total)
            self.d = along
        else:
            self.d = [float(d) for d in dist_m if d is not None]
        self.boxes = []
        for start in range(0, max(len(points) - 1, 0), CHUNK):
            xs, ys = self.x[start : start + CHUNK + 1], self.y[start : start + CHUNK + 1]
            self.boxes.append((start, min(xs), max(xs), min(ys), max(ys)))

    def path(self, start_m: float, end_m: float, most: int = 150) -> list[list[float]]:
        """[lon, lat] points from start_m to end_m metres along, thinned to at most ``most``."""
        inside = [i for i, d in enumerate(self.d) if start_m < d < end_m]
        step = max(1, math.ceil(len(inside) / most))
        points = [self._at(start_m), *(self._lonlat(self.x[i], self.y[i]) for i in inside[::step]), self._at(end_m)]
        return [[round(lon, 6), round(lat, 6)] for lon, lat in points]

    def _at(self, along: float) -> tuple[float, float]:
        i = min(max(bisect_right(self.d, along), 1), len(self.d) - 1)
        span = self.d[i] - self.d[i - 1]
        share = min(max((along - self.d[i - 1]) / span, 0.0), 1.0) if span > 0 else 0.0
        x0, y0, x1, y1 = self.x[i - 1], self.y[i - 1], self.x[i], self.y[i]
        return self._lonlat(x0 + share * (x1 - x0), y0 + share * (y1 - y0))

    @staticmethod
    def _lonlat(x: float, y: float) -> tuple[float, float]:
        return x / M_PER_DEG_LON + WARSAW_LON, y / M_PER_DEG_LAT + WARSAW_LAT

    def project(self, x: float, y: float) -> list[tuple[float, float]]:
        """(metres along, metres off) of the nearest point of each pass within OFF_ROUTE_M.

        A shape can pass the same place twice (a loop, or both directions of a street); each contiguous run
        of nearby segments is one pass.
        """
        near: list[tuple[int, float, float]] = []
        sx, sy, sd = self.x, self.y, self.d
        for start, x0, x1, y0, y1 in self.boxes:
            if not (x0 - OFF_ROUTE_M <= x <= x1 + OFF_ROUTE_M and y0 - OFF_ROUTE_M <= y <= y1 + OFF_ROUTE_M):
                continue
            for i in range(start, min(start + CHUNK, len(sx) - 1)):
                ax, ay, dx, dy = sx[i], sy[i], sx[i + 1] - sx[i], sy[i + 1] - sy[i]
                length2 = dx * dx + dy * dy
                t = 0.0 if length2 == 0 else max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / length2))
                off = math.hypot(x - ax - t * dx, y - ay - t * dy)
                if off <= OFF_ROUTE_M:
                    near.append((i, off, sd[i] + t * (sd[i + 1] - sd[i])))
        passes: list[tuple[float, float]] = []
        previous = -2
        for i, off, along in near:
            if i == previous + 1 and passes and off < passes[-1][1]:
                passes[-1] = (along, off)
            elif i != previous + 1:
                passes.append((along, off))
            previous = i
        return passes


# --- matching -----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Reading:
    fix: Fix
    cost: float


@dataclass(frozen=True, slots=True)
class _Seen:
    """One ping on the network day, with the vehicle's last fix."""

    ping: Ping
    at: int
    previous: Fix | None

    def fix(self, trip: int, along: float, delay: float, *, waiting: bool, arrived: int | None = None) -> Fix:
        ping = self.ping
        return Fix(
            trip, ping.vehicle, ping.mode, ping.line, ping.lat, ping.lon, self.at, along, round(delay), waiting, arrived
        )


class Matcher:
    """Matches pings to one network's trips, remembering each vehicle's last fix between feed objects."""

    def __init__(self, net: Network, shapes: Callable[[Iterable[str]], dict[str, Shape | None]]) -> None:
        """Index the network's bus and tram trips by line and brigade, and each duty's trips in order."""
        self.net = net
        self.load_shapes = shapes
        self.shapes: dict[str, Shape | None] = {}
        self.by_brigade: dict[tuple[str, str, str], list[int]] = {}
        duties: dict[str, list[int]] = {}
        for trip, (mode, line, brigade, duty, _) in enumerate(net.trip_meta):
            if mode in MODES and brigade is not None and duty is not None:
                self.by_brigade.setdefault((mode, line, brigade), []).append(trip)
                duties.setdefault(duty, []).append(trip)
        self.next_trip: dict[int, int] = {}
        for trips in duties.values():
            trips.sort(key=self.first_sched)
            self.next_trip.update(pairwise(trips))
        self.previous_trip = {after: before for before, after in self.next_trip.items()}
        self.last: dict[tuple[str, str], Fix] = {}
        self.updated_at: datetime | None = None
        self.fixes: dict[int, Fix] = {}

    def first_sched(self, trip: int) -> int:
        """Timetable at the trip's first stop."""
        return self.net.sched[self.net.trip_rows[trip]]

    def rows(self, trip: int) -> range:
        """The trip's rows in the network's per-stop columns."""
        row, length = self.net.trip_index[self.net.trip_keys[trip]]
        return range(row, row + length)

    def at(self, trip: int, dist: float) -> tuple[float, float]:
        """Timetable and usual delay at dist metres along the trip, interpolated between its stops."""
        net, rows = self.net, self.rows(trip)
        dists = net.shape_dist[rows.start : rows.stop]
        i = bisect_right(dists, dist)
        if i == 0 or i == len(dists):
            row = rows.start if i == 0 else rows.stop - 1
            return net.sched[row], net.expected[row] - net.sched[row]
        a, b = rows.start + i - 1, rows.start + i
        span = dists[i] - dists[i - 1]
        share = (dist - dists[i - 1]) / span if span > 0 else 0.0
        sched = net.sched[a] + share * (net.sched[b] - net.sched[a])
        usual_a, usual_b = net.expected[a] - net.sched[a], net.expected[b] - net.sched[b]
        return sched, usual_a + share * (usual_b - usual_a)

    def update(self, updated_at: datetime, pings: list[Ping], day: date) -> None:
        """Match a new feed object; pings are placed on the network day by wall-clock seconds from midnight."""
        if updated_at == self.updated_at:
            return
        now = seconds(updated_at, day)
        candidates: list[tuple[_Seen, list[int]]] = []
        for ping in pings:
            at = seconds(datetime.fromtimestamp(ping.time, UTC), day)
            if now - at > STALE_PING_S:
                continue
            trips = [
                t
                for t in self.by_brigade.get((ping.mode, ping.line, ping.brigade), ())
                if self.first_sched(t) - BEFORE_TRIP_S <= at <= self.net.sched[self.rows(t).stop - 1] + AFTER_TRIP_S
            ]
            if trips:
                candidates.append((_Seen(ping, at, self.last.get((ping.mode, ping.vehicle))), trips))
        missing = {self.net.trip_meta[t][4] for _, trips in candidates for t in trips} - self.shapes.keys()
        wanted = sorted(m for m in missing if m is not None)
        if wanted:
            self.shapes.update(self.load_shapes(wanted))
        # Remember vehicles that are silent, stale or unmatched now: a stuck vehicle must not look newly arrived
        # after a gap, nor a detour start its trip afresh.
        last = {key: fix for key, fix in self.last.items() if now - fix.seen <= MEMORY_S}
        held: dict[int, _Reading] = {}
        for seen, trips in candidates:
            reading = self._match(seen, trips)
            if reading is None:
                continue
            last[seen.ping.mode, seen.ping.vehicle] = reading.fix
            other = held.get(reading.fix.trip)
            # Two vehicles on one trip (a swap or a brigade mix-up): the one in service, else the better reading.
            if other is None or (other.fix.waiting, other.cost) > (reading.fix.waiting, reading.cost):
                held[reading.fix.trip] = reading
        self.last, self.updated_at = last, updated_at
        self.fixes = {trip: reading.fix for trip, reading in held.items()}

    def _match(self, seen: _Seen, trips: list[int]) -> _Reading | None:
        net = self.net
        x, y = to_metres(seen.ping.lat, seen.ping.lon)
        best: _Reading | None = None
        for trip in trips:
            shape = self.shapes.get(net.trip_meta[trip][4] or "")
            rows = self.rows(trip)
            first_d, last_d = net.shape_dist[rows.start], net.shape_dist[rows.stop - 1]
            if shape is None or first_d < 0 or last_d < 0:
                continue
            for along, _ in shape.project(x, y):
                if along > last_d + TERMINUS_REACH_M or along < first_d - TERMINUS_REACH_M:
                    continue
                if along >= last_d - TERMINAL_M:
                    # At the end of the trip: waiting for the duty's next one, if any.
                    after = self.next_trip.get(trip)
                    reading = None if after is None else self._waiting(seen, after)
                elif along <= first_d + TERMINAL_M:
                    reading = self._waiting(seen, trip)
                else:
                    reading = self._running(seen, trip, along)
                if reading is not None and (best is None or reading.cost < best.cost):
                    best = reading
        return best

    def _waiting(self, seen: _Seen, trip: int) -> _Reading | None:
        """At the first stop of trip, before it leaves.

        Its arrival comes from history only: when it was first seen here after running the trip before. Without
        that (a gap, a restart, the start of a duty) it is unknown, and the delay alone decides whether it is stuck.
        """
        net, previous = self.net, seen.previous
        start = self.rows(trip).start
        delay = seen.at - net.sched[start]
        if not -BEFORE_TRIP_S <= delay <= LATEST_S:
            return None
        # A vehicle waiting ahead of time is not early: it will leave when due.
        cost = abs(max(delay, 0) - (net.expected[start] - net.sched[start]))
        arrived = None
        if previous is not None and previous.trip == trip and previous.waiting:
            cost -= CONTINUES
            arrived = previous.arrived
        elif previous is not None and not previous.waiting and self.next_trip.get(previous.trip) == trip:
            cost -= CONTINUES
            arrived = seen.at
        if delay > STUCK_S and (arrived is None or seen.at - arrived > STUCK_S):
            return None
        return _Reading(seen.fix(trip, net.shape_dist[start], delay, waiting=True, arrived=arrived), cost)

    def _running(self, seen: _Seen, trip: int, along: float) -> _Reading | None:
        sched, usual = self.at(trip, along)
        delay = seen.at - sched
        if not EARLIEST_S <= delay <= LATEST_S:
            return None
        cost = abs(delay - usual)
        previous = seen.previous
        if previous is not None and previous.trip == trip:
            reach = max(0, seen.at - previous.seen) * MAX_SPEED_MPS + CONTINUITY_SLACK_M
            if previous.dist_m - CONTINUITY_SLACK_M <= along <= previous.dist_m + reach:
                cost -= CONTINUES
        return _Reading(seen.fix(trip, along, delay, waiting=False), cost)


def seconds(moment: datetime, day: date) -> int:
    """Wall-clock seconds from day's midnight in Warsaw, as GTFS counts them."""
    local = moment.astimezone(live_status.WARSAW).replace(tzinfo=None)
    return round((local - datetime.combine(day, clock())).total_seconds())


# --- per network --------------------------------------------------------------------------------

_matchers_lock = threading.Lock()


def _shape_loader(path: Path, build_id: str) -> Callable[[Iterable[str]], dict[str, Shape | None]]:
    def load(shape_ids: Iterable[str]) -> dict[str, Shape | None]:
        wanted = list(shape_ids)
        found: dict[str, Shape | None] = dict.fromkeys(wanted)
        with read_connection(path) as connection:
            # A newer build may have replaced the file; its shape ids belong to another snapshot.
            build = connection.execute("select build_id from planner_metadata limit 1").fetchone()
            if build is None or str(build[0]) != build_id:
                return {}
            for shape_id, lat, lon, dist in _shape_rows(connection, wanted):
                if len(lat) >= 2 and len(lat) == len(lon) == len(dist):  # noqa: PLR2004
                    found[shape_id] = Shape(lat, lon, dist)
        return found

    return load


def _shape_rows(connection: duckdb.DuckDBPyConnection, shape_ids: list[str]) -> list[tuple[Any, ...]]:
    return connection.execute(
        "select shape_id, lat, lon, dist_m from planner_shape where shape_id in (select unnest(?::varchar[]))",
        [shape_ids],
    ).fetchall()


def current(path: Path, net: Network, day: date, now: datetime) -> Live | None:
    """Live fixes for day's network, or None without a fresh feed or when day is not today."""
    if now.astimezone(live_status.WARSAW).date() != day:
        return None
    loaded = feed()
    if loaded is None:
        return None
    updated_at, pings = loaded
    if (now - updated_at).total_seconds() > STALE_FEED_S:
        return None
    with _matchers_lock:
        # Kept on the network, not in a map keyed by it: the matcher refers to its network, so it must die with it.
        matcher = net.live
        if not isinstance(matcher, Matcher):
            matcher = net.live = Matcher(net, _shape_loader(path, net.build_id))
        matcher.update(updated_at, pings, day)
        return Live(updated_at, dict(matcher.fixes))
