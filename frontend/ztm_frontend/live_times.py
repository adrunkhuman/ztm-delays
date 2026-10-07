"""Planned times moved by live positions: a patched copy of today's network per feed object.

A running vehicle's delay beyond its usual one at its position carries down the route: at a stop h seconds of
timetable ahead, alpha(h) of it remains, with the weekly calibration's error quantiles for vehicles that late
around it (``planner_live_persistence``). Expected times use the median error and the late bound the 90th.
"Be at the stop by" uses the 1st percentile plus the stop tables' 30 s tolerance, but moves later than the
timetable-based one only for stops due within LEAVE_LATER_WITHIN_S. Farther ahead the live error has heavier
tails than the calibration week shows: replaying 6 Oct 2026, allowing it everywhere missed 1.6% of buses 30-90
min ahead. It may always move earlier. Stops the vehicle has passed can
no longer be boarded; beyond the calibrated horizon a trip keeps its usual times.

A duty's next trip starts late when its vehicle cannot make it: it leaves a median turnaround after reaching the
terminus (``planner_live_turnaround``), from a waiting vehicle's seen arrival or a running one's predicted one.
Only its expected and late times move: another vehicle may take the trip over on time, so "be at the stop by"
stays as it was. A waiting vehicle whose arrival is unknown changes nothing.
"""

from __future__ import annotations

import math
import threading
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import duckdb

from ztm_frontend import live
from ztm_frontend.db import read_connection
from ztm_frontend.journey import NO_BOARD

if TYPE_CHECKING:
    from datetime import date, datetime
    from pathlib import Path

    from ztm_frontend.journey import Network

TOLERANCE_S: Final = 30  # as the stop tables: at most 1% of vehicles more than this before "be at the stop by"
PASSED_M: Final = 50  # a stop this far behind the vehicle along the shape is passed
# Replaying 5 and 6 Oct 2026 with the week before's calibration, every 10-minute band missed at most 0.7% of
# vehicles at 15 min; at 20 min, buses 10-30 min ahead reached 0.9%.
LEAVE_LATER_WITHIN_S: Final = 15 * 60

RowTimes = tuple[int, int, int]  # board_by, expected, late


Quantiles = tuple[float, float, float]  # low, mid, high


@dataclass(frozen=True)
class Calibration:
    """Weekly live calibration of one artifact build."""

    horizons: dict[bool, list[int]]  # by is_tram: upper bounds in seconds, ascending
    alphas: dict[bool, list[float]]  # per horizon
    bands: dict[bool, list[list[int]]]  # per horizon: lower bounds of the excess-delay bands, ascending
    errors: dict[bool, list[list[Quantiles]]]  # per horizon and band
    turnaround: dict[bool, Quantiles]

    @classmethod
    def from_rows(cls, persistence: list[tuple], turnaround: list[tuple]) -> Calibration | None:
        """From (is_tram, horizon_min, excess_s, alpha, low, mid, high) and (is_tram, low, mid, high) rows."""
        horizons: dict[bool, list[int]] = {}
        alphas: dict[bool, list[float]] = {}
        bands: dict[bool, list[list[int]]] = {}
        errors: dict[bool, list[list[Quantiles]]] = {}
        for tram, minutes, excess, alpha, low, mid, high in sorted(persistence, key=lambda row: row[:3]):
            if not horizons.get(tram) or horizons[tram][-1] != minutes * 60:
                horizons.setdefault(tram, []).append(minutes * 60)
                alphas.setdefault(tram, []).append(alpha)
                bands.setdefault(tram, []).append([])
                errors.setdefault(tram, []).append([])
            bands[tram][-1].append(excess)
            errors[tram][-1].append((low, mid, high))
        if not horizons:
            return None
        return cls(horizons, alphas, bands, errors, {row[0]: tuple(row[1:4]) for row in turnaround})

    def ahead(self, tram: bool, seconds: float, excess: float) -> tuple[float, Quantiles] | None:
        """Alpha and the error quantiles this far ahead of a vehicle this late beyond usual; None past the horizons."""
        bounds = self.horizons.get(tram, [])
        i = bisect_left(bounds, seconds)
        if i >= len(bounds):
            return None
        band = max(bisect_right(self.bands[tram][i], excess) - 1, 0)
        return self.alphas[tram][i], self.errors[tram][i][band]


def load_calibration(connection: duckdb.DuckDBPyConnection) -> Calibration | None:
    """The artifact's calibration, or None when it has none (no live adjustments then)."""
    try:
        persistence = connection.execute(
            "select is_tram, horizon_min, excess_s, alpha, low_s, mid_s, high_s from planner_live_persistence"
        ).fetchall()
        turnaround = connection.execute("select is_tram, low_s, mid_s, high_s from planner_live_turnaround").fetchall()
    except duckdb.CatalogException:  # an artifact from before live calibration
        return None
    return Calibration.from_rows(persistence, turnaround)


@dataclass
class _View:
    calibration: Calibration | None
    updated_at: datetime | None = None
    net: Network | None = None
    fixes: dict[int, live.Fix] | None = None


_lock = threading.Lock()


@dataclass(frozen=True)
class LiveView:
    """Today's network with live times, the fixes behind it and when the feed was written."""

    net: Network
    fixes: dict[int, live.Fix]
    updated_at: datetime


def view(path: Path, net: Network, day: date, now: datetime) -> LiveView | None:
    """Day's network patched to the current feed object, or None without live data for it."""
    found = live.current(path, net, day, now)
    if found is None or not isinstance(net.live, live.Matcher):
        return None
    with _lock:
        state = net.live_view
        if not isinstance(state, _View):
            with read_connection(path) as connection:
                build = connection.execute("select build_id from planner_metadata limit 1").fetchone()
                same = build is not None and str(build[0]) == net.build_id
                state = net.live_view = _View(load_calibration(connection) if same else None)
        if state.calibration is None:
            return None
        if state.updated_at != found.updated_at or state.net is None:
            at = live.seconds(found.updated_at, day)
            state.net = net.patched(row_times(net.live, found.fixes, state.calibration, at))
            state.updated_at, state.fixes = found.updated_at, found.fixes
        return LiveView(state.net, state.fixes or {}, found.updated_at)


def row_times(
    matcher: live.Matcher, fixes: dict[int, live.Fix], calibration: Calibration, now: int
) -> dict[int, RowTimes]:
    """New times of every stop row the fixes move, by row; now is the feed's time on the network day."""
    out: dict[int, RowTimes] = {}
    starts: dict[int, float] = {}  # trip: predicted start
    for trip, fix in fixes.items():
        turnaround = calibration.turnaround.get(fix.mode == "tram")
        if fix.waiting:
            if fix.arrived is not None and turnaround is not None:
                starts[trip] = fix.arrived + turnaround[1]
            continue
        arrival = _running(matcher, calibration, trip, fix, now, out)
        after = matcher.next_trip.get(trip)
        if arrival is not None and after is not None and after not in fixes and turnaround is not None:
            starts[after] = arrival + turnaround[1]
    for trip, start in starts.items():
        _starting(matcher, calibration, trip, start, out)
    return out


def _running(  # noqa: PLR0913
    matcher: live.Matcher, calibration: Calibration, trip: int, fix: live.Fix, now: int, out: dict[int, RowTimes]
) -> float | None:
    """Patch the stops ahead of a running vehicle; its predicted arrival at the last one."""
    net, tram = matcher.net, fix.mode == "tram"
    excess = fix.delay_s - matcher.at(trip, fix.dist_m)[1]
    position_sched = fix.seen - fix.delay_s
    rows = matcher.rows(trip)
    previous = -math.inf
    arrival = None
    for row in rows:
        if net.shape_dist[row] < fix.dist_m - PASSED_M:
            out[row] = (NO_BOARD, net.expected[row], net.late_base[row])
            continue
        quantiles = calibration.ahead(tram, net.sched[row] - position_sched, excess)
        if quantiles is None:
            _keep_order(net, row, previous, out)
            continue
        alpha, (low, mid, high) = quantiles
        base = net.expected[row] + alpha * excess
        expected = max(base + mid, previous)
        board = net.board[row]
        if board < NO_BOARD:
            live_board = min(math.floor(base + low + TOLERANCE_S), math.floor(expected))
            # Later only for a stop the vehicle has yet to reach: at or just past one, it may already have left.
            ahead = net.shape_dist[row] > fix.dist_m
            if live_board < board or (ahead and expected - now <= LEAVE_LATER_WITHIN_S):
                board = live_board
        out[row] = (board, round(expected), math.ceil(max(base + high, expected)))
        previous = expected
        arrival = expected if row == rows.stop - 1 else None
    return arrival


def _starting(
    matcher: live.Matcher, calibration: Calibration, trip: int, start_at: float, out: dict[int, RowTimes]
) -> None:
    """Move the expected and late times of a trip that will leave late, from its predicted start.

    The delay at its first stop carries down the route as a running vehicle's would from there.
    """
    net = matcher.net
    rows = matcher.rows(trip)
    first = rows.start
    excess = start_at - net.expected[first]
    if excess <= 0:
        return
    tram = net.trip_meta[trip][0] == "tram"
    previous = -math.inf
    for row in rows:
        quantiles = calibration.ahead(tram, net.sched[row] - net.sched[first], excess)
        if quantiles is None:
            _keep_order(net, row, previous, out)
            continue
        alpha, (_, mid, high) = quantiles
        if row == first:
            expected, late = start_at, start_at + high - mid
        else:
            expected = max(net.expected[row] + alpha * excess + mid, previous)
            late = net.expected[row] + alpha * excess + high
        out[row] = (net.board[row], round(expected), math.ceil(max(late, expected, net.late_base[row])))
        previous = expected


def _keep_order(net: Network, row: int, previous: float, out: dict[int, RowTimes]) -> None:
    """Past the horizon: usual times, but never before the stop ahead of it."""
    if net.expected[row] < previous:
        expected = math.ceil(previous)
        out[row] = (net.board[row], expected, max(net.late_base[row], expected))
