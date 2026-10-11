"""Independent Python routing oracle, deliberately kept outside the installed package.

This is the pre-native search, including its own mutable bounds and suffix cache.
Only immutable query inputs and result value types are shared with production.
"""

# ruff: noqa: SLF001
from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from typing import TYPE_CHECKING

from ztm_frontend.journey import INF, MAX_JOURNEY_S, MAX_VEHICLES, Network, Walk, _Label

if TYPE_CHECKING:
    from array import array

SUFFIX_POSITIONS = 131_072
WALK_PERMISSION_WORDS = 32_768
_UNREACHED_WALK = (INF,) * (MAX_VEHICLES + 1)


def _lower(columns: list[list[int]], k: int, stop: int, value: int) -> None:
    """Lower stop's entry in column k and every later column (each is non-increasing in k)."""
    for column in columns[k:]:
        if value >= column[stop]:
            break
        column[stop] = value


def _lower_walk(states: dict[tuple[int, int], list[int]], k: int, key: tuple[int, int], value: int) -> None:
    """Lower a sparse walking state's bound with one dictionary lookup, not one per vehicle count."""
    bounds = states.get(key)
    if bounds is None:
        states[key] = [INF] * k + [value] * (MAX_VEHICLES + 1 - k)
        return
    for count in range(k, MAX_VEHICLES + 1):
        if value >= bounds[count]:
            break
        bounds[count] = value


def _deadline(
    ride: list[int], downstream: list[int], shortest: list[float], reach: list[float], target_best: int
) -> float:
    # Scalar comparisons avoid a generator and two builtin calls per suffix position in this hot loop.
    deadline = -math.inf
    for dest, seconds, remaining in zip(downstream, shortest, reach, strict=True):
        bound = ride[dest] - seconds
        if bound <= deadline:
            continue
        target_bound = target_best - remaining
        if target_bound < bound:  # noqa: PLR1730
            bound = target_bound
        if bound > deadline:  # noqa: PLR1730
            deadline = bound
    return deadline


class _Suffixes:
    """Lazy incidence bounds shared by a query's windows, never their mutable round labels.

    A long pattern has quadratically many suffix positions. Bound their total size rather than just the
    number of cached incidences; oversized individual suffixes are computed but not retained.
    """

    def __init__(self, net: Network, to_target: list[float]) -> None:
        self.net, self.to_target = net, to_target
        self.entries: OrderedDict[tuple[int, int], tuple[tuple[int, ...], list[int], list[float], list[float]]] = (
            OrderedDict()
        )
        self.positions = 0

    def get(self, pattern: int, pos: int) -> tuple[tuple[int, ...], list[int], list[float], list[float]]:
        key = (pattern, pos)
        cached = self.entries.get(key)
        if cached is not None:
            return cached
        net = self.net
        alights = net.pattern_alights[pattern]
        alights = alights[bisect_right(alights, pos) :]
        downstream = [net.patterns[pattern][p] for p in alights]
        prefix = net.pattern_prefix[pattern]
        at = prefix[pos]
        shortest = [prefix[p] - at for p in alights]
        reach = [self.to_target[d] + ride for d, ride in zip(downstream, shortest, strict=True)]
        cached = (alights, downstream, shortest, reach)
        size = len(alights)
        if size <= SUFFIX_POSITIONS:
            while self.entries and self.positions + size > SUFFIX_POSITIONS:
                self.positions -= len(self.entries.popitem(last=False)[1][0])
            self.entries[key] = cached
            self.positions += size
        return cached


class _WalkPermissions:
    """Query-local, bounded signatures of the boardings permitted after a transfer walk."""

    def __init__(self, net: Network) -> None:
        self.net = net
        self.entries: OrderedDict[tuple[int, int], tuple[int, int]] = OrderedDict()
        self.words = 0

    def get(self, stop: int, source: int) -> int:
        key = (stop, source)
        cached = self.entries.get(key)
        if cached is not None:
            return cached[0]
        board_at = self.net.board_at[source]
        mask = sum(
            1 << i
            for i, (pattern, pos, _, _) in enumerate(self.net.incidence[stop])
            if board_at.get(pattern, pos) >= pos
        )
        words = max(1, (mask.bit_length() + 63) // 64)
        if words <= WALK_PERMISSION_WORDS:
            while self.entries and self.words + words > WALK_PERMISSION_WORDS:
                self.words -= self.entries.popitem(last=False)[1][1]
            self.entries[key] = (mask, words)
            self.words += words
        return mask


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
        suffixes: _Suffixes | None = None,
        permissions: _WalkPermissions | None = None,
    ) -> None:
        n, columns = len(net.stop_ids), range(MAX_VEHICLES + 1)
        self.net, self.origins, self.targets = net, origins, targets
        self.access, self.egress = access or {}, egress or {}
        self.target_best = [INF] * (MAX_VEHICLES + 1)
        # A label that cannot beat the target's even riding on without waiting leads nowhere; the target's
        # labels only fall, so the pruning holds for every later run.
        weighted = {stop: self.egress[stop].walk_s if stop in self.egress else 0 for stop in targets}
        self.to_target = net.lower_bounds(weighted) if to_target is None else to_target
        self.suffixes = suffixes if suffixes is not None else _Suffixes(net, self.to_target)
        self.permissions = permissions if permissions is not None else _WalkPermissions(net)
        self.best = [[INF] * n for _ in columns]
        # Unrestricted labels can board every pattern and start a walk. A walked label can do neither,
        # so it must not hide an unrestricted label even when it arrives earlier.
        self.best_ride = [[INF] * n for _ in columns]
        # Walks from different posts forbid different boardings. Keep sparse bounds by (stop, walk origin);
        # -1 denotes the initial walk, whose restriction covers all origin posts.
        self.best_walk: dict[tuple[int, int], list[int]] = {}
        # Once boarding is permitted, its downstream timings and permissions no longer depend on the incoming
        # walking history. Share scan intervals per incidence, but retain every vehicle-count column.
        self.scan_entries: dict[int, list[tuple[tuple[int, int, array, array], list[int]]]] = {}
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

    def run(self, after: int, boarding: set[int] | None = None) -> list[_Label]:  # noqa: C901, PLR0912, PLR0915
        """Target labels from a departure no later than any previous run's, per improved vehicle count.

        boarding, after the first run, names the origin or nearby posts that have a departure exactly at after:
        the others have no trip between this label and the one their trips were last scanned from.
        """
        net, origins, targets = self.net, self.origins, self.targets
        late_base, cumulative, trip_rows = net.late_base, net.cumulative, net.trip_rows
        best, best_ride, to_target = self.best, self.best_ride, self.to_target
        best_walk = self.best_walk
        marked: dict[tuple[int, int | None], _Label] = {}
        origin = _Label(after)
        for stop, walk in self.access.items():
            arrival, key = after + walk.walk_s, (stop, -1)
            if arrival < best_walk.get(key, _UNREACHED_WALK)[0]:
                _lower_walk(best_walk, 0, key, arrival)
                _lower(best, 0, stop, arrival)
                if boarding is None or stop in boarding:
                    marked[key] = _Label(arrival, origin, walk)
        for stop in origins:
            if after < best_ride[0][stop]:
                _lower(best_ride, 0, stop, after)
                _lower(best, 0, stop, after)
                if boarding is None or stop in boarding:
                    marked[stop, None] = origin
        for stop, dest, seconds in self.origin_walks:
            arrival = after + seconds
            key = (dest, -1)
            if arrival < best_walk.get(key, _UNREACHED_WALK)[0] and arrival < best_ride[0][dest]:
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
            best_k, ride_k = best[vehicles], best_ride[vehicles]
            target_best = min(horizon, self.target_best[vehicles])
            reached: list[_Label] = []
            rides: dict[int, _Label] = {}
            improved: dict[tuple[int, int | None], _Label] = {}
            # Earlier labels tend to establish tighter downstream bounds first.
            # This changes scan order only; it assumes nothing about trip overtaking.
            for (stop, walk_origin), previous in sorted(marked.items(), key=lambda item: item[1].time):
                remaining = target_best - to_target[stop]
                if previous.time >= remaining:
                    continue
                # Reached on foot: skip trips that already stop, boardable, where the walk started (for a first
                # walk, any origin post). The same vehicle boarded later only looks better through the
                # boarding-dependent late bound; it would also let a rider "chase" a missed vehicle, which the
                # planner does not offer.
                walked_from = None
                if walk_origin is not None:
                    walked_from = self.origin_board if walk_origin == -1 else net.board_at[walk_origin]
                entries = self.scan_entries.get(stop)
                if entries is None:
                    entries = [(entry, [INF] * (MAX_VEHICLES + 1)) for entry in net.incidence[stop]]
                    self.scan_entries[stop] = entries
                for (pattern, pos, times, trips), scan_bounds in entries:
                    if walked_from is not None and walked_from.get(pattern, pos) < pos:
                        continue
                    # Every feasible trip at or after until was already considered by an allowed label with
                    # no more vehicles. Ride/target bounds only decrease, so none can improve now. This shares
                    # work across walking histories without treating a forbidden boarding as scanned.
                    until = scan_bounds[vehicles - 1]
                    if previous.time >= until:
                        continue
                    for count in range(vehicles - 1, MAX_VEHICLES + 1):
                        if previous.time >= scan_bounds[count]:
                            break
                        scan_bounds[count] = previous.time
                    begin = bisect_left(times, previous.time)
                    limit = min(until, remaining)
                    if begin == len(times) or times[begin] >= limit:
                        continue
                    end = bisect_left(times, limit, begin + 1)
                    stops = net.patterns[pattern]
                    alights, downstream, shortest, reach = self.suffixes.get(pattern, pos)
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
                            arrival = net._late(board, row + alight_pos)
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
                                    walk = self.egress.get(dest)
                                    final = _Label(arrival + walk.walk_s, label, walk) if walk else label
                                    if final.time < target_best:
                                        target_best = final.time
                                        reached.append(final)
                        if changed:
                            deadline = _deadline(ride_k, downstream, shortest, reach, target_best)
            for stop, previous in rides.items():
                for dest, seconds in net.footpaths[stop]:
                    arrival = previous.time + seconds
                    key = (dest, stop)
                    if (
                        arrival < best_walk.get(key, _UNREACHED_WALK)[vehicles]
                        and arrival < ride_k[dest]
                        and arrival + to_target[dest] < target_best
                    ):
                        _lower_walk(best_walk, vehicles, key, arrival)
                        _lower(best, vehicles, dest, arrival)
                        improved[key] = _Label(arrival, previous, Walk(net.stop_ids[stop], net.stop_ids[dest], seconds))
                        if dest in targets and not self.egress:
                            target_best = arrival
                            reached.append(improved[key])
            if reached:
                final = min(reached, key=lambda label: label.time)
                for k in range(vehicles, MAX_VEHICLES + 1):
                    self.target_best[k] = min(self.target_best[k], final.time)
                results.append(final)
            marked = self._prune_walks(improved)
        return results

    def _prune_walks(self, marked: dict[tuple[int, int | None], _Label]) -> dict[tuple[int, int | None], _Label]:
        """Keep the earliest transfer-walk label at each post for each exact set of permitted boardings.

        Neither walked label can start another walk. The earlier one can board every trip the later one can,
        and the incoming history has no effect after boarding. Keep unrestricted labels separate. Filter the
        original dictionary, rather than replacing a group's value in place: a later-inserted winner must not
        inherit an earlier loser's position among equal-time labels from other groups.
        """
        winners: dict[tuple[int, int], tuple[tuple[int, int | None], _Label]] = {}
        for key, label in marked.items():
            stop, source = key
            if source is None:
                continue
            group = (stop, self.permissions.get(stop, source))
            previous = winners.get(group)
            if previous is None or label.time < previous[1].time:
                winners[group] = (key, label)
        keep = {key for key, _ in winners.values()}
        return {key: label for key, label in marked.items() if key[1] is None or key in keep}

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
