"""Monthly route-map geometry: align observed stop-to-stop delay segments onto GTFS shapes.

Standard library only; never queries a service. Unsafe or ambiguous snaps are
excluded, never replaced by stop-to-stop chords. Recovery sums are magnitudes.
"""

from __future__ import annotations

import calendar
import json
import math
import re
from collections import Counter, defaultdict
from datetime import date
from itertools import pairwise
from typing import Any

type Row = dict[str, Any]
type Point = list[float]
type Candidate = tuple[float, float, Point]  # (progress along shape m, snap distance m, coordinate)
type Stop = tuple[int, Point]  # (stop_sequence, coordinate)
type Alignment = tuple[dict[int, Candidate], dict[int, str]]  # (projections, per-stop exclusion reasons)

PERIODS = ("weekday", "weekend")
MAX_SNAP = 150.0
MAX_SHAPE_STEP = 2000.0
AMBIGUITY_COST = 25.0**2  # Near-optimal assignments within 25 m RMS evidence.
AMBIGUITY_PROGRESS = 20.0
# Same stop pair, paths this close = one corridor. GTFS shape variants of one road
# differ by float noise to a few metres; real alternative paths differ by 50 m+.
MERGE_TOLERANCE = 10.0
SUM_FIELDS = ("sum_delta_seconds", "sum_gain_seconds", "sum_recovery_seconds")
COUNT_FIELDS = ("observation_count", "observed_days", "gain_count", "recovery_count", "unchanged_count")
MEAN_FIELDS = (
    "mean_from_delay_seconds",
    "mean_to_delay_seconds",
    "mean_scheduled_elapsed_seconds",
    "mean_actual_elapsed_seconds",
)
COVERAGE_FIELDS = (
    "candidate_pairs",
    "usable_pairs",
    "missing_endpoint_count",
    "invalid_coordinate_count",
    "invalid_time_count",
    "missing_shape_id_count",
)
WINDOW_PAIR_FIELDS = tuple(
    f"{window}_{field}" for window in ("daytime", "outside") for field in ("candidate_pairs", "usable_pairs")
)
_MONTH_PATTERN = re.compile(r"(\d{4})-(0[1-9]|1[0-2])")


def number(value: object, field: str) -> int | float:
    """Parse a finite number from a number or numeric string; booleans and None are rejected."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field}: missing or invalid number")
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}: invalid number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field}: nonfinite number")
    return int(result) if result.is_integer() else result


def integer(value: object, field: str, minimum: int = 0) -> int:
    """Parse an integer of at least ``minimum`` from an int-valued number or string."""
    parsed = number(value, field)
    if int(parsed) != parsed or parsed < minimum:
        raise ValueError(f"{field}: expected integer >= {minimum}")
    return int(parsed)


def coordinate(lon: object, lat: object) -> Point:
    """Validate a WGS84 longitude/latitude pair (latitude limited to the Web Mercator range)."""
    lon, lat = number(lon, "longitude"), number(lat, "latitude")
    if not (-180 <= lon <= 180 and -85 <= lat <= 85):
        raise ValueError("coordinate out of range")
    return [lon, lat]


def normalize_segment(source: Row, month: str) -> Row:
    """Validate and normalize one aggregated segment row; raises ValueError on any inconsistency."""
    row = dict(source)
    if "time_window" in row and row["time_window"] not in ("daytime", "outside"):
        raise ValueError("time_window must be daytime or outside when supplied")
    row.setdefault("time_window", None)  # Legacy aggregates are all-day only.
    if row.get("period") not in PERIODS or row.get("mode") not in ("bus", "tram"):
        raise ValueError("segment period/mode must be weekday/weekend and bus/tram")
    for field in (
        "gtfs_snapshot_id",
        "shape_id",
        "line",
        "from_stop_id",
        "to_stop_id",
        "from_stop_name",
        "to_stop_name",
        "from_stop_post_code",
        "to_stop_post_code",
    ):
        row[field] = str(row.get(field) if row.get(field) is not None else "")
    if not row["gtfs_snapshot_id"]:
        raise ValueError("segment missing gtfs_snapshot_id")
    if row.get("direction_id") is not None:
        row["direction_id"] = integer(row["direction_id"], "direction_id")
    for side in ("from", "to"):
        lon, lat = coordinate(row.get(f"{side}_lon"), row.get(f"{side}_lat"))
        row[f"{side}_lon"], row[f"{side}_lat"] = lon, lat
        row[f"{side}_stop_sequence"] = integer(row.get(f"{side}_stop_sequence"), "stop_sequence")
    # GTFS sequence numbers need not be contiguous integers. SQL owns adjacency.
    if row["to_stop_sequence"] <= row["from_stop_sequence"]:
        raise ValueError("segment stop_sequence must increase")
    for field in COUNT_FIELDS:
        row[field] = integer(row.get(field), field, 1 if field in ("observation_count", "observed_days") else 0)
    for field in (*SUM_FIELDS, *MEAN_FIELDS):
        row[field] = number(row.get(field), field)
    count = row["observation_count"]
    if row["observed_days"] > count:
        raise ValueError("observed_days exceeds traversals")
    if sum(row[f] for f in ("gain_count", "recovery_count", "unchanged_count")) != count:
        raise ValueError("gain/recovery/unchanged counts must sum to observation_count")
    if row["sum_gain_seconds"] < 0 or row["sum_recovery_seconds"] < 0:
        raise ValueError("gain and recovery sums must be positive magnitudes")
    if not math.isclose(row["sum_delta_seconds"], row["sum_gain_seconds"] - row["sum_recovery_seconds"], abs_tol=0.01):
        raise ValueError("net sum must equal gain minus recovery")
    if not math.isclose(
        row["sum_delta_seconds"] / count,
        row["mean_to_delay_seconds"] - row["mean_from_delay_seconds"],
        abs_tol=0.02,
    ):
        raise ValueError("net change must use signed arrival delays, not clipped lateness")
    if row["mean_scheduled_elapsed_seconds"] < 0 or row["mean_actual_elapsed_seconds"] < 0:
        raise ValueError("arrival elapsed time must be nonnegative")
    # The production extract may omit observed_service_dates to save memory;
    # pooled_stats then reports bounds on observed days instead of an exact union.
    if "observed_service_dates" in row:
        dates = sorted(set(row["observed_service_dates"]))
        for day in dates:
            parsed = date.fromisoformat(day)
            if not day.startswith(month + "-") or PERIODS[parsed.weekday() >= 5] != row["period"]:
                raise ValueError("observed_service_dates disagree with month/period")
        if len(dates) != row["observed_days"]:
            raise ValueError("observed_service_dates disagree with observed_days")
        row["observed_service_dates"] = dates
    return row


class Shape:
    """Polyline projections in a local metric plane, with a small spatial index."""

    def __init__(self, geometry: object) -> None:
        """Build from a GeoJSON LineString (dict or JSON string); raises ValueError if unusable."""
        if isinstance(geometry, str):
            geometry = json.loads(geometry)
        if not isinstance(geometry, dict) or geometry.get("type") != "LineString":
            raise ValueError("shape must be a GeoJSON LineString")
        self.coords: list[Point] = []
        for raw_point in geometry.get("coordinates", []):
            if not isinstance(raw_point, list) or len(raw_point) < 2:
                raise ValueError("invalid shape coordinate")
            point = coordinate(*raw_point[:2])
            if not self.coords or point != self.coords[-1]:
                self.coords.append(point)
        if len(self.coords) < 2:
            raise ValueError("shape needs two distinct vertices")
        self.origin = self.coords[0]
        self.scale = 111320 * math.cos(math.radians(self.origin[1]))
        self.xy = [self.project(p) for p in self.coords]
        self.progress = [0.0]
        self.grid: defaultdict[tuple[int, int], list[int]] = defaultdict(list)
        for i, (a, b) in enumerate(pairwise(self.xy)):
            length = math.dist(a, b)
            if not 0 < length <= MAX_SHAPE_STEP:
                raise ValueError("shape has zero/over-2km step; possible gap")
            self.progress.append(self.progress[-1] + length)
            for x in range(math.floor(min(a[0], b[0]) / 200), math.floor(max(a[0], b[0]) / 200) + 1):
                for y in range(math.floor(min(a[1], b[1]) / 200), math.floor(max(a[1], b[1]) / 200) + 1):
                    self.grid[x, y].append(i)

    def project(self, point: Point) -> tuple[float, float]:
        """Project a lon/lat point to metres relative to the shape's first vertex."""
        return ((point[0] - self.origin[0]) * self.scale, (point[1] - self.origin[1]) * 111320)

    def candidate(self, point: tuple[float, float], i: int) -> Candidate:
        """Closest point on edge ``i`` to a projected point."""
        a, b = self.xy[i : i + 2]
        dx, dy = b[0] - a[0], b[1] - a[1]
        t = max(0, min(1, ((point[0] - a[0]) * dx + (point[1] - a[1]) * dy) / (dx * dx + dy * dy)))
        distance = math.hypot(point[0] - a[0] - t * dx, point[1] - a[1] - t * dy)
        progress = self.progress[i] + t * (self.progress[i + 1] - self.progress[i])
        coord = [self.coords[i][j] + t * (self.coords[i + 1][j] - self.coords[i][j]) for j in (0, 1)]
        return (progress, distance, coord)

    def candidates(self, coord: Point) -> list[Candidate]:
        """Local-minimum snap candidates within MAX_SNAP of a stop, ordered along the shape."""
        point = self.project(coord)
        cell = [math.floor(v / 200) for v in point]
        indices: set[int] = set()
        for x in range(cell[0] - 1, cell[0] + 2):
            for y in range(cell[1] - 1, cell[1] + 2):
                indices.update(self.grid.get((x, y), ()))
        # Only local minima on the polyline: adjacent edge endpoints are not
        # independent occurrences. Repeated visits to the same stop remain.
        projected = {i: self.candidate(point, i) for i in indices}
        minima: list[Candidate] = []
        for i, candidate in sorted(projected.items()):
            if candidate[1] > MAX_SNAP:
                continue
            if any(projected.get(j, (0, math.inf))[1] < candidate[1] - 1e-7 for j in (i - 1, i + 1)):
                continue
            if minima and abs(candidate[0] - minima[-1][0]) < 0.5:
                if candidate[1] < minima[-1][1]:
                    minima[-1] = candidate
            else:
                minima.append(candidate)
        return minima

    def clip(self, start: Candidate, end: Candidate) -> list[Point]:
        """Sub-polyline between two snapped candidates, with duplicate/collinear vertices removed."""
        coords = [start[2]] + [p for p, d in zip(self.coords, self.progress, strict=True) if start[0] < d < end[0]]
        coords.append(end[2])
        # Canonicalize duplicate and collinear vertices, not bends; ~0.1 m
        # coordinate precision enables identical subpaths to pool across feeds.
        result: list[Point] = []
        for raw_point in coords:
            point = [round(v, 6) for v in raw_point]
            if result and point == result[-1]:
                continue
            while len(result) >= 2:
                a, b, c = map(self.project, (result[-2], result[-1], point))
                length = math.dist(a, c)
                if not length:
                    break
                t = ((b[0] - a[0]) * (c[0] - a[0]) + (b[1] - a[1]) * (c[1] - a[1])) / length**2
                cross = abs((b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])) / length
                if not (0 <= t <= 1 and cross < 0.15):
                    break
                result.pop()
            result.append(point)
        return result


def path_samples(coords: list[Point] | tuple[Point, ...], scale: float, step: float = 2.0) -> list[tuple[float, float]]:
    """Points every ~step metres along a path, in a shared local metric plane."""
    xy = [(lon * scale, lat * 110540) for lon, lat in coords]
    points = []
    for a, b in pairwise(xy):
        n = max(1, math.ceil(math.dist(a, b) / step))
        points += [(a[0] + (b[0] - a[0]) * i / n, a[1] + (b[1] - a[1]) * i / n) for i in range(n)]
    return [*points, xy[-1]]


def paths_within(a: list[tuple[float, float]], b: list[tuple[float, float]], tolerance: float) -> bool:
    """Same orientation (matching ends) and symmetric Hausdorff within tolerance.

    Hausdorff alone ignores direction: a path and its reverse would match.
    """
    if math.dist(a[0], b[0]) > tolerance or math.dist(a[-1], b[-1]) > tolerance:
        return False
    return all(any(math.dist(p, q) <= tolerance for q in other) for one, other in ((a, b), (b, a)) for p in one)


def merge_near_duplicates(pooled: dict[Any, list[Any]]) -> dict[Any, list[Any]]:
    """Merge exact-geometry groups that share an oriented stop pair and nearly the same path.

    The busiest member's geometry represents the merged corridor; statistics stay
    traversal-weighted because rows are pooled, never their means.
    """
    keys = list(pooled)
    parent = list(range(len(keys)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    by_pair: defaultdict[tuple[str, str], set[int]] = defaultdict(set)
    for i, key in enumerate(keys):
        for row, _ in pooled[key]:
            by_pair[row["from_stop_id"], row["to_stop_id"]].add(i)
    samples: dict[int, list[tuple[float, float]]] = {}
    for pair_members in by_pair.values():
        members = sorted(pair_members)
        for x, i in enumerate(members):
            for j in members[x + 1 :]:
                if find(i) == find(j):
                    continue
                scale = 111320 * math.cos(math.radians(keys[i][0][1]))
                a = samples.setdefault(i, path_samples(keys[i], scale))
                b = samples.setdefault(j, path_samples(keys[j], scale))
                if paths_within(a, b, MERGE_TOLERANCE):
                    parent[find(i)] = find(j)
    groups: defaultdict[int, list[Any]] = defaultdict(list)
    for i in range(len(keys)):
        groups[find(i)].append(keys[i])
    merged = {}
    for members in groups.values():
        busiest = max(members, key=lambda key: (sum(row["observation_count"] for row, _ in pooled[key]), key))
        merged[busiest] = [value for key in members for value in pooled[key]]
    return merged


def align_partial(stops: list[Stop], candidates: list[list[Candidate]]) -> Alignment:
    """Salvage safe intervals when one local inconsistency breaks the full DP.

    Find the maximum-cardinality monotone assignment first, then minimize snap
    cost. A stop omitted by ANY near-optimal maximum-cardinality assignment is
    rejected. Retained stops still need an unambiguous occurrence. This never
    emits an interval across an omitted stop: process() clips input pairs only.
    """
    nodes = [(i, choice) for i, choices in enumerate(candidates) for choice in choices]

    def solve(omit: int | None = None) -> tuple[list[tuple[int, float]], list[int], int]:
        scores: list[tuple[int, float]] = []
        parents: list[int] = []
        for i, choice in nodes:
            if i == omit:
                scores.append((0, math.inf))
                parents.append(-1)
                continue
            previous = [
                (count, -cost, k)
                for k, (count, cost) in enumerate(scores)
                if nodes[k][0] < i and nodes[k][1][0] < choice[0] - 0.5 and count
            ]
            count, negative_cost, parent = max(previous, default=(0, 0, -1))
            scores.append((count + 1, -negative_cost + choice[1] ** 2))
            parents.append(parent)
        best_index = max(range(len(nodes)), key=lambda j: (scores[j][0], -scores[j][1]))
        return scores, parents, best_index

    forward, parents, last = solve()
    count, cost = forward[last]
    chosen: dict[int, Candidate] = {}
    while last >= 0:
        i, choice = nodes[last]
        chosen[i] = choice
        last = parents[last]
    backward: list[tuple[int, float]] = [(0, 0.0)] * len(nodes)
    for j in range(len(nodes) - 1, -1, -1):
        i, choice = nodes[j]
        following = [
            (n, -c)
            for k, (n, c) in enumerate(backward[j + 1 :], j + 1)
            if nodes[k][0] > i and nodes[k][1][0] > choice[0] + 0.5
        ]
        n, negative_cost = max(following, default=(0, 0))
        backward[j] = (n + 1, -negative_cost + choice[1] ** 2)
    projections: dict[int, Candidate] = {}
    reasons: dict[int, str] = {}
    for i, (seq, _) in enumerate(stops):
        scores, _, last = solve(omit=i)
        if scores[last][0] == count and scores[last][1] <= cost + AMBIGUITY_COST:
            reasons[seq] = "nonmonotonic_snap"
            continue
        alternatives = [
            choice
            for j, (node_i, choice) in enumerate(nodes)
            if node_i == i
            and forward[j][0] + backward[j][0] - 1 == count
            and forward[j][1] + backward[j][1] - choice[1] ** 2 <= cost + AMBIGUITY_COST
        ]
        if i not in chosen or not alternatives:
            reasons[seq] = "nonmonotonic_snap"
        elif any(abs(choice[0] - chosen[i][0]) > AMBIGUITY_PROGRESS for choice in alternatives):
            reasons[seq] = "ambiguous_snap"
        else:
            projections[seq] = chosen[i]
    return projections, reasons


def align(shape: Shape, stops: list[Stop]) -> Alignment:
    """Joint monotone DP; near-optimal alternate loop occurrences are unsafe.

    Returns projections plus per-stop reasons. Disconnected observed intervals
    may constrain alignment, but only actual input pairs are ever clipped.
    """
    candidates = [shape.candidates(coord) for _, coord in stops]
    if any(not choices for choices in candidates):
        # Split at unsnappable stops so one bad endpoint does not discard a route.
        projections: dict[int, Candidate] = {}
        reasons: dict[int, str] = {}
        run: list[Stop] = []
        for stop, choices in zip(stops, candidates, strict=True):
            if not choices:
                if run:
                    p, r = align(shape, run)
                    projections.update(p)
                    reasons.update(r)
                    run = []
                reasons[stop[0]] = "failed_snap"
            else:
                run.append(stop)
        if run:
            p, r = align(shape, run)
            projections.update(p)
            reasons.update(r)
        return projections, reasons
    if any(len(choices) > 128 for choices in candidates):
        return {}, {seq: "ambiguous_snap" for seq, _ in stops}
    forward: list[list[float]] = []
    parents: list[list[int]] = []
    for i, choices in enumerate(candidates):
        costs, links = [], []
        for choice in choices:
            previous = (
                [(cost, j) for j, cost in enumerate(forward[i - 1]) if candidates[i - 1][j][0] < choice[0] - 0.5]
                if i
                else [(0, -1)]
            )
            cost, parent = min(previous, default=(math.inf, -1))
            costs.append(cost + choice[1] ** 2)
            links.append(parent)
        forward.append(costs)
        parents.append(links)
    best = min(forward[-1])
    if not math.isfinite(best):
        return align_partial(stops, candidates)
    index = forward[-1].index(best)
    chosen: list[Candidate] = [candidates[0][0]] * len(stops)
    for i in range(len(stops) - 1, -1, -1):
        chosen[i] = candidates[i][index]
        index = parents[i][index]
    backward: list[list[float]] = [[] for _ in stops]
    for i in range(len(stops) - 1, -1, -1):
        backward[i] = [
            choice[1] ** 2
            + (
                min(
                    (cost for j, cost in enumerate(backward[i + 1]) if candidates[i + 1][j][0] > choice[0] + 0.5),
                    default=math.inf,
                )
                if i + 1 < len(stops)
                else 0
            )
            for choice in candidates[i]
        ]
    projections, reasons = {}, {}
    for i, (seq, _) in enumerate(stops):
        alternatives = [
            choice
            for j, choice in enumerate(candidates[i])
            if forward[i][j] + backward[i][j] - choice[1] ** 2 <= best + AMBIGUITY_COST
        ]
        if any(abs(choice[0] - chosen[i][0]) > AMBIGUITY_PROGRESS for choice in alternatives):
            reasons[seq] = "ambiguous_snap"
        else:
            projections[seq] = chosen[i]
    return projections, reasons


def coverage_rows(rows: list[Row], month: str) -> list[Row]:
    """Validate daily coverage rows against the month calendar; returns them sorted by date."""
    result: list[Row] = []
    seen: set[str] = set()
    for source in rows:
        row = dict(source)
        day = str(row.get("service_date", ""))
        parsed = date.fromisoformat(day)
        if not day.startswith(month + "-") or day in seen or row.get("period") != PERIODS[parsed.weekday() >= 5]:
            raise ValueError("coverage date/period invalid or duplicate")
        seen.add(day)
        for field in COVERAGE_FIELDS:
            row[field] = integer(row.get(field), field)
        if any(field in row for field in WINDOW_PAIR_FIELDS):
            for field in WINDOW_PAIR_FIELDS:
                row[field] = integer(row.get(field), field)
            for field in ("candidate_pairs", "usable_pairs"):
                if row[f"daytime_{field}"] + row[f"outside_{field}"] != row[field]:
                    raise ValueError(f"coverage {field}: daytime + outside must equal all-day")
        for window in ("daytime", "outside"):
            for field in COVERAGE_FIELDS[2:]:
                key = f"{window}_{field}"
                if key in row:
                    row[key] = integer(row[key], key)
        result.append(row)
    return sorted(result, key=lambda row: row["service_date"])


def pooled_stats(rows: list[Row], period: str, month: str) -> Row:
    """Traversal-weighted statistics for pooled rows, with bounds on distinct observed days.

    Without observed_service_dates the exact day union is unknown, so the result
    carries lower/upper bounds and ``observed_days`` is None unless they coincide.
    """
    count = sum(row["observation_count"] for row in rows)
    totals: Row = {
        field: sum(row[field] for row in rows)
        for field in (*SUM_FIELDS, "observation_count", "gain_count", "recovery_count", "unchanged_count")
    }
    totals.update({field: sum(row[field] * row["observation_count"] for row in rows) / count for field in MEAN_FIELDS})
    totals["mean_delta_seconds"] = totals["sum_delta_seconds"] / count
    totals["mean_gain_seconds"] = totals["sum_gain_seconds"] / totals["gain_count"] if totals["gain_count"] else None
    totals["mean_recovery_seconds"] = (
        totals["sum_recovery_seconds"] / totals["recovery_count"] if totals["recovery_count"] else None
    )
    known = set().union(*(set(row.get("observed_service_dates", [])) for row in rows))
    unknown = [row for row in rows if "observed_service_dates" not in row]
    year, month_number = int(month[:4]), int(month[5:7])
    days_in_month = calendar.monthrange(year, month_number)[1]
    calendar_days = sum(
        PERIODS[date.fromisoformat(f"{month}-{day:02d}").weekday() >= 5] == period
        for day in range(1, days_in_month + 1)
    )
    lower = max([len(known)] + [row["observed_days"] for row in unknown])
    upper = min(calendar_days, len(known) + sum(row["observed_days"] for row in unknown))
    if lower > upper:
        raise ValueError("observed_days exceeds calendar days")
    totals.update(
        observed_days=lower if lower == upper else None,
        observed_days_lower=lower,
        observed_days_upper=upper,
        lines=sorted({row["line"] for row in rows}),
        endpoints=sorted(
            {
                f"{row['from_stop_name']} {row['from_stop_post_code']} → {row['to_stop_name']} {row['to_stop_post_code']}"
                for row in rows
            }
        ),
        stop_pairs=sorted({f"{row['from_stop_id']} → {row['to_stop_id']}" for row in rows}),
        directions=sorted({str(row["direction_id"]) for row in rows}),
        source_segment_rows=len(rows),
    )
    return totals


def period_stats(rows: list[Row], month: str) -> Row:
    """Pooled statistics per period and mode (all/bus/tram), omitting empty groups."""
    result: Row = {}
    for period in PERIODS:
        period_rows = [row for row in rows if row["period"] == period]
        if period_rows:
            result[period] = {
                mode: pooled_stats(selected, period, month)
                for mode in ("all", "bus", "tram")
                if (selected := [row for row in period_rows if mode == "all" or row["mode"] == mode])
            }
    return result


def window_report(
    rows: list[Row],
    mapped: list[tuple[Row, list[Point], float]],
    features: list[Row],
    coverage: list[Row],
    issues: list[Row],
    hours: str,
) -> Row:
    """Per-period input/mapped/excluded counts and coverage totals for one hours selection."""
    result: Row = {}
    stats_field = "stats" if hours == "all" else "daytime_stats"
    prefix = "" if hours == "all" else "daytime_"
    for period in PERIODS:
        incoming = [
            row for row in rows if row["period"] == period and (hours == "all" or row["time_window"] == "daytime")
        ]
        accepted = [
            row
            for row, _, _ in mapped
            if row["period"] == period and (hours == "all" or row["time_window"] == "daytime")
        ]
        rejected = [
            row for row in issues if row["period"] == period and (hours == "all" or row["time_window"] == "daytime")
        ]
        daily = [row for row in coverage if row["period"] == period]
        totals = {
            field: sum(row[prefix + field] for row in daily)
            if daily and all(prefix + field in row for row in daily)
            else None
            for field in COVERAGE_FIELDS
        }
        rejected_counts = Counter(row["reason"] for row in rejected)
        rejected_traversals: Counter[str] = Counter()
        for row in rejected:
            rejected_traversals[row["reason"]] += row["observation_count"]
        result[period] = {
            "input_segment_rows": len(incoming),
            "mapped_segment_rows": len(accepted),
            "excluded_segment_rows": len(rejected),
            "input_traversals": sum(row["observation_count"] for row in incoming),
            "mapped_traversals": sum(row["observation_count"] for row in accepted),
            "excluded_traversals": sum(row["observation_count"] for row in rejected),
            "mapped_corridors": sum(period in feature["properties"][stats_field] for feature in features),
            "exclusions": dict(rejected_counts),
            "excluded_traversals_by_reason": dict(rejected_traversals),
            "coverage": totals,
        }
    return {"periods": result}


def process(data: Row, month: str) -> tuple[Row, Row]:
    """Align a month's segments onto shapes, pool corridors, and compute statistics.

    Args:
        data: Mapping with ``segments``, ``shapes`` and ``coverage`` lists.
        month: Calendar month as ``YYYY-MM``.

    Returns:
        A GeoJSON FeatureCollection of oriented corridors and a report dict.

    Raises:
        ValueError: If the input or month is malformed or internally inconsistent.
    """
    if not _MONTH_PATTERN.fullmatch(month) or month.startswith("0000"):
        raise ValueError("month must be YYYY-MM")
    if not isinstance(data, dict) or any(
        not isinstance(data.get(field), list) for field in ("segments", "shapes", "coverage")
    ):
        raise ValueError("input requires segments, shapes, coverage arrays")
    coverage = coverage_rows(data["coverage"], month)
    rows = [normalize_segment(row, month) for row in data["segments"]]
    has_windows = any(row["time_window"] is not None for row in rows) or any(
        all(field in row for field in WINDOW_PAIR_FIELDS) for row in coverage
    )
    shape_entries: defaultdict[tuple[str, str], list[Row]] = defaultdict(list)
    for row in data["shapes"]:
        shape_entries[str(row.get("gtfs_snapshot_id", "")), str(row.get("shape_id", ""))].append(row)
    shapes: dict[tuple[str, str], Shape] = {}
    shape_errors: dict[tuple[str, str], str] = {}
    for key, entries in shape_entries.items():
        try:
            versions = [Shape(row.get("geometry")) for row in entries]
            if any(version.coords != versions[0].coords for version in versions[1:]):
                raise ValueError("conflicting duplicate shapes")  # noqa: TRY301
            shapes[key] = versions[0]
        except (ValueError, TypeError, KeyError) as exc:
            shape_errors[key] = str(exc)
    groups: defaultdict[tuple[Any, ...], list[Row]] = defaultdict(list)
    for row in rows:
        groups[row["gtfs_snapshot_id"], row["shape_id"], row["line"], row["direction_id"]].append(row)
    excluded: Counter[str] = Counter()
    excluded_traversals: Counter[str] = Counter()
    issues: list[Row] = []
    mapped: list[tuple[Row, list[Point], float]] = []

    def exclude(row: Row, reason: str) -> None:
        excluded[reason] += 1
        excluded_traversals[reason] += row["observation_count"]
        issues.append(
            {
                "reason": reason,
                **{
                    field: row[field]
                    for field in (
                        "period",
                        "time_window",
                        "gtfs_snapshot_id",
                        "shape_id",
                        "line",
                        "direction_id",
                        "from_stop_sequence",
                        "to_stop_sequence",
                        "observation_count",
                    )
                },
            }
        )

    for key, group in groups.items():
        shape_key = key[:2]
        if shape_key not in shapes:
            reason = "invalid_shape" if shape_key in shape_errors else "missing_shape"
            for row in group:
                exclude(row, reason)
            continue
        shape = shapes[shape_key]
        stops: dict[int, tuple[str, Point]] = {}
        conflicts: set[int] = set()
        for row in group:
            for side in ("from", "to"):
                seq = row[f"{side}_stop_sequence"]
                value = (row[f"{side}_stop_id"], [row[f"{side}_lon"], row[f"{side}_lat"]])
                if seq in stops and (
                    value[0] != stops[seq][0] or math.dist(shape.project(value[1]), shape.project(stops[seq][1])) > 2
                ):
                    conflicts.add(seq)
                stops[seq] = value
        # Shared shapes can contain incompatible stop patterns/sequence numbering.
        # Do not invent an order when the extract cannot identify one.
        if conflicts:
            for row in group:
                exclude(row, "conflicting_stop_order")
            continue
        projections, reasons = align(shape, [(seq, value[1]) for seq, value in sorted(stops.items())])
        # Splitting statistical rows into hours must not change the geometry
        # decision. Use each pair's ALL-DAY scheduled interval for the guard.
        scheduled: defaultdict[tuple[str, int, int], list[float]] = defaultdict(lambda: [0.0, 0])
        for row in group:
            pair = (row["period"], row["from_stop_sequence"], row["to_stop_sequence"])
            scheduled[pair][0] += row["mean_scheduled_elapsed_seconds"] * row["observation_count"]
            scheduled[pair][1] += row["observation_count"]
        for row in group:
            a, b = row["from_stop_sequence"], row["to_stop_sequence"]
            if a not in projections or b not in projections:
                exclude(row, reasons.get(a, reasons.get(b, "failed_snap")))
                continue
            start, end = projections[a], projections[b]
            # A near stop on a repeat can otherwise select a huge detour. This
            # conservative cap uses supplied arrival schedule, not observed delay.
            length = end[0] - start[0]
            direct = math.dist(shape.project(start[2]), shape.project(end[2]))
            elapsed_sum, traversals = scheduled[row["period"], a, b]
            if length > max(3000, direct * 8) or length > max(2000, elapsed_sum / traversals * 40):
                exclude(row, "pathological_interval")
                continue
            coords = shape.clip(start, end)
            if len(coords) < 2:
                exclude(row, "degenerate_interval")
                continue
            mapped.append((row, coords, max(start[1], end[1])))
    exact: defaultdict[tuple[tuple[float, ...], ...], list[tuple[Row, float]]] = defaultdict(list)
    for row, coords, snap in mapped:
        exact[tuple(map(tuple, coords))].append((row, snap))
    pooled = merge_near_duplicates(exact)
    features: list[Row] = []
    for i, (coords, values) in enumerate(sorted(pooled.items())):
        source_rows = [value[0] for value in values]
        stats = period_stats(source_rows, month)
        daytime_stats = period_stats([row for row in source_rows if row["time_window"] == "daytime"], month)
        features.append(
            {
                "type": "Feature",
                "id": i,
                "geometry": {"type": "LineString", "coordinates": [list(point) for point in coords]},
                "properties": {
                    "corridor_id": i,
                    "stats": stats,
                    "daytime_stats": daytime_stats,
                    "max_snap_meters": round(max(v[1] for v in values), 1),
                },
            }
        )
    time_windows = {
        hours: window_report(rows, mapped, features, coverage, issues, hours) for hours in ("all", "daytime")
    }
    report = {
        "month": month,
        "coverage": coverage,
        "periods": time_windows["all"]["periods"],
        "has_time_window_data": has_windows,
        "time_windows": time_windows,
        "time_window_definition": (
            "daytime: both scheduled arrivals in Europe/Warsaw [06:00,22:00), same local calendar date; "
            "outside: complement"
        ),
        "csv_note": "hours=all includes daytime; mode=all includes bus/tram. These summary rows overlap; do not add them together.",
        "exclusions": dict(excluded),
        "excluded_traversals_by_reason": dict(excluded_traversals),
        "shape_errors": [{"gtfs_snapshot_id": k[0], "shape_id": k[1], "reason": v} for k, v in shape_errors.items()],
        "input_shapes": len(data["shapes"]),
        "valid_shapes": len(shapes),
        "mapped_corridors": len(features),
        "issues": issues,
        "geometry_policy": {
            "max_snap_meters": MAX_SNAP,
            "max_shape_step_meters": MAX_SHAPE_STEP,
            "ambiguity_cost_meters_squared": AMBIGUITY_COST,
            "ambiguity_progress_meters": AMBIGUITY_PROGRESS,
            "canonical_coordinate_decimals": 6,
            "no_straight_line_fallback": True,
        },
    }
    return {"type": "FeatureCollection", "features": features}, report
