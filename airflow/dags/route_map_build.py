"""Monthly route-map artifacts: extract checks, frontend files, mini-maps, and atomic publication.

Standard library only, so the DAG's BigQuery boundary and these rules can be tested separately.
The frontend reads <maps_dir>/<YYYY-MM>/ (see frontend/ztm_frontend/app.py); a month is visible once
its directory holds segments.json, and publish() only ever renames a complete directory into place.
"""

from __future__ import annotations

import calendar
import json
import math
import re
import shutil
import tempfile
import uuid
from datetime import date, timedelta
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
PERIODS = {"weekday": "wd", "weekend": "we"}
MODES = ("bus", "tram")
# Corridors with fewer daytime traversals in a period are too noisy to colour.
MIN_TRIPS = 100
# Geometry rejects ambiguous snaps conservatively; September 2026 kept 98.9% of daytime traversals.
MIN_MAPPED_SHARE = 0.95
HOURS_LABEL = "06:00\N{EN DASH}22:00"
FIELDS = {"d": "mean_delta_seconds", "n": "observation_count", "g": "gain_count", "r": "recovery_count"}
EXTRACT_KINDS = {"segment": "segments", "shape": "shapes", "coverage": "coverage"}
PUBLISHED_FILES = ("routes.geojson", "segments.json", "mini-bus.svg", "mini-tram.svg", "report.json")

# Overview mini-maps: same bins, colours and width scale as frontend static/map.js, weekdays only.
STEPS = (-60, -30, -10, 10, 30, 60)
PALETTE = ("#4a9eff", "#3f80d0", "#33608f", "#4a4a4a", "#a03d3d", "#d84a45", "#ff5c5c")
WIDTH_SCALE = (1.75, 1.2, 0.85, 0.55, 0.85, 1.2, 1.75)
# Neutral first, extremes last, so hotspots draw on top as on the interactive map.
DRAW_ORDER = (3, 2, 4, 1, 5, 0, 6)
MINI_SIZE = (600, 440)
MINI_PAD = 10
# Tram is framed tighter and sparser; heavier strokes keep it legible beside bus.
MINI_BASE_WIDTH = {"bus": 1.5, "tram": 2.4}
MINI_SIMPLIFY = 0.6
# Fixed frames keep the pre-rendered street backgrounds (route_map_assets/) aligned month to month.
MINI_FRAMES = {
    "bus": ((20.86, 52.10), (21.26, 52.36)),
    "tram": ((20.88, 52.15), (21.13, 52.35)),
}
ASSETS_DIR = Path(__file__).with_name("route_map_assets")


class Frame(NamedTuple):
    """A mini-map viewBox and the projection of its fixed geographic frame into it."""

    project: Callable[[float, float], tuple[float, float]]
    width: int
    height: int
    scale: float


def previous_month(day: date) -> str:
    """Return the YYYY-MM month before the one containing day."""
    return (day.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def month_dates(month: str) -> list[date]:
    """Return every calendar date in a YYYY-MM month."""
    if not MONTH_PATTERN.fullmatch(month):
        raise ValueError(f"Route map month must be YYYY-MM, got {month!r}")
    year, number = map(int, month.split("-"))
    return [date(year, number, day) for day in range(1, calendar.monthrange(year, number)[1] + 1)]


def expected_service_dates(month: str, eligible_start: date, exclusions: Iterable[date]) -> list[date]:
    """Dates the warehouse should hold for a month, skipping pre-archive and known-bad dates."""
    excluded = set(exclusions)
    return [day for day in month_dates(month) if day >= eligible_start and day not in excluded]


def parse_extract(rows: Iterable[tuple[str, str]]) -> dict[str, list[dict[str, Any]]]:
    """Group (kind, JSON payload) result rows into the geometry input."""
    data: dict[str, list[dict[str, Any]]] = {name: [] for name in EXTRACT_KINDS.values()}
    for kind, payload in rows:
        if kind not in EXTRACT_KINDS:
            raise ValueError(f"Unexpected route extract row kind {kind!r}")
        data[EXTRACT_KINDS[kind]].append(json.loads(payload))
    return data


def validate_extract(data: Mapping[str, list[dict[str, Any]]], expected_dates: Iterable[date]) -> None:
    """Reconcile pooled segment traversals with per-date coverage before any geometry work."""
    expected = sorted(day.isoformat() for day in expected_dates)
    covered = sorted(str(row["service_date"]) for row in data["coverage"])
    if covered != expected:
        missing, extra = sorted(set(expected) - set(covered)), sorted(set(covered) - set(expected))
        raise ValueError(f"Route extract coverage dates differ: missing {missing}, unexpected {extra}")
    if not data["segments"]:
        raise ValueError("Route extract has no segments")
    for period in PERIODS:
        for window in ("daytime", "outside"):
            traversals = sum(
                int(row["observation_count"])
                for row in data["segments"]
                if row["period"] == period and row["time_window"] == window
            )
            usable = sum(int(row[f"{window}_usable_pairs"]) for row in data["coverage"] if row["period"] == period)
            if traversals != usable:
                raise ValueError(f"{period}/{window}: {traversals} segment traversals, {usable} usable coverage pairs")
    shape_keys = [(row["gtfs_snapshot_id"], row["shape_id"]) for row in data["shapes"]]
    if len(shape_keys) != len(set(shape_keys)):
        raise ValueError("Route extract repeats a pooled shape")


def check_mapped_share(report: Mapping[str, Any]) -> None:
    """Fail when geometry dropped an unusual share of daytime traversals."""
    for period, values in report["time_windows"]["daytime"]["periods"].items():
        if values["input_traversals"] and values["mapped_traversals"] / values["input_traversals"] < MIN_MAPPED_SHARE:
            raise ValueError(
                f"{period}: only {values['mapped_traversals']} of {values['input_traversals']} daytime traversals mapped"
            )


def site_files(geojson: Mapping[str, Any], report: Mapping[str, Any], month: str) -> dict[str, str]:
    """Build the frontend's files for one month from the geometry output."""
    features = []
    totals = {period: {mode: {"corridors": 0, "traversals": 0} for mode in MODES} for period in PERIODS}
    for feature in geojson["features"]:
        stats = feature["properties"].get("daytime_stats") or {}
        props: dict[str, Any] = {}
        names: set[str] = set()
        lines: dict[str, set[str]] = {mode: set() for mode in MODES}
        for period, prefix in PERIODS.items():
            for mode in MODES:
                row = (stats.get(period) or {}).get(mode)
                if not row or row["observation_count"] < MIN_TRIPS:
                    continue
                for key, field in FIELDS.items():
                    value = row[field]
                    props[f"{prefix}_{mode}_{key}"] = value if isinstance(value, int) else round(value, 1)
                lines[mode].update(row["lines"])
                names.update(row["endpoints"])
                totals[period][mode]["corridors"] += 1
                totals[period][mode]["traversals"] += row["observation_count"]
        if not props:
            continue
        for mode, mode_lines in lines.items():
            if mode_lines:
                props[mode] = sorted(mode_lines, key=lambda line: (len(line), line))
        props["stops"] = sorted(names)[0]
        props["aliases"] = len(names) - 1
        coords = [[round(lng, 5), round(lat, 5)] for lng, lat in feature["geometry"]["coordinates"]]
        features.append({"id": feature["id"], "coordinates": coords, "properties": props})
    if not features:
        raise ValueError(f"No corridor reaches {MIN_TRIPS} daytime traversals in {month}")

    tram = [c for f in features if any("_tram_" in k for k in f["properties"]) for c in f["coordinates"]]
    meta = {
        "month": month,
        "anchor": month_dates(month)[-1].isoformat(),
        "hours": HOURS_LABEL,
        "min_trips": MIN_TRIPS,
        "totals": totals,
        "tram_bounds": _bounds(tram) if tram else [list(corner) for corner in MINI_FRAMES["tram"]],
    }
    # The browser only needs what paints a line; everything shown in popups stays server-side.
    routes = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": f["id"],
                "geometry": {"type": "LineString", "coordinates": f["coordinates"]},
                "properties": {k: v for k, v in f["properties"].items() if k.endswith("_d")},
            }
            for f in features
        ],
    }
    summary = {key: value for key, value in report.items() if key != "issues"}
    return {
        "routes.geojson": _compact_json(routes),
        "segments.json": _compact_json({"meta": meta, "segments": {f["id"]: f["properties"] for f in features}}),
        "mini-bus.svg": minimap(features, "bus"),
        "mini-tram.svg": minimap(features, "tram"),
        "report.json": json.dumps(summary, ensure_ascii=False, indent=1) + "\n",
    }


def publish(maps_dir: Path, month: str, files: Mapping[str, str]) -> Path:
    """Replace <maps_dir>/<month> with files; readers never see a partial month.

    A rebuild is two renames, so the month is absent for an instant between them (the frontend
    answers 404). The DAG allows one active run, so leftovers from a crashed run are safe to remove.
    """
    if not MONTH_PATTERN.fullmatch(month) or set(files) != set(PUBLISHED_FILES):
        raise ValueError(f"Refusing to publish {month!r} with files {sorted(files)}")
    maps_dir.mkdir(parents=True, exist_ok=True)
    for leftover in maps_dir.glob(f".{month}.*"):
        shutil.rmtree(leftover, ignore_errors=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{month}.staging-", dir=maps_dir))
    try:
        for name, content in files.items():
            (staging / name).write_text(content, encoding="utf-8")
            # The frontend container reads as a different user.
            (staging / name).chmod(0o644)
        staging.chmod(0o755)
        target = maps_dir / month
        if target.exists():
            retired = maps_dir / f".{month}.retired-{uuid.uuid4().hex}"
            target.rename(retired)
            staging.rename(target)
            shutil.rmtree(retired)
        else:
            staging.rename(target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return target


def minimap(features: list[dict[str, Any]], mode: str) -> str:
    """Weekday overview mini-map: binned route strokes over a pre-rendered street background."""
    key = f"wd_{mode}_d"
    frame = mini_frame(mode)
    bins: list[list[list[list[float]]]] = [[] for _ in PALETTE]
    for feature in features:
        if key in feature["properties"]:
            bins[sum(feature["properties"][key] > step for step in STEPS)].append(feature["coordinates"])
    paths = "".join(
        f'<path stroke="{PALETTE[i]}" stroke-width="{MINI_BASE_WIDTH[mode] * WIDTH_SCALE[i]:.2f}" d="{data}"/>'
        for i in DRAW_ORDER
        if (data := path_data(bins[i], frame))
    )
    background = (ASSETS_DIR / f"mini-background-{mode}.svg").read_text(encoding="utf-8")
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {frame.width} {frame.height}" fill="none" '
        f'stroke-linecap="round" stroke-linejoin="round">{background}{paths}</svg>'
    )


def mini_frame(mode: str) -> Frame:
    """Equirectangular projection of a mode's fixed frame into the mini-map viewBox."""
    (west, south), (east, north) = MINI_FRAMES[mode]
    width, height = MINI_SIZE
    kx = math.cos(math.radians((south + north) / 2))
    scale = min((width - 2 * MINI_PAD) / ((east - west) * kx), (height - 2 * MINI_PAD) / (north - south))
    cx, cy = (west + east) / 2, (south + north) / 2

    def project(lon: float, lat: float) -> tuple[float, float]:
        return (width / 2 + (lon - cx) * kx * scale, height / 2 - (lat - cy) * scale)

    return Frame(project, width, height, scale)


def path_data(lines: Iterable[list[list[float]]], frame: Frame, tolerance: float = MINI_SIMPLIFY, digits: int = 1) -> str:
    """One SVG path for many polylines: simplified, culled to the frame, relative moves after the first point."""
    factor = 10**digits

    def fmt(value: int) -> str:
        return f"{value / factor:g}" if digits else str(value)

    parts = []
    for coords in lines:
        points = simplify([frame.project(*c) for c in coords], tolerance)
        if all(not (0 <= x <= frame.width and 0 <= y <= frame.height) for x, y in points):
            continue
        # Deltas between already-rounded points, so relative steps never accumulate error.
        rounded = [(round(x * factor), round(y * factor)) for x, y in points]
        steps = [f"{fmt(x1 - x0)} {fmt(y1 - y0)}" for (x0, y0), (x1, y1) in pairwise(rounded)]
        parts.append(f"M{fmt(rounded[0][0])} {fmt(rounded[0][1])}l" + " ".join(steps))
    return "".join(parts)


def simplify(points: list[tuple[float, float]], tolerance: float) -> list[tuple[float, float]]:
    """Douglas-Peucker on projected points."""
    if len(points) < 3:  # noqa: PLR2004
        return points
    (ax, ay), (bx, by) = points[0], points[-1]
    length = math.hypot(bx - ax, by - ay) or 1e-9
    distances = [abs((bx - ax) * (ay - y) - (ax - x) * (by - ay)) / length for x, y in points[1:-1]]
    i = max(range(len(distances)), key=distances.__getitem__)
    if distances[i] <= tolerance:
        return [points[0], points[-1]]
    return simplify(points[: i + 2], tolerance)[:-1] + simplify(points[i + 1 :], tolerance)


def _bounds(coords: list[list[float]]) -> list[list[float]]:
    return [[min(c[0] for c in coords), min(c[1] for c in coords)], [max(c[0] for c in coords), max(c[1] for c in coords)]]


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
