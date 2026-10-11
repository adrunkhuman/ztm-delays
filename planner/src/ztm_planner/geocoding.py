"""Optional, offline OSM address/road SQLite artifact. No downloads or automatic refresh."""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import tempfile
import time
import unicodedata
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Literal

import osmium
import osmium.osm

log = logging.getLogger(__name__)
type BBox = tuple[float, float, float, float]  # west, south, east, north
DEFAULT_BBOX: BBox = (20.3, 51.8, 21.8, 52.7)
BATCH_SIZE = 2000
ATTRIBUTION = "© OpenStreetMap contributors; ODbL 1.0; https://www.openstreetmap.org/copyright"
SCHEMA = """
CREATE TABLE places (
    id INTEGER PRIMARY KEY,
    kind TEXT CHECK (kind IN ('address', 'street')),
    street TEXT,
    house TEXT,
    city TEXT,
    lat REAL,
    lon REAL,
    osm_id TEXT,
    geometry TEXT
);
CREATE VIRTUAL TABLE places_fts USING fts5(text, tokenize='unicode61', prefix='2 3 4');
CREATE VIRTUAL TABLE places_rtree USING rtree(id, min_lon, max_lon, min_lat, max_lat);
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
"""


def normalize(text: str) -> str:
    """Fold accents and Polish ł without changing whitespace or display tags."""
    folded = unicodedata.normalize("NFKD", text.casefold().replace("ł", "l"))
    return "".join(char for char in folded if not unicodedata.combining(char))


def validate_bbox(bbox: BBox) -> None:
    west, south, east, north = bbox
    if not all(math.isfinite(v) for v in bbox) or not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("bbox must be finite west south east north within lon/lat bounds, without antimeridian wrap")


def _valid_point(point: tuple[float, float]) -> bool:
    lon, lat = point
    return math.isfinite(lon) and math.isfinite(lat) and -180 <= lon <= 180 and -90 <= lat <= 90


def _in_bbox(point: tuple[float, float], bbox: BBox) -> bool:
    lon, lat = point
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


def _intersects(a: tuple[float, float], b: tuple[float, float], bbox: BBox) -> bool:
    """Segment/rectangle intersection, including edges and crossing segments with both ends outside."""
    low, high = 0.0, 1.0
    for start, end, lower, upper in ((a[0], b[0], bbox[0], bbox[2]), (a[1], b[1], bbox[1], bbox[3])):
        delta = end - start
        if delta == 0:
            if not lower <= start <= upper:
                return False
        else:
            t1, t2 = (lower - start) / delta, (upper - start) / delta
            low, high = max(low, min(t1, t2)), min(high, max(t1, t2))
            if low > high:
                return False
    return True


def _centroid(points: list[tuple[float, float]]) -> tuple[float, float] | None:
    """Planar area centroid of a closed building ring; translate first to avoid cancellation."""
    if len(points) < 4 or points[0] != points[-1] or len(set(points[:-1])) < 3:
        return None
    ox, oy = points[0]
    area = xsum = ysum = 0.0
    for (ax, ay), (bx, by) in pairwise(points):
        ax, ay, bx, by = ax - ox, ay - oy, bx - ox, by - oy
        cross = ax * by - bx * ay
        area += cross
        xsum += (ax + bx) * cross
        ysum += (ay + by) * cross
    if abs(area) < 1e-16:
        return None
    point = (ox + xsum / (3 * area), oy + ysum / (3 * area))
    # Reject obviously malformed rings; this is not a full polygon topology validator.
    if not (
        min(p[0] for p in points) <= point[0] <= max(p[0] for p in points)
        and min(p[1] for p in points) <= point[1] <= max(p[1] for p in points)
    ):
        return None
    return point


def _midpoint(points: list[tuple[float, float]]) -> tuple[float, float]:
    """Halfway along a road, weighted by equirectangular segment lengths (not vertex count)."""
    scale = math.cos(math.radians(sum(p[1] for p in points) / len(points)))
    lengths = [math.hypot((b[0] - a[0]) * scale, b[1] - a[1]) for a, b in pairwise(points)]
    remaining = sum(lengths) / 2
    for (a, b), length in zip(pairwise(points), lengths, strict=True):
        if length and remaining <= length:
            fraction = remaining / length
            return (a[0] + fraction * (b[0] - a[0]), a[1] + fraction * (b[1] - a[1]))
        remaining -= length
    return points[0]


class _Builder(osmium.SimpleHandler):
    def __init__(self, connection: sqlite3.Connection, bbox: BBox) -> None:
        super().__init__()
        self.connection = connection
        self.bbox = bbox
        self.rows: list[tuple[int, str, str, str, str, float, float, str, str | None]] = []
        self.bounds: list[tuple[int, float, float, float, float]] = []
        self.fts: list[tuple[int, str]] = []
        self.next_id = 1
        self.counts: Counter[str] = Counter(
            dict.fromkeys(
                (
                    "address",
                    "street",
                    "address_nodes",
                    "address_ways",
                    "street_ways",
                    "invalid_address_nodes",
                    "invalid_relevant_ways",
                    "invalid_address_rings",
                ),
                0,
            )
        )

    def add(
        self,
        kind: Literal["address", "street"],
        tags: osmium.osm.TagList,
        osm_id: str,
        point: tuple[float, float],
        geometry: list[tuple[float, float]] | None = None,
    ) -> None:
        street = (tags.get("name") if kind == "street" else tags.get("addr:street")) or ""
        house = (tags.get("addr:housenumber") or "") if kind == "address" else ""
        city = tags.get("addr:city") or ""
        lon, lat = point
        row_id = self.next_id
        self.next_id += 1
        self.rows.append(
            (
                row_id,
                kind,
                street,
                house,
                city,
                lat,
                lon,
                osm_id,
                json.dumps(geometry, separators=(",", ":")) if geometry is not None else None,
            )
        )
        points = geometry if geometry is not None else [point]
        self.bounds.append(
            (
                row_id,
                min(p[0] for p in points),
                max(p[0] for p in points),
                min(p[1] for p in points),
                max(p[1] for p in points),
            )
        )
        self.fts.append((row_id, normalize(" ".join((street, house, city)))))
        self.counts[kind] += 1
        self.counts[
            "address_nodes" if osm_id.startswith("node/") else "address_ways" if kind == "address" else "street_ways"
        ] += 1
        if len(self.rows) >= BATCH_SIZE:
            self.flush()

    def flush(self) -> None:
        self.connection.executemany("INSERT INTO places VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.rows)
        self.connection.executemany("INSERT INTO places_rtree VALUES (?, ?, ?, ?, ?)", self.bounds)
        self.connection.executemany("INSERT INTO places_fts (rowid, text) VALUES (?, ?)", self.fts)
        self.rows.clear()
        self.bounds.clear()
        self.fts.clear()

    def node(self, node: osmium.osm.Node) -> None:
        if not (node.tags.get("addr:housenumber") or node.tags.get("addr:street")):
            return
        if not node.location.valid():
            self.counts["invalid_address_nodes"] += 1
            return
        point = (node.location.lon, node.location.lat)
        if not _valid_point(point):
            self.counts["invalid_address_nodes"] += 1
            return
        if _in_bbox(point, self.bbox):
            self.add("address", node.tags, f"node/{node.id}", point)

    def way(self, way: osmium.osm.Way) -> None:
        is_address = way.tags.get("building") not in (None, "", "no") and bool(
            way.tags.get("addr:housenumber") or way.tags.get("addr:street")
        )
        is_street = bool(way.tags.get("highway") and way.tags.get("name"))
        if not (is_address or is_street):
            return
        # Missing references invalidate the whole way: do not invent a segment across a gap.
        if len(way.nodes) < 2 or any(not node.location.valid() for node in way.nodes):
            self.counts["invalid_relevant_ways"] += 1
            return
        points = [(node.lon, node.lat) for node in way.nodes]
        if len(set(points)) < 2 or any(not _valid_point(point) for point in points):
            self.counts["invalid_relevant_ways"] += 1
            return
        west, south, east, north = self.bbox
        if (
            max(p[0] for p in points) < west
            or min(p[0] for p in points) > east
            or max(p[1] for p in points) < south
            or min(p[1] for p in points) > north
        ):
            return
        if is_address:
            point = _centroid(points)
            if point is None:
                self.counts["invalid_address_rings"] += 1
            elif _in_bbox(point, self.bbox):
                self.add("address", way.tags, f"way/{way.id}", point)
        if is_street and any(_intersects(a, b, self.bbox) for a, b in pairwise(points)):
            self.add("street", way.tags, f"way/{way.id}", _midpoint(points), points)


def _validate(connection: sqlite3.Connection, counts: Counter[str]) -> None:
    """Reject empty artifacts, damaged indexes, or incomplete row/index publication."""
    expected = counts["address"] + counts["street"]
    if not expected:
        raise ValueError("OSM source contains no usable addresses or named highways in the bbox")
    if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
        raise ValueError("SQLite integrity check failed")
    connection.execute("INSERT INTO places_fts(places_fts) VALUES ('integrity-check')")
    if connection.execute("SELECT rtreecheck('places_rtree')").fetchone() != ("ok",):
        raise ValueError("SQLite spatial index integrity check failed")
    for table in ("places", "places_fts", "places_rtree"):
        if connection.execute(f"SELECT count(*) FROM {table}").fetchone() != (expected,):
            raise ValueError(f"Incomplete {table}")
    missing = connection.execute("""
        SELECT count(*) FROM places p
        LEFT JOIN places_fts f ON f.rowid = p.id
        LEFT JOIN places_rtree r ON r.id = p.id
        WHERE f.rowid IS NULL OR r.id IS NULL
    """).fetchone()
    actual = dict(connection.execute("SELECT kind, count(*) FROM places GROUP BY kind"))
    if missing != (0,) or any(actual.get(kind, 0) != counts[kind] for kind in ("address", "street")):
        raise ValueError("Place/index counts do not agree")


def build(source: Path, output: Path, bbox: BBox = DEFAULT_BBOX) -> dict[str, str | int | float]:
    """Build in a unique sibling file, validate, then atomically replace ``output``.

    Reads PBF or OSM XML through pyosmium. Failures leave any previous artifact intact;
    only this invocation's temporary file is removed. The finished artifact is mode 0644.
    """
    started = time.monotonic()
    validate_bbox(bbox)
    source_bytes = source.stat().st_size  # also fails early for a missing source
    if not source.is_file() or not source_bytes:
        raise ValueError(f"OSM source must be a nonempty local file: {source}")
    if source.resolve() == output.resolve() or (output.exists() and source.samefile(output)):
        raise ValueError("OSM source and output must be different files")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".tmp", dir=output.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        with closing(sqlite3.connect(temporary)) as connection:
            # The disposable DB is never visible to readers; no journal/WAL sidecars to publish or clean.
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA cache_size=-65536")
            connection.execute("PRAGMA temp_store=FILE")
            connection.executescript(SCHEMA)
            handler = _Builder(connection, bbox)
            log.info("geocoding source=%s output=%s bbox=%s", source, output, bbox)
            handler.apply_file(str(source), locations=True, idx="flex_mem")
            handler.flush()
            connection.execute("INSERT INTO places_fts(places_fts) VALUES ('optimize')")
            _validate(connection, handler.counts)
            metadata = {
                "schema_version": "1",
                "status": "complete",
                "bbox": json.dumps(bbox),
                "source": str(source.resolve()),
                "source_bytes": str(source_bytes),
                "timestamp": datetime.now(UTC).isoformat(),
                "counts": json.dumps(dict(handler.counts), sort_keys=True),
                "osm_attribution": ATTRIBUTION,
                "normalization": "casefold, ł->l, NFKD, remove combining characters; preserve whitespace",
                "city_policy": "addr:city only; missing cities are blank",
                "build_elapsed_seconds": f"{time.monotonic() - started:.3f}",
            }
            connection.executemany("INSERT INTO metadata VALUES (?, ?)", metadata.items())
            connection.commit()
        temporary.chmod(0o644)
        with temporary.open("rb") as artifact:
            os.fsync(artifact.fileno())
        result: dict[str, str | int | float] = {
            "output": str(output),
            "records": handler.counts["address"] + handler.counts["street"],
            "addresses": handler.counts["address"],
            "streets": handler.counts["street"],
            "db_bytes": temporary.stat().st_size,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        os.replace(temporary, output)
        return result
    finally:
        temporary.unlink(missing_ok=True)
