"""Bounded, read-only lookups against the offline OSM SQLite artifact.

Each call owns a read transaction and connection: an atomic artifact replacement
is visible on the next call, while metadata and candidates share one snapshot.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import unicodedata
from collections import Counter
from contextlib import contextmanager
from itertools import pairwise
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

MAX_QUERY_LENGTH = 160
MAX_RESULTS = 5
SEARCH_CANDIDATES = 250
REVERSE_CANDIDATES = 1000
MAX_GEOMETRY_BYTES = 65_536
MAX_GEOMETRY_POINTS = 4096
MAX_LABEL_LENGTH = 512
NUMBERED_DISTANCE_M = 80
STREET_DISTANCE_M = 120
CANDIDATE_RADIUS_M = 150
EARTH_RADIUS_M = 6_371_000
METERS_PER_DEGREE = 111_320
CENTER = (52.2297, 21.0122)
_TOKEN = re.compile(r"[^\W_]+")
_NUMBER = re.compile(r"(?<!\w)\d+[a-z]?(?:[/\-]\d+[a-z]?)*(?!\w)")
_MONTHS = frozenset(
    (
        "stycznia",
        "lutego",
        "marca",
        "kwietnia",
        "maja",
        "czerwca",
        "lipca",
        "sierpnia",
        "wrzesnia",
        "pazdziernika",
        "listopada",
        "grudnia",
    )
)


class GeocodingError(Exception):
    """The configured SQLite address artifact is unavailable or invalid."""


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text.casefold().replace("ł", "l"))
    return "".join(char for char in text if not unicodedata.combining(char))


def _coordinates(lat: object, lon: object) -> tuple[float, float] | None:
    if (
        isinstance(lat, bool)
        or isinstance(lon, bool)
        or not isinstance(lat, (int, float))
        or not isinstance(lon, (int, float))
    ):
        return None
    try:
        latitude, longitude = float(lat), float(lon)
    except OverflowError:
        return None
    if not (
        math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90 <= latitude <= 90  # noqa: PLR2004 - geographic limits
        and -180 <= longitude <= 180  # noqa: PLR2004
    ):
        return None
    return latitude, longitude


@contextmanager
def _database(path: Path) -> Iterator[tuple[sqlite3.Connection, tuple[float, float, float, float]]]:
    connection = None
    try:
        # Do not use immutable=1: normal SQLite locking protects a live read snapshot.
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA cache_size=-4096")
        connection.execute("BEGIN")
        bounds = _validate_database(connection)
        yield connection, bounds
    except (sqlite3.Error, OSError, ValueError) as error:
        raise GeocodingError("SQLite address lookup unavailable") from error
    finally:
        if connection is not None:
            connection.close()


def _validate_database(connection: sqlite3.Connection) -> tuple[float, float, float, float]:
    # Probe all required columns even when the requested lookup would use only one index.
    connection.execute("SELECT id, kind, street, house, city, lat, lon, osm_id, geometry FROM places LIMIT 0")
    connection.execute("SELECT rowid, text FROM places_fts LIMIT 0")
    connection.execute("SELECT id, min_lon, max_lon, min_lat, max_lat FROM places_rtree LIMIT 0")
    for name, module in (("places_fts", "fts5"), ("places_rtree", "rtree")):
        schema = connection.execute("SELECT sql FROM sqlite_master WHERE name = ?", (name,)).fetchone()
        if schema is None or not re.search(rf"\busing\s+{module}\s*\(", schema[0] or "", re.IGNORECASE):
            raise GeocodingError("invalid SQLite geocoding index")
    required_keys = ("schema_version", "status", "bbox", "source", "timestamp", "counts", "osm_attribution")
    metadata = dict(
        connection.execute("SELECT key, value FROM metadata WHERE key IN (?, ?, ?, ?, ?, ?, ?)", required_keys)
    )
    if any(not isinstance(metadata.get(key), str) or not metadata[key].strip() for key in required_keys):
        raise GeocodingError("missing SQLite geocoding metadata")
    if metadata["schema_version"] != "1":
        raise GeocodingError("unsupported SQLite geocoding schema version")
    if metadata["status"] != "complete":
        raise GeocodingError("incomplete SQLite geocoding artifact")
    try:
        bounds = json.loads(metadata["bbox"])
    except (TypeError, ValueError, RecursionError) as error:
        raise GeocodingError("invalid SQLite geocoding bounds") from error
    if not isinstance(bounds, list) or len(bounds) != 4:  # noqa: PLR2004 - bbox shape
        raise GeocodingError("invalid SQLite geocoding bounds")
    lower, upper = _coordinates(bounds[1], bounds[0]), _coordinates(bounds[3], bounds[2])
    if lower is None or upper is None or lower[0] >= upper[0] or lower[1] >= upper[1]:
        raise GeocodingError("invalid SQLite geocoding bounds")
    return lower[1], lower[0], upper[1], upper[0]


def _text(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > MAX_LABEL_LENGTH:
        return None
    return " ".join(value.split())


def _place(row: sqlite3.Row) -> dict[str, Any] | None:
    if not isinstance(row["id"], int) or row["id"] <= 0:
        return None
    street, house, city = (_text(row[key]) for key in ("street", "house", "city"))
    point = _coordinates(row["lat"], row["lon"])
    if not street or house is None or city is None or point is None or row["kind"] not in {"address", "street"}:
        return None
    if row["kind"] == "street" and house:
        return None
    address = f"{street} {house}".strip()
    return {"name": ", ".join(part for part in (address, city) if part), "lat": point[0], "lon": point[1]}


def _distance(lat: float, lon: float, other_lat: float, other_lon: float) -> float:
    a, b = math.radians(lat), math.radians(other_lat)
    haversine = (
        math.sin((b - a) / 2) ** 2 + math.cos(a) * math.cos(b) * math.sin(math.radians(other_lon - lon) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(min(1.0, max(0.0, haversine))))


def _matches(row: sqlite3.Row, tokens: list[str], numbers: list[tuple[str, bool]]) -> bool | None:
    """Return house intent, or None for a false FTS/composite-number match."""
    street, house, city = (_normalize(row[key]) for key in ("street", "house", "city"))
    words = set(_TOKEN.findall(f"{street} {house} {city}"))
    if any(token not in words for token in tokens[:-1]) or not any(word.startswith(tokens[-1]) for word in words):
        return None
    street_numbers = Counter(_NUMBER.findall(street))
    # OSM separates alternative house numbers with semicolons, not slashes/hyphens.
    house_numbers = {"".join(part.split()) for part in house.split(";")}
    house_intent = False
    for number, date_like in numbers:
        if street_numbers[number]:
            street_numbers[number] -= 1
            continue
        if date_like or number not in house_numbers or row["kind"] != "address":
            return None
        house_intent = True
    return house_intent


def _duplicates(first: sqlite3.Row, second: sqlite3.Row) -> bool:
    if any(
        _normalize(" ".join(first[key].split())) != _normalize(" ".join(second[key].split()))
        for key in ("street", "house")
    ):
        return False
    city, other_city = _normalize(first["city"].strip()), _normalize(second["city"].strip())
    if city and other_city and city != other_city:
        return False
    # Keep spatially distinct streets/addresses; collapse nearby node/building twins.
    return _distance(first["lat"], first["lon"], second["lat"], second["lon"]) <= NUMBERED_DISTANCE_M


def search(path: Path, query: str) -> list[dict[str, Any]]:
    """Return up to five prefix/token matches, biased towards central Warsaw.

    Numbers belonging to a street (including simple day/month names) are not
    house constraints. Other numbers must match the complete house number.
    """
    if not isinstance(query, str) or not 3 <= len(query) <= MAX_QUERY_LENGTH:  # noqa: PLR2004 - minimum query length
        return []
    if any(unicodedata.category(char).startswith("C") and not char.isspace() for char in query):
        return []
    normalized = _normalize(" ".join(query.split()))
    if len(normalized) < 3:  # noqa: PLR2004
        return []
    tokens = [token for token in _TOKEN.findall(normalized) if token not in {"ul", "ulica", "nr"}]
    if not tokens:
        return []
    numbers = []
    for match in _NUMBER.finditer(normalized):
        following = _TOKEN.findall(normalized[match.end() :])
        numbers.append((match.group(), bool(following and following[0] in _MONTHS)))
    expression = " AND ".join(
        f'"{token}"' + ("*" if index == len(tokens) - 1 else "") for index, token in enumerate(tokens)
    )
    with _database(path) as (connection, _):
        rows = connection.execute(
            """SELECT p.* FROM places_fts f JOIN places p ON p.id = f.rowid
               WHERE places_fts MATCH ? AND p.street != '' ORDER BY f.rank LIMIT ?""",
            (expression, SEARCH_CANDIDATES),
        ).fetchall()
    candidates = []
    for row in rows:
        place = _place(row)
        if place is None or (house_intent := _matches(row, tokens, numbers)) is None:
            continue
        rank = (
            row["kind"] != ("address" if house_intent else "street"),
            _distance(*CENTER, place["lat"], place["lon"]),
            row["id"],
        )
        candidates.append((rank, row, place))
    return _select_search_results(candidates)


def _select_search_results(
    candidates: list[tuple[tuple[bool, float, int], sqlite3.Row, dict[str, Any]]],
) -> list[dict[str, Any]]:
    selected: list[tuple[sqlite3.Row, dict[str, Any]]] = []
    for _, row, place in sorted(candidates, key=lambda item: item[0]):
        duplicate = next((index for index, (other, _) in enumerate(selected) if _duplicates(row, other)), None)
        if duplicate is not None:
            if row["city"].strip() and not selected[duplicate][0]["city"].strip():
                selected[duplicate] = row, place
        elif len(selected) < MAX_RESULTS:
            selected.append((row, place))
    return [place for _, place in selected]


def _road_distance(row: sqlite3.Row, lat: float, lon: float) -> float | None:
    geometry = row["geometry"]
    if not isinstance(geometry, str) or len(geometry) > MAX_GEOMETRY_BYTES:
        return None
    try:
        coordinates = json.loads(geometry)
    except (ValueError, RecursionError):
        return None
    if not isinstance(coordinates, list) or not 2 <= len(coordinates) <= MAX_GEOMETRY_POINTS:  # noqa: PLR2004
        return None
    points = []
    for coordinate in coordinates:
        if not isinstance(coordinate, list) or len(coordinate) != 2:  # noqa: PLR2004 - lon/lat pair
            return None
        point = _coordinates(coordinate[1], coordinate[0])
        if point is None:
            return None
        points.append(point)
    scale_x = METERS_PER_DEGREE * math.cos(math.radians(lat))
    closest, best = None, math.inf
    for (lat1, lon1), (lat2, lon2) in pairwise(points):
        ax, ay = (lon1 - lon) * scale_x, (lat1 - lat) * METERS_PER_DEGREE
        dx, dy = (lon2 - lon1) * scale_x, (lat2 - lat1) * METERS_PER_DEGREE
        length = dx * dx + dy * dy
        fraction = max(0.0, min(1.0, -(ax * dx + ay * dy) / length)) if length else 0.0
        squared_distance = (ax + fraction * dx) ** 2 + (ay + fraction * dy) ** 2
        if squared_distance < best:
            closest, best = (lat1 + fraction * (lat2 - lat1), lon1 + fraction * (lon2 - lon1)), squared_distance
    return _distance(lat, lon, *closest) if closest is not None else None


def _candidate_distance(row: sqlite3.Row, lat: float, lon: float) -> float | None:
    if row["kind"] == "address" and row["house"].strip():
        distance = _distance(lat, lon, row["lat"], row["lon"])
        threshold = NUMBERED_DISTANCE_M
    elif row["kind"] == "street":
        distance = _road_distance(row, lat, lon)
        threshold = STREET_DISTANCE_M
    else:
        return None
    return distance if distance is not None and distance <= threshold else None


def reverse(path: Path, lat: float, lon: float) -> dict[str, Any] | None:
    """Label a click with an address within 80 m, otherwise a road within 120 m.

    Returned coordinates are candidate metadata, not replacements for the click.
    Road distance uses the closest segment projection, not the label midpoint.
    """
    point = _coordinates(lat, lon)
    if point is None:
        return None
    lat, lon = point
    with _database(path) as (connection, bounds):
        if not (bounds[0] <= lon <= bounds[2] and bounds[1] <= lat <= bounds[3]):
            return None
        dy = CANDIDATE_RADIUS_M / METERS_PER_DEGREE
        dx = dy / max(0.01, math.cos(math.radians(lat)))
        rows = connection.execute(
            """SELECT p.* FROM places_rtree r JOIN places p ON p.id = r.id
               WHERE r.max_lon >= ? AND r.min_lon <= ? AND r.max_lat >= ? AND r.min_lat <= ?
               LIMIT ?""",
            (lon - dx, lon + dx, lat - dy, lat + dy, REVERSE_CANDIDATES + 1),
        ).fetchall()
    # Fail closed in unexpectedly dense/corrupt data instead of selecting an arbitrary subset.
    if len(rows) > REVERSE_CANDIDATES:
        return None
    candidates = []
    for row in rows:
        place = _place(row)
        if place is None:
            continue
        distance = _candidate_distance(row, lat, lon)
        if distance is not None:
            candidates.append(
                (row["kind"] != "address", distance, not bool(row["city"].strip()), row["id"], row, place)
            )
    if not candidates:
        return None
    best = min(candidates, key=lambda candidate: candidate[:4])
    # Missing city on a near-identical duplicate is not evidence of a different town.
    if not best[4]["city"].strip():
        labeled = [
            candidate for candidate in candidates if candidate[4]["city"].strip() and _duplicates(best[4], candidate[4])
        ]
        if labeled:
            label = min(labeled, key=lambda candidate: candidate[:4])
            return {**label[5], "distance_m": label[1]}
    return {**best[5], "distance_m": best[1]}
