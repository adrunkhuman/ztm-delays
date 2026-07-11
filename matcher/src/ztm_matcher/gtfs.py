"""Pinned GTFS ZIP loader and current/prior service-day selection."""

import csv
import hashlib
import io
import json
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ztm_matcher.errors import fail

WARSAW = ZoneInfo("Europe/Warsaw")
REQUIRED = {
    "trips.txt": {
        "trip_id",
        "route_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "block_id",
        "block_short_name",
        "shape_id",
    },
    "stop_times.txt": {
        "trip_id",
        "stop_id",
        "stop_sequence",
        "arrival_time",
        "departure_time",
        "pickup_type",
        "drop_off_type",
    },
    "stops.txt": {
        "stop_id",
        "stop_name",
        "stop_code",
        "stop_lat",
        "stop_lon",
        "zone_id",
        "stop_name_stem",
        "town_name",
    },
    "shapes.txt": {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"},
    "routes.txt": {"route_id", "route_short_name", "route_type"},
    "calendar_dates.txt": {"service_id", "date", "exception_type"},
}
MAX_GTFS_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
MAX_GTFS_ROWS = 750_000
SNAPSHOT_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z_([0-9a-f]{12})$")


@dataclass(frozen=True)
class Snapshot:
    """Validated snapshot rows with explicit lineage."""

    snapshot_id: str
    sha256: str
    trips: list[dict[str, Any]]
    stop_times: list[dict[str, Any]]
    stops: dict[str, dict[str, Any]]
    routes: dict[str, dict[str, Any]]
    active: set[tuple[str, date]]


def _string(value: str | None) -> str:
    return (value or "").strip()


def _integer(value: str | None, label: str) -> int:
    try:
        return int(_string(value))
    except ValueError as exc:
        raise fail("invalid_data", f"invalid integer {label}={value!r}", 12) from exc


def _seconds(value: str | None, label: str) -> int:
    parts = _string(value).split(":")
    if len(parts) != 3:
        raise fail("invalid_data", f"invalid GTFS time {label}={value!r}", 12)
    try:
        hours, minutes, seconds = map(int, parts)
    except ValueError as exc:
        raise fail("invalid_data", f"invalid GTFS time {label}={value!r}", 12) from exc
    if hours < 0 or not 0 <= minutes < 60 or not 0 <= seconds < 60:
        raise fail("invalid_data", f"invalid GTFS time {label}={value!r}", 12)
    return hours * 3600 + minutes * 60 + seconds


def _service_date(value: str | None) -> date:
    raw = _string(value)
    try:
        return datetime.strptime(raw, "%Y%m%d").date() if len(raw) == 8 and raw.isdigit() else date.fromisoformat(raw)
    except ValueError as exc:
        raise fail("invalid_data", f"invalid calendar date {raw!r}", 12) from exc


def _read_member(archive: zipfile.ZipFile, name: str, remaining_rows: int) -> list[dict[str, str]]:
    try:
        with archive.open(name) as member:
            reader = csv.DictReader(io.TextIOWrapper(member, encoding="utf-8-sig", newline=""))
            missing = REQUIRED[name] - set(reader.fieldnames or [])
            if missing:
                raise fail("schema_drift", f"{name} missing columns: {', '.join(sorted(missing))}", 11)
            rows = []
            for row in reader:
                if len(rows) >= remaining_rows:
                    raise fail("resource_limit", f"GTFS input exceeds {MAX_GTFS_ROWS:,} rows", 14)
                rows.append(dict(row))
            return rows
    except KeyError as exc:
        raise fail("missing_input", f"GTFS ZIP missing required member: {name}", 10) from exc


def load(path: Path, snapshot_id: str) -> Snapshot:
    """Load exactly six UTF-8 GTFS tables and verify hash-bearing snapshot IDs."""
    if not path.is_file():
        raise fail("missing_input", f"GTFS ZIP does not exist: {path}", 10)
    digest_builder = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest_builder.update(chunk)
    digest = digest_builder.hexdigest()
    match = SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id)
    if snapshot_id != "synthetic" and match is None:
        raise fail("snapshot_mismatch", "snapshot ID is not in the canonical timestamp_hash format", 13)
    if match and not digest.startswith(match.group(1)):
        raise fail("snapshot_mismatch", "GTFS ZIP hash does not match snapshot ID", 13)
    try:
        with zipfile.ZipFile(path) as archive:
            members = {info.filename: info for info in archive.infolist()}
            total_size = sum(members[name].file_size for name in REQUIRED if name in members)
            if total_size > MAX_GTFS_UNCOMPRESSED_BYTES:
                raise fail("resource_limit", "GTFS ZIP exceeds the uncompressed input limit", 14)
            raw: dict[str, list[dict[str, str]]] = {}
            row_count = 0
            for name in REQUIRED:
                rows = _read_member(archive, name, MAX_GTFS_ROWS - row_count)
                row_count += len(rows)
                raw[name] = rows
    except zipfile.BadZipFile as exc:
        raise fail("invalid_data", f"corrupt GTFS ZIP: {path}", 12) from exc
    routes = {
        _string(row["route_id"]): {
            "route_short_name": _string(row["route_short_name"]),
            "route_type": _integer(row["route_type"], "route_type"),
        }
        for row in raw["routes.txt"]
    }
    for row in raw["shapes.txt"]:
        _integer(row["shape_pt_sequence"], "shape_pt_sequence")
        try:
            float(_string(row["shape_pt_lat"]))
            float(_string(row["shape_pt_lon"]))
        except ValueError as exc:
            raise fail("invalid_data", "invalid shapes.txt coordinates", 12) from exc
    stops: dict[str, dict[str, Any]] = {}
    for row in raw["stops.txt"]:
        try:
            lat, lon = float(_string(row["stop_lat"])), float(_string(row["stop_lon"]))
        except ValueError as exc:
            raise fail("invalid_data", "invalid stops.txt coordinates", 12) from exc
        if 51.0 <= lat <= 53.5 and 19.5 <= lon <= 22.5:
            stops[_string(row["stop_id"])] = {"stop_name": _string(row["stop_name"]), "stop_lat": lat, "stop_lon": lon}
    stop_times = []
    for row in raw["stop_times.txt"]:
        pickup, dropoff = (
            _integer(row.get("pickup_type") or "0", "pickup_type"),
            _integer(row.get("drop_off_type") or "0", "drop_off_type"),
        )
        klass = (
            "not_in_passenger_service"
            if pickup == dropoff == 1
            else "request"
            if pickup in {2, 3} or dropoff in {2, 3}
            else "regular"
        )
        stop_times.append(
            {
                "trip_id": _string(row["trip_id"]),
                "stop_id": _string(row["stop_id"]),
                "stop_sequence": _integer(row["stop_sequence"], "stop_sequence"),
                "arrival_time_seconds": _seconds(row["arrival_time"], "arrival_time"),
                "departure_time_seconds": _seconds(row["departure_time"], "departure_time"),
                "pickup_type": pickup,
                "drop_off_type": dropoff,
                "stop_service_class": klass,
            }
        )
    trips = []
    for row in raw["trips.txt"]:
        block = _string(row["block_id"]) or None
        short = _string(row["block_short_name"]) or None
        trips.append(
            {
                "trip_id": _string(row["trip_id"]),
                "line": _string(row["route_id"]),
                "service_id": _string(row["service_id"]),
                "trip_headsign": _string(row["trip_headsign"]),
                "direction_id": _integer(row["direction_id"], "direction_id"),
                "block_id": block,
                "block_short_name": short,
                "brigade": (short or "0").lstrip("0") or "0",
                "shape_id": _string(row["shape_id"]),
            }
        )
    active: set[tuple[str, date]] = set()
    for row in raw["calendar_dates.txt"]:
        if _integer(row["exception_type"], "exception_type") == 1:
            active.add((_string(row["service_id"]), _service_date(row["date"])))
    if not trips or not stop_times or not active:
        raise fail("invalid_data", "GTFS snapshot has no trips, stop times, or active dates", 12)
    return Snapshot(snapshot_id, digest, trips, stop_times, stops, routes, active)


def select(snapshot: Snapshot, processing_date: date) -> list[dict[str, Any]]:
    """Retain prior/current active trips whose UTC interval overlaps the Warsaw day."""
    dates = (processing_date - timedelta(days=1), processing_date)
    for service_date in dates:
        if not any(active_date == service_date for _, active_date in snapshot.active):
            raise fail("snapshot_mismatch", f"no active GTFS service on required date {service_date}", 13)
    by_trip: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in snapshot.stop_times:
        by_trip[row["trip_id"]].append(row)
    day_start = datetime.combine(processing_date, time.min, WARSAW).astimezone(UTC)
    day_end = datetime.combine(processing_date + timedelta(days=1), time.min, WARSAW).astimezone(UTC)
    selected: list[dict[str, Any]] = []
    for trip in snapshot.trips:
        times = by_trip.get(trip["trip_id"], [])
        if not times:
            continue
        start = min(min(row["arrival_time_seconds"], row["departure_time_seconds"]) for row in times)
        end = max(max(row["arrival_time_seconds"], row["departure_time_seconds"]) for row in times)
        for service_date in dates:
            if (trip["service_id"], service_date) not in snapshot.active:
                continue
            midnight = datetime.combine(service_date, time.min, WARSAW).astimezone(UTC)
            if midnight + timedelta(seconds=end) >= day_start and midnight + timedelta(seconds=start) < day_end:
                route = snapshot.routes.get(trip["line"], {})
                selected.append(
                    {
                        **trip,
                        "service_date": service_date,
                        "processing_date": processing_date,
                        "gtfs_snapshot_id": snapshot.snapshot_id,
                        "route_short_name": route.get("route_short_name", ""),
                        "mode": {0: "tram", 3: "bus"}.get(route.get("route_type"), "unknown"),
                        "trip_start_seconds": start,
                        "trip_end_seconds": end,
                        "stop_count": len(times),
                        "scheduled_start_time": midnight + timedelta(seconds=start),
                        "scheduled_end_time": midnight + timedelta(seconds=end),
                    }
                )
    return sorted(
        selected,
        key=lambda row: (row["service_date"], row["trip_start_seconds"], row["trip_end_seconds"], row["trip_id"]),
    )


def chain_id(snapshot_id: str, service_date: date, source: str, source_id: str) -> str:
    """Create stable duty identity from dbt's grouping keys."""
    value = json.dumps(
        {
            "gtfs_snapshot_id": snapshot_id,
            "service_date": str(service_date),
            "duty_chain_source": source,
            "duty_chain_source_id": source_id,
        },
        separators=(",", ":"),
    )
    return hashlib.md5(value.encode(), usedforsecurity=False).hexdigest()
