"""Pinned GTFS ZIP loader and current/prior service-day selection."""

import csv
import hashlib
import io
import json
import re
import zipfile
from collections import defaultdict
from collections.abc import Callable
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
        "stop_name_stem",
        "town_name",
    },
    "shapes.txt": {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"},
    "routes.txt": {"route_id", "route_short_name", "route_type"},
    "calendar_dates.txt": {"service_id", "date", "exception_type"},
}
MAX_GTFS_UNCOMPRESSED_BYTES = 640 * 1024 * 1024
MAX_GTFS_ROWS = 1_800_000
SNAPSHOT_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z_([0-9a-f]{12})$")


@dataclass(frozen=True, slots=True)
class StopTime:
    """Normalized fields needed from one selected stop_times.txt row."""

    stop_id: str
    stop_sequence: int
    arrival_time_seconds: int
    departure_time_seconds: int
    pickup_type: int
    drop_off_type: int
    stop_service_class: str


@dataclass(frozen=True, slots=True)
class StopInfo:
    """Compact validated stop metadata, including ranking-zone lineage."""

    stop_name: str
    stop_lat: float
    stop_lon: float
    zone_id: str | None
    effective_zone_id: str | None


@dataclass(frozen=True, slots=True)
class Trip:
    """Normalized fields needed from one selected trips.txt row."""

    trip_id: str
    line: str
    service_id: str
    trip_headsign: str
    direction_id: int
    block_id: str | None
    block_short_name: str | None
    brigade: str
    shape_id: str


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Validated snapshot rows with explicit lineage."""

    snapshot_id: str
    sha256: str
    trips: list[Trip]
    stop_times: dict[str, list[StopTime]]
    stops: dict[str, StopInfo]
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


def _read_member(
    archive: zipfile.ZipFile,
    name: str,
    remaining_rows: int,
    predicate: Callable[[dict[str, str]], bool] | None = None,
) -> list[dict[str, str]]:
    try:
        with archive.open(name) as member:
            reader = csv.DictReader(io.TextIOWrapper(member, encoding="utf-8-sig", newline=""))
            missing = REQUIRED[name] - set(reader.fieldnames or [])
            if missing:
                raise fail("schema_drift", f"{name} missing columns: {', '.join(sorted(missing))}", 11)
            rows = []
            for row in reader:
                value = dict(row)
                if predicate is not None and not predicate(value):
                    continue
                if len(rows) >= remaining_rows:
                    raise fail("resource_limit", f"GTFS input exceeds {MAX_GTFS_ROWS:,} rows", 14)
                rows.append(value)
            return rows
    except KeyError as exc:
        raise fail("missing_input", f"GTFS ZIP missing required member: {name}", 10) from exc


def _read_trips(archive: zipfile.ZipFile, active_service_ids: set[str]) -> list[Trip]:
    """Stream active trips into compact records rather than retaining CSV dictionaries."""
    try:
        with archive.open("trips.txt") as member:
            reader = csv.DictReader(io.TextIOWrapper(member, encoding="utf-8-sig", newline=""))
            missing = REQUIRED["trips.txt"] - set(reader.fieldnames or [])
            if missing:
                raise fail("schema_drift", f"trips.txt missing columns: {', '.join(sorted(missing))}", 11)
            trips = []
            input_rows = 0
            for row in reader:
                if input_rows >= MAX_GTFS_ROWS:
                    raise fail("resource_limit", f"GTFS input exceeds {MAX_GTFS_ROWS:,} rows", 14)
                input_rows += 1
                service_id = _string(row["service_id"])
                if service_id not in active_service_ids:
                    continue
                block_id = _string(row["block_id"]) or None
                block_short_name = _string(row["block_short_name"]) or None
                trips.append(
                    Trip(
                        trip_id=_string(row["trip_id"]),
                        line=_string(row["route_id"]),
                        service_id=service_id,
                        trip_headsign=_string(row["trip_headsign"]),
                        direction_id=_integer(row["direction_id"], "direction_id"),
                        block_id=block_id,
                        block_short_name=block_short_name,
                        brigade=(block_short_name or "0").lstrip("0") or "0",
                        shape_id=_string(row["shape_id"]),
                    )
                )
            return trips
    except KeyError as exc:
        raise fail("missing_input", "GTFS ZIP missing required member: trips.txt", 10) from exc


def _read_stop_times(archive: zipfile.ZipFile, selected_trip_ids: set[str]) -> dict[str, list[StopTime]]:
    """Stream selected stop times into compact, trip-indexed records."""
    try:
        with archive.open("stop_times.txt") as member:
            reader = csv.DictReader(io.TextIOWrapper(member, encoding="utf-8-sig", newline=""))
            missing = REQUIRED["stop_times.txt"] - set(reader.fieldnames or [])
            if missing:
                raise fail("schema_drift", f"stop_times.txt missing columns: {', '.join(sorted(missing))}", 11)
            stop_times: dict[str, list[StopTime]] = defaultdict(list)
            selected_rows = 0
            for row in reader:
                trip_id = _string(row["trip_id"])
                if trip_id not in selected_trip_ids:
                    continue
                if selected_rows >= MAX_GTFS_ROWS:
                    raise fail("resource_limit", f"selected GTFS schedule exceeds {MAX_GTFS_ROWS:,} stop rows", 14)
                pickup_type = _integer(row["pickup_type"] or "0", "pickup_type")
                drop_off_type = _integer(row["drop_off_type"] or "0", "drop_off_type")
                stop_times[trip_id].append(
                    StopTime(
                        stop_id=_string(row["stop_id"]),
                        stop_sequence=_integer(row["stop_sequence"], "stop_sequence"),
                        arrival_time_seconds=_seconds(row["arrival_time"], "arrival_time"),
                        departure_time_seconds=_seconds(row["departure_time"], "departure_time"),
                        pickup_type=pickup_type,
                        drop_off_type=drop_off_type,
                        stop_service_class=(
                            "not_in_passenger_service"
                            if pickup_type == drop_off_type == 1
                            else "request"
                            if pickup_type in {2, 3} or drop_off_type in {2, 3}
                            else "regular"
                        ),
                    )
                )
                selected_rows += 1
            return dict(stop_times)
    except KeyError as exc:
        raise fail("missing_input", "GTFS ZIP missing required member: stop_times.txt", 10) from exc


def load(path: Path, snapshot_id: str, processing_date: date | None = None) -> Snapshot:
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
            calendar_rows = _read_member(archive, "calendar_dates.txt", MAX_GTFS_ROWS)
            active: set[tuple[str, date]] = set()
            for row in calendar_rows:
                if _integer(row["exception_type"], "exception_type") == 1:
                    active.add((_string(row["service_id"]), _service_date(row["date"])))
            required_dates = (
                {processing_date - timedelta(days=1), processing_date} if processing_date is not None else None
            )
            active_service_ids = {
                service_id
                for service_id, service_date in active
                if required_dates is None or service_date in required_dates
            }
            trips = _read_trips(archive, active_service_ids)
            selected_trip_ids = {trip.trip_id for trip in trips}
            stop_times = _read_stop_times(archive, selected_trip_ids)
            stop_rows = _read_member(archive, "stops.txt", MAX_GTFS_ROWS)
            route_rows = _read_member(archive, "routes.txt", MAX_GTFS_ROWS)
            # Shapes are not needed for runtime preparation, but their header remains an input gate.
            _read_member(archive, "shapes.txt", 0, lambda _row: False)
    except zipfile.BadZipFile as exc:
        raise fail("invalid_data", f"corrupt GTFS ZIP: {path}", 12) from exc
    routes = {
        _string(row["route_id"]): {
            "route_short_name": _string(row["route_short_name"]),
            "route_type": _integer(row["route_type"], "route_type"),
        }
        for row in route_rows
    }
    stops: dict[str, StopInfo] = {}
    for row in stop_rows:
        try:
            lat, lon = float(_string(row["stop_lat"])), float(_string(row["stop_lon"]))
        except ValueError as exc:
            raise fail("invalid_data", "invalid stops.txt coordinates", 12) from exc
        if 51.0 <= lat <= 53.5 and 19.5 <= lon <= 22.5:
            zone_id = _string(row.get("zone_id")) or None
            stops[_string(row["stop_id"])] = StopInfo(
                stop_name=_string(row["stop_name"]),
                stop_lat=lat,
                stop_lon=lon,
                zone_id=zone_id,
                effective_zone_id="1" if zone_id == "1+2" else zone_id,
            )
    if not trips or not stop_times or not active:
        raise fail("invalid_data", "GTFS snapshot has no trips, stop times, or active dates", 12)
    return Snapshot(snapshot_id, digest, trips, stop_times, stops, routes, active)


def select(snapshot: Snapshot, processing_date: date) -> list[dict[str, Any]]:
    """Retain prior/current active trips whose UTC interval overlaps the Warsaw day."""
    dates = (processing_date - timedelta(days=1), processing_date)
    for service_date in dates:
        if not any(active_date == service_date for _, active_date in snapshot.active):
            raise fail("snapshot_mismatch", f"no active GTFS service on required date {service_date}", 13)
    day_start = datetime.combine(processing_date, time.min, WARSAW).astimezone(UTC)
    day_end = datetime.combine(processing_date + timedelta(days=1), time.min, WARSAW).astimezone(UTC)
    selected: list[dict[str, Any]] = []
    for trip in snapshot.trips:
        times = snapshot.stop_times.get(trip.trip_id, [])
        if not times:
            continue
        start = min(min(row.arrival_time_seconds, row.departure_time_seconds) for row in times)
        end = max(max(row.arrival_time_seconds, row.departure_time_seconds) for row in times)
        for service_date in dates:
            if (trip.service_id, service_date) not in snapshot.active:
                continue
            midnight = datetime.combine(service_date, time.min, WARSAW).astimezone(UTC)
            if midnight + timedelta(seconds=end) >= day_start and midnight + timedelta(seconds=start) < day_end:
                route = snapshot.routes.get(trip.line, {})
                selected.append(
                    {
                        "trip_id": trip.trip_id,
                        "line": trip.line,
                        "service_id": trip.service_id,
                        "trip_headsign": trip.trip_headsign,
                        "direction_id": trip.direction_id,
                        "block_id": trip.block_id,
                        "block_short_name": trip.block_short_name,
                        "brigade": trip.brigade,
                        "shape_id": trip.shape_id,
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
