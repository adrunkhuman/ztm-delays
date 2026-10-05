"""Bounded, durable attempt counters; independent of the GPS buffer spool."""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypedDict, cast

from google.api_core.exceptions import GoogleAPIError

if TYPE_CHECKING:
    from pathlib import Path

    from google.cloud import storage

    from poller import PollResult

LOGGER = logging.getLogger(__name__)
CHECKPOINT_NAME = "poller-health-v1.json"
DEFAULT_PREFIX = "health/poller/hourly"
DEFAULT_MAX_BYTES = 8 * 1024 * 1024
MINUTES_PER_HOUR = 60
ATTEMPT_RESERVE_BYTES = 1024


class MinuteCounters(TypedDict):
    """Exact v1 wire counters for an observed minute."""

    minute: int
    attempts: int
    successes: int
    parsed_rows: int
    accepted_rows: int
    dropped_stale_rows: int
    dropped_future_rows: int
    accepted_vehicle_count_sum: int
    accepted_line_count_sum: int


class ModeCounters(TypedDict):
    """Observed minutes only; missing minutes are not zero polls."""

    minutes: list[MinuteCounters]


class HourCounters(TypedDict):
    """Exact v1 hourly cumulative snapshot."""

    version: int
    hour_start: str
    collection_started_at: str
    poll_interval_seconds: float
    vehicle_types: dict[str, ModeCounters]


def utc_timestamp(value: datetime) -> str:
    """Format an aware timestamp in UTC, regardless of the input timezone."""
    if value.utcoffset() is None:
        raise ValueError("health timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def utc_hour(value: datetime) -> str:
    """Key counters by attempt time, never GPS event time or Warsaw-local hour."""
    utc_timestamp(value)  # Reject naive inputs before astimezone can infer the machine timezone.
    return utc_timestamp(value.astimezone(UTC).replace(minute=0, second=0, microsecond=0))


def _encoded(payload: object, max_bytes: int) -> bytes:
    # Stop encoding at the cap instead of allocating an unbounded discarded JSON string.
    data = bytearray()
    for chunk in json.JSONEncoder(sort_keys=True, separators=(",", ":")).iterencode(payload):
        data.extend(chunk.encode())
        if len(data) > max_bytes:
            raise RuntimeError(
                "health checkpoint exceeds POLLER_HEALTH_MAX_BYTES; diagnostics retained, polling stopped"
            )
    return bytes(data)


class HealthCounters:
    """Checkpoint every attempt and retry cumulative hour objects without double-counting."""

    def __init__(self, spool_dir: Path, poll_interval_seconds: float, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        """Restore pending hours without touching buffers.json or contacting GCS."""
        self.path = spool_dir / CHECKPOINT_NAME
        self.poll_interval_seconds = poll_interval_seconds
        self.max_bytes = max_bytes
        self.collection_started_at: str | None = None
        self.hours: dict[str, HourCounters] = {}
        self._restore()

    def check_capacity(self) -> None:
        """Stop before calling the API if another hour/minute could exceed the cap.

        One KiB covers hour metadata and nine counters whose inputs are bounded by
        Python list lengths. Pending hours are never evicted to make room.
        """
        _encoded(self._payload(), self.max_bytes - ATTEMPT_RESERVE_BYTES)

    def _payload(self) -> dict[str, object]:
        return {"version": 1, "collection_started_at": self.collection_started_at, "hours": self.hours}

    def record(self, result: PollResult) -> None:
        """Count an attempt, then atomically persist it before the next API call."""
        timestamp = utc_timestamp(result.attempted_at)
        hour = utc_hour(result.attempted_at)
        if self.collection_started_at is None:
            self.collection_started_at = timestamp
        if hour not in self.hours:
            self.hours[hour] = {
                "version": 1,
                "hour_start": hour,
                "collection_started_at": self.collection_started_at,
                "poll_interval_seconds": self.poll_interval_seconds,
                "vehicle_types": {name: {"minutes": []} for name in ("bus", "tram")},
            }
        minutes = self.hours[hour]["vehicle_types"][result.vehicle_type_name]["minutes"]
        minute_number = result.attempted_at.astimezone(UTC).minute
        minute = next((item for item in minutes if item["minute"] == minute_number), None)
        if minute is None:
            minute = MinuteCounters(
                minute=minute_number,
                attempts=0,
                successes=0,
                parsed_rows=0,
                accepted_rows=0,
                dropped_stale_rows=0,
                dropped_future_rows=0,
                accepted_vehicle_count_sum=0,
                accepted_line_count_sum=0,
            )
            minutes.append(minute)
            minutes.sort(key=lambda item: item["minute"])
        minute["attempts"] += 1
        if result.succeeded:
            minute["successes"] += 1
            minute["parsed_rows"] += result.parsed_rows
            minute["accepted_rows"] += result.accepted_rows
            minute["dropped_stale_rows"] += result.dropped_stale
            minute["dropped_future_rows"] += result.dropped_future
            minute["accepted_vehicle_count_sum"] += result.accepted_vehicle_count
            minute["accepted_line_count_sum"] += result.accepted_line_count
        # On a local write/cap failure leave the last attempt in memory for shutdown upload.
        # Propagate the error: continuing would silently lose diagnostics or grow indefinitely.
        try:
            self.save()
        except (OSError, RuntimeError):
            LOGGER.critical(
                "cannot checkpoint health attempt mode=%s at=%s counters=%s; stopping",
                result.vehicle_type_name,
                timestamp,
                minute,
            )
            raise

    def save(self) -> None:
        """Atomically replace the independent versioned checkpoint, including pending uploads."""
        data = _encoded(self._payload(), self.max_bytes)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        try:
            with temporary.open("wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.path)
            descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    def upload(self, bucket: storage.Bucket, prefix: str, now: datetime, *, include_partial: bool = False) -> bool:
        """Retry closed hours; retain the current cumulative hour even after partial upload."""
        current_hour = utc_hour(now)
        succeeded = True
        for hour, snapshot in sorted(self.hours.items()):
            if hour >= current_hour and not include_partial:
                continue
            start = datetime.fromisoformat(hour)
            key = f"{prefix}/{start:%Y-%m-%d}/{start:%H}.json"
            try:
                # Unconditional replacement is intentional: these are cumulative snapshots,
                # not parts. A retry after an ambiguous success sends identical counts.
                bucket.blob(key).upload_from_string(_encoded(snapshot, self.max_bytes), content_type="application/json")
            except (GoogleAPIError, OSError):
                LOGGER.exception("failed to upload poller health hour=%s key=%s; retained for retry", hour, key)
                succeeded = False
                continue
            if hour < current_hour:
                del self.hours[hour]
                try:
                    self.save()
                except (OSError, RuntimeError):
                    self.hours[hour] = snapshot
                    raise
            LOGGER.info("uploaded poller health hour=%s key=%s", hour, key)
        return succeeded

    def _restore(self) -> None:
        if not self.path.exists():
            return
        try:
            self.collection_started_at, self.hours = self._read_checkpoint()
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise RuntimeError(
                f"invalid health checkpoint path={self.path}: {exc}; refusing to discard diagnostics"
            ) from exc
        LOGGER.info("restored poller health path=%s pending_hours=%d", self.path, len(self.hours))

    def _read_checkpoint(self) -> tuple[str | None, dict[str, HourCounters]]:
        if self.path.stat().st_size > self.max_bytes:
            raise ValueError("checkpoint exceeds POLLER_HEALTH_MAX_BYTES")
        payload = json.loads(self.path.read_bytes())
        if not isinstance(payload, dict) or type(payload.get("version")) is not int or payload["version"] != 1:
            raise ValueError("unsupported checkpoint version")
        started = payload["collection_started_at"]
        if started is not None and (
            not isinstance(started, str) or utc_timestamp(datetime.fromisoformat(started)) != started
        ):
            raise ValueError("invalid collection start")
        hours = payload["hours"]
        if not isinstance(hours, dict) or (hours and started is None):
            raise TypeError("hours must be an object with a collection start")
        for hour, snapshot in hours.items():
            self._validate_hour(hour, snapshot, started)
        return started, cast("dict[str, HourCounters]", hours)

    @staticmethod
    def _validate_hour(hour: str, payload: object, started: str | None) -> None:
        if utc_hour(datetime.fromisoformat(hour)) != hour or not isinstance(payload, dict):
            raise ValueError("invalid UTC hour")
        if set(payload) != set(HourCounters.__annotations__):
            raise ValueError("invalid hour fields")
        snapshot = cast("HourCounters", payload)
        if (
            type(snapshot["version"]) is not int
            or snapshot["version"] != 1
            or snapshot["hour_start"] != hour
            or snapshot["collection_started_at"] != started
        ):
            raise ValueError("invalid hour metadata")
        interval = snapshot["poll_interval_seconds"]
        if not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval <= 0:
            raise ValueError("invalid poll interval")
        modes = snapshot["vehicle_types"]
        if not isinstance(modes, dict) or set(modes) != {"bus", "tram"}:
            raise ValueError("invalid modes")
        for mode in modes.values():
            HealthCounters._validate_minutes(mode)

    @staticmethod
    def _validate_minutes(payload: object) -> None:
        if not isinstance(payload, dict) or set(payload) != {"minutes"}:
            raise ValueError("invalid mode fields")
        mode = cast("ModeCounters", payload)
        if not isinstance(mode["minutes"], list):
            raise TypeError("minutes must be an array")
        seen = set()
        for item in mode["minutes"]:
            if not isinstance(item, dict) or set(item) != set(MinuteCounters.__annotations__):
                raise ValueError("invalid minute fields")
            minute = cast("MinuteCounters", item)
            if any(type(value) is not int or value < 0 for value in minute.values()):
                raise ValueError("invalid counters")
            number = minute["minute"]
            if number >= MINUTES_PER_HOUR or number in seen:
                raise ValueError("invalid or duplicate minute")
            seen.add(number)
            if (
                minute["attempts"] == 0
                or minute["successes"] > minute["attempts"]
                or minute["parsed_rows"]
                != (minute["accepted_rows"] + minute["dropped_stale_rows"] + minute["dropped_future_rows"])
            ):
                raise ValueError("invalid counter invariants")
            if (minute["successes"] == 0 and minute["parsed_rows"] != 0) or max(
                minute["accepted_vehicle_count_sum"], minute["accepted_line_count_sum"]
            ) > minute["accepted_rows"]:
                raise ValueError("invalid accepted counts")
