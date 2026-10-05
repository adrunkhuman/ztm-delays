"""Allowlisted hourly feed-health snapshots for serving metadata, not live monitoring."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

from poller_health import MAX_COUNTER, MODES, iso, timestamp

STALE_AFTER_SECONDS = 2 * 60 * 60
MAX_INTERVALS = 48
STATUSES = {"healthy", "degraded", "warming_up", "partial", "monitoring_gap", "not_monitored"}
REASONS = {
    "stale_heavy",
    "api_failures",
    "no_accepted",
    "low_fleet",
    "summary_absent",
    "invalid_summary",
    "insufficient_minute_coverage",
    "collection_started_mid_hour",
    "collection_not_confirmed",
    "before_collection",
    "no_positive_fleet_baseline",
    "recovery_unconfirmed",
    "monitoring_evaluation_gap",
    "baseline_reset",
}
COUNTS = ("parsed_rows", "accepted_rows", "dropped_stale_rows", "dropped_future_rows")


def _count(value: object, maximum: int = MAX_COUNTER) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError("invalid feed-health count")
    return value


def _interval(value: object, end: datetime) -> dict[str, str]:
    if not isinstance(value, dict) or value.get("reason") not in REASONS:
        raise ValueError("invalid feed-health interval")
    start, stop = timestamp(value.get("start_at")), timestamp(value.get("end_at"))
    if start >= stop or stop > end:
        raise ValueError("invalid feed-health interval times")
    return {"start_at": iso(start), "end_at": iso(stop), "reason": value["reason"]}


def _intervals(values: object, end: datetime) -> list[dict[str, str]]:
    if not isinstance(values, list) or len(values) > MAX_INTERVALS:
        raise ValueError("invalid feed-health intervals")
    return [_interval(value, end) for value in values]


def _mode(value: object, end: datetime, *, stale: bool) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("status") not in STATUSES:
        raise ValueError("invalid feed-health mode")
    reasons = value.get("reasons")
    if not isinstance(reasons, list) or len(reasons) > len(REASONS):
        raise ValueError("invalid feed-health reasons")
    result = {
        "status": "stale" if stale else "fresh" if value["status"] == "healthy" else value["status"],
        "status_at_evaluation": value["status"],
        "reasons": [reason for reason in reasons if isinstance(reason, str) and reason in REASONS],
        "intervals": _intervals(value.get("intervals"), end),
        "monitored_minutes": _count(value.get("monitored_minutes"), 60),
        "baseline_samples": _count(value.get("baseline_samples"), 4),
        **{field: _count(value.get(field)) for field in COUNTS},
    }
    if result["monitored_minutes"] is None or result["baseline_samples"] is None:
        raise ValueError("missing feed-health coverage")
    for field in ("mean_accepted_vehicles", "mean_accepted_lines"):
        mean = value.get(field)
        if mean is not None and (
            type(mean) not in (int, float) or not math.isfinite(mean) or not 0 <= mean <= MAX_COUNTER
        ):
            raise ValueError("invalid feed-health mean")
        result[field] = mean
    return result


def serving_snapshot(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Validate v1 public fields, strip extras, and mark old evaluated hours stale."""
    if type(payload.get("version")) is not int or payload["version"] != 1:
        raise ValueError("unsupported feed-health snapshot")
    hour = timestamp(payload.get("hour_start"))
    evaluated = timestamp(payload.get("evaluated_at"))
    end = hour + timedelta(hours=1)
    if hour.minute or hour.second or hour.microsecond or evaluated < end or evaluated > now.astimezone(UTC):
        raise ValueError("invalid feed-health snapshot timestamps")
    collection = payload.get("collection_started_at")
    collection = iso(timestamp(collection)) if collection is not None else None
    stale = (now.astimezone(UTC) - end).total_seconds() > STALE_AFTER_SECONDS
    modes = payload.get("vehicle_types")
    if not isinstance(modes, dict) or set(modes) != set(MODES):
        raise ValueError("invalid feed-health snapshot modes")
    recent = payload.get("recent_intervals")
    if not isinstance(recent, list) or len(recent) > MAX_INTERVALS:
        raise ValueError("invalid recent feed-health intervals")
    intervals = []
    for item in recent:
        if not isinstance(item, dict) or item.get("mode") not in MODES:
            raise ValueError("invalid recent feed-health mode")
        intervals.append({"mode": item["mode"], **_interval(item, end)})
    return {
        "version": 1,
        "evaluated_at": iso(evaluated),
        "hour_start": iso(hour),
        "hour_end": iso(end),
        "collection_started_at": collection,
        "stale_after_seconds": STALE_AFTER_SECONDS,
        "vehicle_types": {mode: _mode(modes[mode], end, stale=stale) for mode in MODES},
        "recent_intervals": intervals,
    }
