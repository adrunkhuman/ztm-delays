"""Public feed history for the status page: allowlisted, chart-ready, no private state.

The page's service account can read only ``health/poller/public/``, so this
object carries everything the history charts and the live "usual now" need:
24 hours of fresh fleet and baseline per minute, the baseline for the next
three hours, and recent incidents.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from poller_health import LEGACY_REASONS, MINUTES_PER_HOUR, MODES, Config, iso, timestamp

HISTORY_PATH = "health/poller/public/feed-history.json"
HISTORY_HOURS = 24
LOOKAHEAD_HOURS = 3
INCIDENT_DAYS = 14
HISTORY_MAX_BYTES = 1024 * 1024
UNKNOWN = [None] * MINUTES_PER_HOUR


def series(rows: list[dict[str, Any] | None], pick: str) -> list[float | None]:
    """Concatenate hourly minute series; a missing hour is unknown, never zero."""
    out: list[float | None] = []
    for row in rows:
        if row is None:
            out.extend(UNKNOWN)
        elif pick == "fresh":
            out.extend(row["fresh"])
        else:
            out.extend(row["baseline"][pick])
    return out


def incidents(report: dict[str, Any], end: datetime) -> list[dict[str, Any]]:
    """Last 14 days of fleet/API incidents; legacy stale-share intervals were false alarms."""
    cutoff = end - timedelta(days=INCIDENT_DAYS)
    active = {
        mode: report["vehicle_types"][mode]["state"]["active"]
        for mode in MODES
        if report["vehicle_types"][mode]["state"]["active"]
    }
    out = []
    for interval in report["recent_intervals"]:
        if interval["reason"] in LEGACY_REASONS or timestamp(interval["end_at"]) < cutoff:
            continue
        current = active.get(interval["mode"])
        ongoing = bool(current and current["start_at"] == interval["start_at"] and timestamp(interval["end_at"]) == end)
        out.append({**interval, "ongoing": ongoing})
    return out


def feed_history(
    report: dict[str, Any],
    previous: list[dict[str, Any] | None],
    lookahead: dict[str, list[dict[str, Any]]],
    config: Config,
) -> dict[str, Any]:
    """Build the public object from validated reports.

    ``previous`` holds the 23 reports before ``report``, oldest first, None where
    an hour was never evaluated; ``lookahead`` holds each mode's baselines for
    the next three hours.
    """
    if len(previous) != HISTORY_HOURS - 1 or any(len(lookahead[mode]) != LOOKAHEAD_HOURS for mode in MODES):
        raise ValueError("feed history needs 24 hours of reports and 3 hours of lookahead")
    hour = timestamp(report["hour_start"])
    end = hour + timedelta(hours=1)
    reports = [*previous, report]
    for offset, candidate in enumerate(reports):
        if candidate is not None and timestamp(candidate["hour_start"]) != end - timedelta(
            hours=HISTORY_HOURS - offset
        ):
            raise ValueError("feed history reports are not consecutive hours")
    modes = {}
    for mode in MODES:
        rows = [candidate["vehicle_types"][mode] if candidate else None for candidate in reports]
        ahead = [{"baseline": baseline} for baseline in lookahead[mode]]
        modes[mode] = {
            "status": report["vehicle_types"][mode]["status"],
            "baseline_samples": report["vehicle_types"][mode]["baseline_samples"],
            "fresh": series(rows, "fresh"),
            **{
                key: series([*rows, *ahead], source)
                for key, source in (("usual", "vehicles"), ("low", "low"), ("high", "high"))
            },
        }
    return {
        "version": 1,
        "evaluated_at": report["evaluated_at"],
        "hour_start": report["hour_start"],
        "series_start": iso(end - timedelta(hours=HISTORY_HOURS)),
        "history_minutes": HISTORY_HOURS * MINUTES_PER_HOUR,
        "rules": {
            "threshold": config.threshold,
            "duration_minutes": config.duration_minutes,
            "minimum_fleet": config.minimum_fleet,
            "lookback_days": config.lookback_days,
        },
        "vehicle_types": modes,
        "incidents": incidents(report, end),
    }
