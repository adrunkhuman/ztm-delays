"""Live poller status and GPS feed history, read from the poller's public GCS objects.

The poller writes `live.json` (heartbeat plus last-poll counts) and the `poller_health` DAG writes
`feed-history.json` (24 h of per-minute fleet, usual ranges and flagged incidents). Both live under
`health/poller/public/` in `ZTM_STATUS_GCS_BUCKET`; the frontend's service account can read only that
prefix. Neither object carries a verdict about the live feed, so the state shown here is derived on read.
An unset bucket disables the feature. Any missing, oversized or malformed object degrades to
"unavailable" for its part of the page and never raises. Reads that keep failing also end in
"unavailable": a broken frontend credential must not look like a stopped poller.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import TYPE_CHECKING, Any, Final, cast

import pytz
from google.api_core.exceptions import NotFound
from google.cloud import storage

if TYPE_CHECKING:
    from collections.abc import Callable

LOGGER = logging.getLogger(__name__)

BUCKET_ENV: Final = "ZTM_STATUS_GCS_BUCKET"
LIVE_OBJECT: Final = "health/poller/public/live.json"
HISTORY_OBJECT: Final = "health/poller/public/feed-history.json"
LIVE_TTL_SECONDS: Final = 30
HISTORY_TTL_SECONDS: Final = 300
LIVE_MAX_BYTES: Final = 16 * 1024
HISTORY_MAX_BYTES: Final = 1024 * 1024
READ_TIMEOUT_SECONDS: Final = 5

MODES: Final = ("bus", "tram")
WARSAW = pytz.timezone("Europe/Warsaw")
SILENT_AFTER_SECONDS: Final = 180  # three missed 60 s heartbeats
FAILING_AFTER: Final = 3  # consecutive failed polls
# The monitor publishes at :25 for the hour that just ended; two missed runs mean history is stale.
HISTORY_STALE_AFTER: Final = timedelta(hours=3)
RULE_BOUNDS: Final = {
    "threshold": (0, 1),
    "duration_minutes": (1, 60),
    "minimum_fleet": (1, 1000),
    "lookback_days": (7, 28),
}
DEFAULT_RULES: Final = {"threshold": 0.5, "duration_minutes": 15, "minimum_fleet": 20, "lookback_days": 28}
MIN_HISTORY_MINUTES: Final = 2
MAX_HISTORY_MINUTES: Final = 2880
MAX_SERIES_EXTRA: Final = 360  # the usual/low/high lookahead past the evaluated hour
MAX_INCIDENTS: Final = 200
MIN_SHARE_MINUTES: Final = 5
EMPTY_SHARE: Final = 0.05
INCIDENT_REASONS: Final = ("api_failures", "no_accepted", "low_fleet")
# Chart geometry in SVG units; the figure scales to its container.
CHART_W, CHART_H, CHART_LEFT, CHART_RIGHT, CHART_TOP, CHART_BOTTOM = 960, 210, 44, 8, 10, 24
NO_SHARE_REASON: Final = "below half of usual {noun} fleet"


@dataclass
class _Cached:
    """One object's last parsed value, refreshed at most once per TTL."""

    ttl: float
    lock: threading.Lock = field(default_factory=threading.Lock)
    fetched_at: float | None = None
    read_at: float | None = None  # last read that reached GCS, found or not
    value: dict[str, Any] | None = None
    state: str = "unavailable"  # ok | missing | unavailable


_live = _Cached(LIVE_TTL_SECONDS)
_history = _Cached(HISTORY_TTL_SECONDS)
_client_lock = threading.Lock()
_client_instance: storage.Client | None = None


def _client() -> storage.Client:
    """Create the GCS client on first use; credentials come from GOOGLE_APPLICATION_CREDENTIALS."""
    global _client_instance  # noqa: PLW0603
    with _client_lock:
        if _client_instance is None:
            _client_instance = storage.Client()
        return _client_instance


def clear_cache() -> None:
    """Forget cached objects and the client (tests)."""
    global _client_instance  # noqa: PLW0603
    with _client_lock:
        _client_instance = None
    for cached in (_live, _history):
        with cached.lock:
            cached.fetched_at, cached.read_at, cached.value, cached.state = None, None, None, "unavailable"


def enabled() -> bool:
    """Return whether a status bucket is configured."""
    return bool(os.environ.get(BUCKET_ENV))


def download(bucket: str, name: str, max_bytes: int) -> bytes:
    """Fetch at most `max_bytes` of an object; larger objects are rejected before decoding."""
    # The inclusive range reads one byte past the limit, so oversize shows up without a metadata call.
    # No retries: a failed read is simply tried again after the cache TTL.
    data = (
        _client()
        .bucket(bucket)
        .blob(name)
        .download_as_bytes(start=0, end=max_bytes, timeout=READ_TIMEOUT_SECONDS, retry=None)
    )
    if len(data) > max_bytes:
        raise ValueError(f"{name} exceeds {max_bytes} bytes")
    return data


def _refresh(
    cached: _Cached, name: str, max_bytes: int, parse: Callable[[object], dict[str, Any] | None]
) -> tuple[dict[str, Any] | None, str]:
    bucket = os.environ.get(BUCKET_ENV)
    if not bucket:
        return None, "unavailable"
    with cached.lock:
        now = time.monotonic()
        if cached.fetched_at is None or now - cached.fetched_at >= cached.ttl:
            try:
                value = parse(json.loads(download(bucket, name, max_bytes)))
                cached.value, cached.state, cached.read_at = value, ("ok" if value is not None else "unavailable"), now
            except NotFound:
                cached.value, cached.state, cached.read_at = None, "missing", now
            except Exception:  # noqa: BLE001 - credentials, network, size, JSON: one degraded outcome
                LOGGER.warning("Could not read status object %s", name, exc_info=True)
                # A transient error keeps the last good copy. A persistent one is our problem, not the
                # poller's: drop the copy before its age would read as "poller offline".
                if cached.read_at is None or now - cached.read_at > 2 * cached.ttl + SILENT_AFTER_SECONDS:
                    cached.value, cached.state = None, "unavailable"
            cached.fetched_at = now
        return cached.value, cached.state


# --- validation ---------------------------------------------------------------------------------


def _obj(value: object) -> dict[str, Any]:
    """The value if it is a JSON object, else an empty one that fails every later check."""
    return cast("dict[str, Any]", value) if isinstance(value, dict) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        return None
    return value


def _count(value: object) -> float | None:
    number = _number(value)
    return number if number is not None and number >= 0 else None


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_live(payload: object) -> dict[str, Any] | None:
    """Validate `live.json`; return None for any shape the page cannot trust."""
    data = _obj(payload)
    updated_at = _timestamp(data.get("updated_at"))
    modes = data.get("vehicle_types")
    if data.get("version") != 1 or updated_at is None or not isinstance(modes, dict):
        return None
    parsed: dict[str, dict[str, Any]] = {}
    for mode in MODES:
        row = _obj(modes.get(mode))
        failures = _count(row.get("consecutive_failures"))
        parsed[mode] = {
            "last_success_at": _timestamp(row.get("last_success_at")),
            "consecutive_failures": int(failures) if failures is not None else 0,
            "fresh_vehicles": _count(row.get("fresh_vehicles")),
            "fresh_lines": _count(row.get("fresh_lines")),
        }
    return {"updated_at": updated_at, "modes": parsed}


def _series(value: object, length: int | None = None, *, at_least: int | None = None) -> list[float | None] | None:
    """Return a bounded list of numbers and nulls of the expected length, else None."""
    if not isinstance(value, list) or len(value) > MAX_HISTORY_MINUTES + MAX_SERIES_EXTRA:
        return None
    if (length is not None and len(value) != length) or (at_least is not None and len(value) < at_least):
        return None
    if not all(item is None or _number(item) is not None for item in value):
        return None
    return cast("list[float | None]", value)


def _rules(value: object) -> dict[str, float]:
    """Published detection rules, each kept only within the monitor's own bounds."""
    rules = dict(DEFAULT_RULES)
    for key, given in _obj(value).items():
        number = _number(given)
        if key in RULE_BOUNDS and number is not None:
            low, high = RULE_BOUNDS[key]
            if low < number <= high:
                rules[key] = number
    return rules


def _incident(item: object) -> dict[str, Any] | None:
    data = _obj(item)
    start, end = _timestamp(data.get("start_at")), _timestamp(data.get("end_at"))
    if data.get("mode") not in MODES or data.get("reason") not in INCIDENT_REASONS:
        return None
    if start is None or end is None or end <= start:
        return None
    return {
        "mode": data["mode"],
        "start": start,
        "end": end,
        "reason": data["reason"],
        "ongoing": data.get("ongoing") is True,
    }


def parse_history(payload: object) -> dict[str, Any] | None:
    """Validate `feed-history.json`; return None unless both modes carry well-formed series."""
    data = _obj(payload)
    series_start = _timestamp(data.get("series_start"))
    minutes = data.get("history_minutes")
    modes = data.get("vehicle_types")
    if data.get("version") != 1 or series_start is None or not isinstance(modes, dict):
        return None
    if not isinstance(minutes, int) or isinstance(minutes, bool):
        return None
    if not MIN_HISTORY_MINUTES <= minutes <= MAX_HISTORY_MINUTES:
        return None
    parsed: dict[str, dict[str, list[float | None]]] = {}
    for mode in MODES:
        row = _obj(modes.get(mode))
        fresh = _series(row.get("fresh"), minutes)
        usual = _series(row.get("usual"), at_least=minutes)
        low = _series(row.get("low"), len(usual)) if usual is not None else None
        high = _series(row.get("high"), len(usual)) if usual is not None else None
        if fresh is None or usual is None or low is None or high is None:
            return None
        parsed[mode] = {"fresh": fresh, "usual": usual, "low": low, "high": high}
    raw_incidents = data.get("incidents")
    incidents = (
        [i for item in raw_incidents[-MAX_INCIDENTS:] if (i := _incident(item))]
        if isinstance(raw_incidents, list)
        else []
    )
    return {
        "series_start": series_start,
        "minutes": minutes,
        "evaluated_at": _timestamp(data.get("evaluated_at")),
        "rules": _rules(data.get("rules")),
        "modes": parsed,
        "incidents": incidents,
    }


def read_live() -> tuple[dict[str, Any] | None, str]:
    """Return the cached, validated heartbeat and its state: ok, missing or unavailable."""
    return _refresh(_live, LIVE_OBJECT, LIVE_MAX_BYTES, parse_live)


def read_history() -> dict[str, Any] | None:
    """Return the cached, validated feed history, or None."""
    return _refresh(_history, HISTORY_OBJECT, HISTORY_MAX_BYTES, parse_history)[0]


# --- derived live state -------------------------------------------------------------------------


def history_stale(history: dict[str, Any] | None, now: datetime) -> bool:
    """History stops at the evaluated hour; past HISTORY_STALE_AFTER the monitor has stopped publishing."""
    if history is None:
        return False
    return now - (history["series_start"] + timedelta(minutes=history["minutes"])) > HISTORY_STALE_AFTER


def usual_now(history: dict[str, Any] | None, mode: str, now: datetime) -> tuple[float, float, float] | None:
    """Usual, low and high fleet for the current minute, or None outside the series or without a baseline."""
    if history is None:
        return None
    index = int((now - history["series_start"]).total_seconds() // 60)
    row = history["modes"][mode]
    if not 0 <= index < len(row["usual"]):
        return None
    usual, low, high = row["usual"][index], row["low"][index], row["high"][index]
    return (usual, low, high) if usual is not None and low is not None and high is not None else None


def mode_state(
    *, silent: bool, failures: int, fresh: float | None, usual: float | None, rules: dict[str, float]
) -> tuple[str, str]:
    """Return (state label, tone) for one mode; tone is "problem", "quiet" or empty."""
    if silent:
        state = ("poller silent", "problem")
    elif failures >= FAILING_AFTER:
        state = ("API not answering", "problem")
    elif usual is None:
        state = ("answering", "")
    elif usual < rules["minimum_fleet"]:
        state = ("night service", "quiet")
    elif fresh is None:
        state = ("no data yet", "quiet")
    elif fresh < rules["threshold"] * usual:
        state = ("thin feed", "problem")
    else:
        state = ("normal", "")
    return state


def _meter(fresh: float | None, usual: float, low: float, high: float) -> dict[str, float | None]:
    scale = (high * 1.5) or 1
    return {
        "low": round(low / scale * 100, 1),
        "width": round((high - low) / scale * 100, 1),
        "usual": round(usual / scale * 100, 1),
        "now": round(min(fresh / scale, 1) * 100, 1) if fresh is not None else None,
    }


def build_live(live: dict[str, Any] | None, history: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    """Derive the pill and per-mode state from a heartbeat, the history baseline and the clock."""
    updated = live["updated_at"] if live else None
    silent = updated is None or (now - updated).total_seconds() > SILENT_AFTER_SECONDS
    rules = history["rules"] if history else dict(DEFAULT_RULES)
    local = now.astimezone(WARSAW)
    modes = {}
    for mode in MODES:
        row = (live or {}).get("modes", {}).get(mode, {})
        fresh = row.get("fresh_vehicles")
        failures = row.get("consecutive_failures", 0)
        baseline = usual_now(history, mode, now)
        usual, low, high = baseline or (None, None, None)
        state, tone = mode_state(silent=silent, failures=failures, fresh=fresh, usual=usual, rules=rules)
        success = row.get("last_success_at")
        modes[mode] = {
            "state": state,
            "tone": tone,
            "fresh": int(fresh) if fresh is not None else None,
            "lines": int(row["fresh_lines"]) if row.get("fresh_lines") is not None else None,
            "usual": round(usual) if usual is not None else None,
            "low": round(low) if low is not None else None,
            "high": round(high) if high is not None else None,
            "failures": failures,
            "success_age": max(0, int((now - success).total_seconds())) if success else None,
            "meter": _meter(fresh, *baseline) if baseline else None,
        }
    # A stale history's "ongoing" may long since have recovered; trust only the live heartbeat then.
    ongoing = not history_stale(history, now) and any(i["ongoing"] for i in (history or {}).get("incidents", []))
    problem = any(m["tone"] == "problem" for m in modes.values()) or ongoing
    if silent:
        overall = ("offline", "poller offline")
    elif problem:
        overall = ("problems", "feed problems")
    else:
        overall = ("online", "all systems normal")
    return {
        "overall": overall,
        "now": local,
        "heartbeat_age": max(0, int((now - updated).total_seconds())) if updated else None,
        "silent": silent,
        "modes": modes,
        "weeks": int(rules["lookback_days"] // 7) or 1,
    }


def live_view(now: datetime | None = None) -> dict[str, Any]:
    """Build the live panel context; `{"available": False}` when the feature is off or unreadable."""
    if not enabled():
        return {"available": False}
    live, state = read_live()
    if state == "unavailable":
        return {"available": False}
    try:
        view = build_live(live, read_history(), now or datetime.now(UTC))
    except (ArithmeticError, ValueError):
        LOGGER.warning("Could not derive live status", exc_info=True)
        return {"available": False}
    return {"available": True, **view}


# --- history charts and incident table ----------------------------------------------------------


def _nice_max(value: float) -> int:
    for step in (50, 100, 200, 250, 500):
        if value / step <= 5:  # noqa: PLR2004
            return int(-(-value // step) * step) or step
    return int(-(-value // 1000) * 1000)


def _chart(history: dict[str, Any], mode: str, incidents: list[dict[str, Any]]) -> dict[str, Any]:
    """Server-side SVG geometry; the client adapts its width and adds the crosshair readout."""
    n = history["minutes"]
    start = history["series_start"]
    row = history["modes"][mode]
    fresh, usual, low, high = row["fresh"], row["usual"][:n], row["low"][:n], row["high"][:n]
    w, h, left, right, top, bottom = CHART_W, CHART_H, CHART_LEFT, CHART_RIGHT, CHART_TOP, CHART_BOTTOM
    top_value = _nice_max(max([v for v in (*fresh, *high) if v is not None], default=0) * 1.05)

    def x(i: float) -> float:
        return round(left + (w - left - right) * i / (n - 1), 1)

    def y(v: float) -> float:
        return round(top + (h - top - bottom) * (1 - v / top_value), 1)

    def path(values: list[float | None]) -> str:
        out, pen = [], "M"
        for i, value in enumerate(values):
            if value is None:
                pen = "M"
                continue
            out.append(f"{pen}{x(i)},{y(value)}")
            pen = "L"
        return "".join(out)

    inside = [i for i in range(n) if usual[i] is not None and low[i] is not None and high[i] is not None]
    band = ""
    if inside:
        band = "M" + "L".join(f"{x(i)},{y(high[i])}" for i in inside)  # type: ignore[arg-type]
        band += "L" + "L".join(f"{x(i)},{y(low[i])}" for i in reversed(inside)) + "Z"  # type: ignore[arg-type]
    shades = []
    for inc in (i for i in incidents if i["mode"] == mode):
        first = int((inc["start"] - start).total_seconds() // 60)
        last = int((inc["end"] - start).total_seconds() // 60)
        if last > 0 and first < n:
            first, last = max(0, first), min(n - 1, last)
            shades.append({"x": x(first), "w": max(x(last) - x(first), 2), "gap": inc["reason"] == "api_failures"})
    return {
        "w": w,
        "h": h,
        "left": left,
        "right": right,
        "top": top,
        "bottom": bottom,
        "actual": path(fresh),
        "usual": path(usual),
        "band": band,
        "ticks_y": [{"y": y(v), "label": f"{v:,}"} for v in range(0, top_value + 1, top_value // 4)],
        "ticks_x": _time_ticks(start, n, x),
        "shades": shades,
        "data": [[fresh[i], usual[i]] for i in range(n)],
        "start": start.astimezone(WARSAW).isoformat(),
    }


def _time_ticks(start: datetime, n: int, x: Callable[[float], float]) -> list[dict[str, Any]]:
    """Label every third Warsaw hour; midnight shows the weekday and date."""
    ticks = []
    for i in range(n):
        t = (start + timedelta(minutes=i)).astimezone(WARSAW)
        if t.minute == 0 and t.hour % 3 == 0:
            ticks.append({"x": x(i), "label": t.strftime("%H:%M") if t.hour else t.strftime("%a %d")})
    return ticks


def _interval_medians(history: dict[str, Any], inc: dict[str, Any]) -> tuple[float, float] | None:
    """Median fresh and usual fleet over an incident, when the series covers all of it."""
    start = int((inc["start"] - history["series_start"]).total_seconds() // 60)
    end = int((inc["end"] - history["series_start"]).total_seconds() // 60)
    if start < 0 or end > history["minutes"]:
        return None
    row = history["modes"][inc["mode"]]
    pairs = [
        (f, u) for f, u in zip(row["fresh"][start:end], row["usual"][start:end], strict=True) if f is not None and u
    ]
    if len(pairs) < MIN_SHARE_MINUTES:
        return None
    return median(f for f, _ in pairs), median(u for _, u in pairs)


def _describe(inc: dict[str, Any], medians: tuple[float, float] | None) -> str:
    noun = "bus" if inc["mode"] == "bus" else "tram"
    if inc["reason"] == "api_failures":
        return "API not answering"
    # A low-fleet stretch that is almost empty reads as an empty feed, as it does on the chart.
    if inc["reason"] == "no_accepted" or (medians and medians[0] < EMPTY_SHARE * medians[1]):
        return f"no {'buses' if noun == 'bus' else 'trams'} in feed"
    if medians:
        return f"{medians[0] / medians[1]:.0%} of usual {noun} fleet"
    return NO_SHARE_REASON.format(noun=noun)


def _incident_rows(history: dict[str, Any]) -> list[dict[str, Any]]:
    # A poller outage hits both modes at once; show it as one event.
    seen: dict[tuple, dict[str, Any]] = {}
    for inc in sorted(history["incidents"], key=lambda i: (i["start"], i["mode"])):
        key = (
            (inc["start"], inc["end"], inc["reason"])
            if inc["reason"] == "api_failures"
            else (inc["start"], inc["end"], inc["reason"], inc["mode"])
        )
        if key in seen:
            seen[key]["mode"] = "all"
            seen[key]["ongoing"] = seen[key]["ongoing"] or inc["ongoing"]
        else:
            seen[key] = dict(inc)
    rows = []
    for inc in seen.values():
        medians = None if inc["reason"] == "api_failures" else _interval_medians(history, inc)
        local_start, local_end = inc["start"].astimezone(WARSAW), inc["end"].astimezone(WARSAW)
        rows.append(
            {
                "mode": inc["mode"],
                "start": local_start,
                "end": local_end,
                "minutes": int((inc["end"] - inc["start"]).total_seconds() // 60),
                "ongoing": inc["ongoing"],
                "gap": inc["reason"] == "api_failures",
                "what": _describe(inc, medians),
                "fresh": medians[0] if medians else None,
                "usual": medians[1] if medians else None,
            }
        )
    return list(reversed(rows))


def build_history(history: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Build the chart and incident-table context from validated feed history."""
    rules = history["rules"]
    end = history["series_start"] + timedelta(minutes=history["minutes"])
    return {
        "available": True,
        "stale_since": end.astimezone(WARSAW) if history_stale(history, now) else None,
        "hours": round(history["minutes"] / 60),
        "charts": {mode: _chart(history, mode, history["incidents"]) for mode in MODES},
        "incidents": _incident_rows(history),
        "days": 14,
        "rules": {
            "ratio": int(rules["threshold"] * 100),
            "run": int(rules["duration_minutes"]),
            "floor": int(rules["minimum_fleet"]),
            "weeks": int(rules["lookback_days"] // 7) or 1,
        },
    }


def history_view(now: datetime | None = None) -> dict[str, Any]:
    """Build the history section context; `{"available": False}` when off or unreadable."""
    if not enabled():
        return {"available": False}
    history = read_history()
    if not history:
        return {"available": False}
    try:
        return build_history(history, now or datetime.now(UTC))
    except (ArithmeticError, ValueError):
        LOGGER.warning("Could not build feed history", exc_info=True)
        return {"available": False}
