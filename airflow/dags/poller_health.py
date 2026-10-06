"""Evaluate private v1 poll summaries without Airflow, GPS scans, or service assumptions.

The signal is the fresh fleet: distinct vehicles with a ping younger than the
poller's age cutoff, per successful poll. Each minute is compared with the SAME
Warsaw weekday and wall-clock minute in the preceding 28 local dates (DST folds
are folded into one daily sample). Weeks that were themselves outages at that
minute are dropped, so a past incident cannot lower the bar. Minutes where fewer
than ``minimum_fleet`` vehicles usually run are never flagged: quiet nights are
not outages. The share of stale rows is not a signal; the API keeps serving the
last positions of parked vehicles, so it tracks how much of the fleet is idle.

Before collection began, raw-GPS seeds (``baseline-seed/``) stand in for missing
summaries, so baselines exist from the first monitored hour. Gaps never count as
zero feed.

Reports retain private incident state; ``snapshot`` is the public allowlist. Use
GCS lifecycle rules for 90-day report archives and at least 28 days of summaries;
no per-run archive listing or deletion is necessary.
"""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

MODES = ("bus", "tram")
WARSAW = ZoneInfo("Europe/Warsaw")
SUMMARY_MAX_BYTES = 128 * 1024
REPORT_MAX_BYTES = 256 * 1024
MAX_COUNTER = 10**9
MINUTES_PER_HOUR = 60
MINIMUM_COVERAGE = 0.8
MINIMUM_SAMPLES = 3
MAXIMUM_SAMPLES = 4
MINIMUM_LOOKBACK_DAYS = 21
MAXIMUM_LOOKBACK_DAYS = 28
MAX_TIMESTAMP_LENGTH = 40
MAX_RECENT_INTERVALS = 48
MAX_REPORT_EVENTS = MINUTES_PER_HOUR * len(MODES) * 2 + len(MODES)
REPORT_VERSION = 2
SEED_MAX_BYTES = 64 * 1024
MAX_MINIMUM_FLEET = 1000
COUNT_FIELDS = (
    "attempts",
    "successes",
    "parsed_rows",
    "accepted_rows",
    "dropped_stale_rows",
    "dropped_future_rows",
    "accepted_vehicle_count_sum",
    "accepted_line_count_sum",
)
METRIC_FIELDS = (
    "parsed_rows",
    "accepted_rows",
    "dropped_stale_rows",
    "dropped_future_rows",
    "mean_accepted_vehicles",
    "mean_accepted_lines",
)
PUBLIC_MODE_FIELDS = ("status", "reasons", "intervals", "monitored_minutes", "baseline_samples", *METRIC_FIELDS)


@dataclass(frozen=True)
class Config:
    """Detection controls; ``threshold`` bounds both poll success and fleet share."""

    duration_minutes: int = 15
    coverage_fraction: float = 0.8
    threshold: float = 0.5
    minimum_samples: int = 3
    lookback_days: int = 28
    minimum_fleet: int = 20

    def __post_init__(self) -> None:
        """Reject unsafe or unbounded configuration."""
        if not 1 <= self.duration_minutes <= MINUTES_PER_HOUR:
            raise ValueError("duration_minutes must be 1..60")
        if not MINIMUM_COVERAGE <= self.coverage_fraction <= 1 or not 0 < self.threshold < 1:
            raise ValueError("coverage must be >=0.8 and threshold must be between 0 and 1")
        if (
            not MINIMUM_SAMPLES <= self.minimum_samples <= MAXIMUM_SAMPLES
            or not MINIMUM_LOOKBACK_DAYS <= self.lookback_days <= MAXIMUM_LOOKBACK_DAYS
        ):
            raise ValueError("need >=3 samples within a bounded 21..28 day lookback")
        if not 1 <= self.minimum_fleet <= MAX_MINIMUM_FLEET:
            raise ValueError("minimum_fleet must be 1..1000")


DEFAULT_CONFIG = Config()


def timestamp(value: str) -> datetime:
    """Parse a UTC ISO timestamp, never treating a naive time as UTC."""
    if not isinstance(value, str) or len(value) > MAX_TIMESTAMP_LENGTH:
        raise ValueError("invalid UTC timestamp")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("invalid UTC timestamp") from exc
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return result.astimezone(UTC)


def iso(value: datetime) -> str:
    """Canonical public UTC representation."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def completed_hour(data_interval_end: datetime) -> datetime:
    """At :25, evaluate the hour preceding the interval end's UTC clock hour."""
    if data_interval_end.tzinfo is None:
        raise ValueError("data_interval_end must be timezone aware")
    return data_interval_end.astimezone(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def hour_path(prefix: str, hour: datetime) -> str:
    """UTC object identity, including repeated local hours at DST rollback."""
    return f"health/poller/{prefix}/{hour.astimezone(UTC):%Y-%m-%d/%H}.json"


def decode_json(payload: bytes, limit: int) -> dict[str, Any]:
    """Bound bytes before decoding; reject duplicate JSON keys and nonfinite values."""
    if len(payload) > limit:
        raise ValueError("health payload too large")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValueError("nonfinite JSON number")

    try:
        result = json.loads(payload, object_pairs_hook=unique, parse_constant=reject_constant)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("invalid JSON") from exc
    if not isinstance(result, dict):
        raise ValueError("health payload must be an object")  # noqa: TRY004 - all wire failures share one boundary
    return result


def validate_summary(data: dict[str, Any], hour: datetime) -> dict[str, Any]:  # noqa: C901 - explicit wire checks
    """Validate the exact contracts/poller_health_v1.json wire schema and invariants."""
    if set(data) != {"version", "hour_start", "collection_started_at", "poll_interval_seconds", "vehicle_types"}:
        raise ValueError("invalid summary fields")
    if type(data["version"]) is not int or data["version"] != 1:
        raise ValueError("unsupported summary version")
    start = timestamp(data["hour_start"])
    collection = timestamp(data["collection_started_at"])
    if start != hour or start.minute or start.second or start.microsecond or collection >= start + timedelta(hours=1):
        raise ValueError("invalid summary timestamps")
    interval = data["poll_interval_seconds"]
    if type(interval) not in (int, float) or not math.isfinite(interval) or interval <= 0:
        raise ValueError("poll interval must be a finite positive number")
    if not isinstance(data["vehicle_types"], dict) or set(data["vehicle_types"]) != set(MODES):
        raise ValueError("invalid vehicle types")
    for mode in MODES:
        mode_data = data["vehicle_types"][mode]
        if not isinstance(mode_data, dict) or set(mode_data) != {"minutes"}:
            raise ValueError("invalid mode fields")
        minutes = mode_data["minutes"]
        if not isinstance(minutes, list) or len(minutes) > MINUTES_PER_HOUR:
            raise ValueError("invalid minute array")
        seen = set()
        for item in minutes:
            validate_minute(item)
            minute = item["minute"]
            if minute in seen or start + timedelta(minutes=minute + 1) <= collection:
                raise ValueError("duplicate minute or minute precedes collection")
            seen.add(minute)
    return data


def validate_minute(item: dict[str, Any]) -> None:
    """Check nonnegative counters and successful-response count invariants."""
    if not isinstance(item, dict) or set(item) != {"minute", *COUNT_FIELDS}:
        raise ValueError("invalid minute fields")
    if type(item["minute"]) is not int or not 0 <= item["minute"] < MINUTES_PER_HOUR:
        raise ValueError("invalid minute")
    if any(type(item[key]) is not int or not 0 <= item[key] <= MAX_COUNTER for key in COUNT_FIELDS):
        raise ValueError("invalid count")
    if item["successes"] > item["attempts"]:
        raise ValueError("successes exceed attempts")
    if item["parsed_rows"] != sum(item[key] for key in ("accepted_rows", "dropped_stale_rows", "dropped_future_rows")):
        raise ValueError("parsed row invariant")
    if not item["successes"] and any(item[key] for key in COUNT_FIELDS[2:]):
        raise ValueError("failed attempts have rows")
    if any(item[key] > item["accepted_rows"] for key in ("accepted_vehicle_count_sum", "accepted_line_count_sum")):
        raise ValueError("distinct counts exceed accepted rows")


def comparable_hours(hour: datetime, config: Config) -> list[datetime]:
    """Return unique UTC candidates for same local weekday/hour, at most five."""
    local = hour.astimezone(WARSAW)
    candidates = set()
    for days in range(7, config.lookback_days + 1, 7):
        wall = (local - timedelta(days=days)).replace(tzinfo=None)
        for fold in (0, 1):
            candidate = wall.replace(tzinfo=WARSAW, fold=fold).astimezone(UTC)
            if candidate.astimezone(WARSAW).replace(tzinfo=None) == wall:
                candidates.add(candidate)
    return sorted(candidates)


def metrics(minutes: list[dict[str, int]] | None) -> dict[str, int | float | None]:
    """Aggregate successful responses; absent summaries have nullable raw metrics."""
    if minutes is None:
        return dict.fromkeys(METRIC_FIELDS)
    result = {key: sum(item[key] for item in minutes) for key in METRIC_FIELDS[:4]}
    successes = sum(item["successes"] for item in minutes)
    return {
        **result,
        "mean_accepted_vehicles": sum(item["accepted_vehicle_count_sum"] for item in minutes) / successes
        if successes
        else None,
        "mean_accepted_lines": sum(item["accepted_line_count_sum"] for item in minutes) / successes
        if successes
        else None,
    }


def monitored(item: dict[str, int] | None, interval: float, config: Config) -> bool:
    """Coverage measures attempts, not successful responses."""
    return item is not None and item["attempts"] >= math.ceil(60 / interval * config.coverage_fraction)


def validate_seed(data: dict[str, Any], hour: datetime) -> dict[str, Any]:
    """Raw-GPS stand-in for a pre-collection summary: fresh fleet per UTC minute only."""
    if set(data) != {"version", "hour_start", "source", "vehicle_types"} or data["source"] != "raw_gps_pings":
        raise ValueError("invalid seed fields")
    if type(data["version"]) is not int or data["version"] != 1 or timestamp(data["hour_start"]) != hour:
        raise ValueError("invalid seed header")
    if not isinstance(data["vehicle_types"], dict) or set(data["vehicle_types"]) != set(MODES):
        raise ValueError("invalid seed vehicle types")
    for mode in MODES:
        minutes = data["vehicle_types"][mode]
        if not isinstance(minutes, dict) or set(minutes) != {"minutes"} or not isinstance(minutes["minutes"], list):
            raise ValueError("invalid seed mode")
        seen = set()
        for item in minutes["minutes"]:
            if (
                not isinstance(item, dict)
                or set(item) != {"minute", "fresh_vehicles"}
                or type(item["minute"]) is not int
                or not 0 <= item["minute"] < MINUTES_PER_HOUR
                or item["minute"] in seen
                or type(item["fresh_vehicles"]) is not int
                or not 0 <= item["fresh_vehicles"] <= MAX_COUNTER
            ):
                raise ValueError("invalid seed minute")
            seen.add(item["minute"])
    return data


def build_seeds(rows: list[tuple[str, datetime, int]]) -> dict[datetime, dict[str, Any]]:
    """Group raw-GPS fresh fleet per (mode, UTC minute) into hourly seed objects."""
    seeds: dict[datetime, dict[str, Any]] = {}
    for mode, minute_at, fresh in sorted(rows, key=lambda row: (row[1], row[0])):
        if mode not in MODES:
            raise ValueError("unknown seed mode")
        at = minute_at.astimezone(UTC)
        hour = at.replace(minute=0, second=0, microsecond=0)
        seed = seeds.setdefault(
            hour,
            {
                "version": 1,
                "hour_start": iso(hour),
                "source": "raw_gps_pings",
                "vehicle_types": {name: {"minutes": []} for name in MODES},
            },
        )
        seed["vehicle_types"][mode]["minutes"].append({"minute": at.minute, "fresh_vehicles": int(fresh)})
    for hour, seed in seeds.items():
        validate_seed(seed, hour)
    return seeds


def fresh_fleet(item: dict[str, int]) -> float:
    """Mean distinct fresh vehicles per successful poll; callers ensure successes."""
    return item["accepted_vehicle_count_sum"] / item["successes"]


def sample_minutes(source: dict[str, Any], mode: str, config: Config) -> dict[int, float]:
    """Fresh fleet for minutes trustworthy enough to describe normal service.

    Summaries contribute only minutes whose polls mostly succeeded; seeds were
    built from raw pings, where a minute present for either mode was collected.
    """
    minutes = source["vehicle_types"][mode]["minutes"]
    if "source" in source:
        present = {item["minute"] for other in MODES for item in source["vehicle_types"][other]["minutes"]}
        values = {item["minute"]: float(item["fresh_vehicles"]) for item in minutes}
        return {minute: values.get(minute, 0.0) for minute in present}
    interval = source["poll_interval_seconds"]
    hour = timestamp(source["hour_start"])
    collection = timestamp(source["collection_started_at"])
    needed = math.ceil(60 / interval * config.coverage_fraction)
    return {
        item["minute"]: fresh_fleet(item)
        for item in minutes
        if item["successes"] >= needed
        and monitored(item, interval, config)
        and hour + timedelta(minutes=item["minute"]) >= collection
    }


def baseline(
    samples: list[dict[str, Any]],
    mode: str,
    config: Config,
    *,
    reset_at: str | None = None,
) -> dict[str, Any]:
    """Per-minute usual fleet from one sample per local date, dropping outage weeks."""
    epoch = timestamp(reset_at) if reset_at else None
    daily: dict[str, dict[int, list[float]]] = {}
    for source in samples:
        hour = timestamp(source["hour_start"])
        if epoch is not None and hour < epoch:
            continue
        date = daily.setdefault(hour.astimezone(WARSAW).date().isoformat(), {})
        for minute, value in sample_minutes(source, mode, config).items():
            date.setdefault(minute, []).append(value)
    usual: list[float | None] = []
    low: list[float | None] = []
    high: list[float | None] = []
    for minute in range(MINUTES_PER_HOUR):
        values = [median(day[minute]) for day in daily.values() if minute in day]
        if len(values) < config.minimum_samples:
            usual.append(None)
            low.append(None)
            high.append(None)
            continue
        typical = median(values)
        kept = [value for value in values if value >= config.threshold * typical] or values
        usual.append(round(median(kept), 1))
        low.append(round(min(kept), 1))
        high.append(round(max(kept), 1))
    samples_used = sum(1 for day in daily.values() if day)
    return {"samples": min(samples_used, MAXIMUM_SAMPLES), "vehicles": usual, "low": low, "high": high}


def empty_baseline() -> dict[str, Any]:
    """No usual fleet known for any minute."""
    return {"samples": 0, **{key: [None] * MINUTES_PER_HOUR for key in ("vehicles", "low", "high")}}


def counts_for_fleet(expected: float | None, config: Config) -> bool:
    """Only minutes with real usual service can show a fleet outage or prove recovery."""
    return expected is not None and expected >= config.minimum_fleet


def bad_reason(item: dict[str, int], expected: float | None, config: Config) -> str | None:
    """API failure needs no history; fleet loss needs a usual fleet of at least ``minimum_fleet``."""
    if not item["attempts"] or item["successes"] / item["attempts"] < config.threshold:
        return "api_failures"
    if not counts_for_fleet(expected, config):
        return None
    fleet = fresh_fleet(item)
    if not fleet:
        return "no_accepted"
    if fleet < config.threshold * expected:
        return "low_fleet"
    return None


def event(mode: str, active: dict[str, str], transition: str, at: str) -> dict[str, Any]:
    """Stable event identity supports receiver-side dedupe after ambiguous POSTs."""
    identity = f"poller-health-v1:{mode}:{active['start_at']}:{transition}"
    return {
        "event_id": hashlib.sha256(identity.encode()).hexdigest(),
        "mode": mode,
        "transition": transition,
        "start_at": active["start_at"],
        "at": at,
        "reason": active["reason"],
        "delivered_at": None,
        "logged_at": None,
    }


def preceding_bad_tail(
    hour: datetime,
    mode: str,
    source: dict[str, Any] | None,
    prior: dict[str, Any],
    config: Config,
) -> list[dict[str, str]]:
    """Recheck the preceding hour's trailing evidence using its own fleet baseline."""
    if source is None:
        return []
    expected = prior.get("baseline", empty_baseline())["vehicles"]
    minutes = {item["minute"]: item for item in source["vehicle_types"][mode]["minutes"]}
    tail = []
    for minute in range(MINUTES_PER_HOUR):
        item = minutes.get(minute)
        reason = (
            bad_reason(item, expected[minute], config)
            if monitored(item, source["poll_interval_seconds"], config)
            else None
        )
        if reason:
            tail.append({"at": iso(hour - timedelta(hours=1) + timedelta(minutes=minute)), "reason": reason})
        else:
            tail = []
    return tail[-config.duration_minutes :]


def mode_status(  # noqa: PLR0911, PLR0913 - one ordered decision table
    *,
    hour: datetime,
    collection: datetime | None,
    active: dict[str, str] | None,
    monitored_minutes: int,
    gaps: bool,
    has_baseline: bool,
    positive_baseline: bool,
    has_bad_minutes: bool,
    reasons: set[str],
) -> str:
    """Choose a confidence-aware status without equating missing telemetry to zero.

    ``has_baseline`` means some minute has a usual fleet at all; ``positive_baseline``
    means some minute's usual fleet is large enough to judge (quiet nights are not).
    """
    end = hour + timedelta(hours=1)
    partial = collection is not None and hour < collection < end
    if collection is None or collection >= end:
        reasons.add("collection_not_confirmed" if collection is None else "before_collection")
        return "not_monitored"
    if active and not positive_baseline and active["reason"] in FLEET_REASONS:
        reasons.add("recovery_unconfirmed")
    if active and monitored_minutes and (positive_baseline or has_bad_minutes):
        return "degraded"
    if gaps:
        return "partial" if partial else "monitoring_gap"
    if partial:
        return "partial"
    if not has_baseline:
        reasons.add("recovery_unconfirmed" if active else "no_positive_fleet_baseline")
        return "warming_up"
    if active:
        # An outage carried into quiet hours stays open until normal service proves recovery.
        return "degraded"
    return "healthy"


def recovery_evidence(
    item: dict[str, int], active: dict[str, str] | None, expected: float | None, config: Config
) -> bool:
    """Fleet recovery needs a judgeable usual fleet; answered polls alone prove only the API."""
    if active and active["reason"] in FLEET_REASONS:
        return counts_for_fleet(expected, config)
    # The caller has already ruled out API failure and fleet loss.
    return item["successes"] > 0


def minute_fresh(
    summary: dict[str, Any] | None,
    item: dict[str, int] | None,
    at: datetime,
    collection: datetime | None,
    config: Config,
) -> float | None:
    """Public chart value: null for unmonitored minutes, never a fake zero."""
    if summary is None or collection is None or at + timedelta(minutes=1) <= collection:
        return None
    if not monitored(item, summary["poll_interval_seconds"], config) or not item["successes"]:
        return None
    return round(fresh_fleet(item), 1)


def summary_after_reset(source: dict[str, Any] | None, reset_at: str | None) -> dict[str, Any] | None:
    """Pre-epoch minute evidence cannot reopen an administratively reset incident."""
    if source is None or reset_at is None or timestamp(source["hour_start"]) >= timestamp(reset_at):
        return source
    return None


def monitoring_transitions(  # noqa: PLR0913 - independent telemetry evidence and prior durable state
    hour: datetime,
    mode: str,
    summary: dict[str, Any] | None,
    collection: datetime | None,
    prior: dict[str, Any],
    config: Config,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Alert on sustained loss/restoration of telemetry, never infer a feed outage."""
    state = prior.get("monitoring", {})
    active = state.get("active")
    gap_tail = list(state.get("gap_tail", []))
    good_tail = list(state.get("good_tail", []))
    minutes = {item["minute"]: item for item in summary["vehicle_types"][mode]["minutes"]} if summary else {}
    events = []
    for minute in range(MINUTES_PER_HOUR):
        at = hour + timedelta(minutes=minute)
        # Do not assess expected coverage before collection or in its first partial minute.
        if collection is None or at < collection:
            gap_tail, good_tail = [], []
            continue
        if summary is None or not monitored(minutes.get(minute), summary["poll_interval_seconds"], config):
            good_tail = []
            gap_tail = [*gap_tail, iso(at)][-config.duration_minutes :]
            if active is None and len(gap_tail) >= config.duration_minutes:
                active = gap_tail[0]
                events.append(
                    event(
                        mode,
                        {"start_at": active, "reason": "monitoring_gap"},
                        "monitoring_gap",
                        iso(at + timedelta(minutes=1)),
                    )
                )
        else:
            gap_tail = []
            good_tail = [*good_tail, iso(at)][-config.duration_minutes :]
            if active is not None and len(good_tail) >= config.duration_minutes:
                events.append(
                    event(mode, {"start_at": active, "reason": "monitoring_gap"}, "monitoring_restored", good_tail[0])
                )
                active = None
    return {"active": active, "gap_tail": gap_tail, "good_tail": good_tail}, events


def retire_legacy_incident(
    hour: datetime,
    mode: str,
    active: dict[str, str] | None,
    intervals: list[dict[str, str]],
    events: list[dict[str, Any]],
) -> bool:
    """Close an incident opened by a retired rule at the first evaluation under the new rules.

    Left open, it would absorb a real fleet outage at deploy time: new bad
    minutes cannot open their own incident while one is active.
    """
    if not active or active["reason"] not in LEGACY_REASONS:
        return False
    intervals.append({**active, "end_at": iso(hour)})
    events.append(event(mode, active, "recovered", iso(hour)))
    return True


def evaluate_mode(  # noqa: C901, PLR0912, PLR0913, PLR0915 - keep the minute state walk in one place
    hour: datetime,
    mode: str,
    summary: dict[str, Any] | None,
    expected: dict[str, Any],
    previous_summary: dict[str, Any] | None,
    previous_report: dict[str, Any] | None,
    collection: datetime | None,
    config: Config,
    source_reason: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Walk minute evidence and carry incidents through gaps without fake recovery."""
    end = hour + timedelta(hours=1)
    prior = previous_report["vehicle_types"][mode] if previous_report else {}
    active = dict(prior["state"]["active"]) if prior.get("state", {}).get("active") else None
    reset_at = prior.get("state", {}).get("baseline_reset_at")
    previous_summary = summary_after_reset(previous_summary, reset_at)
    bad_tail = preceding_bad_tail(hour, mode, previous_summary, prior, config)
    good_tail = list(prior.get("state", {}).get("good_tail", [])) if previous_summary else []
    minutes = summary["vehicle_types"][mode]["minutes"] if summary else None
    by_minute = {item["minute"]: item for item in minutes or []}
    intervals = []
    monitoring, events = monitoring_transitions(hour, mode, summary, collection, prior.get("state", {}), config)
    if retire_legacy_incident(hour, mode, active, intervals, events):
        active, good_tail = None, []
    reasons = set()
    monitored_minutes = 0
    gaps = False
    has_bad_minutes = False
    partial = collection is not None and hour < collection < end
    for minute in range(MINUTES_PER_HOUR):
        at = hour + timedelta(minutes=minute)
        if collection is None or at + timedelta(minutes=1) <= collection:
            bad_tail, good_tail = [], []
            continue
        item = by_minute.get(minute)
        if not summary or not monitored(item, summary["poll_interval_seconds"], config):
            gaps = True
            bad_tail, good_tail = [], []
            continue
        monitored_minutes += 1
        reason = bad_reason(item, expected["vehicles"][minute], config)
        if reason:
            has_bad_minutes = True
            good_tail = []
            bad_tail.append({"at": iso(at), "reason": reason})
            bad_tail = bad_tail[-config.duration_minutes :]
            if not active and len(bad_tail) >= config.duration_minutes:
                active = {"start_at": bad_tail[0]["at"], "reason": bad_tail[0]["reason"]}
                events.append(event(mode, active, "degraded", iso(at + timedelta(minutes=1))))
            if active:
                reasons.add(reason)
        else:
            bad_tail = []
            good_tail = (
                [*good_tail, iso(at)][-config.duration_minutes :]
                if recovery_evidence(item, active, expected["vehicles"][minute], config)
                else []
            )
            if active and len(good_tail) >= config.duration_minutes:
                intervals.append({**active, "end_at": good_tail[0]})
                events.append(event(mode, active, "recovered", good_tail[0]))
                active = None
    if active:
        intervals.append({**active, "end_at": iso(end)})
        reasons.add(active["reason"])
    if gaps:
        reasons.add(source_reason if summary is None else "insufficient_minute_coverage")
    if partial:
        reasons.add("collection_started_mid_hour")
    if intervals:
        reasons.update(item["reason"] for item in intervals)
    return {
        "status": mode_status(
            hour=hour,
            collection=collection,
            active=active,
            monitored_minutes=monitored_minutes,
            gaps=gaps,
            has_baseline=any(value is not None for value in expected["vehicles"]),
            positive_baseline=any(counts_for_fleet(value, config) for value in expected["vehicles"]),
            has_bad_minutes=has_bad_minutes,
            reasons=reasons,
        ),
        "reasons": sorted(reasons),
        "intervals": intervals,
        "monitored_minutes": monitored_minutes,
        "baseline_samples": expected["samples"],
        **metrics(minutes),
        "fresh": [
            minute_fresh(summary, by_minute.get(minute), hour + timedelta(minutes=minute), collection, config)
            for minute in range(MINUTES_PER_HOUR)
        ],
        "baseline": expected,
        "state": {
            "active": active,
            "bad_tail": bad_tail,
            "good_tail": good_tail,
            "baseline_reset_at": reset_at,
            "monitoring": monitoring,
        },
    }, events


def validate_reset_modes(value: object) -> tuple[str, ...]:
    """Only explicit, unique bus/tram labels authorize administrative rebaselining."""
    if not isinstance(value, (list, tuple)) or any(not isinstance(mode, str) or mode not in MODES for mode in value):
        raise ValueError("rebaseline_modes must contain only bus/tram labels")
    if len(value) != len(set(value)):
        raise ValueError("rebaseline_modes must be unique")
    return tuple(mode for mode in MODES if mode in value)


def rebaseline_prior_report(
    previous: dict[str, Any] | None,
    hour: datetime,
    modes: tuple[str, ...],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Explicit operator action clears fleet state, not history, and never claims recovery."""
    if not modes:
        return previous, []
    if previous is None:
        raise ValueError("rebaseline requires a durable active fleet incident")
    restored = deepcopy(previous)
    events = []
    for mode in modes:
        state = restored["vehicle_types"][mode]["state"]
        active = state["active"]
        if not active or active["reason"] not in {"low_fleet", "no_accepted"}:
            raise ValueError(f"rebaseline requires an active low_fleet/no_accepted incident for {mode}")
        events.append(event(mode, active, "rebaseline", iso(hour)))
        state.update(active=None, bad_tail=[], good_tail=[], baseline_reset_at=iso(hour))
    return restored, events


def evaluate(  # noqa: PLR0913
    hour: datetime,
    summary: dict[str, Any] | None,
    samples: list[dict[str, Any]],
    *,
    previous_summary: dict[str, Any] | None = None,
    previous_report: dict[str, Any] | None = None,
    collection_started_at: str | None = None,
    config: Config = DEFAULT_CONFIG,
    evaluated_at: datetime | None = None,
    source_reason: str = "summary_absent",
    reset_modes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Evaluate one closed UTC hour; inputs must already pass source validation."""
    if previous_report and timestamp(previous_report["hour_start"]) != hour - timedelta(hours=1):
        raise ValueError("previous report must be the preceding UTC hour")
    reset_modes = validate_reset_modes(reset_modes)
    previous_report, reset_events = rebaseline_prior_report(previous_report, hour, reset_modes)
    starts = [
        timestamp(value)
        for value in (
            collection_started_at,
            summary["collection_started_at"] if summary else None,
            previous_report.get("collection_started_at") if previous_report else None,
            previous_summary["collection_started_at"] if previous_summary else None,
        )
        if value
    ]
    collection = min(starts) if starts else None
    report = {
        "version": REPORT_VERSION,
        "evaluated_at": iso(evaluated_at or datetime.now(UTC)),
        "hour_start": iso(hour),
        "collection_started_at": iso(collection) if collection else None,
        "vehicle_types": {},
        "events": reset_events,
    }
    candidates = set(comparable_hours(hour, config))
    samples = [source for source in samples if timestamp(source["hour_start"]) in candidates]
    for mode in MODES:
        prior_state = previous_report["vehicle_types"][mode]["state"] if previous_report else {}
        row, events = evaluate_mode(
            hour,
            mode,
            summary,
            baseline(samples, mode, config, reset_at=prior_state.get("baseline_reset_at")),
            previous_summary,
            previous_report,
            collection,
            config,
            source_reason,
        )
        if mode in reset_modes:
            row["reasons"] = sorted({*row["reasons"], "baseline_reset"})
        report["vehicle_types"][mode] = row
        report["events"].extend(events)
    recent = list(previous_report.get("recent_intervals", [])) if previous_report else []
    for mode in MODES:
        for interval in report["vehicle_types"][mode]["intervals"]:
            recent = [old for old in recent if not (old["mode"] == mode and old["start_at"] == interval["start_at"])]
            recent.append({"mode": mode, **interval})
    report["recent_intervals"] = sorted(recent, key=lambda item: (item["start_at"], item["mode"]))[-48:]
    return report


FLEET_REASONS = {"low_fleet", "no_accepted"}
# stale_heavy is no longer produced; reports from before the fleet-based rules may still carry it.
LEGACY_REASONS = {"stale_heavy"}
INCIDENT_REASONS = {"api_failures", *FLEET_REASONS, *LEGACY_REASONS}
REPORT_REASONS = INCIDENT_REASONS | {
    "collection_not_confirmed",
    "before_collection",
    "recovery_unconfirmed",
    "no_positive_fleet_baseline",
    "summary_absent",
    "invalid_summary",
    "insufficient_minute_coverage",
    "collection_started_mid_hour",
    "monitoring_evaluation_gap",
    "baseline_reset",
}
STATUSES = {"healthy", "degraded", "warming_up", "partial", "monitoring_gap", "not_monitored"}


def validate_report_interval(value: dict[str, Any], end: datetime) -> None:
    """Check persisted interval structure before it crosses the public boundary."""
    if not isinstance(value, dict) or set(value) != {"start_at", "end_at", "reason"}:
        raise ValueError("invalid persisted poller report interval")
    if not isinstance(value["reason"], str) or value["reason"] not in INCIDENT_REASONS:
        raise ValueError("invalid persisted poller report reason")
    if not timestamp(value["start_at"]) <= timestamp(value["end_at"]) <= end:
        raise ValueError("invalid persisted poller report interval timestamps")


def validate_report_state(state: dict[str, Any], end: datetime) -> None:
    """Validate bounded, contiguous minute tails and any still-active incident."""
    fields = {"active", "bad_tail", "good_tail"}
    optional = {"baseline_reset_at", "monitoring"}
    if not isinstance(state, dict) or not fields <= set(state) <= fields | optional:
        raise ValueError("invalid persisted poller report state")
    reset_at = state.get("baseline_reset_at")
    if reset_at is not None:
        epoch = timestamp(reset_at)
        if epoch.minute or epoch.second or epoch.microsecond or epoch > end - timedelta(hours=1):
            raise ValueError("invalid persisted poller report baseline reset epoch")
    active = state["active"]
    if active is not None:
        if not isinstance(active, dict) or set(active) != {"start_at", "reason"}:
            raise ValueError("invalid persisted poller report active incident")
        validate_report_interval({**active, "end_at": iso(end)}, end)
    for field in ("bad_tail", "good_tail"):
        validate_report_tail(state[field], end, bad=field == "bad_tail")
    if state["bad_tail"] and state["good_tail"]:
        raise ValueError("invalid persisted poller report conflicting tails")
    if "monitoring" in state:
        validate_monitoring_state(state["monitoring"], end)


def validate_monitoring_state(state: dict[str, Any], end: datetime) -> None:
    """Legacy reports may omit this separate telemetry-loss transition state."""
    if not isinstance(state, dict) or set(state) != {"active", "gap_tail", "good_tail"}:
        raise ValueError("invalid persisted monitoring state")
    active = state["active"]
    if active is not None and timestamp(active) >= end:
        raise ValueError("invalid persisted monitoring start")
    for field in ("gap_tail", "good_tail"):
        validate_report_tail(state[field], end, bad=False)
    if state["gap_tail"] and state["good_tail"]:
        raise ValueError("invalid persisted monitoring conflicting tails")


def validate_report_tail(tail: list[Any], end: datetime, *, bad: bool) -> None:
    """A retained tail must consist of consecutive UTC minute evidence."""
    if not isinstance(tail, list) or len(tail) > MINUTES_PER_HOUR:
        raise ValueError("invalid persisted poller report minute tail")
    times = []
    for item in tail:
        if bad:
            if not isinstance(item, dict) or set(item) != {"at", "reason"}:
                raise ValueError("invalid persisted poller report bad minute")
            if not isinstance(item["reason"], str) or item["reason"] not in INCIDENT_REASONS:
                raise ValueError("invalid persisted poller report bad minute reason")
            at = timestamp(item["at"])
        else:
            at = timestamp(item)
        if at.second or at.microsecond or not end - timedelta(hours=1) <= at < end:
            raise ValueError("invalid persisted poller report tail timestamp")
        times.append(at)
    if any(right - left != timedelta(minutes=1) for left, right in pairwise(times)):
        raise ValueError("invalid persisted poller report nonconsecutive tail")


def validate_report_mode(row: dict[str, Any], end: datetime) -> None:  # noqa: C901 - explicit persisted schema checks
    """Check snapshot fields, baseline metadata and private transition state."""
    if not isinstance(row, dict) or set(row) != {*PUBLIC_MODE_FIELDS, "fresh", "baseline", "state"}:
        raise ValueError("invalid persisted poller report mode fields")
    if not isinstance(row["status"], str) or row["status"] not in STATUSES:
        raise ValueError("invalid persisted poller report status")
    if not isinstance(row["reasons"], list) or any(
        not isinstance(value, str) or value not in REPORT_REASONS for value in row["reasons"]
    ):
        raise ValueError("invalid persisted poller report reasons")
    for field, maximum in (("monitored_minutes", MINUTES_PER_HOUR), ("baseline_samples", MAXIMUM_SAMPLES)):
        if type(row[field]) is not int or not 0 <= row[field] <= maximum:
            raise ValueError("invalid persisted poller report coverage")
    validate_report_metrics(row)
    intervals = row["intervals"]
    if not isinstance(intervals, list) or len(intervals) > MINUTES_PER_HOUR:
        raise ValueError("invalid persisted poller report intervals")
    for interval in intervals:
        validate_report_interval(interval, end)
    expected = row["baseline"]
    if not isinstance(expected, dict) or set(expected) != {"samples", "vehicles", "low", "high"}:
        raise ValueError("invalid persisted poller report baseline")
    if type(expected["samples"]) is not int or expected["samples"] != row["baseline_samples"]:
        raise ValueError("invalid persisted poller report baseline samples")
    for series in (row["fresh"], expected["vehicles"], expected["low"], expected["high"]):
        validate_minute_series(series)
    validate_report_state(row["state"], end)


def validate_minute_series(series: object) -> None:
    """Sixty nullable, finite, bounded per-minute fleet values."""
    if not isinstance(series, list) or len(series) != MINUTES_PER_HOUR:
        raise ValueError("invalid persisted poller report minute series")
    for value in series:
        if value is not None and (
            type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= MAX_COUNTER
        ):
            raise ValueError("invalid persisted poller report minute value")


def validate_report_metrics(row: dict[str, Any]) -> None:
    """Keep absent-summary metrics nullable; enforce invariants for observed counts."""
    for field in METRIC_FIELDS:
        value = row[field]
        if value is not None and (
            type(value) not in (int, float)
            or not 0 <= value <= MAX_COUNTER * MINUTES_PER_HOUR
            or (not field.startswith("mean_") and type(value) is not int)
        ):
            raise ValueError("invalid persisted poller report metric")
    if row["parsed_rows"] is None:
        if any(row[field] is not None for field in METRIC_FIELDS):
            raise ValueError("invalid persisted poller report absent metrics")
    elif any(row[field] is None for field in METRIC_FIELDS[:4]) or row["parsed_rows"] != sum(
        row[field] for field in METRIC_FIELDS[1:4]
    ):
        raise ValueError("invalid persisted poller report count invariant")


def upgrade_report(report: dict[str, Any]) -> dict[str, Any]:
    """Read a report from the stale-share rules (v1) as v2 with unknown minute series.

    Its hourly baseline is not comparable with per-minute baselines, so the
    preceding-hour recheck finds no fleet loss in a v1 predecessor; only
    api_failures can carry a bad tail across the deploy. Incidents and the
    alert outbox survive unchanged.
    """
    if report.get("version") != 1 or not isinstance(report.get("vehicle_types"), dict):
        return report
    upgraded = deepcopy(report)
    upgraded["version"] = REPORT_VERSION
    for row in upgraded["vehicle_types"].values():
        if isinstance(row, dict):
            row["baseline"] = {**empty_baseline(), "samples": row.get("baseline_samples")}
            row["fresh"] = [None] * MINUTES_PER_HOUR
    return upgraded


def validate_report(report: dict[str, Any], hour: datetime) -> None:
    """Reject corrupt persisted JSON with ValueError, not incidental type/key errors."""
    fields = {
        "version",
        "evaluated_at",
        "hour_start",
        "collection_started_at",
        "vehicle_types",
        "events",
        "recent_intervals",
    }
    if (
        set(report) != fields
        or type(report["version"]) is not int
        or report["version"] != REPORT_VERSION
        or timestamp(report["hour_start"]) != hour
    ):
        raise ValueError("invalid persisted poller report header")
    timestamp(report["evaluated_at"])
    if report["collection_started_at"] is not None:
        timestamp(report["collection_started_at"])
    end = hour + timedelta(hours=1)
    modes = report["vehicle_types"]
    if not isinstance(modes, dict) or set(modes) != set(MODES):
        raise ValueError("invalid persisted poller report vehicle types")
    for mode in MODES:
        validate_report_mode(modes[mode], end)
    recent = report["recent_intervals"]
    if not isinstance(recent, list) or len(recent) > MAX_RECENT_INTERVALS:
        raise ValueError("invalid persisted poller report recent intervals")
    for interval in recent:
        if (
            not isinstance(interval, dict)
            or set(interval) != {"mode", "start_at", "end_at", "reason"}
            or not isinstance(interval["mode"], str)
            or interval["mode"] not in MODES
        ):
            raise ValueError("invalid persisted poller report recent interval")
        validate_report_interval({key: value for key, value in interval.items() if key != "mode"}, end)
    validate_report_events(report["events"], end)


def validate_report_events(events: list[dict[str, Any]], end: datetime) -> None:
    """Bound one report's outbox and reject duplicate identities within it."""
    if not isinstance(events, list) or len(events) > MAX_REPORT_EVENTS:
        raise ValueError("invalid persisted poller report events")
    seen = set()
    for entry in events:
        validate_report_event(entry, end)
        if entry["event_id"] in seen:
            raise ValueError("invalid persisted poller report duplicate event_id")
        seen.add(entry["event_id"])


def validate_report_event(entry: dict[str, Any], end: datetime) -> None:
    """Validate the outbox before indexing it or sending any private report fields."""
    fields = {"event_id", "mode", "transition", "start_at", "at", "reason", "delivered_at"}
    if (
        not isinstance(entry, dict)
        or set(entry) not in (fields, fields | {"logged_at"})
        or not isinstance(entry["mode"], str)
        or entry["mode"] not in MODES
    ):
        raise ValueError("invalid persisted poller report event")
    transition = entry["transition"]
    if not isinstance(transition, str) or transition not in {
        "degraded",
        "recovered",
        "rebaseline",
        "monitoring_gap",
        "monitoring_restored",
    }:
        raise ValueError("invalid persisted poller report transition")
    if transition in {"monitoring_gap", "monitoring_restored"}:
        if entry["reason"] != "monitoring_gap" or not timestamp(entry["start_at"]) <= timestamp(entry["at"]) <= end:
            raise ValueError("invalid persisted monitoring event")
    else:
        validate_report_interval({"start_at": entry["start_at"], "end_at": entry["at"], "reason": entry["reason"]}, end)
    if entry["transition"] == "rebaseline" and (
        timestamp(entry["at"]) != end - timedelta(hours=1) or entry["reason"] not in {"low_fleet", "no_accepted"}
    ):
        raise ValueError("invalid persisted poller report rebaseline event")
    expected_id = event(
        entry["mode"], {"start_at": entry["start_at"], "reason": entry["reason"]}, entry["transition"], entry["at"]
    )["event_id"]
    if entry["event_id"] != expected_id:
        raise ValueError("invalid persisted poller report event identity")
    if entry["delivered_at"] is not None:
        timestamp(entry["delivered_at"])
    if entry.get("logged_at") is not None:
        timestamp(entry["logged_at"])
        if entry["delivered_at"] is not None:
            raise ValueError("invalid persisted poller report conflicting alert acknowledgements")


def snapshot(report: dict[str, Any]) -> dict[str, Any]:
    """Public status boundary: never expose source blobs or private incident state."""
    return {
        **{key: report[key] for key in ("version", "evaluated_at", "hour_start", "collection_started_at")},
        "vehicle_types": {
            mode: {key: report["vehicle_types"][mode][key] for key in PUBLIC_MODE_FIELDS} for mode in MODES
        },
        "recent_intervals": report["recent_intervals"][-48:],
    }
