from __future__ import annotations

import copy
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "poller_health_under_test", Path(__file__).parents[1] / "dags/poller_health.py"
)
assert _SPEC
assert _SPEC.loader
health = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = health
_SPEC.loader.exec_module(health)
HOUR = datetime(2026, 10, 5, 12, tzinfo=UTC)
START = health.iso(HOUR - timedelta(days=28))


def summary(hour: datetime = HOUR, vehicles: int = 100, lines: int = 10, ratio: float = 1) -> dict[str, Any]:
    parsed = 1000 if vehicles else 0
    accepted = int(parsed * ratio)
    minutes = [
        {
            "minute": minute,
            "attempts": 12,
            "successes": 12,
            "parsed_rows": parsed * 12,
            "accepted_rows": accepted * 12,
            "dropped_stale_rows": (parsed - accepted) * 12,
            "dropped_future_rows": 0,
            "accepted_vehicle_count_sum": vehicles * 12 if accepted else 0,
            "accepted_line_count_sum": lines * 12 if accepted else 0,
        }
        for minute in range(60)
    ]
    return {
        "version": 1,
        "hour_start": health.iso(hour),
        "collection_started_at": START,
        "poll_interval_seconds": 5,
        "vehicle_types": {mode: {"minutes": copy.deepcopy(minutes)} for mode in health.MODES},
    }


def failing(hour: datetime = HOUR) -> dict[str, Any]:
    """Every poll failed: an API outage that needs no fleet history to detect."""
    source = summary(hour)
    for mode in health.MODES:
        for item in source["vehicle_types"][mode]["minutes"]:
            item.update(dict.fromkeys(health.COUNT_FIELDS[1:], 0))
    return source


def set_fleet(source: dict[str, Any], fleet: Any, modes: tuple[str, ...] = health.MODES) -> dict[str, Any]:
    """Give each minute the fresh fleet ``fleet(minute)``; row counts stay valid."""
    for mode in modes:
        for item in source["vehicle_types"][mode]["minutes"]:
            item["accepted_vehicle_count_sum"] = fleet(item["minute"]) * item["successes"]
    return source


def samples(hour: datetime = HOUR, vehicles: int = 100) -> list[dict[str, Any]]:
    return [summary(hour - timedelta(days=days), vehicles=vehicles) for days in (7, 14, 21)]


def row(report: dict[str, Any], mode: str = "bus") -> dict[str, Any]:
    return report["vehicle_types"][mode]


def test_healthy_median_baseline_and_sanitized_snapshot() -> None:
    history = samples()
    history[0] = summary(HOUR - timedelta(days=7), vehicles=900)
    report = health.evaluate(HOUR, summary(), history)
    assert row(report)["status"] == "healthy"
    assert row(report)["baseline"] == {"samples": 3, "vehicles": [100] * 60, "low": [100] * 60, "high": [900] * 60}
    assert row(report)["baseline_samples"] == 3
    assert row(report)["mean_accepted_vehicles"] == 100
    assert row(report)["fresh"] == [100] * 60
    report["private_hostname"] = "private-host"
    public = health.snapshot(report)
    assert set(public["vehicle_types"]["bus"]) == set(health.PUBLIC_MODE_FIELDS)
    assert "private-host" not in json.dumps(public)
    assert "state" not in public["vehicle_types"]["bus"]
    assert "fresh" not in public["vehicle_types"]["bus"]


def test_stale_share_is_not_a_signal() -> None:
    stale = summary(ratio=0.2)
    assert row(health.evaluate(HOUR, stale, samples()))["status"] == "healthy"
    report = health.evaluate(HOUR, stale, [])
    assert row(report)["status"] == "warming_up"
    assert row(report)["dropped_stale_rows"] == 576000
    assert report["events"] == []


def test_api_failures_need_no_fleet_history() -> None:
    report = health.evaluate(HOUR, failing(), [])
    assert row(report)["status"] == "degraded"
    assert row(report)["reasons"] == ["api_failures"]
    assert row(report)["baseline_samples"] == 0
    assert row(report)["fresh"] == [None] * 60
    assert [entry["transition"] for entry in report["events"]] == ["degraded", "degraded"]
    assert all(entry["reason"] == "api_failures" for entry in report["events"])


def test_zero_fleet_needs_a_usual_fleet_above_the_floor() -> None:
    zero = summary(vehicles=0, lines=0)
    assert row(health.evaluate(HOUR, zero, []))["status"] == "warming_up"
    # A baseline of zero is a quiet slot, not a missing one.
    assert row(health.evaluate(HOUR, zero, samples(vehicles=0)))["status"] == "healthy"
    report = health.evaluate(HOUR, zero, samples())
    assert row(report)["status"] == "degraded"
    assert row(report)["reasons"] == ["no_accepted"]
    assert row(report)["intervals"] == [
        {"start_at": health.iso(HOUR), "end_at": health.iso(HOUR + timedelta(hours=1)), "reason": "no_accepted"}
    ]


def test_low_overnight_service_is_not_a_daytime_outage() -> None:
    hour = HOUR.replace(hour=1)
    source = summary(hour, vehicles=2, lines=1)
    report = health.evaluate(hour, source, samples(hour, vehicles=2))
    assert row(report)["status"] == "healthy"
    assert report["events"] == []


def test_fewer_than_three_samples_warm_up_and_missing_minutes_are_gaps() -> None:
    assert row(health.evaluate(HOUR, summary(), samples()[:2]))["status"] == "warming_up"
    source = summary(vehicles=0, lines=0)
    source["vehicle_types"]["bus"]["minutes"] = []
    report = health.evaluate(HOUR, source, samples())
    assert row(report)["status"] == "monitoring_gap"
    assert row(report)["monitored_minutes"] == 0
    assert row(report)["intervals"] == []
    assert row(report)["fresh"] == [None] * 60
    assert row(report, "tram")["status"] == "degraded"


def test_rollout_no_source_before_collection_and_mid_hour_partial() -> None:
    report = health.evaluate(HOUR, None, [])
    assert row(report)["status"] == "not_monitored"
    assert row(report)["parsed_rows"] is None
    assert row(report)["mean_accepted_vehicles"] is None
    assert row(health.evaluate(HOUR, None, [], collection_started_at=START))["status"] == "monitoring_gap"
    assert (
        row(health.evaluate(HOUR, None, [], collection_started_at=health.iso(HOUR + timedelta(hours=1))))["status"]
        == "not_monitored"
    )
    source = summary()
    source["collection_started_at"] = health.iso(HOUR + timedelta(minutes=30, seconds=20))
    for mode in health.MODES:
        source["vehicle_types"][mode]["minutes"] = source["vehicle_types"][mode]["minutes"][30:]
    health.validate_summary(source, HOUR)
    partial = row(health.evaluate(HOUR, source, samples()))
    assert partial["status"] == "partial"
    assert partial["monitored_minutes"] == 30
    assert "collection_started_mid_hour" in partial["reasons"]
    assert partial["fresh"] == [None] * 30 + [100] * 30


def test_bad_minutes_require_coverage_and_consecutive_duration() -> None:
    source = failing()
    for mode in health.MODES:
        for item in source["vehicle_types"][mode]["minutes"]:
            if item["minute"] % 14 == 0:
                item["attempts"] = 9
    report = health.evaluate(HOUR, source, [])
    assert row(report)["status"] == "monitoring_gap"
    assert row(report)["intervals"] == []
    source = failing()
    for mode in health.MODES:
        source["vehicle_types"][mode]["minutes"] = source["vehicle_types"][mode]["minutes"][:14]
    report = health.evaluate(HOUR, source, [], config=health.Config(duration_minutes=10))
    assert row(report)["status"] == "degraded"
    assert row(report)["monitored_minutes"] == 14


def test_cross_hour_detection_repeated_degradation_and_sustained_recovery() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour)
    bad = failing(previous_hour)
    for mode in health.MODES:
        previous["vehicle_types"][mode]["minutes"][50:] = bad["vehicle_types"][mode]["minutes"][50:]
    previous_report = health.evaluate(previous_hour, previous, [])
    assert row(previous_report)["intervals"] == []
    current = failing()
    report = health.evaluate(HOUR, current, [], previous_summary=previous, previous_report=previous_report)
    assert row(report)["status"] == "degraded"
    incident_start = health.iso(previous_hour + timedelta(minutes=50))
    assert row(report)["intervals"][0]["start_at"] == incident_start
    next_hour = HOUR + timedelta(hours=1)
    next_summary = failing(next_hour)
    ongoing = health.evaluate(next_hour, next_summary, [], previous_summary=current, previous_report=report)
    assert row(ongoing)["status"] == "degraded"
    assert ongoing["events"] == []
    recovery_hour = next_hour + timedelta(hours=1)
    recovered = health.evaluate(
        recovery_hour,
        summary(recovery_hour),
        samples(recovery_hour),
        previous_summary=next_summary,
        previous_report=ongoing,
    )
    assert row(recovered)["status"] == "healthy"
    assert row(recovered)["intervals"][0]["end_at"] == health.iso(recovery_hour)
    assert all(
        entry["transition"] == "recovered" and entry["start_at"] == incident_start for entry in recovered["events"]
    )
    rerun = health.evaluate(
        recovery_hour,
        summary(recovery_hour),
        samples(recovery_hour),
        previous_summary=next_summary,
        previous_report=ongoing,
    )
    assert [entry["event_id"] for entry in rerun["events"]] == [entry["event_id"] for entry in recovered["events"]]
    following_hour = recovery_hour + timedelta(hours=1)
    following = health.evaluate(
        following_hour, summary(following_hour), [], previous_summary=summary(recovery_hour), previous_report=recovered
    )
    assert following["events"] == []
    assert len(following["recent_intervals"]) == 2


def test_gaps_do_not_recover_active_incident() -> None:
    previous = failing(HOUR - timedelta(hours=1))
    previous_report = health.evaluate(HOUR - timedelta(hours=1), previous, [])
    report = health.evaluate(HOUR, None, [], previous_summary=previous, previous_report=previous_report)
    assert row(report)["status"] == "monitoring_gap"
    assert row(report)["state"]["active"] is not None
    assert {entry["transition"] for entry in report["events"]} == {"monitoring_gap"}


def test_outage_weeks_and_unreliable_minutes_do_not_lower_the_baseline() -> None:
    history = samples()
    history[0] = summary(HOUR - timedelta(days=7), vehicles=10)
    expected = health.baseline(history, "bus", health.Config())
    assert expected == {"samples": 3, "vehicles": [100] * 60, "low": [100] * 60, "high": [100] * 60}
    assert row(health.evaluate(HOUR, summary(vehicles=40), history))["reasons"] == ["low_fleet"]
    history = samples()
    first = history[0]["vehicle_types"]["bus"]["minutes"]
    first[0]["successes"] = 9
    first[0]["accepted_vehicle_count_sum"] = 900
    first[1]["attempts"] = first[1]["successes"] = 9
    del first[2]
    expected = health.baseline(history, "bus", health.Config())
    assert expected["vehicles"][:4] == [None, None, None, 100]
    assert expected["samples"] == 3
    history = samples()
    history[0]["collection_started_at"] = health.iso(health.timestamp(history[0]["hour_start"]) + timedelta(minutes=1))
    assert health.baseline(history, "bus", health.Config())["vehicles"][:2] == [None, 100]


def test_dst_fold_and_nonexistent_hour_local_comparability() -> None:
    first = datetime(2026, 10, 25, 0, tzinfo=UTC)
    second = first + timedelta(hours=1)
    assert first.astimezone(health.WARSAW).hour == second.astimezone(health.WARSAW).hour == 2
    assert health.hour_path("hourly", first) != health.hour_path("hourly", second)
    assert health.comparable_hours(first, health.Config()) == health.comparable_hours(second, health.Config())
    candidates = health.comparable_hours(datetime(2026, 11, 1, 1, tzinfo=UTC), health.Config())
    assert first in candidates
    assert second in candidates
    historical = []
    for candidate in candidates:
        source = summary(candidate, vehicles=200 if candidate == second else 100)
        source["collection_started_at"] = health.iso(candidate - timedelta(days=30))
        historical.append(source)
    expected = health.baseline(historical, "bus", health.Config())
    assert expected["samples"] == 4
    assert expected["high"][0] == 150
    # Both folds are one local date: with a single other date there are only two samples.
    folds = [source for source in historical if health.timestamp(source["hour_start"]) in {first, second}]
    expected = health.baseline([*folds, historical[0]], "bus", health.Config())
    assert expected["samples"] == 2
    assert expected["vehicles"] == [None] * 60
    spring = health.comparable_hours(datetime(2026, 4, 5, 0, tzinfo=UTC), health.Config())
    assert len(spring) == 3
    assert all(candidate.astimezone(health.WARSAW).hour == 2 for candidate in spring)


def test_schedule_uses_interval_end_not_wall_clock() -> None:
    assert health.completed_hour(HOUR.replace(minute=25)) == HOUR - timedelta(hours=1)
    with pytest.raises(ValueError, match="aware"):
        health.completed_hour(HOUR.replace(tzinfo=None))
    with pytest.raises(ValueError, match="preceding"):
        health.evaluate(HOUR, summary(), [], previous_report=health.evaluate(HOUR, summary(), []))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data.update(version=2),
        lambda data: data.update(version=True),
        lambda data: data.update(hour_start="2026-10-05T12:00:00"),
        lambda data: data.update(hour_start="2026-10-05T12:00:01Z"),
        lambda data: data.update(collection_started_at=health.iso(HOUR + timedelta(hours=1))),
        lambda data: data.update(poll_interval_seconds=0),
        lambda data: data.update(poll_interval_seconds=float("nan")),
        lambda data: data["vehicle_types"].pop("tram"),
        lambda data: data["vehicle_types"]["bus"]["minutes"].append(data["vehicle_types"]["bus"]["minutes"][0]),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(minute=True),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(minute=60),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(attempts=-1),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(successes=13),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(accepted_rows=1),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(successes=0),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(attempts=health.MAX_COUNTER + 1),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(accepted_line_count_sum=999999),
    ],
)
def test_malformed_contract_rejected(mutation: Any) -> None:
    source = summary()
    mutation(source)
    with pytest.raises(ValueError, match=r".+"):
        health.validate_summary(source, HOUR)


@pytest.mark.parametrize("payload", [b'{"version":1,"version":1}', b'{"x":NaN}', b"[]", b"\xff", b"{" * 2000])
def test_json_boundary_rejects_malformed_payloads(payload: bytes) -> None:
    with pytest.raises(ValueError, match=r".+"):
        health.decode_json(payload, health.SUMMARY_MAX_BYTES)


def test_json_payload_bounded_and_recent_intervals_bounded() -> None:
    with pytest.raises(ValueError, match="large"):
        health.decode_json(b" " * (health.SUMMARY_MAX_BYTES + 1), health.SUMMARY_MAX_BYTES)
    previous = health.evaluate(HOUR - timedelta(hours=1), summary(HOUR - timedelta(hours=1)), [])
    previous["recent_intervals"] = [
        {
            "mode": "bus",
            "start_at": health.iso(HOUR - timedelta(hours=100 - i)),
            "end_at": health.iso(HOUR),
            "reason": "api_failures",
        }
        for i in range(80)
    ]
    assert len(health.evaluate(HOUR, summary(), [], previous_report=previous)["recent_intervals"]) == 48
