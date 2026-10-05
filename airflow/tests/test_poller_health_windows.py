from __future__ import annotations

import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from .test_poller_health import HOUR, health, row, samples, summary


def test_low_vehicle_or_line_counts_require_positive_baseline() -> None:
    source = summary(vehicles=40, lines=10)
    assert row(health.evaluate(HOUR, source, []))["status"] == "warming_up"
    report = health.evaluate(HOUR, source, samples())
    assert row(report)["reasons"] == ["low_fleet"]
    assert row(report)["status"] == "degraded"
    assert row(health.evaluate(HOUR, summary(vehicles=100, lines=4), samples()))["status"] == "degraded"
    assert row(health.evaluate(HOUR, source, samples(), config=health.Config(threshold=0.3)))["status"] == "healthy"


def test_fresh_ratio_baseline_median_and_only_parsed_samples() -> None:
    history = samples()
    for source, _report in history:
        for item in source["vehicle_types"]["bus"]["minutes"]:
            item["accepted_rows"] = 10800
            item["dropped_stale_rows"] = 1200
    expected = health.baseline(history, "bus", health.Config())
    assert expected["ratio"] == 0.9
    assert row(health.evaluate(HOUR, summary(vehicles=40, lines=4, ratio=0.4), history))["status"] == "degraded"
    no_rows = samples(vehicles=0, lines=0)
    assert health.baseline(no_rows, "bus", health.Config())["ratio"] is None


def test_recovery_window_spans_hours_and_never_duplicates() -> None:
    earlier_hour = HOUR - timedelta(hours=1)
    bad = summary(earlier_hour, ratio=0.2, vehicles=20, lines=2)
    good = summary(earlier_hour)
    for mode in health.MODES:
        bad["vehicle_types"][mode]["minutes"][50:] = good["vehicle_types"][mode]["minutes"][50:]
    earlier = health.evaluate(earlier_hour, bad, [])
    assert row(earlier)["status"] == "degraded"
    assert len(row(earlier)["state"]["good_tail"]) == 10
    current = health.evaluate(HOUR, summary(), samples(), previous_summary=bad, previous_report=earlier)
    assert row(current)["status"] == "healthy"
    assert row(current)["intervals"][0]["end_at"] == health.iso(earlier_hour + timedelta(minutes=50))
    assert [entry["transition"] for entry in current["events"]] == ["recovered", "recovered"]


def test_unknown_previous_fleet_hour_does_not_extend_low_count_window() -> None:
    preceding_hour = HOUR - timedelta(hours=1)
    preceding = summary(preceding_hour, vehicles=0, lines=0)
    preceding_report = health.evaluate(preceding_hour, preceding, [])
    current = summary()
    zero = summary(vehicles=0, lines=0)
    for mode in health.MODES:
        current["vehicle_types"][mode]["minutes"][:5] = zero["vehicle_types"][mode]["minutes"][:5]
    report = health.evaluate(HOUR, current, samples(), previous_summary=preceding, previous_report=preceding_report)
    assert row(report)["status"] == "healthy"
    assert not report["events"]


def test_noncomparable_future_and_different_weekday_samples_excluded() -> None:
    history = samples()
    different = summary(HOUR - timedelta(days=1))
    different_report = health.evaluate(HOUR - timedelta(days=1), different, [])
    future = summary(HOUR + timedelta(days=7))
    future_report = health.evaluate(HOUR + timedelta(days=7), future, [])
    report = health.evaluate(HOUR, summary(), [history[0], (different, different_report), (future, future_report)])
    assert row(report)["baseline_samples"] == 1
    assert row(report)["status"] == "warming_up"


def test_unsuccessful_or_partial_history_not_comparable() -> None:
    history = samples()
    source = history[0][0]
    source["collection_started_at"] = health.iso(health.timestamp(source["hour_start"]) + timedelta(minutes=1))
    assert health.baseline(history, "bus", health.Config())["samples"] == 2
    history = samples()
    history[0][0]["vehicle_types"]["bus"]["minutes"][0]["successes"] = 9
    assert health.baseline(history, "bus", health.Config())["samples"] == 2


def test_contract_matches_summary_shape_and_zero_attempt_minute_is_a_gap() -> None:
    source = summary()
    contract = health.decode_json(
        (Path(__file__).parents[2] / "contracts/poller_health_v1.json").read_bytes(), health.SUMMARY_MAX_BYTES
    )
    assert set(contract["hour_fields"]) == set(source)
    assert set(contract["minute_fields"]) == {"minute", *health.COUNT_FIELDS}
    assert set(contract["mode_fields"]) == {"minutes"}
    source["poll_interval_seconds"] = 0.5
    health.validate_summary(source, HOUR)
    source = summary()
    for key in health.COUNT_FIELDS:
        source["vehicle_types"]["bus"]["minutes"][0][key] = 0
    health.validate_summary(source, HOUR)
    assert row(health.evaluate(HOUR, source, samples()))["status"] == "monitoring_gap"


def test_duplicate_observed_minute_and_minute_before_collection_rejected() -> None:
    source = summary()
    source["vehicle_types"]["bus"]["minutes"][1] = copy.deepcopy(source["vehicle_types"]["bus"]["minutes"][0])
    with pytest.raises(ValueError, match="duplicate"):
        health.validate_summary(source, HOUR)
    source = summary()
    source["collection_started_at"] = health.iso(HOUR + timedelta(minutes=1))
    with pytest.raises(ValueError, match="precedes"):
        health.validate_summary(source, HOUR)


def test_empty_success_without_fleet_history_does_not_fabricate_recovery() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour, ratio=0.2, vehicles=20, lines=2)
    previous_report = health.evaluate(previous_hour, previous, [])
    empty = summary(vehicles=0, lines=0)
    unknown = health.evaluate(HOUR, empty, [], previous_summary=previous, previous_report=previous_report)
    assert row(unknown)["status"] == "warming_up"
    assert "recovery_unconfirmed" in row(unknown)["reasons"]
    assert row(unknown)["state"]["active"] is not None
    assert not unknown["events"]
    next_hour = HOUR + timedelta(hours=1)
    recovered = health.evaluate(next_hour, summary(next_hour), [], previous_summary=empty, previous_report=unknown)
    assert row(recovered)["state"]["active"] is None
    assert all(entry["transition"] == "recovered" for entry in recovered["events"])


@pytest.mark.parametrize("previous_vehicles", [0, 1])
@pytest.mark.parametrize("prior_good_minutes", [0, 10])
def test_fleet_recovery_needs_applicable_new_hour_baseline(previous_vehicles: int, prior_good_minutes: int) -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour, vehicles=previous_vehicles, lines=min(previous_vehicles, 1))
    if prior_good_minutes:
        good = summary(previous_hour)
        for mode in health.MODES:
            previous["vehicle_types"][mode]["minutes"][-prior_good_minutes:] = good["vehicle_types"][mode]["minutes"][
                -prior_good_minutes:
            ]
    prior = health.evaluate(previous_hour, previous, samples(previous_hour))
    assert row(prior)["state"]["active"]["reason"] in {"low_fleet", "no_accepted"}
    one_vehicle = summary(vehicles=1, lines=1)
    unknown = health.evaluate(HOUR, one_vehicle, [], previous_summary=previous, previous_report=prior)
    assert row(unknown)["status"] == "warming_up"
    assert "recovery_unconfirmed" in row(unknown)["reasons"]
    assert row(unknown)["state"]["active"] == row(prior)["state"]["active"]
    assert not row(unknown)["state"]["good_tail"]
    assert not unknown["events"]
    next_hour = HOUR + timedelta(hours=1)
    recovered = health.evaluate(
        next_hour, summary(next_hour), samples(next_hour), previous_summary=one_vehicle, previous_report=unknown
    )
    assert row(recovered)["status"] == "healthy"
    assert row(recovered)["state"]["active"] is None
    assert row(recovered)["intervals"][0]["end_at"] == health.iso(next_hour)
    assert row(recovered)["intervals"][0]["reason"] == row(prior)["state"]["active"]["reason"]
    assert row(recovered)["intervals"][0]["reason"] in row(recovered)["reasons"]
    assert all(entry["transition"] == "recovered" for entry in recovered["events"])
    health.validate_report(unknown, HOUR)
    health.validate_report(recovered, next_hour)


@pytest.mark.parametrize("reason", ["stale_heavy", "api_failures"])
def test_freshness_or_api_failure_recovers_with_fresh_replies_without_fleet_baseline(reason: str) -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour, vehicles=1, lines=1, ratio=0.2)
    if reason == "api_failures":
        for mode in health.MODES:
            for item in previous["vehicle_types"][mode]["minutes"]:
                for field in health.COUNT_FIELDS[1:]:
                    item[field] = 0
    prior = health.evaluate(previous_hour, previous, [])
    assert row(prior)["state"]["active"]["reason"] == reason
    current = health.evaluate(HOUR, summary(vehicles=1, lines=1), [], previous_summary=previous, previous_report=prior)
    assert row(current)["status"] == "warming_up"
    assert row(current)["state"]["active"] is None
    assert row(current)["intervals"][0]["end_at"] == health.iso(HOUR)
    assert row(current)["intervals"][0]["reason"] == reason
    assert reason in row(current)["reasons"]
    assert all(entry["transition"] == "recovered" for entry in current["events"])
    health.validate_report(current, HOUR)


@pytest.mark.parametrize("successes", [1, 5, 6, 12])
def test_sustained_low_request_success_ratio_is_not_healthy(successes: int) -> None:
    source = summary()
    for mode in health.MODES:
        for item in source["vehicle_types"][mode]["minutes"]:
            item["successes"] = successes
            for field in health.COUNT_FIELDS[2:]:
                item[field] = item[field] // 12 * successes
    health.validate_summary(source, HOUR)
    report = health.evaluate(HOUR, source, samples())
    if successes < 6:
        assert row(report)["status"] == "degraded"
        assert row(report)["reasons"] == ["api_failures"]
        assert all(entry["reason"] == "api_failures" for entry in report["events"])
    else:
        assert row(report)["status"] == "healthy"
        assert report["events"] == []


def test_request_success_ratio_honors_configured_threshold_and_minute_duration() -> None:
    source = summary()
    for mode in health.MODES:
        for item in source["vehicle_types"][mode]["minutes"][:14]:
            item["successes"] = 1
            for field in health.COUNT_FIELDS[2:]:
                item[field] //= 12
    report = health.evaluate(HOUR, source, samples())
    assert row(report)["status"] == "healthy"
    assert report["events"] == []
    for mode in health.MODES:
        for item in source["vehicle_types"][mode]["minutes"][14:]:
            item["successes"] = 1
            for field in health.COUNT_FIELDS[2:]:
                item[field] //= 12
    permissive = health.evaluate(HOUR, source, samples(), config=health.Config(threshold=0.05))
    assert row(permissive)["status"] == "healthy"
    assert permissive["events"] == []


@pytest.mark.parametrize("initial_vehicles", [0, 1])
def test_explicit_rebaseline_breaks_expired_fleet_baseline_deadlock_without_recovery(initial_vehicles: int) -> None:
    sources = {}
    reports = {}
    for source, report in samples():
        hour = health.timestamp(source["hour_start"])
        sources[hour], reports[hour] = source, report
    initial_source = summary(vehicles=initial_vehicles, lines=min(initial_vehicles, 1))
    prior = health.evaluate(HOUR, initial_source, samples())
    sources[HOUR], reports[HOUR] = initial_source, prior
    assert row(prior)["state"]["active"]["reason"] in {"low_fleet", "no_accepted"}

    def step(hour: datetime, *, reset_modes: tuple[str, ...] = ()) -> dict[str, Any]:
        nonlocal prior
        predecessor = copy.deepcopy(prior)
        predecessor["hour_start"] = health.iso(hour - timedelta(hours=1))
        for mode in health.MODES:
            predecessor["vehicle_types"][mode]["state"].update(bad_tail=[], good_tail=[])
        candidates = health.comparable_hours(hour, health.Config())
        historical = [(sources[candidate], reports[candidate]) for candidate in candidates if candidate in reports]
        source = summary(hour)
        result = health.evaluate(hour, source, historical, previous_report=predecessor, reset_modes=reset_modes)
        sources[hour], reports[hour], prior = source, result, result
        health.validate_report(result, hour)
        return result

    # A 35-day gap expires all old comparable samples; 22 fresh daily slots
    # cannot clear the fleet incident automatically or qualify as clean history.
    for day in range(35, 57):
        current = step(HOUR + timedelta(days=day))
        assert row(current)["status"] == "warming_up"
        assert row(current)["state"]["active"] is not None
        assert current["events"] == []
        assert row(current)["baseline_samples"] == 0
    old_recent = copy.deepcopy(prior["recent_intervals"])
    reset_hour = HOUR + timedelta(days=57)
    reset = step(reset_hour, reset_modes=("bus",))
    assert [entry["transition"] for entry in reset["events"]] == ["rebaseline"]
    assert reset["events"][0]["at"] == health.iso(reset_hour)
    assert row(reset)["state"]["active"] is None
    assert row(reset)["state"]["baseline_reset_at"] == health.iso(reset_hour)
    assert row(reset, "tram")["state"]["active"] is not None
    assert row(reset)["intervals"] == []
    assert [entry for entry in reset["recent_intervals"] if entry["mode"] == "bus"] == [
        entry for entry in old_recent if entry["mode"] == "bus"
    ]
    for day in range(1, 22):
        current = step(reset_hour + timedelta(days=day))
        assert row(current)["state"]["baseline_reset_at"] == health.iso(reset_hour)
        assert current["events"] == []
        assert "baseline_reset" not in row(current)["reasons"]
        assert row(current)["status"] == ("healthy" if day == 21 else "warming_up")
    assert row(current)["baseline_samples"] == 3
    assert row(current)["baseline"]["vehicles"] == 100


def test_baseline_reset_epoch_excludes_pre_epoch_samples() -> None:
    epoch = health.iso(HOUR - timedelta(days=14))
    expected = health.baseline(samples(), "bus", health.Config(), reset_at=epoch)
    assert expected["samples"] == 2
    assert expected["vehicles"] is None


def test_legacy_state_and_rebaseline_event_validation() -> None:
    prior_hour = HOUR - timedelta(hours=1)
    source = summary(prior_hour, vehicles=1, lines=1)
    prior = health.evaluate(prior_hour, source, samples(prior_hour))
    for mode in health.MODES:
        prior["vehicle_types"][mode]["state"].pop("baseline_reset_at")
    health.validate_report(prior, prior_hour)
    original = copy.deepcopy(prior)
    reset = health.evaluate(HOUR, summary(), [], previous_report=prior, previous_summary=source, reset_modes=("bus",))
    health.validate_report(reset, HOUR)
    assert prior == original
    assert row(reset)["state"]["baseline_reset_at"] == health.iso(HOUR)
    reset["events"][0]["at"] = health.iso(HOUR + timedelta(minutes=1))
    with pytest.raises(ValueError, match="rebaseline event"):
        health.validate_report(reset, HOUR)


def test_operator_rebaseline_requires_active_fleet_incident_not_transport_or_freshness() -> None:
    prior_hour = HOUR - timedelta(hours=1)
    stale = health.evaluate(prior_hour, summary(prior_hour, ratio=0.2, vehicles=20, lines=2), [])
    with pytest.raises(ValueError, match="active low_fleet/no_accepted"):
        health.evaluate(HOUR, summary(), [], previous_report=stale, reset_modes=("bus",))
    with pytest.raises(ValueError, match="durable active fleet"):
        health.evaluate(HOUR, summary(), [], reset_modes=("bus",))
