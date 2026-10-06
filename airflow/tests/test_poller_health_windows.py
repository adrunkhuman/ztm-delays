from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from .test_poller_health import HOUR, failing, health, row, samples, summary


def test_low_fleet_requires_baseline_and_honors_threshold() -> None:
    source = summary(vehicles=40)
    assert row(health.evaluate(HOUR, source, []))["status"] == "warming_up"
    report = health.evaluate(HOUR, source, samples())
    assert row(report)["reasons"] == ["low_fleet"]
    assert row(report)["status"] == "degraded"
    assert row(health.evaluate(HOUR, source, samples(), config=health.Config(threshold=0.3)))["status"] == "healthy"


def test_line_count_is_not_a_signal() -> None:
    assert row(health.evaluate(HOUR, summary(vehicles=100, lines=1), samples()))["status"] == "healthy"


def test_recovery_window_spans_hours_and_never_duplicates() -> None:
    earlier_hour = HOUR - timedelta(hours=1)
    bad = failing(earlier_hour)
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
    future = summary(HOUR + timedelta(days=7))
    report = health.evaluate(HOUR, summary(), [history[0], different, future])
    assert row(report)["baseline_samples"] == 1
    assert row(report)["status"] == "warming_up"


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


def test_fleet_incident_carried_through_quiet_hours_stays_degraded() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour, vehicles=0, lines=0)
    prior = health.evaluate(previous_hour, previous, samples(previous_hour))
    assert row(prior)["state"]["active"]["reason"] == "no_accepted"
    # Overnight the usual fleet is below the floor: normal-looking minutes prove nothing.
    quiet = summary(vehicles=5)
    night = health.evaluate(HOUR, quiet, samples(vehicles=5), previous_summary=previous, previous_report=prior)
    assert row(night)["status"] == "degraded"
    assert "recovery_unconfirmed" in row(night)["reasons"]
    assert row(night)["state"]["active"] == row(prior)["state"]["active"]
    assert row(night)["state"]["good_tail"] == []
    assert night["events"] == []
    next_hour = HOUR + timedelta(hours=1)
    recovered = health.evaluate(
        next_hour, summary(next_hour), samples(next_hour), previous_summary=quiet, previous_report=night
    )
    assert row(recovered)["status"] == "healthy"
    assert row(recovered)["intervals"][0]["end_at"] == health.iso(next_hour)
    assert [entry["transition"] for entry in recovered["events"]] == ["recovered", "recovered"]


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
@pytest.mark.parametrize("quiet_baseline", [False, True])
def test_api_or_legacy_incident_recovers_with_answered_polls_without_fleet_baseline(
    reason: str, quiet_baseline: bool
) -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = failing(previous_hour)
    prior = health.evaluate(previous_hour, previous, [])
    if reason == "stale_heavy":
        # Reports from the stale-share rules may still carry this incident.
        previous = summary(previous_hour, ratio=0.2)
        for mode in health.MODES:
            row(prior, mode)["state"].update(active={"start_at": health.iso(previous_hour), "reason": reason})
    assert row(prior)["state"]["active"]["reason"] == reason
    history = samples(vehicles=1) if quiet_baseline else []
    current = health.evaluate(
        HOUR, summary(vehicles=1, lines=1), history, previous_summary=previous, previous_report=prior
    )
    assert row(current)["status"] == ("healthy" if quiet_baseline else "warming_up")
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


def week(count: int, start: datetime = HOUR) -> datetime:
    """The same Warsaw wall-clock hour ``count`` weeks later, across DST changes."""
    return (start.astimezone(health.WARSAW) + timedelta(weeks=count)).astimezone(UTC)


def weekly_steps(initial_vehicles: int, later_vehicles: int) -> tuple[dict[str, Any], Any]:
    """Evaluate one slot a week, each report the (synthetic) predecessor of the next."""
    sources = {health.timestamp(source["hour_start"]): source for source in samples()}
    prior = health.evaluate(HOUR, summary(vehicles=initial_vehicles, lines=1), samples())
    sources[HOUR] = summary(vehicles=initial_vehicles, lines=1)
    assert row(prior)["state"]["active"]["reason"] in {"low_fleet", "no_accepted"}
    state = {"prior": prior}

    def step(hour: datetime, *, reset_modes: tuple[str, ...] = ()) -> dict[str, Any]:
        predecessor = copy.deepcopy(state["prior"])
        predecessor["hour_start"] = health.iso(hour - timedelta(hours=1))
        for mode in health.MODES:
            predecessor["vehicle_types"][mode]["state"].update(bad_tail=[], good_tail=[])
        candidates = health.comparable_hours(hour, health.Config())
        historical = [sources[candidate] for candidate in candidates if candidate in sources]
        source = summary(hour, vehicles=later_vehicles)
        result = health.evaluate(hour, source, historical, previous_report=predecessor, reset_modes=reset_modes)
        sources[hour], state["prior"] = source, result
        health.validate_report(result, hour)
        return result

    return state, step


def test_fleet_incident_recovers_once_new_weekly_samples_rebuild_the_baseline() -> None:
    # A 35-day collection gap expires every comparable sample; normal service
    # alone cannot prove recovery until three new same-weekday samples exist.
    _, step = weekly_steps(0, 100)
    for count in (5, 6, 7):
        current = step(week(count))
        assert row(current)["status"] == "warming_up"
        assert "recovery_unconfirmed" in row(current)["reasons"]
        assert row(current)["state"]["active"] is not None
        assert current["events"] == []
    recovered = step(week(8))
    assert row(recovered)["status"] == "healthy"
    assert row(recovered)["baseline_samples"] == 3
    assert [entry["transition"] for entry in recovered["events"]] == ["recovered", "recovered"]


@pytest.mark.parametrize("initial_vehicles", [0, 1])
def test_explicit_rebaseline_breaks_sub_floor_baseline_deadlock_without_recovery(initial_vehicles: int) -> None:
    # Service shrinks below the floor for good: once that becomes the usual fleet,
    # no minute can be judged, so the fleet incident can never recover by itself.
    state, step = weekly_steps(initial_vehicles, 5)
    for count in range(1, 6):
        current = step(week(count))
        assert row(current)["state"]["active"] is not None
        assert current["events"] == []
    assert row(current)["status"] == "degraded"
    assert "recovery_unconfirmed" in row(current)["reasons"]
    assert row(current)["baseline"]["vehicles"] == [5] * 60
    old_recent = copy.deepcopy(state["prior"]["recent_intervals"])
    reset_hour = week(6)
    reset = step(reset_hour, reset_modes=("bus",))
    assert [entry["transition"] for entry in reset["events"]] == ["rebaseline"]
    assert reset["events"][0]["at"] == health.iso(reset_hour)
    assert row(reset)["state"]["active"] is None
    assert row(reset)["state"]["baseline_reset_at"] == health.iso(reset_hour)
    assert row(reset)["status"] == "warming_up"
    assert row(reset, "tram")["state"]["active"] is not None
    assert row(reset)["intervals"] == []
    assert [entry for entry in reset["recent_intervals"] if entry["mode"] == "bus"] == [
        entry for entry in old_recent if entry["mode"] == "bus"
    ]
    for count in (1, 2, 3):
        current = step(week(count, reset_hour))
        assert row(current)["state"]["baseline_reset_at"] == health.iso(reset_hour)
        assert [entry for entry in current["events"] if entry["mode"] == "bus"] == []
        assert "baseline_reset" not in row(current)["reasons"]
        assert row(current)["status"] == ("healthy" if count == 3 else "warming_up")
    assert row(current)["baseline_samples"] == 3
    assert row(current)["baseline"]["vehicles"] == [5] * 60


def test_baseline_reset_epoch_excludes_pre_epoch_samples() -> None:
    epoch = health.iso(HOUR - timedelta(days=14))
    expected = health.baseline(samples(), "bus", health.Config(), reset_at=epoch)
    assert expected["samples"] == 2
    assert expected["vehicles"] == [None] * 60


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


def test_operator_rebaseline_requires_active_fleet_incident_not_api_failures() -> None:
    prior_hour = HOUR - timedelta(hours=1)
    api = health.evaluate(prior_hour, failing(prior_hour), [])
    assert row(api)["state"]["active"]["reason"] == "api_failures"
    with pytest.raises(ValueError, match="active low_fleet/no_accepted"):
        health.evaluate(HOUR, summary(), [], previous_report=api, reset_modes=("bus",))
    with pytest.raises(ValueError, match="durable active fleet"):
        health.evaluate(HOUR, summary(), [], reset_modes=("bus",))
