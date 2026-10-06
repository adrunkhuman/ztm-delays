from __future__ import annotations

import copy
from datetime import timedelta
from typing import Any

import pytest

from .test_poller_health import HOUR, START, failing, health, row, samples, summary


def missing_minutes(source: dict[str, Any], start: int, end: int) -> None:
    for mode in health.MODES:
        source["vehicle_types"][mode]["minutes"] = [
            item for item in source["vehicle_types"][mode]["minutes"] if not start <= item["minute"] < end
        ]


def transitions(report: dict[str, Any], mode: str = "bus") -> list[str]:
    return [entry["transition"] for entry in report["events"] if entry["mode"] == mode]


@pytest.mark.parametrize("source_kind", ["absent", "insufficient"])
def test_sustained_telemetry_loss_alerts_without_claiming_zero_feed(source_kind: str) -> None:
    source = None
    if source_kind == "insufficient":
        source = summary()
        for mode in health.MODES:
            for item in source["vehicle_types"][mode]["minutes"]:
                for field in health.COUNT_FIELDS:
                    item[field] //= 6
        health.validate_summary(source, HOUR)
    report = health.evaluate(HOUR, source, samples(), collection_started_at=START)
    health.validate_report(report, HOUR)
    assert row(report)["status"] == "monitoring_gap"
    assert row(report)["intervals"] == []
    assert row(report)["state"]["active"] is None
    assert transitions(report) == ["monitoring_gap"]
    assert all(entry["reason"] == "monitoring_gap" for entry in report["events"])
    assert all(entry["at"] == health.iso(HOUR + timedelta(minutes=15)) for entry in report["events"])


@pytest.mark.parametrize("gap_minutes", [14, 15])
def test_transient_gap_is_quiet_and_sustained_gap_has_separate_restoration(gap_minutes: int) -> None:
    source = summary()
    missing_minutes(source, 20, 20 + gap_minutes)
    report = health.evaluate(HOUR, source, [])
    health.validate_report(report, HOUR)
    assert transitions(report) == ([] if gap_minutes == 14 else ["monitoring_gap", "monitoring_restored"])
    assert row(report)["state"]["monitoring"]["active"] is None
    assert row(report)["intervals"] == []


def test_gap_duration_and_restoration_carry_across_consecutive_report_hours() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour)
    missing_minutes(previous, 50, 60)
    prior = health.evaluate(previous_hour, previous, [])
    assert prior["events"] == []
    current = summary()
    missing_minutes(current, 0, 5)
    report = health.evaluate(HOUR, current, [], previous_summary=previous, previous_report=prior)
    health.validate_report(report, HOUR)
    assert transitions(report) == ["monitoring_gap", "monitoring_restored"]
    assert {entry["start_at"] for entry in report["events"]} == {health.iso(previous_hour + timedelta(minutes=50))}
    assert {entry["at"] for entry in report["events"]} == {health.iso(HOUR + timedelta(minutes=5))}


def test_missing_hours_do_not_repeat_alert_and_restoration_is_not_feed_recovery() -> None:
    prior = health.evaluate(HOUR, None, [], collection_started_at=START)
    next_hour = HOUR + timedelta(hours=1)
    ongoing = health.evaluate(next_hour, None, [], previous_report=prior)
    health.validate_report(ongoing, next_hour)
    assert ongoing["events"] == []
    restored_hour = next_hour + timedelta(hours=1)
    restored = health.evaluate(restored_hour, summary(restored_hour), [], previous_report=ongoing)
    health.validate_report(restored, restored_hour)
    assert transitions(restored) == ["monitoring_restored"]
    assert row(restored)["status"] == "warming_up"
    assert row(restored)["intervals"] == []
    assert "monitoring" not in health.snapshot(restored)["vehicle_types"]["bus"]
    repeated = health.evaluate(restored_hour, summary(restored_hour), [], previous_report=ongoing)
    assert [entry["event_id"] for entry in repeated["events"]] == [entry["event_id"] for entry in restored["events"]]


def test_restored_monitoring_does_not_clear_an_active_feed_incident() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = failing(previous_hour)
    prior = health.evaluate(previous_hour, previous, [])
    gap = health.evaluate(HOUR, None, [], previous_report=prior, previous_summary=previous)
    next_hour = HOUR + timedelta(hours=1)
    restored = health.evaluate(next_hour, failing(next_hour), [], previous_report=gap)
    health.validate_report(restored, next_hour)
    assert transitions(restored) == ["monitoring_restored"]
    assert row(restored)["status"] == "degraded"
    assert row(restored)["state"]["active"] == row(prior)["state"]["active"]


def test_monitoring_restoration_duration_can_span_hour_boundary() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour)
    missing_minutes(previous, 0, 50)
    prior = health.evaluate(previous_hour, previous, [])
    assert transitions(prior) == ["monitoring_gap"]
    report = health.evaluate(HOUR, summary(), [], previous_report=prior, previous_summary=previous)
    health.validate_report(report, HOUR)
    assert transitions(report) == ["monitoring_restored"]
    assert {entry["at"] for entry in report["events"]} == {health.iso(previous_hour + timedelta(minutes=50))}


def test_monitoring_restoration_does_not_require_api_success() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    prior = health.evaluate(previous_hour, None, [], collection_started_at=START)
    current = summary()
    for mode in health.MODES:
        for item in current["vehicle_types"][mode]["minutes"]:
            item["successes"] = 0
            for field in health.COUNT_FIELDS[2:]:
                item[field] = 0
    health.validate_summary(current, HOUR)
    report = health.evaluate(HOUR, current, [], previous_report=prior)
    health.validate_report(report, HOUR)
    assert set(transitions(report)) == {"monitoring_restored", "degraded"}
    assert row(report)["status"] == "degraded"
    assert row(report)["state"]["active"]["reason"] == "api_failures"


def test_collection_not_confirmed_and_before_rollout_never_alert() -> None:
    for collection in (None, health.iso(HOUR + timedelta(hours=1))):
        report = health.evaluate(HOUR, None, [], collection_started_at=collection)
        health.validate_report(report, HOUR)
        assert report["events"] == []
    report = health.evaluate(HOUR, None, [], collection_started_at=health.iso(HOUR + timedelta(minutes=46, seconds=20)))
    health.validate_report(report, HOUR)
    assert report["events"] == []


def test_legacy_report_state_remains_readable() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    source = summary(previous_hour)
    prior = health.evaluate(previous_hour, source, [])
    for mode in health.MODES:
        prior["vehicle_types"][mode]["state"].pop("monitoring")
    health.validate_report(prior, previous_hour)
    report = health.evaluate(HOUR, summary(), [], previous_report=prior, previous_summary=source)
    health.validate_report(report, HOUR)
    assert report["events"] == []


@pytest.mark.parametrize("corruption", ["both_tails", "future_active", "bad_event_reason", "bad_event_time"])
def test_monitoring_state_and_event_validation_rejects_corruption(corruption: str) -> None:
    report = health.evaluate(HOUR, None, [], collection_started_at=START)
    report = copy.deepcopy(report)
    if corruption == "both_tails":
        row(report)["state"]["monitoring"]["good_tail"] = [health.iso(HOUR + timedelta(minutes=59))]
    elif corruption == "future_active":
        row(report)["state"]["monitoring"]["active"] = health.iso(HOUR + timedelta(hours=1))
    elif corruption == "bad_event_reason":
        report["events"][0]["reason"] = "no_accepted"
    else:
        report["events"][0]["at"] = health.iso(HOUR + timedelta(hours=2))
    with pytest.raises(ValueError, match="monitoring"):
        health.validate_report(report, HOUR)
