from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pendulum
import pytest

from .test_poller_health import health, row, summary
from .test_poller_health_baseline import seed


@pytest.mark.parametrize("use_pendulum", [False, True])
@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (
            "2026-10-06T21:00:00+00:00",
            [
                "2026-09-08T21:00:00+00:00",
                "2026-09-15T21:00:00+00:00",
                "2026-09-22T21:00:00+00:00",
                "2026-09-29T21:00:00+00:00",
            ],
        ),
        (
            "2026-01-20T22:00:00+00:00",
            [
                "2025-12-23T22:00:00+00:00",
                "2025-12-30T22:00:00+00:00",
                "2026-01-06T22:00:00+00:00",
                "2026-01-13T22:00:00+00:00",
            ],
        ),
        (
            "2026-11-01T01:00:00+00:00",
            [
                "2026-10-04T00:00:00+00:00",
                "2026-10-11T00:00:00+00:00",
                "2026-10-18T00:00:00+00:00",
                "2026-10-25T00:00:00+00:00",
                "2026-10-25T01:00:00+00:00",
            ],
        ),
        (
            "2026-04-05T00:00:00+00:00",
            ["2026-03-08T01:00:00+00:00", "2026-03-15T01:00:00+00:00", "2026-03-22T01:00:00+00:00"],
        ),
    ],
    ids=["summer", "winter", "autumn-fold", "spring-gap"],
)
def test_comparable_hours_preserve_warsaw_wall_clock(hour: str, expected: list[str], use_pendulum: bool) -> None:
    current = datetime.fromisoformat(hour)
    if use_pendulum:
        current = pendulum.instance(current)
    candidates = health.comparable_hours(current, health.Config())
    assert candidates == [datetime.fromisoformat(value) for value in expected]
    assert all(
        candidate.astimezone(health.WARSAW).hour == current.astimezone(health.WARSAW).hour for candidate in candidates
    )


@pytest.mark.parametrize("interval_end", ["2026-10-06T22:25:00+00:00", "2026-01-20T23:25:00+00:00"])
def test_airflow_interval_selects_same_time_seeds_without_false_evening_incidents(interval_end: str) -> None:
    hour = health.completed_hour(pendulum.parse(interval_end))
    expected_hour = datetime.fromisoformat(interval_end).replace(minute=0) - timedelta(hours=1)
    assert hour == expected_hour
    correct_hours = health.comparable_hours(expected_hour, health.Config())
    offset = expected_hour.astimezone(health.WARSAW).utcoffset()
    assert offset is not None
    sources = {candidate: seed(candidate, bus=40, tram=40) for candidate in correct_hours}
    # Earlier hours have a larger fleet: selecting them would falsely flag normal evening service.
    sources.update({candidate - offset: seed(candidate - offset, bus=100, tram=100) for candidate in correct_hours})
    history = [sources[candidate] for candidate in health.comparable_hours(hour, health.Config())]
    current = summary(expected_hour, vehicles=40)
    current["collection_started_at"] = health.iso(expected_hour - timedelta(days=30))
    report = health.evaluate(hour, current, history)
    for mode in health.MODES:
        assert row(report, mode)["baseline"]["vehicles"] == [40] * 60
        assert row(report, mode)["status"] == "healthy"
    assert report["events"] == []
    assert health.hour_path("hourly", hour) == f"health/poller/hourly/{expected_hour.astimezone(UTC):%Y-%m-%d/%H}.json"
