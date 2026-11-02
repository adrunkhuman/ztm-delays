from __future__ import annotations

import sys
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from .test_poller_health import health, row, summary

# Loaded by poller_health from the DAG folder that test_poller_health puts on sys.path.
schedule_days = sys.modules["poller_health_schedule"]
RETENTION_DAYS = schedule_days.RETENTION_DAYS
load_schedule = schedule_days.load_schedule
merge_days = schedule_days.merge_days
profile = schedule_days.profile
service_class = schedule_days.service_class

# Wednesday 2026-11-11, 07:00 Warsaw: a holiday on the Sunday timetable.
HOLIDAY = datetime(2026, 11, 11, 6, tzinfo=UTC)
NOV_1 = date(2026, 11, 1)


def stored(overrides: dict[date, tuple[str, int]] | None = None) -> dict[str, Any]:
    """Five weeks of ordinary days: 2000 blocks on working days, 1000 at weekends."""
    days = {}
    for offset in range(35):
        day = date(2026, 10, 10) + timedelta(days=offset)
        kind = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")[day.weekday()]
        days[day] = (kind, 1000 if day.weekday() >= 5 else 2000)
    days.update(overrides or {})
    return {"version": 1, "days": {day.isoformat(): {"class": k, "blocks": b} for day, (k, b) in days.items()}}


SCHEDULE = load_schedule(stored({date(2026, 11, 11): ("sunday", 1000), NOV_1: ("sunday", 1500)}))


@pytest.mark.parametrize(
    ("service_id", "expected"),
    [
        ("2026-11-08:NdS", "sunday"),
        ("2026-11-07:SbS", "saturday"),
        ("2026-11-10:PtS", "friday"),
        ("2026-11-04:PcS", "wednesday"),
        ("2026-11-04:PcM", None),
        ("PcS", None),
    ],
)
def test_service_class_reads_surface_timetables_only(service_id: str, expected: str | None) -> None:
    assert service_class(service_id) == expected


def test_merge_replaces_newer_dates_keeps_past_and_drops_expired() -> None:
    today = date(2026, 11, 11)
    expired = (today - timedelta(days=RETENTION_DAYS + 1)).isoformat()
    existing = {
        "version": 1,
        "days": {
            expired: {"class": "sunday", "blocks": 1000},
            "2026-11-01": {"class": "sunday", "blocks": 1879},
            "2026-11-11": {"class": "wednesday", "blocks": 2332},
        },
    }
    rows = [(date(2026, 11, 11), "2026-11-08:NdS", 1267), (date(2026, 11, 12), "2026-11-05:PcM", 50)]
    assert merge_days(existing, rows, today) == {
        "version": 1,
        "days": {"2026-11-01": {"class": "sunday", "blocks": 1879}, "2026-11-11": {"class": "sunday", "blocks": 1267}},
    }


def test_profile_compares_plan_with_class_median_and_falls_back_to_weekday() -> None:
    assert profile(NOV_1, SCHEDULE) == ("sunday", 1.5)
    assert profile(date(2026, 11, 11), SCHEDULE) == ("sunday", 1.0)
    assert profile(date(2027, 1, 6), SCHEDULE) == ("wednesday", 1.0)
    assert profile(date(2027, 1, 6), None) == ("wednesday", 1.0)


@pytest.mark.parametrize(
    "data",
    [
        {"version": 2, "days": {}},
        {"version": 1, "days": {"2026-11-01": {"class": "holiday", "blocks": 1}}},
        {"version": 1, "days": {"2026-11-01": {"class": "sunday", "blocks": 0}}},
        {"version": 1, "days": {"not-a-date": {"class": "sunday", "blocks": 1}}},
    ],
)
def test_load_rejects_invalid_objects(data: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"schedule|isoformat"):
        load_schedule(data)


def local_dates(hours: list[datetime]) -> list[date]:
    return [hour.astimezone(health.WARSAW).date() for hour in hours]


def test_without_schedule_candidates_stay_same_weekday() -> None:
    expected = [date(2026, 10, 14), date(2026, 10, 21), date(2026, 10, 28), date(2026, 11, 4)]
    assert local_dates(health.comparable_hours(HOLIDAY, health.Config())) == expected


def test_holiday_compares_with_regular_sundays_only() -> None:
    sundays = [date(2026, 10, 18), date(2026, 10, 25), date(2026, 11, 8)]
    assert local_dates(health.comparable_hours(HOLIDAY, health.Config(), SCHEDULE)) == sundays


def test_weekday_holiday_is_not_an_outage() -> None:
    history = [summary(hour, vehicles=100) for hour in health.comparable_hours(HOLIDAY, health.Config())]
    history += [summary(hour, vehicles=40) for hour in health.comparable_hours(HOLIDAY, health.Config(), SCHEDULE)]
    history.append(summary(datetime(2026, 11, 1, 6, tzinfo=UTC), vehicles=60))
    current = summary(HOLIDAY, vehicles=40)
    assert row(health.evaluate(HOLIDAY, current, history))["status"] == "degraded"
    report = health.evaluate(HOLIDAY, current, history, schedule=SCHEDULE)
    assert row(report)["status"] == "healthy"
    assert row(report)["baseline"]["vehicles"] == [40] * 60


def test_smaller_plan_lowers_expected_fleet_but_larger_never_raises_it() -> None:
    history = [summary(hour, vehicles=100) for hour in health.comparable_hours(HOLIDAY, health.Config())]
    wednesday = date(2026, 11, 11)
    for blocks, expected in ((1400, 70), (3000, 100)):
        schedule = load_schedule(stored({wednesday: ("wednesday", blocks)}))
        report = health.evaluate(HOLIDAY, summary(HOLIDAY, vehicles=60), history, schedule=schedule)
        assert row(report)["baseline"]["vehicles"] == [expected] * 60
