"""Day types, Polish public holidays and time bands, matching the warehouse definitions.

Holidays mirror dbt/models/marts/dim_date.sql, which labelled the training data.
"""

from __future__ import annotations

from datetime import date, timedelta

WEEKDAY, SATURDAY, SUNDAY_HOLIDAY = 0, 1, 2
HOUR_BAND_EDGES = (6, 10, 14, 19)  # hour bands 0..4 for recent-conditions features


def easter_sunday(year: int) -> date:
    """Gregorian Easter (anonymous algorithm, as in dim_date)."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month = (h + m - 7 * n + 114) // 31
    day = (h + m - 7 * n + 114) % 31 + 1
    return date(year, month, day)


def holidays(year: int) -> set[date]:
    """Public holidays used by the warehouse's is_holiday flag."""
    fixed = {(1, 1), (1, 6), (5, 1), (5, 3), (8, 15), (11, 1), (11, 11), (12, 25), (12, 26)}
    easter = easter_sunday(year)
    movable = {easter, easter + timedelta(days=1), easter + timedelta(days=49), easter + timedelta(days=60)}
    return {date(year, m, d) for m, d in fixed} | movable


def holiday_dates(start: date, end: date) -> list[date]:
    """Holidays within [start, end]."""
    found = set().union(*(holidays(year) for year in range(start.year, end.year + 1)))
    return sorted(d for d in found if start <= d <= end)


def day_type(day: date, holiday: bool) -> int:
    """Weekday, Saturday, or Sunday/holiday."""
    if holiday or day.isoweekday() == 7:
        return SUNDAY_HOLIDAY
    return SATURDAY if day.isoweekday() == 6 else WEEKDAY


def time_band(hour: int, weekday: bool) -> str:
    """Bands used as the fallback when an hour lacks calibration data."""
    hour %= 24
    if hour >= 23 or hour < 5:
        return "night"
    if weekday and hour in {7, 8, 15, 16, 17}:
        return "peak"
    return "other"
