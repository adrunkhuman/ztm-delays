"""Which days are comparable, following the timetable ZTM publishes in GTFS.

Each date maps to the surface (bus/tram) service it runs, e.g. 2026-11-11 runs
``2026-11-08:NdS``, the Sunday timetable, and 2026-11-10 runs a Friday one. Feed
health compares a date only with dates of the same class. Planned blocks (one
per vehicle's working day) mark irregular days such as the Nov 1 cemetery
service: they never become baseline samples, and a smaller plan than usual
lowers the expected fleet. A date without an entry falls back to its calendar
weekday, which is the plain same-weekday rule.

GTFS snapshots only list dates from their publication onwards, so past days are
kept in the GCS object instead of rescanning old snapshots on every refresh.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from statistics import median
from typing import Any

SCHEDULE_PATH = "health/poller/schedule-days.json"
SCHEDULE_MAX_BYTES = 64 * 1024
RETENTION_DAYS = 42
IRREGULAR = 0.15
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
Schedule = dict[date, tuple[str, float]]
TOKENS = {"Pt": "friday", "Sb": "saturday", "Nd": "sunday"}
SURFACE_SERVICE = re.compile(r"^(\d{4}-\d{2}-\d{2}):(Pc|Pt|Sb|Nd)S$")

# Per date, the newest snapshot published on or before it: later ones no longer list past dates.
QUERY = """
with calendar as (
    select date as service_date, service_id, gtfs_snapshot_id
    from `{dataset}.raw_gtfs_calendar_dates`
    where exception_type = 1
        and regexp_contains(service_id, r'(Pc|Pt|Sb|Nd)S$')
        and date(substr(gtfs_snapshot_id, 1, 10)) between @first_snapshot_date and date
    qualify dense_rank() over (partition by date order by gtfs_snapshot_id desc) = 1
),

blocks as (
    select gtfs_snapshot_id, service_id, count(distinct block_id) as blocks
    from `{dataset}.raw_gtfs_trips`
    where gtfs_snapshot_date >= @first_snapshot_date
        and regexp_contains(service_id, r'(Pc|Pt|Sb|Nd)S$')
    group by 1, 2
)

select calendar.service_date, calendar.service_id, blocks.blocks
from calendar
inner join blocks using (gtfs_snapshot_id, service_id)
"""


def service_class(service_id: str) -> str | None:
    """Pc is any working day; its pattern date names which one."""
    match = SURFACE_SERVICE.fullmatch(service_id)
    if not match:
        return None
    pattern, token = match.groups()
    return TOKENS.get(token) or WEEKDAYS[date.fromisoformat(pattern).weekday()]


def merge_days(existing: dict[str, Any] | None, rows: list[tuple[date, str, int]], today: date) -> dict[str, Any]:
    """Newer snapshot rows replace stored dates; dates past retention are dropped."""
    days = dict(existing["days"]) if existing else {}
    for service_date, service_id, blocks in rows:
        kind = service_class(service_id)
        if kind and blocks > 0:
            days[service_date.isoformat()] = {"class": kind, "blocks": blocks}
    first = (today - timedelta(days=RETENTION_DAYS)).isoformat()
    return {"version": 1, "days": {day: days[day] for day in sorted(days) if day >= first}}


def load_schedule(data: dict[str, Any]) -> Schedule:
    """Validate the stored object; return each date's class and plan relative to its class median."""
    if not isinstance(data, dict) or set(data) != {"version", "days"} or data["version"] != 1:
        raise ValueError("invalid schedule days header")
    if not isinstance(data["days"], dict):
        raise ValueError("invalid schedule days")  # noqa: TRY004 - one validation failure boundary
    parsed: dict[date, tuple[str, int]] = {}
    for day, entry in data["days"].items():
        if (
            not isinstance(entry, dict)
            or set(entry) != {"class", "blocks"}
            or entry["class"] not in WEEKDAYS
            or type(entry["blocks"]) is not int
            or entry["blocks"] <= 0
        ):
            raise ValueError("invalid schedule day")
        parsed[date.fromisoformat(day)] = (entry["class"], entry["blocks"])
    by_class: dict[str, list[int]] = {}
    for kind, blocks in parsed.values():
        by_class.setdefault(kind, []).append(blocks)
    usual = {kind: median(values) for kind, values in by_class.items()}
    return {day: (kind, blocks / usual[kind]) for day, (kind, blocks) in parsed.items()}


def profile(day: date, schedule: Schedule | None) -> tuple[str, float]:
    """A date's schedule class and planned size relative to its class."""
    return (schedule or {}).get(day) or (WEEKDAYS[day.weekday()], 1.0)
