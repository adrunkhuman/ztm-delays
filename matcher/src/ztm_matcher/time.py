"""GTFS schedule-time conversion shared by matcher components."""

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

WARSAW = ZoneInfo("Europe/Warsaw")


def warsaw_scheduled_time(service_date: date, seconds: int | None) -> datetime | None:
    """Resolve GTFS seconds as elapsed time from Warsaw noon minus 12 h.

    The GTFS origin is local midnight except on DST change days, where it is 23:00 or 01:00. ZTM vehicles
    follow it: on 2026-10-25 night trips ran on elapsed time, not on the repeated wall-clock hour.
    """
    if seconds is None:
        return None
    # Subtract in UTC: aware-datetime arithmetic in WARSAW would be wall-clock arithmetic.
    origin = datetime.combine(service_date, time(12), WARSAW).astimezone(UTC) - timedelta(hours=12)
    return origin + timedelta(seconds=int(seconds))
