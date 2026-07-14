"""Warsaw wall-clock schedule conversion shared by matcher components."""

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

WARSAW = ZoneInfo("Europe/Warsaw")


def warsaw_scheduled_time(service_date: date, seconds: int | None) -> datetime | None:
    """Resolve GTFS seconds with fold=0 and normalized Warsaw spring-forward gaps."""
    if seconds is None:
        return None
    local = (datetime.combine(service_date, time()) + timedelta(seconds=int(seconds))).replace(tzinfo=WARSAW, fold=0)
    return local.astimezone(UTC).astimezone(WARSAW).astimezone(UTC)
