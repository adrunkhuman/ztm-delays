"""Hourly Warsaw weather from an Open-Meteo JSON response (archive or forecast) and derived features."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

WEATHER_FEATURES = ("precip", "precip_3h", "snow_24h", "temp", "wind", "freeze_risk", "hours_since_rain")
HOURLY_FIELDS = ("precipitation", "snowfall", "temperature_2m", "wind_speed_10m")


def load(con: duckdb.DuckDBPyConnection, response_json: Path, target: str = "weather") -> None:
    """Create ``target(wx_ts, <WEATHER_FEATURES>)``; times are Warsaw wall-clock, as requested from the API."""
    hourly = json.loads(response_json.read_text(encoding="utf-8"))["hourly"]
    missing = [f for f in ("time", *HOURLY_FIELDS) if f not in hourly]
    if missing:
        raise ValueError(f"weather response lacks hourly fields: {missing}")
    rows = list(zip(hourly["time"], *(hourly[f] for f in HOURLY_FIELDS), strict=True))
    con.execute(
        "create or replace temp table weather_raw (t varchar, precip double, snow double, temp double, wind double)"
    )
    con.executemany("insert into weather_raw values (?, ?, ?, ?, ?)", rows)
    con.execute(
        f"""
        create or replace table {target} as
        with w as (
            select strptime(t, '%Y-%m-%dT%H:%M') as wx_ts, coalesce(precip, 0) as precip, coalesce(snow, 0) as snow,
                temp, wind, row_number() over (order by t) as i
            from weather_raw
        ),
        rolled as (
            select *,
                sum(precip) over (order by wx_ts rows between 2 preceding and current row) as precip_3h,
                sum(precip) over (order by wx_ts rows between 5 preceding and current row) as precip_6h,
                sum(snow) over (order by wx_ts rows between 23 preceding and current row) as snow_24h,
                max(case when precip >= 0.1 then i end) over (order by wx_ts rows unbounded preceding) as last_rain
            from w
        )
        select wx_ts, precip, precip_3h, snow_24h, temp, wind,
            (precip_6h > 0 and temp <= 1)::int::double as freeze_risk,
            least(coalesce(i - last_rain, 168), 168)::double as hours_since_rain
        from rolled
        """
    )
