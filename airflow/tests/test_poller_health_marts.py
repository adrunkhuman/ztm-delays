"""Exercise health-view behavior locally; only BigQuery function spellings are adapted."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[2] / "dbt"


def render(path: str) -> str:
    template = Environment(autoescape=False).from_string((ROOT / path).read_text(encoding="utf-8"))  # noqa: S701 - trusted SQL, not HTML
    sql = template.render(config=lambda **kwargs: "", ref=lambda name: name, source=lambda *args: "raw_health")
    return (
        sql.replace("datetime(hour_start, 'Europe/Warsaw')", "timezone('Europe/Warsaw', hour_start)")
        .replace("date(hour_start, 'Europe/Warsaw')", "cast(timezone('Europe/Warsaw', hour_start) as date)")
        .replace(
            "unnest(json_query_array(intervals)) as incident_interval",
            "unnest(cast(intervals as json[])) as incident(incident_interval)",
        )
        .replace("safe_cast(", "try_cast(")
        .replace("json_value(", "json_extract_string(")
        .replace(" as timestamp)", " as timestamptz)")
        .replace("timestamp_add(hour_start, interval 1 hour)", "(hour_start + interval '1 hour')")
    )


def connection() -> duckdb.DuckDBPyConnection:
    db = duckdb.connect()
    db.execute("""
        create table raw_health (
            version integer, evaluated_at timestamptz, hour_start timestamptz,
            collection_started_at timestamptz, mode varchar, status varchar, reasons varchar, intervals varchar,
            monitored_minutes integer, baseline_samples integer, parsed_rows bigint, accepted_rows bigint,
            dropped_stale_rows bigint, dropped_future_rows bigint, mean_accepted_vehicles double, mean_accepted_lines double
        )
    """)
    db.execute("create view mart_poller_hourly_health as " + render("models/marts/mart_poller_hourly_health.sql"))
    db.execute("create view mart_poller_daily_health as " + render("models/marts/mart_poller_daily_health.sql"))
    return db


def add(
    db: duckdb.DuckDBPyConnection,
    hour: datetime,
    status: str,
    *,
    observed: bool = True,
    interval: tuple[datetime, datetime] | None = None,
) -> None:
    db.execute(
        """
        insert into raw_health values (1, ?, ?, ?, 'bus', ?, '[]', ?, ?, 0, ?, ?, ?, ?, ?, ?)
    """,
        [
            hour + timedelta(hours=1),
            hour,
            hour,
            status,
            json.dumps(
                [{"start_at": interval[0].isoformat(), "end_at": interval[1].isoformat(), "reason": "stale_heavy"}]
                if interval
                else []
            ),
            60 if observed else 0,
            100 if observed else None,
            20 if observed else None,
            80 if observed else None,
            0 if observed else None,
            20 if observed else None,
            2 if observed else None,
        ],
    )


@pytest.mark.parametrize("status", ["degraded", "healthy"])
def test_confirmed_degradation_does_not_disappear_without_fleet_baseline(status: str) -> None:
    with connection() as db:
        hour = datetime(2026, 10, 5, 10, tzinfo=UTC)
        add(db, hour, status, interval=(hour, hour + timedelta(minutes=15)))
        assert db.execute("select status, degraded_hours from mart_poller_daily_health").fetchall() == [("degraded", 1)]
        assert db.execute(render("tests/assert_mart_poller_hourly_health_contract.sql")).fetchall() == []


@pytest.mark.parametrize("status", ["healthy", "warming_up"])
@pytest.mark.parametrize("end_minutes", [-10, 0, 5])
def test_recovery_confirmed_after_warsaw_midnight_counts_only_overlapping_hours(status: str, end_minutes: int) -> None:
    midnight = datetime(2026, 10, 4, 22, tzinfo=UTC)
    preceding_hour = midnight - timedelta(hours=1)
    interval = (preceding_hour, midnight + timedelta(minutes=end_minutes))
    with connection() as db:
        add(db, preceding_hour, "degraded", interval=interval)
        add(db, midnight, status, interval=interval)
        assert db.execute(
            "select gps_date::varchar, status, degraded_hours from mart_poller_daily_health order by gps_date"
        ).fetchall() == [
            ("2026-10-04", "degraded", 1),
            ("2026-10-05", "degraded" if end_minutes > 0 else status, int(end_minutes > 0)),
        ]


@pytest.mark.parametrize("start_minutes", [60, 65])
def test_interval_start_at_or_after_hour_end_does_not_degrade_hour(start_minutes: int) -> None:
    hour = datetime(2026, 10, 4, 22, tzinfo=UTC)
    start = hour + timedelta(minutes=start_minutes)
    with connection() as db:
        add(db, hour, "healthy", interval=(start, start + timedelta(minutes=15)))
        assert db.execute("select status, degraded_hours from mart_poller_daily_health").fetchall() == [("healthy", 0)]


def test_degraded_status_counts_without_intervals() -> None:
    with connection() as db:
        add(db, datetime(2026, 10, 4, 22, tzinfo=UTC), "degraded")
        assert db.execute("select status, degraded_hours from mart_poller_daily_health").fetchall() == [("degraded", 1)]


def test_missing_telemetry_remains_nullable_and_absent_history_is_not_fabricated() -> None:
    with connection() as db:
        add(db, datetime(2026, 10, 5, 10, tzinfo=UTC), "monitoring_gap", observed=False)
        assert db.execute("select status, parsed_rows, accepted_rows from mart_poller_daily_health").fetchall() == [
            ("monitoring_gap", None, None)
        ]
        assert db.execute("select * from mart_poller_daily_health where gps_date = date '2026-07-08'").fetchall() == []


def test_warsaw_midnight_and_dst_repeated_hours_keep_utc_grain() -> None:
    with connection() as db:
        add(db, datetime(2026, 10, 4, 22, tzinfo=UTC), "warming_up")
        assert db.execute("select gps_date::varchar, gps_hour from mart_poller_hourly_health").fetchall() == [
            ("2026-10-05", 0)
        ]
        add(db, datetime(2026, 10, 25, 0, tzinfo=UTC), "warming_up")
        add(db, datetime(2026, 10, 25, 1, tzinfo=UTC), "warming_up")
        assert db.execute(
            "select gps_hour from mart_poller_hourly_health where gps_date = date '2026-10-25'"
        ).fetchall() == [(2,), (2,)]
        assert db.execute(
            "select status, evaluated_hours from mart_poller_daily_health where gps_date = date '2026-10-25'"
        ).fetchall() == [("warming_up", 2)]
        assert db.execute(render("tests/assert_mart_poller_hourly_health_contract.sql")).fetchall() == []
