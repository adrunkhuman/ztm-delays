from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.parametrize(("headsign", "direction"), [("Charlie", 0), (None, 0), ("Charlie", None), (None, None)])
def test_warehouse_route_reconciliation_accepts_nullable_display_keys(
    headsign: str | None, direction: int | None
) -> None:
    duckdb = pytest.importorskip("duckdb")
    jinja2 = pytest.importorskip("jinja2")
    root = Path(__file__).resolve().parents[2]
    template = root / "dbt/tests/assert_line_route_pattern_window_contract.sql"
    variables = {"processing_date": "2026-01-15"}
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    environment.globals.update(
        var=lambda name, default=None: variables.get(name, default),
        ref=lambda name: name,
        serving_route_pattern_window_ctes=lambda: "eligible as (select * from fixture_execution)",
    )
    sql = environment.from_string(template.read_text()).render()
    with duckdb.connect() as connection:
        connection.execute(
            """
            create table fixture_execution as
            select '143' as line, 'bus' as mode, ?::integer as direction_id,
                ?::varchar as trip_headsign, 'pattern' as route_pattern_id,
                'day' as window_type, '2026-01-15' as window_key,
                date '2026-01-15' as source_end_date
            """,
            [direction, headsign],
        )
        connection.execute("""
            create table mart_line_course_window as
            select *, 1 as trip_count, 1 as stop_call_count from fixture_execution;
            create table mart_line_course_stop_window as
            select *, 1 as call_position, 1 as display_rank, 1 as trip_count,
                0 as arrival_count, 0 as early_count, 0 as on_time_count, 0 as late_count,
                false as has_min_sample, null::double as mean_delay_seconds,
                null::double as median_delay_seconds, null::double as p90_delay_seconds,
                null::double as early_rate, null::double as on_time_rate, null::double as late_rate,
                null::integer[] as delay_histogram
            from fixture_execution
        """)
        assert connection.execute(sql).fetchall() == []

        connection.execute("delete from mart_line_course_stop_window")
        assert connection.execute(sql).fetchall() == [("scheduled_occurrences", 1)]
