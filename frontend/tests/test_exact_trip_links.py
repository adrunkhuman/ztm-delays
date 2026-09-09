from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from tests.test_queries import _create_line_smoke_db
from ztm_frontend import queries
from ztm_frontend.app import create_app


def test_grouped_widgets_keep_exact_run_identity(tmp_path: Path) -> None:
    db_path = tmp_path / "serving.duckdb"
    _create_line_smoke_db(db_path)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("alter table mart_line_window_summary add column source_start_date date default '2026-06-29'")
        conn.execute("""
            create or replace table mart_line_reliability_daily as
            select service_date, 'weekday' as schedule_day_type, 'bus' as mode, '1' as line,
                0 as direction_id, 'Bus destination' as trip_headsign,
                1 as clean_count, 0 as partial_count, 0 as broken_count, 1 as display_rank,
                [{'trip_id': 'trip-1', 'vehicle_number': vehicle, 'scheduled_start_time': timestamp '2026-06-29 08:00:00',
                  'outcome': 'clean', 'label': 'Clean run'}] as outcomes
            from (values (date '2026-06-29', '2002'), (date '2026-06-30', '1001')) as runs(service_date, vehicle)
        """)
        conn.execute("""
            insert into mart_worst_delay_event
            select * replace (date '2026-06-29' as service_date, '2002' as vehicle_number)
            from mart_worst_delay_event where mode = 'bus'
        """)
        conn.execute("""
            insert into mart_worst_delay_event
            select * replace ('stop_post' as scope_type, '7002' as scope_id)
            from mart_worst_delay_event where mode = 'bus'
        """)
    result = queries.get_lines(db_path, "1", "bus", "2026-06-30", None, None, "month")
    widgets = result["line_widgets"]
    expected = {(date(2026, 6, 29), "trip-1", "2002"), (date(2026, 6, 30), "trip-1", "1001")}
    outcomes = widgets["reliability"][0]["outcomes"]
    assert {(row["service_date"], row["trip_id"], row["vehicle_number"]) for row in outcomes} == expected
    summary = {**result["summary"], "stop_id": "7002"}
    stop = queries._stop_widgets(  # noqa: SLF001
        db_path,
        [],
        summary,
        [],
        {"selected_date": "2026-06-30", "selected_mode": "bus", "selected_window": "month", "window_key": "2026-06"},
    )
    for rows in [widgets["worst"], stop["worst"]]:
        assert {(row["service_date"], row["trip_id"], row["vehicle_number"]) for row in rows} == expected


@pytest.mark.parametrize("missing", ["service_date", "trip_id", "vehicle_number"])
@pytest.mark.parametrize("square", [False, True])
def test_missing_identity_is_not_linked(missing: str, square: bool) -> None:
    trip = {
        "service_date": "2026-06-30",
        "trip_id": "trip-1",
        "vehicle_number": "1001",
        "outcome": "clean",
        "label": "Clean run",
        "time": "08:00",
    }
    trip.pop(missing)
    app = create_app()
    with app.test_request_context():
        widgets: Any = app.jinja_env.get_template("_widgets.html").module
        html = str(widgets.exact_trip_link(trip, square=square))
    assert "href=" not in html
    assert ('class="reliability-square clean"' if square else "08:00") in html
