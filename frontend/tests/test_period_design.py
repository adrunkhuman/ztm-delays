from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from flask import render_template

from ztm_frontend import queries
from ztm_frontend.app import create_app

if TYPE_CHECKING:
    from typing import Any


@pytest.mark.parametrize("window", ["day", "weekdays", "weekend", "month"])
@pytest.mark.parametrize("view", ["overview", "lines", "line", "stops", "stop", "post", "schedule", "runs"])
def test_period_layout_renders_across_views(window: str, view: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _path: True)
    app = create_app()
    count = 7 if window == "day" else 31 if window == "month" else 60
    bars = queries._trend_bars(  # noqa: SLF001
        [{"bucket_date": date(2026, 7, 31) - timedelta(days=count - i - 1), "delay": i - 2} for i in range(count)],
        "%d %b",
    )
    if window == "day":
        for bar in bars:
            bar.pop("daily")
    widget = {
        "shape": {"buckets": [], "median_x": 20, "p90_x": 50},
        "segments": {"early": 0.1, "on_time": 0.8, "late": 0.1},
        "hours": [],
        "comparison": bars[-12:],
        "daily": bars,
        "comparison_label": queries._comparison_label(window),  # noqa: SLF001
        "timeline": [],
        "worst": [],
        "reliability": [],
        "posts": [],
        "line_rows": [],
    }
    trips = [
        {
            "service_date": day,
            "trip_id": "same-trip",
            "vehicle_number": vehicle,
            "outcome": "clean",
            "label": vehicle,
            "time": "15 Jul · 12:00",
            "stop_group_id": "1001",
            "stop_name": "Central",
            "direction": "Destination",
            "line": "148",
            "mode": "bus",
            "headsign": "Destination",
            "delay_seconds": 60,
        }
        for day, vehicle in [("2026-07-15", "1234"), ("2026-07-16", "5678")]
    ]
    widget["worst"] = trips
    widget["reliability"] = [
        {"direction": "Destination", "counts": {"clean": 2, "partial": 0, "broken": 0}, "outcomes": trips}
    ]
    summary = {
        "route_short_name": "148",
        "mode": "bus",
        "on_time_rate": 0.8,
        "arrival_count": 100,
        "stop_group_id": "1001",
        "stop_group_name": "Franciszkańska",
    }
    pager = {"page": 1, "first_item": 1, "has_previous": False, "has_next": False}
    context: dict[str, Any] = {
        "selected_window": window,
        "selected_date": "2026-07-31",
        "selected_mode": "bus",
        "selected_line": "148" if view in {"line", "runs"} else None,
        "selected_stop_group_id": "1001" if view in {"stop", "post"} else None,
        "selected_post": summary if view == "post" else None,
        "selected_view": "post",
        "selected_rank": "worst",
        "selected_sort": "departure",
        "search": "central",
        "summary": summary,
        "pagination": pager,
        "picker_pagination": pager,
        "line_groups": [],
        "stop_list": [],
        "trip_groups": [],
        "courses": [{"trip_headsign": f"Destination {i}", "trip_count": 100 - i, "stops": []} for i in range(3)],
        "line_landing_summary": {},
        "stop_landing_summary": {},
        "trip_landing_summary": {},
        "line_landing_rows": [],
        "stop_landing_rows": [],
        "trip_landing_rows": [],
        "line_widgets": widget,
        "stop_widgets": widget,
        "overview_widgets": {"bus": widget, "tram": widget},
        "mode_stats": {},
        "worst_lines": {},
        "worst_stops": {},
        "date_nav": {"previous": "2026-07-24", "next": None},
        "window_context": {
            "label": "31 Jul 2026" if window == "day" else "Jul 2026" if window == "month" else "02 Jun–31 Jul · 60d",  # noqa: RUF001
            "source_day_count": count,
            "anchor": "2026-07-31",
        },
    }
    template = {"line": "lines", "stop": "stops", "post": "stops", "runs": "schedule"}.get(view, view)
    path = {"overview": "/", "line": "/lines/148", "stop": "/stops/1001", "post": "/stops/1001/02"}.get(
        view, f"/{template}/"
    )
    with app.test_request_context(f"{path}?window={window}&date=2026-07-31&mode=bus&q=central&sort=departure"):
        html = render_template(f"{template}.html", **context)
    assert f"<b>{context['window_context']['label']}</b>" in html
    assert "period-note" not in html
    assert "period-range" not in html
    assert "GTFS" not in html
    assert "Linear scale" not in html
    assert "Previous period anchor" in html
    assert "q=central" in html
    assert "sort=departure" in html
    if view in {"line", "post"}:
        for trip in trips:
            href = f"/trips/same-trip?date={trip['service_date']}&amp;vehicle={trip['vehicle_number']}&amp;window={window}&amp;return_date=2026-07-31"
            assert html.count(f'href="{href}"') == (2 if view == "line" else 1)
        if view == "line":
            assert 'class="data-link" href="/stops/1001?mode=bus&amp;date=2026-07-31' in html
        else:
            assert 'href="/lines/148?mode=bus&amp;date=2026-07-31' in html
    if view == "line":
        assert html.count('class="route-pattern" open') == 2  # noqa: PLR2004
        assert 'class="route-pattern" >' in html
        assert "Destination 2" in html
        assert 'href="#departures"' in html
    if window != "day":
        if view == "overview":
            assert all(f"{mode} median by day" in html for mode in ["Bus", "Tram"])
        assert not any(text in html for text in ['class="daily-value"', "Scale ±"])
        assert f"window={window}" in html
        if view in {"line", "post"}:
            assert f"--points: {count}" in html
            assert 'class="daily-strip"' in html
            assert 'class="week-plot"' not in html
    else:
        assert "Rolling sample" not in html
        assert 'class="daily-strip"' not in html


def test_compact_times_and_missing_chart_values() -> None:
    app = create_app()
    with app.test_request_context():
        widgets: Any = app.jinja_env.get_template("_widgets.html").module
        event = str(widgets.event_time("31 Jul · 25:10"))
        day = str(widgets.event_time("25:10"))
        empty = str(widgets.week_bars([]))
        trip = str(
            widgets.trip_time(
                {
                    "display_date": "31 Jul · ",
                    "service_date": "2026-07-31",
                    "scheduled_start_time": datetime(2026, 7, 31, 10, tzinfo=UTC),
                }
            )
        )
    assert '<span class="event-date">31 Jul</span><span>25:10</span>' in event
    assert "event-date" not in day
    assert "No service-day data" in empty
    assert 'title="Service date 2026-07-31"' in trip
    assert '<span class="event-date">31 Jul</span><span>12:00</span>' in trip


@pytest.mark.parametrize("window", ["day", "weekdays", "weekend", "month"])
@pytest.mark.parametrize(
    ("status", "label"),
    [
        ("missed", "Not observed"),
        ("skipped_optional", "Not observed · request stop"),
        ("uncertain", "uncertain"),
        ("observed", "+15s"),
    ],
)
def test_trip_detail_retains_run_date_and_return_period(
    window: str,
    status: str,
    label: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _path: True)
    trip = {
        "route_short_name": "148",
        "line": "148",
        "mode": "bus",
        "vehicle_number": "1234",
        "origin_stop_name": "Origin",
        "destination_stop_name": "Destination",
        "trace": [],
        "scheduled_start_time": datetime(2026, 7, 15, 10, tzinfo=UTC),
        "scheduled_end_time": datetime(2026, 7, 15, 11, tzinfo=UTC),
        "start_delay_seconds": -30,
        "end_delay_seconds": 0,
    }
    stop = {
        "stop_group_id": "1001",
        "post_label": "02",
        "stop_name": "Central",
        "scheduled_arrival_time": datetime(2026, 7, 15, 10, 30, tzinfo=UTC),
        "observation_status": status,
        "delay_seconds": 15,
    }
    app = create_app()
    with app.test_request_context(f"/trips/run?window={window}&date=2026-07-15&return_date=2026-07-31"):
        html = render_template(
            "trip_detail.html",
            trip=trip,
            trip_stops=[stop],
            selected_date="2026-07-15",
            return_date="2026-07-31",
            return_window=window,
        )
    assert "2026-07-15 · vehicle 1234" in html
    assert "Single run" not in html
    assert "trip-detail-key" not in html
    assert label in html
    if status in {"missed", "skipped_optional"}:
        assert 'title="No sufficiently confident GPS observation at this stop. ' in html
        assert "This does not establish that the vehicle skipped it." in html
        assert f">{status}</abbr>" not in html
        assert ">skipped</abbr>" not in html
    else:
        assert "Not observed" not in html
    assert f"window={window}" in html
    assert "date=2026-07-31" in html
    assert "-30s" in html
    assert "0s" in html


@pytest.mark.parametrize(
    ("previous", "following"), [(None, None), ("/previous", None), (None, "/next"), ("/previous", "/next")]
)
def test_period_arrows_only_link_available_anchors(previous: str | None, following: str | None) -> None:
    app = create_app()
    with app.test_request_context():
        widgets: Any = app.jinja_env.get_template("_widgets.html").module
        html = str(widgets.datebox("31 Jul 2026", previous, following, show_scopes=False))
    for label, href, arrow in [("Previous", previous, "‹"), ("Next", following, "›")]:  # noqa: RUF001
        if href:
            assert f'<a aria-label="{label} period anchor" href="{href}">{arrow}</a>' in html
        else:
            assert (
                f'<span class="disabled" aria-label="{label} period anchor" aria-disabled="true">{arrow}</span>' in html
            )
    assert 'href="#"' not in html
    assert "tabindex" not in html


def test_recent_chart_labels_leave_space_before_final_date() -> None:
    rows = [{"bucket_date": date(2026, 8, 1) + timedelta(days=i), "delay": i} for i in range(12)]
    bars = queries._trend_bars(rows, "%d %b")  # noqa: SLF001
    assert [i for i, bar in enumerate(bars) if bar["label"]] == [0, 3, 6, 11]


def test_daily_values_table_replaces_per_point_keyboard_stops() -> None:
    point_count = 60
    rows = [
        {"bucket_date": date(2026, 7, 31) - timedelta(days=i), "delay": None if i == 0 else i - 2}
        for i in reversed(range(point_count))
    ]
    bars = queries._trend_bars(rows, "%d %b")  # noqa: SLF001
    app = create_app()
    with app.test_request_context():
        widgets: Any = app.jinja_env.get_template("_widgets.html").module
        html = str(widgets.week_bars(bars))
    assert html.count('tabindex="0"') == 1
    assert 'class="daily-chart" tabindex="0" role="region"' in html
    assert 'title="2026-07-30 · -1s"' in html
    assert 'class="daily-point missing selected"' in html
    assert '<div class="sr-only">\n      <table>' in html
    assert '<table class="sr-only">' not in html
    assert "daily-key" not in html
    assert "<details" not in html
    assert "<summary" not in html
    assert '<th scope="col">Date</th><th scope="col">Median</th>' in html
    table = html.split("<tbody>", 1)[1].split("</tbody>", 1)[0]
    assert table.count("<tr>") == point_count
    assert "<td>2026-07-31</td><td>n/a</td>" in table
    assert "<td>2026-07-30</td><td>-1s</td>" in table
    assert "<td>2026-07-29</td><td>0s</td>" in table
    assert "<td>2026-07-28</td><td>+1s</td>" in table
