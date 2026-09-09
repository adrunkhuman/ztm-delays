from __future__ import annotations

from datetime import UTC, datetime
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest

from ztm_frontend import queries
from ztm_frontend.app import create_app

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "unknown"),
        ("", "unknown"),
        ("not a timestamp", "unknown"),
        ("2026-07-14T05:12:34.123456+02:00", "2026-07-14 03:12:34 UTC"),
        ("2026-07-14T03:12:34Z", "2026-07-14 03:12:34 UTC"),
        (datetime(2026, 7, 14, 3, 12, 34, tzinfo=UTC), "2026-07-14 03:12:34 UTC"),
        ("2026-07-14 03:12:34", "2026-07-14 03:12:34 UTC"),
    ],
)
def test_utc_timestamp(value: object, expected: str) -> None:
    app = create_app()
    assert app.jinja_env.from_string("{{ value|utc_timestamp }}").render(value=value) == expected


@pytest.mark.parametrize("metadata", [{}, {"poller_status": None}, {"poller_status": {"vehicle_types": None}}])
def test_empty_status(monkeypatch: pytest.MonkeyPatch, metadata: dict[str, object]) -> None:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _: False)
    monkeypatch.setattr(
        queries, "get_status", lambda _: {"metadata": metadata, "status_summary": {}, "status_days": []}
    )
    response = create_app().test_client().get("/status")
    assert response.status_code == HTTPStatus.OK
    html = response.get_data(as_text=True)
    assert "no recent data" in html
    assert "unknown" in html
    assert "n/a" in html
    assert "landing-meta" not in html
    assert "<table>" not in html


def test_poller_snapshot_timestamp_and_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    original = "2026-07-14T05:12:34.123456+02:00"
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _: True)
    monkeypatch.setattr(
        queries,
        "get_status",
        lambda _: {
            "metadata": {
                "last_export_at": original,
                "poller_status": {
                    "status": "degraded",
                    "updated_at": original,
                    "last_success_at": "invalid",
                    "vehicle_types": {"bus": {"consecutive_failures": 3}, "tram": {"consecutive_failures": 0}},
                },
            },
            "status_summary": {},
            "status_days": [],
        },
    )
    html = create_app().test_client().get("/status?window=month").get_data(as_text=True)
    assert f'title="{original}"' in html
    assert "2026-07-14 03:12:34 UTC" in html
    assert '<dd class="problem">degraded</dd>' in html
    assert '<dd class="problem">3</dd>' in html
    assert '<dd class="">0</dd>' in html
    assert '<span title="invalid">unknown</span>' in html
    assert "poller snapshot" in html


def test_partial_summary_requires_complete_window(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(
        queries,
        "fetch_all",
        lambda _, sql: (
            [{"mode": "bus", "first_date": "2026-07-12", "last_date": "2026-07-13", "day_count": 2}]
            if "recent_summary" in sql
            else [{"mode": "bus", "service_date": "2026-07-13", "trips_partial": 4}]
        ),
    )
    assert queries.get_status(tmp_path / "unused.duckdb")["status_summary"]["bus"]["trips_partial"] is None
