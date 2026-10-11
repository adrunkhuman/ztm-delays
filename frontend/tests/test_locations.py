"""Planner location endpoints, form controls and point pagination."""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest

from tests.test_planner import _client
from ztm_frontend import planner_text, sqlite_geocoding

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def disabled_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ZTM_GEOCODING_DB", raising=False)


def test_disabled_database_retains_stop_suggestions_and_coordinate_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_lookup(*_args: object, **_kwargs: object) -> None:
        pytest.fail("disabled geocoding must not open a database")

    monkeypatch.setattr(sqlite_geocoding, "search", unexpected_lookup)
    monkeypatch.setattr(sqlite_geocoding, "reverse", unexpected_lookup)
    client = _client(tmp_path, monkeypatch)
    for field in ("from", "to"):
        response = client.get(f"/planner/suggest/{field}?q_{field}=Centrum")
        assert response.status_code == HTTPStatus.OK
        assert response.headers["Cache-Control"] == "no-store"
        html = response.get_data(as_text=True)
        assert 'data-stop-id="4004"' in html
        assert "Centrum" in html
        assert str(planner_text.TEXT["en"]["address_error"]) not in html
        assert "href=" not in html
    empty = client.get("/planner/suggest/from?q_from=Marszalkowska+10&lang=en").get_data(as_text=True)
    assert str(planner_text.TEXT["en"]["address_error"]) not in empty
    response = client.get("/planner/reverse?lat=52.23&lon=21.01")
    assert response.json == {"name": None}
    assert response.headers["Cache-Control"] == "no-store"


def test_location_endpoints_reject_unknown_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    assert client.get("/planner/suggest/via?q_via=x").status_code == HTTPStatus.NOT_FOUND
    assert client.get("/planner/address/to?q_to=Centrum").status_code == HTTPStatus.NOT_FOUND


def test_location_controls_have_no_gps_button_and_keep_swap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    page = client.get("/planner?date=2026-09-23&time=07:00").get_data(as_text=True)
    assert "pl-map-toggle" not in page
    assert "pl-picker-actions" not in page
    assert page.count('class="pl-swap"') == 1
    assert "pl-address" not in page
    assert "For an address, type street" not in page
    assert "Walks to and from map points" not in page
    assert "pl-coordinate-inputs" not in page
    assert "pl-picker-footer" not in page
    assert "pl-picker-heading" not in page
    assert "pl-picker-field" not in page
    assert "pl-point-label" not in page
    assert 'data-city="Warsaw"' in page
    assert "pl-picker-use" not in page
    assert "pl-picker-cancel" not in page
    assert 'type="number"' not in page
    assert page.index('name="q_to"') < page.index('id="pl-picker"')
    assert page.index('name="q_to"') < page.index("pl-swap")


def test_point_coordinates_survive_forms_and_pagination(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    page = client.get(
        "/planner?date=2026-09-23&time=07:00&from_lat=52.331&from_lon=20.921&q_from=My+address&to=2002",
    ).get_data(as_text=True)
    assert 'name="from" value=""' in page
    assert 'name="from_lat" value="52.331"' in page
    assert 'name="from_lon" value="20.921"' in page
    assert 'value="My address"' in page
    assert 'class="pl-card"' in page
    assert "from_lat=52.331&amp;from_lon=20.921&amp;q_from=My+address" in page
