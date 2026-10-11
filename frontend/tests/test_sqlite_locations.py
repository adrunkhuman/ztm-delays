"""Flask location routes use configured SQLite data without external requests."""

from __future__ import annotations

import sqlite3
from http import HTTPStatus
from typing import TYPE_CHECKING

import pytest

from tests.test_planner import _client
from tests.test_sqlite_geocoding import _build, _row
from ztm_frontend import planner_text

if TYPE_CHECKING:
    from pathlib import Path

LAT, LON = 52.2297, 21.0122


def test_sqlite_address_suggestions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row()])
    monkeypatch.setenv("ZTM_GEOCODING_DB", str(path))
    client = _client(tmp_path, monkeypatch)
    response = client.get("/planner/suggest/from?q_from=Marszalkowska+10")
    assert response.status_code == HTTPStatus.OK
    assert response.headers["Cache-Control"] == "no-store"
    html = response.get_data(as_text=True)
    assert "Marszałkowska 10, Warszawa" in html
    assert f'data-lat="{LAT}" data-lon="{LON}"' in html
    stops = client.get("/planner/suggest/from?q_from=Centrum").get_data(as_text=True)
    assert "Centrum" in stops


@pytest.mark.parametrize("lang", ["en", "pl"])
def test_sqlite_reverse_labels_use_the_site_language(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lang: str
) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row()])
    monkeypatch.setenv("ZTM_GEOCODING_DB", str(path))
    client = _client(tmp_path, monkeypatch)
    exact = client.get(f"/planner/reverse?lat={LAT}&lon={LON}&lang={lang}")
    assert exact.json == {"name": "Marszałkowska 10, Warszawa"}
    nearby = client.get(f"/planner/reverse?lat={LAT + 0.0003}&lon={LON}&lang={lang}")
    assert nearby.json == {"name": str(planner_text.TEXT[lang]["near"]).format(name="Marszałkowska 10, Warszawa")}
    assert nearby.headers["Cache-Control"] == "no-store"
    assert client.get("/planner/reverse?lat=nan&lon=inf").json == {"name": None}


@pytest.mark.parametrize("state", ["missing", "corrupt", "schema0", "unversioned", "missing-bbox"])
@pytest.mark.parametrize("lang", ["en", "pl"])
def test_unavailable_sqlite_preserves_stop_search_and_coordinate_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str, lang: str
) -> None:
    path = tmp_path / "unavailable.sqlite"
    if state == "corrupt":
        path.write_bytes(b"not a SQLite database")
    elif state != "missing":
        _build(path, [_row()])
        with sqlite3.connect(path) as connection:
            if state == "schema0":
                connection.execute("UPDATE metadata SET value = '0' WHERE key = 'schema_version'")
            elif state == "unversioned":
                connection.execute("DELETE FROM metadata WHERE key NOT IN ('status', 'bbox')")
            else:
                connection.execute("DELETE FROM metadata WHERE key = 'bbox'")
    before = path.read_bytes() if path.exists() else None
    monkeypatch.setenv("ZTM_GEOCODING_DB", str(path))
    client = _client(tmp_path, monkeypatch)
    response = client.get(f"/planner/suggest/to?q_to=Centrum&lang={lang}")
    assert response.status_code == HTTPStatus.OK
    html = response.get_data(as_text=True)
    assert "Centrum" in html
    # Existing stop results remain selectable even when addresses are unavailable.
    assert 'data-stop-id="4004"' in html
    assert str(planner_text.TEXT[lang]["address_error"]) in html
    error = client.get(f"/planner/suggest/to?q_to=Marszalkowska+10&lang={lang}")
    assert error.status_code == HTTPStatus.OK
    assert str(planner_text.TEXT[lang]["address_error"]) in error.get_data(as_text=True)
    assert error.headers["Cache-Control"] == "no-store"
    reverse = client.get(f"/planner/reverse?lat={LAT}&lon={LON}&lang={lang}")
    assert reverse.json == {"name": None}
    assert reverse.headers["Cache-Control"] == "no-store"
    assert (path.read_bytes() if path.exists() else None) == before


def test_sqlite_replacement_is_visible_without_an_app_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row()])
    monkeypatch.setenv("ZTM_GEOCODING_DB", str(path))
    client = _client(tmp_path, monkeypatch)
    url = f"/planner/reverse?lat={LAT}&lon={LON}"
    assert client.get(url).json == {"name": "Marszałkowska 10, Warszawa"}
    replacement = _build(tmp_path / "addresses-new.sqlite", [_row(house="11")])
    replacement.replace(path)
    assert client.get(url).json == {"name": "Marszałkowska 11, Warszawa"}


def test_suggestions_combine_stops_and_sqlite_addresses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row("Centrum")])
    monkeypatch.setenv("ZTM_GEOCODING_DB", str(path))
    client = _client(tmp_path, monkeypatch)
    response = client.get("/planner/suggest/to?q_to=Centrum")
    html = response.get_data(as_text=True)
    assert 'data-stop-id="4004"' in html
    assert "Centrum 10, Warszawa" in html
    assert f'data-lat="{LAT}" data-lon="{LON}"' in html
    assert html.index('data-stop-id="4004"') < html.index("data-lat=")
    assert "href=" not in html


def test_missing_database_recovers_after_local_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "addresses.sqlite"
    monkeypatch.setenv("ZTM_GEOCODING_DB", str(path))
    client = _client(tmp_path, monkeypatch)
    query = "/planner/suggest/from?q_from=Marszalkowska+10&lang=en"
    assert str(planner_text.TEXT["en"]["address_error"]) in client.get(query).get_data(as_text=True)
    assert client.get(f"/planner/reverse?lat={LAT}&lon={LON}").json == {"name": None}
    assert not path.exists()
    _build(path, [_row()])
    html = client.get(query).get_data(as_text=True)
    assert "Marszałkowska 10, Warszawa" in html
    assert str(planner_text.TEXT["en"]["address_error"]) not in html
    assert client.get(f"/planner/reverse?lat={LAT}&lon={LON}").json == {"name": "Marszałkowska 10, Warszawa"}
