"""Results-only rendering must retain the full planner's cards and URL state."""

from __future__ import annotations

import re
from html import unescape
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlsplit

import pytest

from tests.test_planner import _assert_lazy_stops, _client

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "endpoints",
    [
        "from=1001&to=2002",
        "from=1001&to=4004",
        "from_lat=52.331&from_lon=20.921&q_from=Start&to_lat=52.27&to_lon=20.97&q_to=End",
        "from=1001&to=1001",
    ],
)
@pytest.mark.parametrize("lang", ["en", "pl"])
def test_results_fragment_matches_full_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, endpoints: str, lang: str
) -> None:
    client = _client(tmp_path, monkeypatch)
    query = f"date=2026-09-23&time=07:00&{endpoints}&lang={lang}"
    full = client.get(f"/planner?{query}")
    response = client.get(f"/planner/results?{query}")
    assert full.status_code == response.status_code == HTTPStatus.OK
    fragment = response.get_data(as_text=True).strip()
    section = re.search(r'<section id="pl-results".*?</section>', full.get_data(as_text=True), re.DOTALL)
    assert section is not None
    assert fragment == section.group()
    assert "<html" not in fragment
    assert "<form" not in fragment
    assert "<script" not in fragment
    assert "pl-note" not in fragment
    assert 'hx-trigger="every' not in fragment  # historical results do not poll
    assert set(full.vary) == set(response.vary) == {"Accept-Language", "Cookie"}


def test_fragment_retains_pagination_and_lazy_stop_links(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    fragment = client.get("/planner/results?date=2026-09-23&time=07:00&from=1001&to=2002").get_data(as_text=True)
    _assert_lazy_stops(client, fragment, [(1, "Łomianki", "Metro Marymont"), (-5, "Łomianki", "Metro Marymont")])
    links = re.findall(r'<a class="pl-more" href="([^"]+)"', fragment)
    assert len(links) == 2  # noqa: PLR2004 - earlier and later
    for url, time in zip(links, ["06:30", "08:31"], strict=True):
        parsed = urlsplit(unescape(url))
        assert parsed.path == "/planner"  # pagination navigates to the full page
        assert parse_qs(parsed.query) == {
            "date": ["2026-09-23"],
            "time": [time],
            "from": ["1001"],
            "to": ["2002"],
            "q_from": ["Łomianki"],
            "q_to": ["Metro Marymont"],
        }


@pytest.mark.parametrize("path", ["/planner", "/planner/results"])
def test_page_and_fragment_share_language_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    client = _client(tmp_path, monkeypatch)
    url = f"{path}?date=2026-09-23&time=07:00&from=1001&to=4004"
    polish = client.get(url, headers={"Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8"})
    assert "1 przesiadka" in polish.get_data(as_text=True)
    assert "Set-Cookie" not in polish.headers
    switched = client.get(url + "&lang=pl", headers={"Accept-Language": "en"})
    if path == "/planner":
        assert "planner_lang=pl" in switched.headers["Set-Cookie"]
        assert "SameSite=Lax" in switched.headers["Set-Cookie"]
        assert "Max-Age=31536000" in switched.headers["Set-Cookie"]
    else:
        assert "Set-Cookie" not in switched.headers
        client.set_cookie("planner_lang", "pl")
    assert "1 przesiadka" in client.get(url, headers={"Accept-Language": "en"}).get_data(as_text=True)
    assert "1 change" in client.get(url + "&lang=en", headers={"Accept-Language": "pl"}).get_data(as_text=True)
    assert set(switched.vary) == {"Accept-Language", "Cookie"}


def test_results_refresh_does_not_overwrite_another_tabs_language_choice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch)
    query = "date=2026-09-23&time=07:00&from=1001&to=4004"
    # A fragment pins the original tab's language, even after another tab updates the cookie.
    assert "1 change" in client.get(f"/planner?{query}", headers={"Accept-Language": "en"}).get_data(as_text=True)
    client.get(f"/planner?{query}&lang=pl")
    refreshed = client.get(f"/planner/results?{query}&lang=en")
    assert "1 change" in refreshed.get_data(as_text=True)
    assert "Set-Cookie" not in refreshed.headers
    assert "1 przesiadka" in client.get(f"/planner?{query}", headers={"Accept-Language": "en"}).get_data(as_text=True)


def test_fragment_requires_published_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert _client(tmp_path, monkeypatch, published=False).get("/planner/results").status_code == HTTPStatus.NOT_FOUND


def test_autocomplete_inputs_use_individual_htmx_targets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    page = _client(tmp_path, monkeypatch).get("/planner").get_data(as_text=True)
    assert "data-suggest-url" not in page
    for field in ("from", "to"):
        input_tag = re.search(rf'<input class="pl-input" name="q_{field}"[^>]+>', page)
        assert input_tag is not None
        assert f'hx-get="/planner/suggest/{field}"' in input_tag.group()
        assert 'hx-trigger="input delay:450ms"' in input_tag.group()
        assert f'hx-target="#pl-suggest-{field}"' in input_tag.group()
        assert 'hx-sync="this:replace"' in input_tag.group()
        assert "data-lookup-error=" in input_tag.group()
