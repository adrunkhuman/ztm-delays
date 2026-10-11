from __future__ import annotations

import json
import re
import runpy
import subprocess
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from tests.test_live_status import fresh_fixture
from tests.test_planner import _client
from tests.test_planner_browser import CHROMIUM, CHROMIUM_CI_FLAGS, FRONTEND
from ztm_frontend import live_status, queries

STATIC = FRONTEND / "ztm_frontend/static"


def _offline(page: str, *, script: str = "") -> str:
    page = re.sub(r"<link\b[^>]*>", "", page)
    page = re.sub(r"<script\b[^>]*src=[^>]*>\s*</script>", "", page)
    page = page.replace("</head>", "<style>" + (STATIC / "site.css").read_text() + "</style></head>")
    return page.replace("</body>", "<script>" + script + "</script></body>")


def test_shared_basemap_preserves_picker_roads_and_labels() -> None:
    style = json.loads((STATIC / "map-style.json").read_text())
    build = runpy.run_path(str(FRONTEND / "scripts/build_map_style.py"))
    assert build["dark_style"](style) == style
    layers = {layer["id"]: layer for layer in style["layers"]}
    for name in ("highway_minor", "highway_major_inner", "highway_motorway_inner"):
        road = layers[name]
        assert road["minzoom"] <= 8  # noqa: PLR2004 - roads must be visible at the city scale
        assert road["paint"]["line-color"] == [
            "match",
            ["get", "class"],
            ["motorway", "trunk", "primary"],
            "#565656",
            "secondary",
            "#454545",
            "tertiary",
            "#373737",
            "#2c2c2c",
        ]
    assert layers["street-names"]["source-layer"] == "transportation_name"
    assert layers["warsaw-name"]["layout"]["text-field"] == "Warszawa"
    assert layers["highway-name-major"]["paint"]["text-color"] == "#bbbbbb"
    assert all(not name.startswith("picker-") for name in layers)


def test_shared_style_url_is_content_versioned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    html = client.get("/planner").get_data(as_text=True)
    version = sha256((STATIC / "map-style.json").read_bytes()).hexdigest()[:12]
    assert f'data-style="/static/map-style.json?v={version}"' in html


@pytest.mark.skipif(CHROMIUM is None, reason="Chrome or Chromium is not installed")
@pytest.mark.parametrize("lang", ["en", "pl"])
def test_phone_layout_and_chart_readouts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lang: str) -> None:
    client = _client(tmp_path, monkeypatch)
    planner_page = client.get(f"/planner?lang={lang}&date=2026-09-23").get_data(as_text=True)
    payload = fresh_fixture()[live_status.HISTORY_OBJECT]
    history = live_status.parse_history(payload)
    assert history is not None
    view = live_status.build_history(history, datetime.now(UTC))
    monkeypatch.setattr(live_status, "history_view", lambda: view)
    monkeypatch.setattr(live_status, "live_view", lambda: {"available": False})
    monkeypatch.setattr(queries, "get_status", lambda _: {"metadata": {}, "status_summary": {}, "status_days": []})
    status_page = client.get("/status").get_data(as_text=True)
    assert status_page.count('class="status-chart"') == 2  # noqa: PLR2004 - bus and tram
    config = json.dumps(
        {
            "planner": _offline(planner_page),
            "status": _offline(status_page, script=(STATIC / "status.js").read_text()),
        }
    ).replace("</", "<\\/")
    harness = (Path(__file__).parent / "browser/mobile_layout.js").read_text()
    page = tmp_path / "layout.html"
    page.write_text(
        '<!doctype html><html><body><pre id="layout-result">RUNNING</pre>'
        f"<script>window.layoutFixture={config};</script><script>{harness}</script></body></html>",
    )
    assert CHROMIUM is not None
    result = subprocess.run(  # noqa: S603 - fixed executable and trusted offline fixtures, no shell
        [
            CHROMIUM,
            *CHROMIUM_CI_FLAGS,
            "--headless",
            "--window-size=1400,2200",
            "--disable-gpu",
            "--no-first-run",
            "--disable-background-networking",
            "--no-proxy-server",
            "--host-resolver-rules=MAP * ~NOTFOUND",
            f"--user-data-dir={tmp_path / 'chromium'}",
            "--dump-dom",
            "--virtual-time-budget=5000",
            page.as_uri(),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    match = re.search(r'<pre id="layout-result">(.*?)</pre>', result.stdout, re.DOTALL)
    assert match is not None, result.stdout[-3000:] + result.stderr[-1000:]
    assert match.group(1) == "PASS", match.group(1)
