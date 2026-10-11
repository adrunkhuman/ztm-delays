"""Offline integration checks against the site's pinned htmx, templates and planner script."""

from __future__ import annotations

import json
import re
import subprocess
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, Any

import pytest

from tests.test_planner import _client
from tests.test_planner_browser import CHROMIUM, CHROMIUM_CI_FLAGS, HTMX_JS, PLANNER_JS
from ztm_frontend import planner

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date, datetime

HARNESS = Path(__file__).parent / "browser" / "planner_htmx.js"
QUERY = "date=2026-09-23&time=07:00&from=1001&to=2002"


def _offline(page: str) -> str:
    """Keep production HTML/attributes but replace network-loaded assets with local source."""
    page = re.sub(r"<link\b[^>]*>", "", page)
    page = re.sub(r"<script\b[^>]*src=[^>]*>\s*</script>", "", page)
    return page.replace("</main>", "</main><script>" + PLANNER_JS.read_text() + "</script>")


@pytest.fixture
def snapshots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    client = _client(tmp_path, monkeypatch)
    get_page = planner.get_page

    def live_page(
        path: Path, args: dict[str, str], today: date, now_sod: int, now: datetime | None = None
    ) -> dict[str, Any]:
        page = get_page(path, args, today, now_sod, now)
        # Live-data boundary only: cards, minimaps and lazy trip links are rendered by Jinja.
        card = page["results"][0]
        card["live"] = True
        for item in card["timeline"]:
            if item["kind"] == "ride":
                item["live"] = {
                    "status": "running",
                    "late": 1,
                    "map": {
                        "vehicle": [20.93, 52.32],
                        "stop": [20.921, 52.331],
                        "path": [[20.93, 52.32], [20.921, 52.331]],
                    },
                }
        return page

    monkeypatch.setattr(planner, "get_page", live_page)
    data = {}
    for lang in ("en", "pl"):
        response = client.get(f"/planner?{QUERY}&lang={lang}")
        assert response.status_code == HTTPStatus.OK
        data[lang] = _offline(response.get_data(as_text=True))
        response = client.get(f"/planner/results?{QUERY}&lang={lang}")
        assert response.status_code == HTTPStatus.OK
        fragment = response.get_data(as_text=True)
        assert 'hx-trigger="every[plannerShouldRefresh()] 60s"' in fragment
        # A manual trigger exercises the same request/swap path without waiting a minute.
        data[f"results_{lang}"] = fragment.replace(
            'hx-trigger="every[plannerShouldRefresh()] 60s"',
            'hx-trigger="refresh, every[plannerShouldRefresh()] 60s"',
        )
    response = client.get("/planner/trip/1?date=2026-09-23&board=0&alight=1")
    assert response.status_code == HTTPStatus.OK
    data["trip"] = response.get_data(as_text=True)
    for field, query in (("from", "lom"), ("to", "cent")):
        response = client.get(f"/planner/suggest/{field}?q_{field}={query}&lang=en")
        assert response.status_code == HTTPStatus.OK
        data[f"suggest_{field}"] = response.get_data(as_text=True)
    return data


@contextmanager
def _server(page: str) -> Iterator[tuple[str, list[str]]]:
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append(self.path)
            payload = page.encode() if self.path.startswith("/planner?") else b""
            self.send_response(200 if payload else 404)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *_args: object) -> None:  # noqa: A002 - handler's keyword API
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/planner?{QUERY}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.mark.skipif(CHROMIUM is None, reason="Chromium is not installed")
@pytest.mark.parametrize(
    ("case", "lang"),
    [
        ("autocomplete", "en"),
        ("autocomplete", "pl"),
        ("cancellation", "en"),
        ("navigation", "en"),
        ("results", "en"),
        ("assets-css", "en"),
        ("assets-js", "en"),
    ],
)
def test_planner_htmx_browser(tmp_path: Path, snapshots: dict[str, str], case: str, lang: str) -> None:
    # The result lives outside #page, so boosted swaps and history cannot overwrite diagnostics.
    style = json.loads((PLANNER_JS.parent / "map-style.json").read_text())
    config = json.dumps({"case": case, "lang": lang, "snapshots": snapshots, "mapStyle": style}).replace("</", "<\\/")
    bootstrap = f"<script>window.browserFixture={config};</script><script>{HARNESS.read_text()}</script>"
    initial = snapshots[lang]
    if case == "results":
        initial = initial.replace(
            'hx-trigger="every[plannerShouldRefresh()] 60s"',
            'hx-trigger="refresh, every[plannerShouldRefresh()] 60s"',
        )
    page = initial.replace("</head>", bootstrap + "<script>" + HTMX_JS.read_text() + "</script></head>")
    page = page.replace("</body>", '<pre id="browser-result">RUNNING</pre></body>')
    with _server(page) as (url, server_requests):
        assert CHROMIUM is not None
        result = subprocess.run(  # noqa: S603 - fixed executable and local fixture, no shell
            [
                CHROMIUM,
                *CHROMIUM_CI_FLAGS,
                "--headless",
                "--disable-gpu",
                "--no-first-run",
                "--disable-background-networking",
                "--no-proxy-server",
                "--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1",
                f"--user-data-dir={tmp_path / 'chromium'}",
                "--dump-dom",
                "--virtual-time-budget=5000",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    assert result.returncode == 0, result.stderr
    match = re.search(r'<pre id="browser-result">(.*?)</pre>', result.stdout, re.DOTALL)
    assert match is not None, result.stdout[-3000:] + result.stderr[-1000:]
    assert match.group(1).startswith("PASS:"), match.group(1)
    assert [path for path in server_requests if path != "/favicon.ico"] == [f"/planner?{QUERY}"]
