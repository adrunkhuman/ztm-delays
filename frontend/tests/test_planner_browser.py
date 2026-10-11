"""Browser regression checks without CDN access or a running server."""

from __future__ import annotations

import base64
import hashlib
import os
import shutil
import subprocess
from pathlib import Path

import pytest

# Prefer the runner's packaged stable Chrome over its Chromium build.
CHROMIUM = shutil.which("google-chrome") or shutil.which("chromium")
# Hosted runners may block user-namespace sandboxing; only trusted offline fixtures run here.
CHROMIUM_CI_FLAGS = ("--no-sandbox",) if os.environ.get("GITHUB_ACTIONS") == "true" else ()
FRONTEND = Path(__file__).resolve().parents[1]
PLANNER_JS = FRONTEND / "ztm_frontend" / "static" / "planner.js"
HTMX_JS = Path(__file__).parent / "vendor" / "htmx-4.0.0-beta5.min.js"


def test_browser_fixture_matches_site_htmx_integrity() -> None:
    template = (FRONTEND / "ztm_frontend" / "templates" / "base.html").read_text()
    integrity = base64.b64encode(hashlib.sha384(HTMX_JS.read_bytes()).digest()).decode()
    assert f'htmx.org@4.0.0-beta5/dist/htmx.min.js" integrity="sha384-{integrity}"' in template


@pytest.mark.skipif(CHROMIUM is None, reason="Chrome or Chromium is not installed")
def test_form_controls_survive_boosted_navigation(tmp_path: Path) -> None:
    """Stop/point controls must use the current form after repeated page replacements."""
    page = tmp_path / "planner.html"
    page.write_text(
        "<!doctype html><html><head><script>"
        + HTMX_JS.read_text()
        + """</script></head><body>
        <div id="page"><form class="pl-form">
          <input type="hidden" name="from" value="1001">
          <input type="hidden" name="to" value="2002">
          <input type="hidden" name="from_lat" disabled><input type="hidden" name="from_lon" disabled>
          <input type="hidden" name="to_lat" disabled><input type="hidden" name="to_lon" disabled>
          <div class="pl-field"><input class="pl-input" name="q_from" value="Łomianki" aria-label="From">
            <span class="pl-dot from" aria-hidden="true"></span>
          </div>
          <div class="pl-field"><input class="pl-input" name="q_to" value="Metro Marymont" aria-label="To" hx-get="https://local.test/planner/suggest/to" hx-trigger="input delay:450ms" hx-target="#pl-suggest-to" hx-sync="this:replace">
            <button type="button" class="pl-swap">⇅</button>
          </div>
          <input name="date" value="2026-09-23"><input name="time" value="07:00">
          <div class="pl-suggestions" id="pl-suggest-from"></div>
          <div class="pl-suggestions" id="pl-suggest-to">
            <button type="button" class="pl-suggestion" data-field="to" data-stop-id="4004">
              <span class="pl-suggestion-name">Centrum</span>
            </button>
          </div>
          <section class="pl-location-panel" id="pl-picker" data-style="https://local.test/map-style.json" data-city="Warsaw" data-reverse-url="https://local.test/planner/reverse" hidden>
            <div class="pl-location-map"></div>
            <span class="pl-picker-error" hidden>Map unavailable</span>
          </section>
          <button type="submit" class="pl-go">Search</button>
        </form></div>
        <pre id="result"></pre>
        <script>"""
        + PLANNER_JS.read_text()
        + "</script><script>"
        + PLANNER_JS.read_text()
        + """</script><script>
        // Mock the external map boundary; points enter through map clicks and marker drags.
        window.testMarkers = {};
        window.maplibregl = {
          Map: class {
            constructor(options) { this.options = options; this.handlers = {}; this.jumps = 0; window.testMap = this; }
            on(event, handler) { this.handlers[event] = handler; }
            addControl() {}
            jumpTo() { this.jumps++; }
            remove() {}
          },
          Marker: class {
            constructor({element}) {
              this.handlers = {}; this.element = element;
              window.testMarkers[element.classList.contains('from') ? 'from' : 'to'] = this;
            }
            setLngLat(at) { this.at = at; return this; }
            addTo() { return this; }
            on(event, handler) { this.handlers[event] = handler; }
            getLngLat() { return { lng: this.at[0], lat: this.at[1] }; }
            remove() {}
          },
          NavigationControl: class {},
          LngLatBounds: class { extend() { return this; } },
        };
        // Complete mocked CDN assets without network access; the loader must still wait for CSS.
        const append = document.head.append.bind(document.head);
        document.head.append = (...nodes) => {
          for (const node of nodes) {
            if (/maplibre-gl/.test(node.src || node.href || '')) queueMicrotask(() => node.onload());
            else append(node);
          }
        };
        const lookups = [];
        let reverseName = 'Nearby street 11';
        window.fetch = async (url, options = {}) => {
          lookups.push(String(url));
          if (String(url).includes('map-style.json')) return { ok: true, json: async () => ({
            sources: {}, layers: [{ id: 'highway_major_inner', type: 'line', minzoom: 11, paint: {} }],
          }) };
          const name = reverseName;
          await new Promise(resolve => setTimeout(resolve, 100));
          if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
          if (String(url).includes('/reverse')) return new Response(JSON.stringify({ name }));
          return new Response('<button type="button" class="pl-suggestion" data-field="to" data-lat="52.23" data-lon="21.01"><span class="pl-suggestion-name">Address 11</span></button>');
        };
        const check = (condition, message) => { if (!condition) throw new Error(message); };
        (async () => { try {
          // htmx replaces #page; rerunning the script above must not install duplicate handlers.
          for (let navigation = 0; navigation < 2; navigation++) {
            const page = document.querySelector('#page');
            page.replaceWith(page.cloneNode(true));
          }
          htmx.process(document.body);
          const form = document.querySelector('.pl-form');
          let submissions = 0;
          form.addEventListener('submit', event => { event.preventDefault(); submissions++; });
          form.querySelector('.pl-suggestion-name').click();
          check(form.elements.q_to.value === 'Centrum', 'destination name not updated');
          check(form.elements.to.value === '4004', 'destination ID not updated');
          check(form.elements.q_from.value === 'Łomianki', 'origin changed');
          check(form.elements.date.value === '2026-09-23', 'date changed');
          check(form.elements.time.value === '07:00', 'time changed');
          check(form.querySelector('#pl-suggest-to').childElementCount === 0, 'suggestions not cleared');
          check(submissions === 0, 'selection submitted the form');
          form.querySelector('.pl-swap').click();
          check(form.elements.q_from.value === 'Centrum' && form.elements.from.value === '4004', 'swap failed');
          check(form.elements.q_to.value === 'Łomianki' && form.elements.to.value === '1001', 'swap failed');
          check(submissions === 0, 'swap submitted the form');
          form.elements.q_to.value = '';
          form.elements.q_to.dispatchEvent(new Event('input', { bubbles: true }));
          check(form.elements.to.value === '' && form.elements.from.value === '4004', 'edit did not clear its ID');
          form.querySelector('#pl-suggest-to').innerHTML = '<button type="button" class="pl-suggestion" data-field="to" data-lat="52.23" data-lon="21.01"><span class="pl-suggestion-name">Street 10</span></button>';
          form.querySelector('.pl-suggestion-name').click();
          check(form.elements.q_to.value === 'Street 10' && form.elements.to.value === '', 'address selection failed');
          check(form.elements.to_lat.value === '52.230000' && !form.elements.to_lat.disabled, 'address coordinates missing');
          form.querySelector('.pl-swap').click();
          check(form.elements.from_lat.value === '52.230000' && !form.elements.from_lat.disabled, 'point swap failed');
          check(form.elements.to_lat.disabled && form.elements.to.value === '4004', 'stop swap failed');
          form.elements.q_from.value = 'Other street';
          form.elements.q_from.dispatchEvent(new Event('input', { bubbles: true }));
          check(form.elements.from_lat.disabled && form.elements.from_lon.disabled, 'edit retained stale coordinates');
          form.elements.q_from.click();
          const panel = form.querySelector('#pl-picker');
          check(!panel.hidden, 'map picker did not open');
          check(!panel.querySelector('.pl-picker-heading, .pl-picker-field, .pl-point-label'), 'picker still has header or footer');
          check(!panel.closest('.pl-field'), 'picker hides destination inside origin row');
          check(!panel.querySelector('input'), 'picker still has editable coordinate inputs');
          check(!panel.querySelector('.pl-picker-use, .pl-picker-cancel'), 'confirmation controls remain');
          await new Promise(resolve => setTimeout(resolve, 0));
          check(testMap.options.style.layers[0].minzoom === 8, 'main roads hidden at city zoom');
          const roadColor = testMap.options.style.layers[0].paint['line-color'];
          check(roadColor[0] === 'match' && roadColor[1][1] === 'class', 'road emphasis does not use tile classification');
          check(roadColor.includes('#565656') && roadColor.at(-1) === '#2c2c2c', 'minor roads not subdued');
          const currentMap = testMap;
          form.elements.q_to.click();
          check(testMap === currentMap && !panel.hidden, 'switching input rebuilt or closed map');
          check(form.elements.q_to.closest('.pl-field').classList.contains('is-map-active'), 'destination input did not activate destination');
          form.querySelector('.pl-dot.from').click();
          check(testMap === currentMap && form.elements.q_from.closest('.pl-field').classList.contains('is-map-active'), 'origin row click did not activate origin');
          check(testMap.options.style.layers.some(layer => layer.id === 'picker-street-names'), 'minor street names missing');
          check(testMap.options.style.layers.some(layer => layer.id === 'picker-city-name'), 'Warsaw label missing');
          testMap.handlers.click({ lngLat: { lat: 95, lng: 21.01 } });
          check(form.elements.from_lat.disabled, 'invalid map point accepted');
          testMap.handlers.click({ lngLat: { lat: 52.23, lng: 21.01 } });
          check(form.elements.from_lat.value === '52.230000', 'map click did not commit immediately');
          check(!panel.hidden, 'first click closed the picker');
          check(form.elements.q_to.closest('.pl-field').classList.contains('is-map-active'), 'origin did not advance to destination');
          check(!form.elements.q_from.closest('.pl-field').classList.contains('is-map-active'), 'origin row still highlighted');
          check(testMap.jumps === 0, 'automatic switch moved the view');
          await new Promise(resolve => setTimeout(resolve, 650));
          check(form.elements.q_from.value === 'Nearby street 11', 'origin label lost after auto-switch');
          check(form.elements.to.value === '4004', 'origin lookup changed destination');
          testMap.handlers.click({ lngLat: { lat: 52.24, lng: 21.02 } });
          check(form.elements.to_lat.value === '52.240000', 'second click did not set arrival');
          check(form.elements.from_lat.value === '52.230000', 'second click changed origin');
          check(!panel.hidden && testMap.jumps === 0, 'second click changed view or closed map');
          check(testMarkers.from.element.classList.contains('from') && testMarkers.to.element.classList.contains('to'), 'markers not distinguished');
          check(testMarkers.from.element.getAttribute('aria-label') === 'From' && testMarkers.to.element.getAttribute('aria-label') === 'To', 'marker labels missing without picker header');
          testMarkers.from.setLngLat([21.005, 52.225]);
          testMarkers.from.handlers.dragstart(); testMarkers.from.handlers.dragend();
          check(form.elements.from_lat.value === '52.225000', 'origin drag not committed');
          check(form.elements.to_lat.value === '52.240000', 'origin drag changed destination');
          reverseName = null;
          testMarkers.to.setLngLat([21.025, 52.245]);
          testMarkers.to.handlers.dragstart(); testMarkers.to.handlers.dragend();
          check(form.elements.to_lat.value === '52.245000', 'destination drag not committed');
          await new Promise(resolve => setTimeout(resolve, 650));
          check(form.elements.q_to.value === '52.245000, 21.025000', 'coordinate label fallback missing');
          check(submissions === 0, 'map interaction submitted the form');
          document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape', bubbles: true}));
          check(panel.hidden && !form.querySelector('.is-map-active'), 'close left picker state active');
          form.elements.q_to.click();
          await new Promise(resolve => setTimeout(resolve, 0));
          check(testMarkers.from.at[1] === 52.225 && testMarkers.to.at[1] === 52.245, 'reopening lost a marker');
          check(testMap.options.bounds, 'reopened map does not include both endpoints');
          form.querySelector('.pl-go').click();
          check(panel.hidden && submissions === 1, 'Search did not submit exactly once');
          lookups.length = 0;
          form.elements.q_to.value = 'Address 11';
          form.elements.q_to.dispatchEvent(new Event('input', { bubbles: true }));
          await new Promise(resolve => setTimeout(resolve, 650));
          check(lookups.filter(url => url.includes('/suggest/')).length === 1, 'dynamic lookup missing');
          check(panel.hidden, 'typing did not dismiss map for suggestions');
          form.querySelector('.pl-suggestion-name').click();
          check(form.elements.q_to.value === 'Address 11', 'address response not selectable');
          check(panel.hidden, 'choosing a suggestion reopened map');
          form.elements.q_to.click();
          check(!panel.hidden, 'chosen destination cannot open map');
          await new Promise(resolve => setTimeout(resolve, 0));
          check(form.elements.q_to.closest('.pl-field').classList.contains('is-map-active'), 'chosen destination activated origin');
          form.querySelector('.pl-swap').click();
          check(panel.hidden && submissions === 1, 'swap opened map or submitted');
          document.querySelector('#result').textContent = 'PASS';
        } catch (error) {
          document.querySelector('#result').textContent = 'FAIL: ' + error.message;
        } })();
        </script></body></html>""",
    )
    assert CHROMIUM is not None
    result = subprocess.run(  # noqa: S603 - fixed browser executable and local test inputs, no shell
        [
            CHROMIUM,
            *CHROMIUM_CI_FLAGS,
            "--headless",
            "--disable-gpu",
            "--no-first-run",
            "--disable-background-networking",
            f"--user-data-dir={tmp_path / 'browser'}",
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
    assert '<pre id="result">PASS</pre>' in result.stdout, result.stdout
