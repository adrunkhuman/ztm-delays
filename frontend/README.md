# ZTM Frontend

Server-rendered Flask frontend for the ZTM DuckDB serving artifact.

This is an archive prototype backed by the DuckDB serving artifact produced from the nightly Python reconstruction path.

## Runtime Contract

The app reads one DuckDB file in read-only mode. It does not build or refresh the export.

Required data:

- `ztm.duckdb`: serving database produced by the pipeline export job.
- The database must contain `export_metadata`, `mart_trip_daily`, `fct_expected_stop_event`, entity timeline and window marts, stop/line dimensions, and status marts documented in `docs/serving_contract.md`.

Important quirks:

- DuckDB timestamps from BigQuery are treated as UTC. Trip times display in `Europe/Warsaw`; status timestamps display in UTC, with original values in hover titles.
- Widgets are backed by exported DuckDB rows or derived frontend-serving tables; some presentation transforms still reshape those rows for compact charts.
- Historical quality remains date-dependent and is exposed through the status and quality fields in the serving contract.
- Status shows the exported poller snapshot, not live health. Summary coverage is observed/expected service minutes; daily coverage is observed/expected trips. Clean/partial/broken counts map to the exported trip-quality categories. Summary partial counts are summed from daily rows only when the complete summary window is available; otherwise they show `n/a`.

## Environment Variables

- `ZTM_DUCKDB_PATH`: path to the serving DuckDB file. Defaults to `ztm/ztm.duckdb` relative to `frontend/`.
- `FLASK_DEBUG`: optional Flask debug flag for local development.
- `FLASK_RUN_HOST`: optional local bind host, for example `0.0.0.0`.
- `FLASK_RUN_PORT`: optional local port, for example `5000`.

No GCP, ZTM API, or Tailscale secrets are needed by the frontend process.

## Local Development

From `frontend/`:

```powershell
uv sync
uv run flask --app ztm_frontend.app run --debug
```

With an explicit export path:

```powershell
$env:ZTM_DUCKDB_PATH = "C:\path\to\ztm.duckdb"
uv run flask --app ztm_frontend.app run --debug
```

Useful checks:

```powershell
uv run ruff check .
uv run ruff format --check .
uv run ty check .
```

Basic render smoke test:

```powershell
uv run python -c 'from ztm_frontend.app import create_app; app=create_app(); client=app.test_client(); urls=["/", "/lines/", "/stops/", "/trips/", "/status"]; [print(url, client.get(url).status_code) for url in urls]; assert all(client.get(url).status_code == 200 for url in urls)'
```

## Production

Production should mount the serving DuckDB file read-only and set `ZTM_DUCKDB_PATH` to that mounted path.

The pipeline/export side owns refresh:

1. Build a new DuckDB file at a temporary path.
2. Validate it.
3. Atomically swap it into the stable `ztm.duckdb` path.

The frontend opens DuckDB connections per query, so new requests observe the refreshed file without a frontend container restart once the stable path changes.

Do not bake `ztm.duckdb` into the image. Treat it as runtime data.

## Pages

- `/`: overview.
- `/lines/`: line ranking landing page.
- `/lines/<line>`: line detail.
- `/stops/`: stop ranking landing page.
- `/stops/<stop_group_id>`: stop-group/post selector.
- `/stops/<stop_group_id>/<post>`: stop-post detail.
- `/trips/`: trip ranking landing page.
- `/trips/?line=<line>`: per-line trip browser.
- `/trips/<trip_id>?date=<service_date>&vehicle=<vehicle_number>`: trip detail.
- `/status`: archive coverage/status.

## Period display

The compact top-right date selector shows a date, month/year, or observed range
and count (`29 Jun–07 Sep · 49d`). Underlined scope tabs establish the service type.
Desktop metrics use four columns, reducing to two and then one on smaller screens.
Weekday and weekend/holiday scopes are rolling
samples of up to 60 observed GTFS-classified service dates, not calendar-day
filters. Month is the observed portion of the anchor's calendar month. Scope
links keep the current navigation filters; the arrows move the anchor (weekly
for weekday/weekend scopes). Holidays belong to weekend service; line statistics
use timetable versions active at the anchor.

Grouped comparison charts show up to 12 recent **exact daily medians** from
`mart_entity_window_daily_summary`. They do not combine daily medians into weekly
or monthly statistics. Detail timelines retain every exported member date in a
bounded horizontal strip, with sparse date labels. Each strip plots delay magnitude on its own linear scale,
using the same bottom baseline as the other charts. Bars are gray, with the
selected date in blue, matching the weekly charts. Signed values remain in native
tooltips and the accessible table; nulls use an ×, and zero is a baseline mark.
Hover a point for its date and value;
a visually hidden date-and-median table provides screen-reader access without
adding layout height. The chart has one keyboard stop for scrolling, not one per point. Day scope retains its seven-day mean comparison and departure timeline.

Route patterns have native expand/collapse controls; the first two published
patterns start open, with other patterns' destinations and trip counts always
visible. Date and time use separate, non-wrapping lines for grouped departures.
Trip detail remains a single dated run and links back to the originating period.

Validate locally with `uv run pytest`, `uv run ruff check .` and `uv run ty check`.
The period tests cover all four scopes and the overview, line, stop and trip
views; browser checks should additionally cover 390px, 768px and desktop widths,
60-date strips, long stop names, and expanding minor route patterns against a
real serving export.
