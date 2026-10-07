# Frontend

Server-rendered Flask app for the transit archive. Overview, line, stop, and trip views expose delays and reconstructed service; the status page shows archive coverage, export freshness and, when configured, the live poller and GPS feed history.

The app reads a local DuckDB serving artifact in read-only mode. It does not refresh data or connect to BigQuery or the city API. The only GCS reads are the two public status objects below. Monthly route maps are read from `maps/` beside the DuckDB file, or from `ZTM_MAPS_DIR`. The map page loads MapLibre and OpenFreeMap tiles in the browser.

## Run

From `frontend/`, with an existing serving export:

```sh
uv sync --locked
ZTM_DUCKDB_PATH=/absolute/path/to/ztm.duckdb \
  uv run flask --app ztm_frontend.app run --debug
```

The local default is `ztm/ztm.duckdb`. A partitioned export also requires its referenced Parquet files at the paths stored in the catalog; copying only the DuckDB file is insufficient.

The [container](Dockerfile) uses Waitress on port 5000. Mount serving storage read-only and set `ZTM_DUCKDB_PATH` to the catalog. Connections last for one request, so later requests see refreshed data without restarting the app. The [publication procedure](../docs/operations.md#serving-publication) covers shared paths and export replacement.

## Reading the archive

Trip times display in `Europe/Warsaw`; status timestamps use UTC. Delay is actual minus scheduled arrival time.

| Classification | Delay |
| --- | --- |
| Early | At least 60 seconds early. |
| On time | Strictly between 60 seconds early and 180 seconds late. |
| Late | At least 180 seconds late. |

Delay summaries use complete trips. Detail pages retain lower-quality evidence and distinguish observed, uncertain, and unobserved stops. “No obs.” means insufficient GPS evidence, not a confirmed skipped stop. Request stops are marked separately.

| Period | Included dates |
| --- | --- |
| Day | Selected service date. |
| Weekdays / weekend | Up to 60 observed dates of the corresponding GTFS service type, through the selected date. |
| Month | Observed dates in the selected calendar month, through the selected date. |

Weekday/weekend classification follows timetable service, not a calendar-only filter. Line comparisons in those windows use timetable versions active at the selected anchor; month views retain the whole observed month. Daily chart medians are not recombined into aggregate medians.

Entity rankings use qualifying zone-1 public-service trips and minimum observation counts. They are not rankings of every scheduled line or stop. [Architecture](../docs/architecture.md#interpreting-the-results) explains the wider coverage limitations.

The route map shows **net delay change** between consecutive scheduled stops: the downstream stop's signed arrival delay minus the upstream stop's. Red means delay grows over that segment; blue means time is recovered. It uses complete trips with both scheduled arrivals between 06:00 and 22:00, and corridors with at least 100 traversals in the period. Weekdays and weekends are calendar days. A segment's change includes dwell time at the upstream stop. It does not show where inside the segment the delay arose. `scripts/build_map_style.py` regenerates the dark basemap style.

Status coverage uses observed/expected service minutes in the summary and observed/expected trips in daily rows. Sidecar metadata is accepted only when its export ID matches the database.

## Live status

`/status` shows a live poller panel and 24 h feed-history charts from two small JSON objects the pipeline publishes under `health/poller/public/`: `live.json` (poller heartbeat, about every minute) and `feed-history.json` (hourly). Set `ZTM_STATUS_GCS_BUCKET` to the bucket name to enable it; without it the page says "live status unavailable" and nothing else changes.

The pill and per-mode states are derived on read: the poller is silent after 180 s without a heartbeat, a mode is failing after 3 failed polls in a row, and its feed is thin when the fresh fleet is below half of the usual count for that weekday and minute (usual counts come from `feed-history.json`; minutes with fewer than 20 usual vehicles count as night service). Objects are cached in process (30 s and 5 min), size-limited (16 KiB and 1 MiB) and read with a 5 s timeout. A missing `live.json` shows "poller offline"; unreadable or malformed objects show "unavailable" for that section and never fail the page.

Credentials use the standard Google application default mechanism. Mount a service-account key at runtime and point `GOOGLE_APPLICATION_CREDENTIALS` at it. Never bake the key into the image. Grant the account read-only access (`roles/storage.objectViewer`) limited to the `health/poller/public/` prefix, for example with an IAM condition on `resource.name`, since nothing private lives there.

## Planner

The planner tab reads a separate artifact, `planner/planner.duckdb` beside the export or `ZTM_PLANNER_PATH`, and stays hidden until one exists. The pipeline rebuilds it nightly; [planner.py](ztm_frontend/planner.py) lists its tables.

The [journey router](ztm_frontend/journey.py) searches between stop groups (all posts of one stop) with up to five vehicles and walks before, between and after rides. It is a round-based search in the style of RAPTOR, except that it does not assume trips keep their order, because predicted times let them overtake. A profile search (rRAPTOR) lists every journey not beaten on departure, conservative arrival and number of vehicles, leaving up to 8 h after the requested time. It includes previous-day trips running past midnight but not the next day's trips. [Planner timing](../planner/README.md#journeys-and-walks) defines expected and conservative times.

Each process caches the networks of two service days, keyed by the artifact's `build_id`. Cards show when to be at the stop, the changes and walks, and load a ride's stop list only when expanded. Times differing from the timetable by 2 min or more are flagged with `!`.

[live.py](ztm_frontend/live.py) matches the poller's live positions (`health/poller/public/vehicles.json.gz` in `ZTM_STATUS_GCS_BUCKET`, read at most every 10 s; `ZTM_LIVE_VEHICLES_FILE` reads a local copy) to today's trips: line and brigade name the vehicle's duty, and its position along the trips' shapes gives the trip and its delay there. A vehicle at a terminus waits for its duty's next trip. Replayed on 6 Oct 2026 (07:00–10:00 and 14:00–18:00, 10 s snapshots), 99.8% of fixes on running vehicles named the trip the nightly matcher reconstructed, 99.4% of the vehicles it saw running got one, and the delay was off by 14 s on average for buses and 16 s for trams. Matching a snapshot of about 2,000 vehicles takes about 80 ms.

For today, [live_times.py](ztm_frontend/live_times.py) moves the times of trips with a live vehicle, using the artifact's weekly live calibration ([planner](../planner/README.md#live-calibration)); without one the planner ignores live positions. Stops ahead of a running vehicle keep the share of its current delay beyond usual that the calibration expects that far down the route. Expected and late times follow; "be at the stop by" may move later only for stops due within 15 min, and earlier at any distance. Passed stops can no longer be boarded. A vehicle late for its duty's next trip moves that trip's expected and late times, but not when to be at the stop: another vehicle may run it on time. Each feed object gets one patched copy of the day's network (about 0.25 s for 2,000 vehicles); the cached network is not changed.

Replayed on 5 and 6 Oct 2026 with the week before's calibration, expected times at stops up to 10 min ahead of a running vehicle were off by 48-54 s for buses and 35-36 s for trams, against 137-185 s and 83-86 s without live positions. At most 0.7% of vehicles left more than 30 s before the shown "be at the stop by" in any 10-minute band of distance and mode, against up to 1.2% for the stop tables alone; about half of bus boardings within 10 min moved more than 2 min later.

The planner is in Polish and English. The PL/EN switch stores the choice in a cookie; without one, browsers preferring English get English and others Polish. Wording lives in [planner_text.py](ztm_frontend/planner_text.py).

## Checks

From `frontend/`:

```sh
uv run pytest tests
uv run ruff check .
```

Tests create their own DuckDB fixtures; no serving download is needed. [queries.py](ztm_frontend/queries.py) defines page queries; the export's [source allowlist](../airflow/dags/dag_serving_export.py) defines available tables.
