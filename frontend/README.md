# Frontend

Server-rendered Flask app for the transit archive. Overview, line, stop, and trip views expose delays and reconstructed service; the status page shows archive coverage and export freshness.

The app reads a local DuckDB serving artifact in read-only mode. It does not refresh data or connect to BigQuery, GCS, or the city API. Monthly route maps are read from `maps/` beside the DuckDB file, or from `ZTM_MAPS_DIR`. The map page loads MapLibre and OpenFreeMap tiles in the browser.

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

Status coverage uses observed/expected service minutes in the summary and observed/expected trips in daily rows. Poller status is captured at export time, not live. Sidecar metadata is accepted only when its export ID matches the database.

## Planner

The [journey router](ztm_frontend/journey.py) finds connections between stop groups (all posts of one stop), with up to five vehicles and walks. The planner page shows per-leg cards, walking legs and change deadlines, and loads each ride's stop list only when expanded.

Bus and tram expected arrivals use the boarding stop's usual delay plus the predicted cumulative ride-time difference. Conservative arrivals use the boarding stop's late delay plus a monotone calibrated upper ride-duration envelope (ratios at least 1; fallback 1.1). Departure bases are clamped to the boarding deadline, and the conservative base is never earlier than expected departure. Metro and SKM retain timetable ride durations, with a fixed 60 s late margin for SKM. Every change requires `conservative arrival + walk <= next board_by`; a same-post change has zero walk. These per-ride bounds are planning margins, not a claim of 90% end-to-end reliability.

The bounded round-based earliest-arrival search is inspired by RAPTOR but does not assume FIFO: trips may overtake. Departures come from a profile search (rRAPTOR: windows from the requested time, each run from its latest departure to its earliest, reusing labels), pruned by a no-waiting lower bound to the destination; within the searched windows the list of nondominated alternatives is complete. Previous-day night trips are supported; next service day's daytime trips are not included in the same search, and cross-service-day routing is deferred.

It reads a separate artifact, `planner/planner.duckdb` beside the export or `ZTM_PLANNER_PATH`; the tab is hidden until one exists. [planner.py](ztm_frontend/planner.py) documents its tables. The artifact is rebuilt nightly by the pipeline from the current timetable and the travel-time models; page requests only read it.

## Checks

From `frontend/`:

```sh
uv run pytest tests
uv run ruff check .
```

Tests create their own DuckDB fixtures; no serving download is needed. [queries.py](ztm_frontend/queries.py) defines page queries; the export's [source allowlist](../airflow/dags/dag_serving_export.py) defines available tables.
