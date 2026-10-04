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

The planner tab reads a separate artifact, `planner/planner.duckdb` beside the export or `ZTM_PLANNER_PATH`, and stays hidden until one exists. The pipeline rebuilds it nightly; [planner.py](ztm_frontend/planner.py) lists its tables.

The [journey router](ztm_frontend/journey.py) searches between stop groups (all posts of one stop) with up to five vehicles and walks before, between and after rides. It is a round-based search in the style of RAPTOR, except that it does not assume trips keep their order, because predicted times let them overtake. A profile search (rRAPTOR) lists every journey not beaten on departure, conservative arrival and number of vehicles, leaving up to 8 h after the requested time. It includes previous-day trips running past midnight but not the next day's trips. [Planner timing](../planner/README.md#journeys-and-walks) defines expected and conservative times.

Each process caches the networks of two service days, keyed by the artifact's `build_id`. Cards show when to be at the stop, the changes and walks, and load a ride's stop list only when expanded. Times differing from the timetable by 2 min or more are flagged with `!`.

The planner is in Polish and English. The PL/EN switch stores the choice in a cookie; without one, browsers preferring English get English and others Polish. Wording lives in [planner_text.py](ztm_frontend/planner_text.py).

## Checks

From `frontend/`:

```sh
uv run pytest tests
uv run ruff check .
```

Tests create their own DuckDB fixtures; no serving download is needed. [queries.py](ztm_frontend/queries.py) defines page queries; the export's [source allowlist](../airflow/dags/dag_serving_export.py) defines available tables.
