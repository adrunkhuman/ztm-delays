# Frontend

Server-rendered Flask app for the transit archive. Overview, line, stop, and trip views expose delays and reconstructed service; the status page shows archive coverage and export freshness.

The app reads a local DuckDB serving artifact in read-only mode. It does not refresh data or connect to BigQuery, GCS, or the city API.

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

Status coverage uses observed/expected service minutes in the summary and observed/expected trips in daily rows. Poller status is captured at export time, not live. Sidecar metadata is accepted only when its export ID matches the database.

## Checks

From `frontend/`:

```sh
uv run pytest tests
uv run ruff check .
```

Tests create their own DuckDB fixtures; no serving download is needed. [queries.py](ztm_frontend/queries.py) defines page queries; the export's [source allowlist](../airflow/dags/dag_serving_export.py) defines available tables.
