# ZTM Frontend

Server-rendered Flask frontend for the ZTM DuckDB serving artifact.

This is an alpha archive prototype backed by the DuckDB serving artifact. Some archive facts are still provisional current-pipeline outputs rather than settled nightly matcher results.

## Runtime Contract

The app reads one DuckDB file in read-only mode. It does not build or refresh the export.

Required data:

- `ztm.duckdb`: serving database produced by the pipeline export job.
- The database must contain `export_metadata`, `fct_trip`, `fct_stop_arrival`, `agg_line_daily`, stop/line dimensions, and status marts.

Important quirks:

- DuckDB timestamps from BigQuery are treated as UTC and displayed in `Europe/Warsaw`.
- Widgets are backed by exported DuckDB rows or derived frontend-serving tables; some presentation transforms still reshape those rows for compact charts.
- Current facts are alpha/current-pipeline facts, not settled nightly matcher output.

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
