# ZTM Matcher

Standalone, local-first preparation for one Warsaw GPS processing date and one
pinned GTFS ZIP. It uses bounded DuckDB/Arrow execution, not Airflow, dbt, or
BigQuery.

```shell
uv run --project matcher ztm-matcher prepare --processing-date 2026-07-09 \
  --snapshot-id 2026-07-08T00:00:00Z_37117bdef8d6 --gps-root cache/raw/gps \
  --gtfs-zip cache/gtfs/snapshot.zip --output-dir work/2026-07-09
```

GPS partitions are `vehicle_type={bus,tram}/date=YYYY-MM-DD/hour=HH/*.parquet`.
The runtime validates `raw-gps-v1`, reports missing hours, applies the dbt
Warsaw-day/numeric-coordinate/dedup contract, and retains same-coordinate
pings. GTFS requires six UTF-8 (BOM accepted) files: `trips`, `stop_times`,
`stops`, `shapes`, `routes`, and `calendar_dates`; only exception type `1` is
active. Prior and current service dates are both required, and GTFS times may
exceed 24:00.

All 24 hourly partitions for both bus and tram are required by default. A known
partial-day diagnostic run must opt in with `--allow-missing-hours`; the missing
inventory remains recorded in its manifest.

Success atomically publishes normalized GPS, duty schedule, stop semantics,
manifest, and metrics. Failed `.incomplete-*` work directories are preserved.
Errors use stable codes: `missing_input`, `schema_drift`, `invalid_data`,
`snapshot_mismatch`, `resource_limit`, and `invalid_output`.

DuckDB defaults to two threads, 1024 MB of managed memory, and 20 GB of
temporary disk. The loader additionally rejects GTFS archives above 640 MB
uncompressed or 1,800,000 selected stop rows. It streams the rolling feed and
retains only services active on the required current/prior dates so Python
schedule preparation remains bounded outside DuckDB; stop semantics are written
in 10,000-row Parquet batches. Metrics include wall and CPU time,
peak RSS and swap where the OS exposes them, temporary and artifact disk use,
and vehicle-group sizes.
