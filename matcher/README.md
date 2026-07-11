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
`duty_execution.parquet`, manifest, and metrics. `duty_execution` has exactly
one outcome per scheduled course: `executed`, `missed`, `skipped`,
`short_turned`, `vehicle_change_signal`, or `uncertain`. A vehicle-change
signal is competing GPS evidence, not a claim that a physical vehicle swap
occurred. It retains source observation bounds, candidate counts, reasons, and
evidence; ownership intervals are emitted only for confident executed courses.
Failed `.incomplete-*` work directories are preserved.
Errors use stable codes: `missing_input`, `schema_drift`, `invalid_data`,
`snapshot_mismatch`, `resource_limit`, and `invalid_output`.

DuckDB defaults to two threads, 320 MB of managed memory, and 20 GB of
temporary disk. The loader additionally rejects GTFS archives above 640 MB
uncompressed or 1,800,000 selected stop rows. It streams the rolling feed and
retains only services active on the required current/prior dates so Python
schedule preparation remains bounded outside DuckDB; stop semantics are written
in 10,000-row Parquet batches. Metrics include wall and CPU time,
peak RSS and swap where the OS exposes them, temporary and artifact disk use,
and vehicle-group sizes plus execution-status counts. Terminal visits use a
250 m radius and start a new episode after a GPS gap over 180 seconds. The
runtime processes one normalized vehicle Arrow stream and only its matching
schedule subset at a time; final allocation reads one duty at a time. Allocation
does not reject late journeys by a fixed lateness cutoff: it evolves observed
delay through ordered courses, has an explicit skipped-course state, and never
reuses a terminal traversal. Unknown passenger boundaries and `line_brigade`
duty fallback cannot receive high confidence.

The July 9 VPS proof with duty alignment measured about 1.63 GiB peak process
RSS with this limit and zero matcher swap. The lower DuckDB allowance trades
bounded temporary I/O for enough memory headroom to add stop alignment.
