# ZTM dbt Project

This dbt project transforms BigQuery raw tables for the ZTM pipeline.

Schedule versions now read a persistent daily fingerprint ledger. **Bootstrap is explicit and bounded; `--full-refresh` is forbidden for the ledger.** Flags and local tools are below; see the [rebuild order](../docs/runbook.md#from-scratch-rebuild) for a fresh warehouse.

Use Python `3.13` for local dbt commands. The current dbt stack is verified with `dbt-core 1.11.11` and `dbt-bigquery 1.11.3`.

GPS staging and completeness models require `processing_date`. The Python matcher owns trip and arrival reconstruction; dbt enriches its stable inputs and publishes current/prior service-date facts:

```bash
uvx --python 3.13 --from dbt-core==1.11.11 --with dbt-bigquery==1.11.3 dbt run --select stg_gps__pings --vars '{"processing_date": "YYYY-MM-DD"}'

uvx --python 3.13 --from dbt-core==1.11.11 --with dbt-bigquery==1.11.3 dbt run --select fct_trip fct_stop_arrival fct_expected_stop_event --vars '{"processing_date": "YYYY-MM-DD", "gtfs_snapshot_id": "SNAPSHOT_ID", "publish_service_date": "SERVICE_DATE"}'

uvx --python 3.13 --from dbt-core==1.11.11 --with dbt-bigquery==1.11.3 dbt run --select mart_day_completeness agg_service_coverage mart_pipeline_status --vars '{"processing_date": "YYYY-MM-DD", "aggregation_start_date": "YYYY-MM-DD"}'
```

Archive-safe dimensions rebuild across loaded GTFS snapshots. The following normal refresh requires an already bootstrapped ledger; for a fresh warehouse use the [rebuild order](../docs/runbook.md#from-scratch-rebuild). Current convenience lookups also require the selected `gtfs_snapshot_id`:

```bash
uvx --python 3.13 --from dbt-core==1.11.11 --with dbt-bigquery==1.11.3 dbt run --select dim_line dim_stop_post dim_stop_group dim_date dim_schedule_date int_gtfs_processing_snapshot int_schedule_fingerprint_daily int_gtfs_trip_schedule int_schedule_version dim_schedule_version dim_line_current dim_stop_post_current dim_stop_group_current dim_schedule_date_current --vars '{"gtfs_snapshot_id": "SNAPSHOT_ID"}'
```

Historical facts bake labels from the selected snapshot used for their rebuild. `_current` dimensions are present-day convenience surfaces only and must not be used to relabel historical facts.

The three facts overwrite `publish_service_date`, defaulting to `processing_date`. A normal matcher `gps_date = processing_date` artifact already combines source GPS dates `D-1` and `D`; dbt selects that one artifact by exact `gps_date` when publishing both service-date partitions. `source_gps_date` keeps direct-arrival lineage. Outage-boundary runs explicitly use current-only input rather than reading an excluded prior GPS date. If historical processing date `D` later republishes service date `D`, rows already published from `gps_date > D` are retained, and a newer overlay supersedes the same trip from `D`.

Schedule versions are per-line timetable fingerprints derived from selected snapshots across collected history. They intentionally exclude display labels and unstable GTFS identifiers. Processing dates use their persisted governing snapshot from `int_gtfs_processing_snapshot`. Nightly runs republish the current and prior service dates; ledger reconciliation checks all mapped dates rather than using a runtime snapshot override.

Scheduled GTFS timestamps use one Warsaw wall-clock macro for service date plus GTFS seconds. It normalizes spring-forward gaps and chooses the first fall-back occurrence, matching the Python matcher.

`mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status` incrementally replace the inclusive `[aggregation_start_date, processing_date]` partitions. `mart_day_completeness` summarizes raw GPS presence. `agg_service_coverage` compares scheduled trips with complete/partial trip facts. `mart_pipeline_status` combines ingestion completeness, trip quality, settled service coverage, stop-arrival counts, and GTFS freshness.

## Schedule Ledger

`int_gtfs_processing_snapshot` ranks the complete calendar/snapshot mapping. `int_schedule_fingerprint_daily` persists per-date line/direction/day-type hashes and trip counts; `int_schedule_version` recomputes the original full ordered windows over that small ledger. `dim_schedule_version` keeps its schema, start-date-containing IDs, validity bounds, first/last snapshot and processing-date lineage, and maximum trip counts.

Fingerprints preserve stop-sequence formatting, separators, MD5, and Warsaw absolute start/end/signature ordering; labels and snapshot/trip/service/shape IDs are excluded. History expands service dates D and D−1 under D's governing snapshot **without** the runtime overlap filter. Missing lines do not synthesize disappearance versions; gaps behave as before. Matcher/current-prior fact semantics are unchanged.

During run/build compilation, one small planner query compares **all past/future mapping dates** with ledger markers, returning at most the limit plus one before failing on overflow. Runtime `processing_date`/`gtfs_snapshot_id` do not override this mapping. Changed mappings and explicit repairs replace whole date partitions; unchanged fingerprints still collapse into the same version. Empty schedules retain a date marker; removed mappings retain a null-snapshot marker. Markers are excluded from version windows and make native `insert_overwrite` replace deleted lines. Do not switch to row-key MERGE or dynamic Jinja `partitions=` (config is parsed before planning). `on_schema_change: ignore` requires reviewed schema migrations.

No-change expansion is an empty SELECT without raw-history dependencies, though dbt can still create an empty temp table/MERGE and publish the small dimension. The dimension readiness hook blocks missing, changed, or removed mappings until reconciled, even when selected alone. `int_gtfs_trip_schedule_history` remains a controlled audit reference, not a normal dependency; use explicit DAG selections, not `+dim_schedule_version`.

| Var / option | Default | Contract |
|---|---|---|
| `schedule_ledger_bootstrap` | `false` | Boolean `true` enables creation/bounded replacement; required if ledger is absent. |
| `schedule_ledger_start_date`, `schedule_ledger_end_date` | absent | Inclusive ISO-date bounds required for bootstrap; replace every date, including removals. |
| `schedule_ledger_max_dates` | `31` | Allowed 1–366; overflow fails, never truncates. Raising the limit needs a new estimate. |
| `schedule_ledger_repair_dates` | `[]` | ISO dates to recompute even for unchanged snapshot IDs; unioned with mapping differences, still bounded. |
| `schedule_ledger_plan` | absent | Compile/analysis-only `{processing_date, gtfs_snapshot_id}` pins; null snapshot removes mapping, `[]` is no-op. Run/build refuses override. |
| `--full-refresh` | forbidden | Rejected for ledger, including bootstrap. |
| `DBT_BIGQUERY_MAXIMUM_BYTES_BILLED` | profile setting | Per-query cap, not a total budget. |

### Offline Compilation and Estimates

From the repository root, create a vars file using dates and snapshot IDs from `int_gtfs_processing_snapshot`. A table-data API export can read the mapping without running a query. Replace the example values below with those pins:

```json
{
  "processing_date": "2026-01-15",
  "gtfs_snapshot_id": "snapshot-example",
  "schedule_ledger_plan": [
    {"processing_date": "2026-01-15", "gtfs_snapshot_id": "snapshot-example"}
  ]
}
```

```sh
uv run --no-project --python 3.13 --with dbt-core==1.11.11 --with dbt-bigquery==1.11.3 \
  python dbt/tools/compile_schedule.py --vars-file /tmp/schedule-vars.json
uv run --no-project --with google-cloud-bigquery python dbt/tools/estimate_schedule.py \
  --manifest dbt/target/manifest.json --output dbt/target/schedule_estimate
```

Compilation uses real dbt, anonymous credentials, and blocked sockets; only local target/log files are written.

The estimator requires Google Cloud credentials with metadata and query permissions and uses metadata APIs plus `dry_run=True, use_query_cache=False`; it has no execution mode or table/load destinations. It saves SQL, SHA-256 hashes, and reports. To avoid script dry runs skipping work after CREATE TEMP TABLE, it estimates expansion separately and a MERGE with the source inlined (counting expansion again). This proxy includes pinned target partitions but is **not an upper bound** for temp reads, new ledger bytes, rounding, or retries. Mapping reconstruction and small planner/readiness/version reads are estimated separately when possible; absent-ledger dependencies are missing, not zero.

Compile each planned batch separately and save each report before the manifest is overwritten. Sum the estimates with a retry allowance. Cluster pruning is not a scan cap. These reports exclude storage, transfers, recovery, and other DAG models; missing relations leave the estimate incomplete.

## Test Tiers

Default Airflow runs exclude the full-history audit tests on `int_gtfs_trip_schedule` and `int_schedule_version`. Version audits now read the small ledger; raw trip-history audits remain expensive. Run them explicitly before schedule/matcher/audit-sensitive releases, after ledger bootstrap:

```bash
uvx --python 3.13 --from dbt-core==1.11.11 --with dbt-bigquery==1.11.3 dbt test --select int_gtfs_trip_schedule int_schedule_version --indirect-selection cautious --exclude test_type:unit --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'
```

That manual selector uses singular contract tests for required fields and accepted values instead of repeated generic column tests over the expensive schedule views.

For fact/status audits, pass `aggregation_start_date` and `publish_service_date` explicitly and keep those vars aligned with the rebuild window. Do not run unbounded full-history tests casually.

The local `profiles.yml` uses environment variables for BigQuery connection settings and credentials.

In Airflow, `GOOGLE_APPLICATION_CREDENTIALS` defaults to `/opt/airflow/gcp-key.json` if the environment variable is not set explicitly.
