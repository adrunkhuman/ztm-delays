# dbt

Transforms BigQuery inputs into historical dimensions, service-date facts, coverage measures, and frontend marts. The Python matcher reconstructs observations; dbt enriches and publishes them.

Models are organised as [staging](models/staging/), [intermediate](models/intermediate/), and [marts](models/marts/). The [architecture](../docs/architecture.md) explains snapshot lineage, overnight publication, and fact grains.

## Runtime

The stack uses Python 3.13, `dbt-core==1.11.11`, and `dbt-bigquery==1.11.3`. [profiles.yml](profiles.yml) reads `GCP_PROJECT`, `BIGQUERY_STG_DATASET`, `BIGQUERY_LOCATION`, and `GOOGLE_APPLICATION_CREDENTIALS`; model schemas also use the corresponding `BIGQUERY_RAW_DATASET`, `BIGQUERY_INT_DATASET`, `BIGQUERY_MARTS_DATASET`, and `BIGQUERY_MATCHER_INPUT_DATASET` settings.

| Variable | Purpose |
| --- | --- |
| `processing_date` | GPS date being rebuilt. |
| `gtfs_snapshot_id` | Governing snapshot from the persisted date mapping. |
| `publish_service_date` | Fact partition to replace; defaults to `processing_date`. |
| `aggregation_start_date` | Start of the inclusive coverage/status rebuild window. |

Airflow supplies these per phase. Prefer a targeted DAG run to manually selecting facts: matcher publication, current/prior dates, schedule views, and serving marts must remain consistent. Do not rebuild a multi-date coverage window under one snapshot.

## Schedule ledger

`int_schedule_fingerprint_daily` stores daily timetable hashes from `int_gtfs_processing_snapshot`. `int_schedule_version` derives version windows from this ledger. Reconciliation replaces whole changed date partitions, including empty or removed mappings; unchanged history is not expanded again.

| Variable | Use |
| --- | --- |
| `schedule_ledger_bootstrap` | Explicit `true` for initial creation or bounded replacement. |
| `schedule_ledger_start_date`, `schedule_ledger_end_date` | Required inclusive bootstrap bounds. |
| `schedule_ledger_max_dates` | Batch limit, default 31; overflow fails rather than truncates. |
| `schedule_ledger_repair_dates` | Dates to recompute despite unchanged snapshot IDs. |
| `schedule_ledger_plan` | Offline compile/analysis override; rejected by run/build. |

Never use `--full-refresh` for the ledger or publish schedule versions from a partial bootstrap. Serialise writers through the Airflow pool. Version readiness checks reject unreconciled mappings. Avoid broad selectors such as `+dim_schedule_version` that pull expensive audit dependencies into ordinary runs.

## Checks and estimates

From the repository root, compile a fixture plan without cloud access:

```sh
uv run --no-project --python 3.13 \
  --with dbt-core==1.11.11 --with dbt-bigquery==1.11.3 \
  python dbt/tools/compile_schedule.py \
  --vars-file .github/fixtures/schedule-pinned.json
```

The compiler blocks sockets and writes local `dbt/target` and `dbt/logs` output. For real work, use a vars file containing the exact planned date/snapshot mapping, not the fixture.

Estimate that compiled plan with GCP credentials:

```sh
uv run --no-project --with google-cloud-bigquery \
  python dbt/tools/estimate_schedule.py \
  --manifest dbt/target/manifest.json --output dbt/target/schedule_estimate
```

The estimator uses metadata APIs and BigQuery dry runs, never execution. Its reports are not an upper bound: missing relations, temporary reads, retries, and non-ledger work can leave costs unaccounted for. Save each batch's report before compiling the next. `DBT_BIGQUERY_MAXIMUM_BYTES_BILLED` is a per-query cap, not a total budget.

CI compiles warehouse models and runs dbt unit tests with GCP credentials. Normal Airflow tests exclude `tag:audit`; the weekly audit builds tagged evidence models before testing them. Recovery that depends on those contracts must run the same audit sequence, not assume nightly checks cover history.
