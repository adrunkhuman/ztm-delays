# ZTM dbt Project

This dbt project transforms BigQuery raw tables for the ZTM pipeline.

Use Python `3.13` for local dbt commands. The current dbt stack is verified with `dbt-core 1.11.11` and `dbt-bigquery 1.11.3`.

Models that process historical data require an explicit `processing_date` variable:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select stg_gps_pings --vars '{"processing_date": "YYYY-MM-DD"}'
```

The local `profiles.yml` uses environment variables for BigQuery connection settings and credentials.

In Airflow, `GOOGLE_APPLICATION_CREDENTIALS` defaults to `/opt/airflow/gcp-key.json` if the environment variable is not set explicitly.
