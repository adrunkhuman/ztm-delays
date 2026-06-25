# ZTM dbt Project

This dbt project transforms BigQuery raw tables for the ZTM pipeline.

Models that process historical data require an explicit `processing_date` variable:

```bash
dbt run --select stg_gps_pings --vars '{"processing_date": "YYYY-MM-DD"}'
```

The local `profiles.yml` uses environment variables for BigQuery connection settings and credentials.

In Airflow, `GOOGLE_APPLICATION_CREDENTIALS` defaults to `/opt/airflow/gcp-key.json` if the environment variable is not set explicitly.
