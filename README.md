# ZTM Warsaw Pipeline

Data pipeline for collecting Warsaw ZTM GPS pings, loading raw data into GCS/BigQuery, transforming with dbt, and orchestrating downstream jobs with Airflow.

The poller lives in `poller/` as part of this pipeline repo. It should not have its own Git repo unless it gets a separate release lifecycle from the rest of the pipeline.
