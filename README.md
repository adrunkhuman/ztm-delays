# ZTM Warsaw Pipeline

Data pipeline for collecting Warsaw ZTM GPS pings, loading raw data into GCS/BigQuery, transforming with dbt, and orchestrating downstream jobs with Airflow.

The poller lives in `poller/` as part of this pipeline repo. It should not have its own Git repo unless it gets a separate release lifecycle from the rest of the pipeline.

The dbt project lives in `dbt/`. Date-partitioned GPS models require `processing_date`; trip and arrival matching use the selected GTFS snapshot passed by Airflow. Nightly rebuilds select the latest dimension-built GTFS snapshot available at rebuild time and republish the current and prior service dates.

Airflow DAGs live in `airflow/dags/`. The current DAG scope covers GPS raw loading, GTFS snapshot loading, staging, intermediate GPS/trip/arrival reconstruction, and serving trip/stop-arrival facts.

CI runs on pull requests and pushes to `master`. The dbt job requires the `GCP_SERVICE_ACCOUNT_JSON` GitHub Actions secret containing the service account key JSON used by `profiles.yml`.
