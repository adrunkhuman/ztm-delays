{{ config(materialized='view') }}

select
    version,
    evaluated_at,
    hour_start,
    datetime(hour_start, 'Europe/Warsaw') as warsaw_hour_start,
    date(hour_start, 'Europe/Warsaw') as gps_date,
    extract(hour from datetime(hour_start, 'Europe/Warsaw')) as gps_hour,
    collection_started_at,
    mode,
    status,
    reasons,
    intervals,
    monitored_minutes,
    baseline_samples,
    parsed_rows,
    accepted_rows,
    dropped_stale_rows,
    dropped_future_rows,
    mean_accepted_vehicles,
    mean_accepted_lines
from {{ source('poller_health', 'raw_poller_hourly_health') }}
