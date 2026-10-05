{{ config(materialized='view') }}

with hourly as (
    select *
    from {{ ref('mart_poller_hourly_health') }}
),

summarized as (
    select
        gps_date,
        mode,
        count(*) as evaluated_hours,
        sum(monitored_minutes) as monitored_minutes,
        countif(
            status = 'degraded'
            -- Recovery can be confirmed after the incident's hour (or Warsaw day) ends.
            or exists (
                select 1
                from unnest(json_query_array(intervals)) as incident_interval
                where safe_cast(json_value(incident_interval, '$.start_at') as timestamp)
                    < timestamp_add(hour_start, interval 1 hour)
                    and safe_cast(json_value(incident_interval, '$.end_at') as timestamp) > hour_start
            )
        ) as degraded_hours,
        countif(status in ('monitoring_gap', 'partial')) as telemetry_gap_hours,
        countif(status = 'warming_up') as warming_up_hours,
        sum(parsed_rows) as parsed_rows,
        sum(accepted_rows) as accepted_rows,
        sum(dropped_stale_rows) as dropped_stale_rows,
        sum(dropped_future_rows) as dropped_future_rows,
        min(hour_start) as earliest_hour_start,
        max(hour_start) as latest_hour_start,
        countif(baseline_samples >= 3) as hours_with_comparable_history,
        countif(status = 'healthy') as healthy_hours
    from hourly
    group by gps_date, mode
)

select
    gps_date,
    mode,
    evaluated_hours,
    monitored_minutes,
    degraded_hours,
    telemetry_gap_hours,
    warming_up_hours,
    parsed_rows,
    accepted_rows,
    dropped_stale_rows,
    dropped_future_rows,
    earliest_hour_start,
    latest_hour_start,
    hours_with_comparable_history,
    healthy_hours,
    case
        when degraded_hours > 0 then 'degraded'
        when telemetry_gap_hours > 0 then 'monitoring_gap'
        when warming_up_hours > 0 then 'warming_up'
        when healthy_hours > 0 then 'healthy'
        else 'not_monitored'
    end as status
from summarized
