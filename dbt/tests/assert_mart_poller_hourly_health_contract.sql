{{ config(tags=['feed_health']) }}

with hourly as (
    select *
    from {{ ref('mart_poller_hourly_health') }}
),
violations as (
    select 'duplicate mode/hour' as reason
    from hourly
    group by mode, hour_start
    having count(*) > 1

    union all

    select 'invalid mode' as reason
    from hourly
    where mode not in ('bus', 'tram') or mode is null

    union all

    select 'invalid status' as reason
    from hourly
    where status not in (
        'healthy', 'degraded', 'warming_up', 'partial', 'monitoring_gap', 'not_monitored'
    ) or status is null

    union all

    select 'invalid monitored minutes' as reason
    from hourly
    where monitored_minutes not between 0 and 60 or monitored_minutes is null

    union all

    select 'invalid nullable row counts' as reason
    from hourly
    where (parsed_rows is null and (
            accepted_rows is not null
            or dropped_stale_rows is not null
            or dropped_future_rows is not null
        ))
       or (parsed_rows is not null and (
            accepted_rows is null
            or dropped_stale_rows is null
            or dropped_future_rows is null
            or parsed_rows != accepted_rows + dropped_stale_rows + dropped_future_rows
        ))
)
select * from violations
