{{
    config(
        materialized='table',
        partition_by={"field": "period_start_date", "data_type": "date"},
        cluster_by=["stop_group_id", "line", "hour_bracket"],
    )
}}

with detail as (
    select
        arrivals.service_date,
        arrivals.gtfs_snapshot_id,
        arrivals.line,
        arrivals.route_short_name,
        arrivals.mode,
        arrivals.direction_id,
        arrivals.trip_headsign,
        arrivals.stop_group_id,
        arrivals.stop_group_name,
        extract(hour from arrivals.hour_bracket at time zone 'Europe/Warsaw') as hour_bracket,
        arrivals.schedule_day_type,
        arrivals.schedule_version_id,
        arrivals.delay_seconds,
        dates.day_type,
        lower(dates.weekday_name) as weekday_name,
        date_trunc(arrivals.service_date, month) as month_start_date,
        versions.valid_from_date as schedule_version_start_date,
        versions.valid_to_date as schedule_version_end_date,
        max(versions.valid_from_date) over (
            partition by date_trunc(arrivals.service_date, month), arrivals.line, arrivals.direction_id, arrivals.schedule_day_type
        ) as latest_month_schedule_version_start_date
    from {{ ref('fct_stop_arrival') }} as arrivals
    inner join {{ ref('dim_date') }} as dates
        on arrivals.service_date = dates.service_date
    left join {{ ref('dim_schedule_version') }} as versions
        on arrivals.schedule_version_id = versions.schedule_version_id
    where arrivals.service_date between date('{{ var("aggregation_start_date", "1970-01-01") }}')
        and date('{{ var("processing_date") }}')
      and arrivals.trip_quality = 'complete'
),

monthly_detail as (
    select *
    from detail
    -- Month rows describe the latest in-month timetable; older versions remain available as schedule_version rows.
    where schedule_version_start_date = latest_month_schedule_version_start_date
),

period_rows as (
    select
        *,
        'month' as period_type,
        format_date('%Y-%m', service_date) as period_id,
        month_start_date as period_start_date,
        last_day(service_date, month) as period_end_date
    from monthly_detail

    union all

    select
        *,
        'schedule_version' as period_type,
        schedule_version_id as period_id,
        schedule_version_start_date as period_start_date,
        schedule_version_end_date as period_end_date
    from detail
),

day_class_rows as (
    select *, 'day_type' as day_class_type, day_type as day_class from period_rows
    union all
    select *, 'weekday' as day_class_type, weekday_name as day_class from period_rows
    union all
    select *, 'schedule_day_type' as day_class_type, schedule_day_type as day_class from period_rows
)

select
    period_type,
    period_id,
    period_start_date,
    period_end_date,
    min(service_date) as source_start_date,
    max(service_date) as source_end_date,
    min(service_date) > period_start_date
        or (period_end_date is not null and max(service_date) < period_end_date) as is_partial_period,
    day_class_type,
    day_class,
    schedule_version_id,
    stop_group_id,
    stop_group_name,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    hour_bracket,
    {{ delay_distribution_columns() }},
    array_agg(distinct gtfs_snapshot_id ignore nulls order by gtfs_snapshot_id) as gtfs_snapshot_ids
from day_class_rows
group by
    period_type,
    period_id,
    period_start_date,
    period_end_date,
    day_class_type,
    day_class,
    schedule_version_id,
    stop_group_id,
    stop_group_name,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    hour_bracket
