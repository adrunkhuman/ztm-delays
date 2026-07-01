with detail as (
    select
        arrivals.service_date,
        arrivals.gtfs_snapshot_id,
        arrivals.line,
        arrivals.direction_id,
        arrivals.mode,
        extract(hour from arrivals.hour_bracket at time zone 'Europe/Warsaw') as hour_bracket,
        arrivals.schedule_day_type,
        arrivals.delay_seconds,
        dates.day_type,
        lower(dates.weekday_name) as weekday_name,
        date_trunc(arrivals.service_date, month) as month_start_date,
        versions.valid_from_date as schedule_version_start_date,
        max(versions.valid_from_date) over (
            partition by date_trunc(arrivals.service_date, month), arrivals.line, arrivals.direction_id, arrivals.schedule_day_type
        ) as latest_month_schedule_version_start_date
    from {{ ref('fct_stop_arrival') }} as arrivals
    inner join {{ ref('dim_date') }} as dates
        on arrivals.service_date = dates.service_date
    inner join {{ ref('dim_schedule_version') }} as versions
        on arrivals.schedule_version_id = versions.schedule_version_id
    where arrivals.service_date between date('{{ var("aggregation_start_date", "1970-01-01") }}')
        and date('{{ var("processing_date") }}')
      and arrivals.trip_quality = 'complete'
),

latest_month_detail as (
    select *
    from detail
    where schedule_version_start_date = latest_month_schedule_version_start_date
),

expected as (
    select
        'day_type' as day_class_type,
        day_type as day_class,
        mode,
        hour_bracket,
        count(*) as n
    from latest_month_detail
    group by day_class_type, day_class, mode, hour_bracket

    union all

    select
        'weekday' as day_class_type,
        weekday_name as day_class,
        mode,
        hour_bracket,
        count(*) as n
    from latest_month_detail
    group by day_class_type, day_class, mode, hour_bracket

    union all

    select
        'schedule_day_type' as day_class_type,
        schedule_day_type as day_class,
        mode,
        hour_bracket,
        count(*) as n
    from latest_month_detail
    group by day_class_type, day_class, mode, hour_bracket
),

actual as (
    select
        day_class_type,
        day_class,
        mode,
        hour_bracket,
        n
    from {{ ref('agg_time_period') }}
    where period_type = 'month'
      and line is null
      and direction_id is null
      and schedule_version_id is null
)

(select * from expected
except distinct
select * from actual)

union all

(select * from actual
except distinct
select * from expected)
