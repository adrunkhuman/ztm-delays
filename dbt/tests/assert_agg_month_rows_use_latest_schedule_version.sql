with latest_versions as (
    select
        date_trunc(arrivals.service_date, month) as period_start_date,
        arrivals.line,
        arrivals.direction_id,
        arrivals.schedule_day_type,
        max(versions.valid_from_date) as latest_valid_from_date
    from {{ ref('fct_stop_arrival') }} as arrivals
    inner join {{ ref('dim_schedule_version') }} as versions
        on arrivals.schedule_version_id = versions.schedule_version_id
    where arrivals.service_date between date('{{ var("aggregation_start_date", "1970-01-01") }}')
        and date('{{ var("processing_date") }}')
      and arrivals.trip_quality = 'complete'
    group by
        period_start_date,
        arrivals.line,
        arrivals.direction_id,
        arrivals.schedule_day_type
),

line_stop_month_rows as (
    select
        'agg_line_stop_period' as model_name,
        agg.period_start_date,
        agg.line,
        agg.direction_id,
        agg.schedule_version_id,
        versions.valid_from_date,
        latest_versions.latest_valid_from_date
    from {{ ref('agg_line_stop_period') }} as agg
    inner join {{ ref('dim_schedule_version') }} as versions
        on agg.schedule_version_id = versions.schedule_version_id
    inner join latest_versions
        on agg.period_start_date = latest_versions.period_start_date
        and agg.line = latest_versions.line
        and agg.direction_id = latest_versions.direction_id
        and agg.day_class_type = 'schedule_day_type'
        and agg.day_class = latest_versions.schedule_day_type
    where agg.period_type = 'month'
),

stop_month_rows as (
    select
        'agg_stop_period' as model_name,
        agg.period_start_date,
        agg.line,
        agg.direction_id,
        agg.schedule_version_id,
        versions.valid_from_date,
        latest_versions.latest_valid_from_date
    from {{ ref('agg_stop_period') }} as agg
    inner join {{ ref('dim_schedule_version') }} as versions
        on agg.schedule_version_id = versions.schedule_version_id
    inner join latest_versions
        on agg.period_start_date = latest_versions.period_start_date
        and agg.line = latest_versions.line
        and agg.direction_id = latest_versions.direction_id
        and agg.day_class_type = 'schedule_day_type'
        and agg.day_class = latest_versions.schedule_day_type
    where agg.period_type = 'month'
)

select *
from line_stop_month_rows
where valid_from_date != latest_valid_from_date

union all

select *
from stop_month_rows
where valid_from_date != latest_valid_from_date
