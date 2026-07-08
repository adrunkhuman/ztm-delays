{% set processing_date = var("processing_date", "1970-01-01") %}
{% set aggregation_start_date = var("aggregation_start_date", processing_date) %}
{% set period_source_start_date = var("period_source_start_date", aggregation_start_date) %}
{% set period_partition_dates = var("period_partition_dates", "") %}
{% set partitions_to_replace = [] %}
{% if period_partition_dates %}
    {% for partition_date in period_partition_dates.split('|') %}
        {% do partitions_to_replace.append("date('" ~ partition_date ~ "')") %}
    {% endfor %}
{% endif %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "period_start_date", "data_type": "date"},
        partitions=partitions_to_replace if partitions_to_replace else none,
        cluster_by=["line", "stop_group_id", "hour_bracket"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with affected_service_dates as (
    select service_date
    from unnest(generate_date_array(
        date('{{ aggregation_start_date }}'),
        date('{{ processing_date }}')
    )) as service_date
),

affected_periods as (
    select distinct
        'month' as period_type,
        date_trunc(service_date, month) as period_start_date
    from affected_service_dates

    union distinct

    select distinct
        'schedule_version' as period_type,
        valid_from_date as period_start_date
    from {{ ref('dim_schedule_version') }}
    where valid_from_date <= date('{{ processing_date }}')
      and coalesce(valid_to_date, date '9999-12-31') >= date('{{ aggregation_start_date }}')
),

detail as (
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
        arrivals.stop_id,
        arrivals.stop_post_code,
        arrivals.stop_name,
        arrivals.stop_lat,
        arrivals.stop_lon,
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
    where arrivals.service_date between date('{{ period_source_start_date }}')
        and date('{{ processing_date }}')
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

affected_partition_dates as (
    {% if period_partition_dates %}
        select distinct period_start_date
        from unnest([
            {%- for partition_date in period_partition_dates.split('|') -%}
                date('{{ partition_date }}'){% if not loop.last %}, {% endif %}
            {%- endfor -%}
        ]) as period_start_date
    {% else %}
    select distinct period_start_date
    from affected_periods
    {% endif %}
),

affected_period_rows as (
    select period_rows.*
    from period_rows
    inner join affected_partition_dates
        on period_rows.period_start_date = affected_partition_dates.period_start_date
),

day_class_rows as (
    select *, 'day_type' as day_class_type, day_type as day_class from affected_period_rows
    union all
    select *, 'weekday' as day_class_type, weekday_name as day_class from affected_period_rows
    union all
    select *, 'schedule_day_type' as day_class_type, schedule_day_type as day_class from affected_period_rows
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
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    stop_group_id,
    stop_group_name,
    stop_id,
    stop_post_code,
    stop_name,
    stop_lat,
    stop_lon,
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
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    stop_group_id,
    stop_group_name,
    stop_id,
    stop_post_code,
    stop_name,
    stop_lat,
    stop_lon,
    hour_bracket
