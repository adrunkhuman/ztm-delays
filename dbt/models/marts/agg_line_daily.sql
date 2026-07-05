{% set processing_date = var("processing_date", "1970-01-01") %}
{% set aggregation_start_date = var("aggregation_start_date", processing_date) %}
{% set start_date = modules.datetime.datetime.strptime(aggregation_start_date, "%Y-%m-%d").date() %}
{% set end_date = modules.datetime.datetime.strptime(processing_date, "%Y-%m-%d").date() %}
{% set partition_dates = [] %}
{% for day_offset in range((end_date - start_date).days + 1) %}
    {% do partition_dates.append("date('" ~ (start_date + modules.datetime.timedelta(days=day_offset)).isoformat() ~ "')") %}
{% endfor %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=partition_dates,
        cluster_by=["line", "direction_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with detail as (
    select
        arrivals.service_date,
        arrivals.gtfs_snapshot_id,
        arrivals.trip_id,
        arrivals.vehicle_number,
        arrivals.line,
        arrivals.route_short_name,
        arrivals.mode,
        arrivals.direction_id,
        arrivals.trip_headsign,
        arrivals.schedule_day_type,
        arrivals.schedule_version_id,
        arrivals.stop_group_id,
        arrivals.delay_seconds,
        dates.day_type,
        dates.is_holiday,
        lower(dates.weekday_name) as weekday_name
    from {{ ref('fct_stop_arrival') }} as arrivals
    inner join {{ ref('dim_date') }} as dates
        on arrivals.service_date = dates.service_date
    where arrivals.service_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
      and arrivals.trip_quality = 'complete'
)

select
    service_date,
    day_type,
    is_holiday,
    weekday_name,
    schedule_day_type,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    {{ delay_distribution_columns() }},
    count(distinct concat(gtfs_snapshot_id, '|', trip_id, '|', cast(vehicle_number as string))) as trip_count,
    count(distinct stop_group_id) as stop_group_count,
    array_agg(distinct gtfs_snapshot_id ignore nulls order by gtfs_snapshot_id) as gtfs_snapshot_ids
from detail
group by
    service_date,
    day_type,
    is_holiday,
    weekday_name,
    schedule_day_type,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign
