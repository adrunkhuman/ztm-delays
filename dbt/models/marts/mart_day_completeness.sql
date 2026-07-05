{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["mode"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with date_spine as (
    select date('{{ var("processing_date") }}') as gps_date
),

expected_hours as (
    select
        date_spine.gps_date,
        gps_hour,
        vehicle_type,
        case vehicle_type
            when 1 then 'bus'
            when 2 then 'tram'
        end as mode
    from date_spine
    cross join unnest(generate_timestamp_array(
        timestamp(date_spine.gps_date, 'Europe/Warsaw'),
        timestamp_sub(timestamp(date_add(date_spine.gps_date, interval 1 day), 'Europe/Warsaw'), interval 1 hour),
        interval 1 hour
    )) as gps_hour
    cross join unnest([1, 2]) as vehicle_type
),

hourly_source as (
    select
        gps_date,
        vehicle_type,
        gps_hour,
        first_gps_time,
        last_gps_time,
        row_count,
        vehicle_count,
        coverage_ratio,
        max_gap_seconds
    from {{ ref('int_gps_hourly_completeness') }}
    where gps_date = date('{{ var("processing_date") }}')
),

hourly as (
    select
        expected_hours.gps_date,
        expected_hours.vehicle_type,
        expected_hours.mode,
        expected_hours.gps_hour,
        hourly_source.first_gps_time,
        hourly_source.last_gps_time,
        coalesce(hourly_source.row_count, 0) as row_count,
        coalesce(hourly_source.vehicle_count, 0) as vehicle_count,
        coalesce(hourly_source.coverage_ratio, 0.0) as coverage_ratio,
        coalesce(hourly_source.max_gap_seconds, 0) as max_gap_seconds
    from expected_hours
    left join hourly_source
        on expected_hours.gps_date = hourly_source.gps_date
        and expected_hours.gps_hour = hourly_source.gps_hour
        and expected_hours.vehicle_type = hourly_source.vehicle_type
),

daily as (
    select
        gps_date,
        vehicle_type,
        mode,
        count(*) as expected_hours,
        countif(row_count > 0) as present_hours,
        array_agg(
            if(row_count = 0, extract(hour from gps_hour at time zone 'Europe/Warsaw'), null)
            ignore nulls
            order by gps_hour
        ) as missing_hours,
        min(first_gps_time) as first_observed_time,
        max(last_gps_time) as last_observed_time,
        sum(row_count) as gps_row_count,
        max(vehicle_count) as max_vehicle_count,
        avg(coverage_ratio) as mean_hourly_coverage_ratio,
        min(coverage_ratio) as min_hourly_coverage_ratio,
        max(max_gap_seconds) as max_gap_seconds
    from hourly
    group by gps_date, vehicle_type, mode
)

select
    gps_date,
    vehicle_type,
    mode,
    expected_hours,
    present_hours,
    missing_hours,
    first_observed_time,
    last_observed_time,
    safe_divide(present_hours, expected_hours) as completeness_ratio,
    present_hours = expected_hours as is_complete_day,
    gps_row_count,
    max_vehicle_count,
    mean_hourly_coverage_ratio,
    min_hourly_coverage_ratio,
    max_gap_seconds
from daily
