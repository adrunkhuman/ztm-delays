{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["vehicle_type", "gps_hour"],
    )
}}

with pings as (
    select
        gps_date,
        timestamp_trunc(gps_time, hour, 'Europe/Warsaw') as gps_hour,
        vehicle_type,
        gps_time,
        vehicle_number,
        cast(div(unix_seconds(gps_time), 10) as int64) as ten_second_bucket
    from {{ ref('stg_gps__pings') }}
    where gps_date = date('{{ var("processing_date") }}')
),

expected_hours as (
    select
        date('{{ var("processing_date") }}') as gps_date,
        gps_hour,
        timestamp_add(gps_hour, interval 1 hour) as next_gps_hour,
        vehicle_type
    from unnest(generate_timestamp_array(
        timestamp(date('{{ var("processing_date") }}'), 'Europe/Warsaw'),
        timestamp_sub(timestamp(date_add(date('{{ var("processing_date") }}'), interval 1 day), 'Europe/Warsaw'), interval 1 hour),
        interval 1 hour
    )) as gps_hour
    cross join unnest([1, 2]) as vehicle_type
),

bucketed as (
    select
        gps_date,
        gps_hour,
        vehicle_type,
        min(gps_time) as first_gps_time,
        max(gps_time) as last_gps_time,
        count(*) as row_count,
        count(distinct vehicle_number) as vehicle_count,
        count(distinct ten_second_bucket) as observed_10s_buckets
    from pings
    group by gps_date, gps_hour, vehicle_type
),

gaps as (
    select
        gps_date,
        gps_hour,
        vehicle_type,
        max(timestamp_diff(gps_time, previous_gps_time, second)) as max_gap_seconds
    from (
        select
            gps_date,
            gps_hour,
            vehicle_type,
            gps_time,
            lag(gps_time) over (
                partition by gps_date, gps_hour, vehicle_type
                order by gps_time
            ) as previous_gps_time
        from (
            select distinct gps_date, gps_hour, vehicle_type, gps_time
            from pings
            union distinct
            select gps_date, gps_hour, vehicle_type, gps_hour as gps_time
            from expected_hours
            union distinct
            select gps_date, gps_hour, vehicle_type, next_gps_hour as gps_time
            from expected_hours
        )
    )
    where previous_gps_time is not null
    group by gps_date, gps_hour, vehicle_type
)

select
    expected_hours.gps_date,
    expected_hours.gps_hour,
    expected_hours.vehicle_type,
    bucketed.first_gps_time,
    bucketed.last_gps_time,
    coalesce(bucketed.row_count, 0) as row_count,
    coalesce(bucketed.vehicle_count, 0) as vehicle_count,
    coalesce(bucketed.observed_10s_buckets, 0) as observed_10s_buckets,
    cast(timestamp_diff(expected_hours.next_gps_hour, expected_hours.gps_hour, second) / 10 as int64)
        as expected_10s_buckets,
    safe_divide(
        coalesce(bucketed.observed_10s_buckets, 0),
        cast(timestamp_diff(expected_hours.next_gps_hour, expected_hours.gps_hour, second) / 10 as int64)
    ) as coverage_ratio,
    coalesce(gaps.max_gap_seconds, 0) as max_gap_seconds
from expected_hours
left join bucketed
    on expected_hours.gps_date = bucketed.gps_date
    and expected_hours.gps_hour = bucketed.gps_hour
    and expected_hours.vehicle_type = bucketed.vehicle_type
left join gaps
    on expected_hours.gps_date = gaps.gps_date
    and expected_hours.gps_hour = gaps.gps_hour
    and expected_hours.vehicle_type = gaps.vehicle_type
