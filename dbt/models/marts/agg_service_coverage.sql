{% set processing_date = var("processing_date", "1970-01-01") %}
{% set aggregation_start_date = var("aggregation_start_date", processing_date) %}
{% set max_gps_date = var("max_gps_date", processing_date) %}
{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}
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
        partition_by={"field": "scheduled_start_date", "data_type": "date"},
        partitions=partition_dates,
        cluster_by=["line", "direction_id", "service_hour"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with raw_scheduled_trips as (
    select
        schedule.service_date,
        schedule.processing_date,
        schedule.gtfs_snapshot_id,
        schedule.line,
        routes.route_short_name,
        routes.mode,
        schedule.direction_id,
        schedule.trip_headsign,
        schedule.schedule_day_type,
        schedule.schedule_service_ids,
        schedule.trip_id,
        timestamp_add(timestamp(schedule.service_date, 'Europe/Warsaw'), interval schedule.trip_start_seconds second)
            as scheduled_start_time,
        timestamp_add(timestamp(schedule.service_date, 'Europe/Warsaw'), interval schedule.trip_end_seconds second)
            as scheduled_end_time
    from {{ ref('int_gtfs_trip_schedule') }} as schedule
    left join {{ ref('stg_gtfs__routes') }} as routes
        on schedule.line = routes.route_id
        and schedule.gtfs_snapshot_id = routes.gtfs_snapshot_id
    where schedule.processing_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
      and schedule.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
      and routes.mode in ('bus', 'tram')
),

scheduled_trips as (
    select
        raw_scheduled_trips.*,
        schedule_version.schedule_version_id
    from raw_scheduled_trips
    inner join {{ ref('dim_schedule_version') }} as schedule_version
        on raw_scheduled_trips.line = schedule_version.line
        and raw_scheduled_trips.direction_id = schedule_version.direction_id
        and raw_scheduled_trips.schedule_day_type = schedule_version.schedule_day_type
        and raw_scheduled_trips.processing_date between schedule_version.valid_from_date
        and coalesce(schedule_version.valid_to_date, date '9999-12-31')
    where raw_scheduled_trips.processing_date = date(raw_scheduled_trips.scheduled_start_time, 'Europe/Warsaw')
      and raw_scheduled_trips.processing_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
),

expected_by_hour as (
    select
        service_date,
        date(scheduled_start_time, 'Europe/Warsaw') as scheduled_start_date,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_headsign,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        timestamp_trunc(scheduled_start_time, hour, 'Europe/Warsaw') as service_hour,
        count(distinct trip_id) as expected_trip_count,
        sum(timestamp_diff(scheduled_end_time, scheduled_start_time, second)) / 60.0 as expected_service_minutes
    from scheduled_trips
    group by
        service_date,
        scheduled_start_date,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_headsign,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        service_hour
),

observed_trips as (
    select
        service_date,
        date(scheduled_start_time, 'Europe/Warsaw') as scheduled_start_date,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_headsign,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        trip_id,
        scheduled_start_time,
        actual_start_time,
        actual_end_time,
        service_observation_class,
        case trip_quality
            when 'complete' then 2
            when 'partial' then 1
        end as trip_quality_rank,
        case service_observation_class
            when 'regular' then 3
            when 'truncated' then 2
            when 'modified' then 1
            else 0
        end as service_observation_rank
    from {{ ref('fct_trip') }}
    where service_date between date_sub(date('{{ aggregation_start_date }}'), interval 1 day)
        and date('{{ processing_date }}')
      and gps_date between date('{{ aggregation_start_date }}')
        and date('{{ max_gps_date }}')
      and date(scheduled_start_time, 'Europe/Warsaw') between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
      and service_observation_class in ('regular', 'truncated', 'modified')
),

observed_trip_best_quality as (
    select * except (candidate_rank)
    from (
        select
            *,
            row_number() over (
                partition by service_date, schedule_version_id, trip_id
                order by
                    gtfs_snapshot_id = '{{ gtfs_snapshot_id }}' desc,
                    service_observation_rank desc,
                    trip_quality_rank desc,
                    actual_start_time,
                    actual_end_time
            ) as candidate_rank
        from observed_trips
    )
    where candidate_rank = 1
),

observed_by_hour as (
    select
        service_date,
        scheduled_start_date,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_headsign,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        timestamp_trunc(scheduled_start_time, hour, 'Europe/Warsaw') as service_hour,
        count(distinct trip_id) as observed_trip_count,
        count(distinct if(trip_quality_rank = 2, trip_id, null)) as complete_trip_count,
        count(distinct if(trip_quality_rank = 1, trip_id, null)) as partial_trip_count,
        count(distinct if(service_observation_class = 'regular', trip_id, null)) as regular_trip_count,
        count(distinct if(service_observation_class = 'truncated', trip_id, null)) as truncated_trip_count,
        count(distinct if(service_observation_class = 'modified', trip_id, null)) as modified_trip_count,
        sum(greatest(timestamp_diff(actual_end_time, actual_start_time, second), 0)) / 60.0 as observed_service_minutes
    from observed_trip_best_quality
    group by
        service_date,
        scheduled_start_date,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_headsign,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        service_hour
)

select
    expected.service_date,
    expected.scheduled_start_date,
    expected.gtfs_snapshot_id,
    expected.line,
    expected.route_short_name,
    expected.mode,
    expected.direction_id,
    expected.trip_headsign,
    expected.schedule_day_type,
    expected.schedule_service_ids,
    expected.schedule_version_id,
    expected.service_hour,
    timestamp_add(expected.service_hour, interval 1 hour) as service_hour_end,
    expected.expected_trip_count,
    coalesce(observed.observed_trip_count, 0) as observed_trip_count,
    coalesce(observed.complete_trip_count, 0) as complete_trip_count,
    coalesce(observed.partial_trip_count, 0) as partial_trip_count,
    coalesce(observed.regular_trip_count, 0) as regular_trip_count,
    coalesce(observed.truncated_trip_count, 0) as truncated_trip_count,
    coalesce(observed.modified_trip_count, 0) as modified_trip_count,
    expected.expected_service_minutes,
    coalesce(observed.observed_service_minutes, 0.0) as observed_service_minutes,
    least(1.0, safe_divide(coalesce(observed.observed_trip_count, 0), expected.expected_trip_count))
        as service_coverage_ratio,
    timestamp_add(expected.service_hour, interval 1 hour) < timestamp_sub(current_timestamp(), interval 90 minute)
        as is_settled_hour
from expected_by_hour as expected
left join observed_by_hour as observed
    on expected.service_date = observed.service_date
    and expected.scheduled_start_date = observed.scheduled_start_date
    and expected.line = observed.line
    and expected.direction_id = observed.direction_id
    and expected.trip_headsign = observed.trip_headsign
    and expected.schedule_day_type = observed.schedule_day_type
    and expected.schedule_version_id = observed.schedule_version_id
    and expected.service_hour = observed.service_hour
