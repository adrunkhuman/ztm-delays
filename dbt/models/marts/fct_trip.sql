{% set publish_service_date = var("publish_service_date", var("processing_date")) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ publish_service_date ~ "')"],
        cluster_by=["line", "direction_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

select
    gtfs_snapshot_id,
    gps_date,
    service_date,
    trip_id,
    vehicle_number,
    line,
    route_short_name,
    mode,
    brigade,
    vehicle_type,
    direction_id,
    service_id,
    trip_headsign,
    shape_id,
    day_type,
    schedule_day_type,
    schedule_service_ids,
    schedule_version_id,
    origin_stop_id,
    origin_stop_name,
    destination_stop_id,
    destination_stop_name,
    scheduled_start_time,
    scheduled_end_time,
    actual_start_time,
    actual_end_time,
    start_delay_seconds,
    end_delay_seconds,
    stops_expected,
    stops_detected,
    detected_stop_ratio,
    first_detected_stop_sequence,
    last_detected_stop_sequence,
    max_stop_sequence_gap,
    max_ping_gap_seconds,
    max_speed_mps,
    is_first_stop_observed,
    is_last_stop_observed,
    has_non_monotonic_stop_progression,
    has_impossible_speed_jump,
    has_stale_stop_progression,
    trip_quality,
    quality_flags
from {{ ref('int_trip_summary') }}
where service_date = date('{{ publish_service_date }}')
  and gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
  and gps_date <= date('{{ var("processing_date") }}')
  and scheduled_end_time < timestamp(date_add(date('{{ var("processing_date") }}'), interval 1 day), 'Europe/Warsaw')
qualify row_number() over (
    partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number
    order by
        case trip_quality
            when 'complete' then 3
            when 'partial' then 2
            when 'broken' then 1
            else 0
        end desc,
        gps_date desc,
        actual_end_time desc
) = 1
