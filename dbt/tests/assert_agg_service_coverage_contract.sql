select
    scheduled_start_date,
    line,
    mode,
    direction_id,
    service_hour
from {{ ref('agg_service_coverage') }}
where scheduled_start_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
  and (
      service_date is null
      or gtfs_snapshot_id is null
      or line is null
      or mode is null
      or mode not in ('bus', 'tram')
      or direction_id is null
      or trip_headsign is null
      or schedule_day_type is null
      or schedule_service_ids is null
      or schedule_version_id is null
      or service_hour is null
      or service_hour_end is null
      or expected_trip_count is null
      or expected_trip_count <= 0
      or observed_trip_count is null
      or observed_trip_count < 0
      or complete_trip_count is null
      or complete_trip_count < 0
      or partial_trip_count is null
      or partial_trip_count < 0
      or observed_trip_count != complete_trip_count + partial_trip_count
      or regular_trip_count is null
      or truncated_trip_count is null
      or modified_trip_count is null
      or expected_service_minutes is null
      or expected_service_minutes < 0
      or latest_scheduled_end_time is null
      or observed_service_minutes is null
      or observed_service_minutes < 0
      or service_coverage_ratio is null
      or service_coverage_ratio < 0
      or service_coverage_ratio > 1
      or is_settled_hour is null
      or scheduled_start_date != date(service_hour, 'Europe/Warsaw')
      or extract(minute from service_hour at time zone 'Europe/Warsaw') != 0
      or extract(second from service_hour at time zone 'Europe/Warsaw') != 0
      or service_hour_end != timestamp_add(service_hour, interval 1 hour)
      or is_settled_hour != (
          latest_scheduled_end_time < timestamp_sub(current_timestamp(), interval 90 minute)
          and date(latest_scheduled_end_time, 'Europe/Warsaw') <= date('{{ var("max_gps_date", var("processing_date")) }}')
      )
  )
