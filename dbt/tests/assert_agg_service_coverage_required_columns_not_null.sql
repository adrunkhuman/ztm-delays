select *
from {{ ref('agg_service_coverage') }}
where scheduled_start_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
  and (
      service_date is null
      or gtfs_snapshot_id is null
      or line is null
      or mode is null
      or direction_id is null
      or trip_headsign is null
      or schedule_day_type is null
      or schedule_service_ids is null
      or schedule_version_id is null
      or service_hour is null
      or service_hour_end is null
      or expected_trip_count is null
      or observed_trip_count is null
      or complete_trip_count is null
      or partial_trip_count is null
      or regular_trip_count is null
      or truncated_trip_count is null
      or modified_trip_count is null
      or expected_service_minutes is null
      or observed_service_minutes is null
      or service_coverage_ratio is null
      or is_settled_hour is null
  )
