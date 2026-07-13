select
    service_date,
    vehicle_type,
    mode
from {{ ref('mart_pipeline_status') }}
where service_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
  and (
      vehicle_type is null
      or vehicle_type not in (1, 2)
      or mode is null
      or mode not in ('bus', 'tram')
      or expected_hours is null
      or expected_hours <= 0
      or present_hours is null
      or present_hours < 0
      or present_hours > expected_hours
      or missing_hours is null
      or array_length(missing_hours) != expected_hours - present_hours
      or completeness_ratio is null
      or completeness_ratio < 0
      or completeness_ratio > 1
      or is_complete_day is null
      or is_complete_day != (present_hours = expected_hours)
      or gps_row_count is null
      or gps_row_count < 0
      or max_vehicle_count is null
      or max_vehicle_count < 0
      or mean_hourly_coverage_ratio is null
      or mean_hourly_coverage_ratio < 0
      or mean_hourly_coverage_ratio > 1
      or min_hourly_coverage_ratio is null
      or min_hourly_coverage_ratio < 0
      or min_hourly_coverage_ratio > 1
      or max_gap_seconds is null
      or max_gap_seconds < 0
      or pings_total is null
      or pings_total < 0
      or trips_observed is null
      or trips_observed < 0
      or trips_complete is null
      or trips_complete < 0
      or trips_partial is null
      or trips_partial < 0
      or trips_broken is null
      or trips_broken < 0
      or trips_observed != trips_complete + trips_partial + trips_broken
      or broken_rate is null
      or broken_rate < 0
      or broken_rate > 1
      or expected_trips is null
      or expected_trips < 0
      or observed_trips is null
      or observed_trips < 0
      or observed_trips > expected_trips
      or service_coverage_ratio < 0
      or service_coverage_ratio > 1
      or expected_service_minutes is null
      or expected_service_minutes < 0
      or observed_service_minutes is null
      or observed_service_minutes < 0
      or stop_arrivals_count is null
      or stop_arrivals_count < 0
      or latest_gtfs_snapshot_id is null
      or latest_gtfs_snapshot_at is null
      or gtfs_snapshot_age_hours is null
      or gtfs_snapshot_age_hours < 0
      or schedule_versions_active is null
      or schedule_versions_active < 0
      or status_generated_at is null
  )
