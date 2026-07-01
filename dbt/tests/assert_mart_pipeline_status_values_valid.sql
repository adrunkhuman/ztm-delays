select *
from {{ ref('mart_pipeline_status') }}
where vehicle_type not in (1, 2)
   or mode not in ('bus', 'tram')
   or expected_hours <= 0
   or present_hours < 0
   or present_hours > expected_hours
   or array_length(missing_hours) != expected_hours - present_hours
   or completeness_ratio < 0
   or completeness_ratio > 1
   or is_complete_day != (present_hours = expected_hours)
   or gps_row_count < 0
   or max_vehicle_count < 0
   or mean_hourly_coverage_ratio < 0
   or mean_hourly_coverage_ratio > 1
   or min_hourly_coverage_ratio < 0
   or min_hourly_coverage_ratio > 1
   or max_gap_seconds < 0
   or pings_total < 0
   or pings_matched < 0
   or pings_matched > pings_total
   or match_rate < 0
   or match_rate > 1
   or trips_observed < 0
   or trips_complete < 0
   or trips_partial < 0
   or trips_broken < 0
   or trips_observed != trips_complete + trips_partial + trips_broken
   or broken_rate < 0
   or broken_rate > 1
   or expected_trips < 0
   or observed_trips < 0
   or observed_trips > expected_trips
   or service_coverage_ratio < 0
   or service_coverage_ratio > 1
   or expected_service_minutes < 0
   or observed_service_minutes < 0
   or stop_arrivals_count < 0
   or gtfs_snapshot_age_hours < 0
   or schedule_versions_active < 0
