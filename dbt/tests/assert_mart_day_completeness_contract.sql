select
    gps_date,
    vehicle_type,
    mode
from {{ ref('mart_day_completeness') }}
where gps_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
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
      or max_vehicle_count is null
      or mean_hourly_coverage_ratio is null
      or min_hourly_coverage_ratio is null
      or max_gap_seconds is null
  )
