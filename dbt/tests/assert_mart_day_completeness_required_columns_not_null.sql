select *
from {{ ref('mart_day_completeness') }}
where gps_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
  and (
      vehicle_type is null
      or mode is null
      or expected_hours is null
      or present_hours is null
      or missing_hours is null
      or completeness_ratio is null
      or is_complete_day is null
      or gps_row_count is null
      or max_vehicle_count is null
      or mean_hourly_coverage_ratio is null
      or min_hourly_coverage_ratio is null
      or max_gap_seconds is null
  )
