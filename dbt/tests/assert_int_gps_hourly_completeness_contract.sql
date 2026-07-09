select
    gps_date,
    gps_hour,
    vehicle_type
from {{ ref('int_gps_hourly_completeness') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      gps_date is null
      or gps_hour is null
      or vehicle_type is null
      or vehicle_type not in (1, 2)
      or row_count is null
      or vehicle_count is null
      or observed_10s_buckets is null
      or expected_10s_buckets is null
      or expected_10s_buckets != 360
      or coverage_ratio is null
      or max_gap_seconds is null
  )
