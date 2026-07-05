select *
from {{ ref('mart_day_completeness') }}
where gps_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
  and (
      vehicle_type not in (1, 2)
      or mode not in ('bus', 'tram')
      or expected_hours <= 0
      or present_hours < 0
      or present_hours > expected_hours
      or array_length(missing_hours) != expected_hours - present_hours
      or completeness_ratio < 0
      or completeness_ratio > 1
      or is_complete_day != (present_hours = expected_hours)
  )
