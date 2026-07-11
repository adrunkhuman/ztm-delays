{{ config(tags=['audit']) }}

select gps_date, vehicle_number, gps_time, flag
from {{ ref('int_ping_trip') }}
cross join unnest(matching_flags) as flag
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and flag not in (
    'origin_departure_destination_progression',
    'adjacent_course_executions',
    'line_brigade_fallback',
    'observed_line_mismatch',
    'overlapping_execution_intervals',
    'window_fallback_unsettled',
    'uncertain_window_overlap'
  )
