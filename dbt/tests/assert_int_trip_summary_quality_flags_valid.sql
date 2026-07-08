select quality_flag
from {{ ref('int_trip_summary') }}, unnest(quality_flags) as quality_flag
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and quality_flag not in (
    'missing_first_stop',
    'missing_last_stop',
    'low_stop_coverage',
    'large_ping_gap',
    'non_monotonic_stop_progression',
    'impossible_speed_jump',
    'large_stop_sequence_gap',
    'extreme_delay',
    'stale_stop_progression',
    'likely_wrong_trip_assignment'
  )
