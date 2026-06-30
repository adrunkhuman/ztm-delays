select quality_flag
from {{ ref('fct_trip') }}, unnest(quality_flags) as quality_flag
where service_date = date('{{ var("processing_date") }}')
  and quality_flag not in (
    'missing_first_stop',
    'missing_last_stop',
    'low_stop_coverage',
    'large_ping_gap',
    'non_monotonic_stop_progression',
    'impossible_speed_jump',
    'extreme_delay',
    'likely_wrong_trip_assignment'
  )
