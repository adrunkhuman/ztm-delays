select service_observation_flag
from {{ ref('int_trip_summary') }}, unnest(service_observation_flags) as service_observation_flag
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and service_observation_flag not in (
    'short_start',
    'short_end',
    'large_internal_gap',
    'stale_progress',
    'bad_assignment_evidence'
  )
