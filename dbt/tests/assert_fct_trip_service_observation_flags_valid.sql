{% set test_service_date = var("publish_service_date", var("processing_date")) %}

select service_observation_flag
from {{ ref('fct_trip') }}, unnest(service_observation_flags) as service_observation_flag
where service_date = date('{{ test_service_date }}')
  and service_observation_flag not in (
    'short_start',
    'short_end',
    'large_internal_gap',
    'stale_progress',
    'bad_assignment_evidence'
  )
