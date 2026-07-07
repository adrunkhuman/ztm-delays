{% set test_service_date = var("publish_service_date", var("processing_date")) %}

select *
from {{ ref('fct_expected_stop_event') }}
where service_date = date('{{ test_service_date }}')
  and (
    observation_status not in ('observed', 'missed', 'uncertain')
    or (observation_status = 'observed' and (actual_arrival_time is null or delay_seconds is null))
    or (observation_status = 'missed' and (actual_arrival_time is not null or delay_seconds is not null))
  )
