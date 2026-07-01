{% set test_service_date = var("publish_service_date", var("processing_date")) %}

select *
from {{ ref('fct_stop_arrival') }}
where service_date = date('{{ test_service_date }}')
  and delay_seconds != timestamp_diff(actual_arrival_time, scheduled_arrival_time, second)
