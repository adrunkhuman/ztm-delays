select *
from {{ ref('fct_stop_arrival') }}
where service_date = date('{{ var("processing_date") }}')
  and hour_bracket != timestamp_trunc(scheduled_arrival_time, hour, 'Europe/Warsaw')
