select *
from {{ ref('fct_stop_arrival') }}
where service_date = date('{{ var("processing_date") }}')
  and delay_seconds != timestamp_diff(actual_arrival_time, scheduled_arrival_time, second)
