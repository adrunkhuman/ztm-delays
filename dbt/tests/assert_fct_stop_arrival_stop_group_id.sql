select *
from {{ ref('fct_stop_arrival') }}
where service_date = date('{{ var("processing_date") }}')
  and stop_group_id != substr(stop_id, 1, 4)
