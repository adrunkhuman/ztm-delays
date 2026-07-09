select
    service_date,
    entity_type,
    entity_id,
    mode
from {{ ref('mart_entity_daily_summary') }}
where service_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      entity_type is null
      or entity_type not in ('mode', 'line', 'stop_group', 'stop_post')
      or entity_id is null
      or mode is null
      or mode not in ('bus', 'tram')
      or service_date is null
  )
