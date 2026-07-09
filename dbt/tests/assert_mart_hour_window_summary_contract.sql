select
    source_end_date,
    entity_type,
    entity_id,
    mode,
    window_type,
    local_hour
from {{ ref('mart_hour_window_summary') }}
where source_end_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      entity_type is null
      or entity_type not in ('mode', 'line', 'stop_group', 'stop_post')
      or entity_id is null
      or mode is null
      or mode not in ('bus', 'tram')
      or window_type is null
      or window_type not in ('day', 'weekdays', 'weekend', 'month')
      or local_hour is null
  )
