select
    source_end_date,
    mode,
    window_type,
    window_key
from {{ ref('mart_mode_window_summary') }}
where source_end_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      mode is null
      or mode not in ('bus', 'tram')
      or window_type is null
      or window_type not in ('day', 'weekdays', 'weekend', 'month')
      or window_key is null
      or source_end_date is null
      or arrival_count is null
  )
