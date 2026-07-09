select
    source_end_date,
    stop_id,
    stop_group_id,
    mode,
    universe_type,
    window_type
from {{ ref('mart_stop_post_window_summary') }}
where source_end_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      stop_id is null
      or stop_group_id is null
      or mode is null
      or mode not in ('bus', 'tram')
      or universe_type is null
      or universe_type not in ('all_observed', 'zone1_public')
      or window_type is null
      or window_type not in ('day', 'weekdays', 'weekend', 'month')
      or source_end_date is null
  )
