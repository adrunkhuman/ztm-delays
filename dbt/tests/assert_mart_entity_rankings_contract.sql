select
    source_end_date,
    entity_type,
    entity_id,
    metric,
    rank
from {{ ref('mart_entity_rankings') }}
where source_end_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      entity_type is null
      or entity_type not in ('line', 'stop_group', 'stop_post')
      or entity_id is null
      or metric is null
      or metric not in ('median_delay_seconds', 'on_time_rate', 'arrival_count', 'delay_spread_seconds')
      or rank is null
      or n_entities is null
  )
