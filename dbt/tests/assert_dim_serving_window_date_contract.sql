{% set processing_date = var("processing_date", "1970-01-01") %}

select
    window_type,
    window_key,
    source_end_date,
    service_date,
    count(*) as duplicate_count
from {{ ref('dim_serving_window_date') }}
where source_end_date = date('{{ processing_date }}')
group by window_type, window_key, source_end_date, service_date
having count(*) > 1
   or min(date_rank) < 1
   or max(date_rank) > 60 and window_type in ('weekdays', 'weekend')
