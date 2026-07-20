with grouped as (
    select
        source_end_date,
        entity_type,
        entity_id,
        mode,
        universe_type,
        window_type,
        window_key,
        count(*) as row_count
    from {{ ref('int_serving_entity_window_summary') }}
    where source_end_date = date('{{ var("processing_date") }}')
    group by source_end_date, entity_type, entity_id, mode, universe_type, window_type, window_key
)

select *
from grouped
where entity_type is null
   or entity_id is null
   or mode not in ('bus', 'tram')
   or universe_type not in ('all_observed', 'zone1_public')
   or window_type not in ('day', 'weekdays', 'weekend', 'month')
   or row_count != 1
