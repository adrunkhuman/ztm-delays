{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["entity_type", "mode", "metric"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with entity_metrics as (
    select
        'line' as entity_type,
        line as entity_id,
        mode,
        window_type,
        window_key,
        source_end_date,
        source_day_count,
        arrival_count,
        median_delay_seconds,
        on_time_rate,
        delay_spread_seconds
    from {{ ref('mart_line_window_summary') }}
    where source_end_date = date('{{ processing_date }}')
      and universe_type = 'zone1_public'

    union all

    select
        'stop_group',
        stop_group_id,
        mode,
        window_type,
        window_key,
        source_end_date,
        source_day_count,
        arrival_count,
        median_delay_seconds,
        on_time_rate,
        delay_spread_seconds
    from {{ ref('mart_stop_group_window_summary') }}
    where source_end_date = date('{{ processing_date }}')
      and universe_type = 'zone1_public'

    union all

    select
        'stop_post',
        stop_id,
        mode,
        window_type,
        window_key,
        source_end_date,
        source_day_count,
        arrival_count,
        median_delay_seconds,
        on_time_rate,
        delay_spread_seconds
    from {{ ref('mart_stop_post_window_summary') }}
    where source_end_date = date('{{ processing_date }}')
      and universe_type = 'zone1_public'
),

eligible as (
    select *
    from entity_metrics
    where arrival_count >= case entity_type when 'line' then 20 else 10 end * source_day_count
),

metric_rows as (
    select *, 'median_delay_seconds' as metric, median_delay_seconds as value from eligible
    union all
    select *, 'on_time_rate', on_time_rate from eligible
    union all
    select *, 'arrival_count', cast(arrival_count as float64) from eligible
    union all
    select *, 'delay_spread_seconds', delay_spread_seconds from eligible
),

ranked as (
    select
        *,
        row_number() over (
            partition by entity_type, metric, mode, window_type, window_key
            order by value desc, entity_id
        ) as rank,
        count(*) over (partition by entity_type, metric, mode, window_type, window_key) as n_entities
    from metric_rows
    where value is not null
)

select
    entity_type,
    entity_id,
    mode,
    window_type,
    window_key,
    source_end_date,
    metric,
    value,
    rank,
    n_entities
from ranked
