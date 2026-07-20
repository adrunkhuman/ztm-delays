{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line", "window_type"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

select
    entity_id as line,
    mode,
    route_short_name,
    route_label,
    universe_type,
    window_type,
    window_key,
    source_start_date,
    source_end_date,
    source_day_count,
    arrival_count,
    trip_count,
    mean_delay_seconds,
    median_delay_seconds,
    p90_delay_seconds,
    delay_spread_seconds,
    early_count,
    on_time_count,
    late_count,
    early_rate,
    on_time_rate,
    late_rate,
    delay_histogram
from {{ ref('int_serving_entity_window_summary') }}
where source_end_date = date('{{ processing_date }}')
  and entity_type = 'line'
