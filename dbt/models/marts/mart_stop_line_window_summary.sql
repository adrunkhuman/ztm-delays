{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "entity_type", "entity_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with base as (
    select *
    from {{ ref('fct_stop_arrival') }}
    where service_date = date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

entities as (
    select 'stop_group' as entity_type, stop_group_id as entity_id, * from base
    union all
    select 'stop_post', stop_id, * from base
),

keyed as (
    select
        *,
        'day' as window_type,
        cast(service_date as string) as window_key,
        service_date as source_end_date,
        to_json_string(struct(entity_type, entity_id, line, direction_id, trip_headsign)) as grain_key
    from entities
),

counts as (
    select
        grain_key,
        any_value(entity_type) as entity_type,
        any_value(entity_id) as entity_id,
        any_value(stop_group_id) as stop_group_id,
        any_value(if(entity_type = 'stop_post', stop_id, null)) as stop_id,
        any_value(if(entity_type = 'stop_post', {{ stop_post_code('stop_id') }}, null)) as stop_post_code,
        any_value(line) as line,
        any_value(mode) as mode,
        any_value(route_short_name) as route_short_name,
        any_value(direction_id) as direction_id,
        any_value(trip_headsign) as trip_headsign,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        any_value(source_end_date) as source_end_date,
        {{ serving_delay_count_columns() }}
    from keyed
    group by grain_key
),

quantiles as (
    select distinct grain_key,
        percentile_cont(delay_seconds, 0.5) over (partition by grain_key) as median_delay_seconds,
        percentile_cont(delay_seconds, 0.9) over (partition by grain_key) as p90_delay_seconds
    from keyed
),

joined as (
    select
        counts.*,
        quantiles.median_delay_seconds,
        quantiles.p90_delay_seconds,
        quantiles.p90_delay_seconds - quantiles.median_delay_seconds as delay_spread_seconds
    from counts
    inner join quantiles using (grain_key)
)

select
    entity_type,
    entity_id,
    stop_group_id,
    stop_id,
    stop_post_code,
    line,
    mode,
    route_short_name,
    direction_id,
    trip_headsign,
    window_type,
    window_key,
    source_end_date,
    row_number() over (partition by entity_type, entity_id, mode, window_type, window_key order by median_delay_seconds desc, arrival_count desc) as display_rank,
    arrival_count,
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
    delay_histogram,
    arrival_count >= 3 as has_min_sample
from joined
