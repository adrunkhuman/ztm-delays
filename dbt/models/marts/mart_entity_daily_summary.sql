{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["entity_type", "mode", "entity_id"],
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
    select 'mode' as entity_type, mode as entity_id, mode, service_date, delay_seconds from base
    union all
    select 'line', line, mode, service_date, delay_seconds from base
    union all
    select 'stop_group', stop_group_id, mode, service_date, delay_seconds from base
    union all
    select 'stop_post', stop_id, mode, service_date, delay_seconds from base
),

keyed as (
    select *, to_json_string(struct(entity_type, entity_id, mode, service_date)) as grain_key
    from entities
),

counts as (
    select
        grain_key,
        any_value(entity_type) as entity_type,
        any_value(entity_id) as entity_id,
        any_value(mode) as mode,
        any_value(service_date) as service_date,
        count(*) as arrival_count
    from keyed
    group by grain_key
),

quantiles as (
    select distinct
        grain_key,
        percentile_cont(delay_seconds, 0.5) over (partition by grain_key) as median_delay_seconds
    from keyed
)

select
    counts.entity_type,
    counts.entity_id,
    counts.mode,
    counts.service_date,
    counts.arrival_count,
    quantiles.median_delay_seconds
from counts
inner join quantiles using (grain_key)
