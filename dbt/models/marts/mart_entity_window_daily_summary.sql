{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["entity_type", "mode", "entity_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with base as (
    select windows.window_type, windows.window_key, windows.source_end_date, arrivals.*
    from {{ ref('int_serving_stop_arrival') }} as arrivals
    inner join {{ ref('dim_serving_window_date') }} as windows
        on arrivals.service_date = windows.service_date
        and windows.source_end_date = date('{{ processing_date }}')
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
      and arrivals.trip_quality = 'complete'
      and arrivals.mode in ('bus', 'tram')
),

line_base as (
    select *
    from base
    where window_type in ('day', 'month')
       or exists (
           select 1
           from {{ ref('dim_schedule_version') }} as anchor_version
           where anchor_version.schedule_version_id = base.schedule_version_id
             and date('{{ processing_date }}') between anchor_version.valid_from_date
                 and coalesce(anchor_version.valid_to_date, date '9999-12-31')
       )
),

entities as (
    select 'mode' as entity_type, mode as entity_id, * from base
    union all
    select 'line', line, * from line_base
    union all
    select 'stop_group', stop_group_id, * from base
    union all
    select 'stop_post', stop_id, * from base
),

keyed as (
    select
        *,
        to_json_string(struct(entity_type, entity_id, mode, window_type, window_key, service_date)) as grain_key
    from entities
),

counts as (
    select
        grain_key,
        any_value(entity_type) as entity_type,
        any_value(entity_id) as entity_id,
        any_value(mode) as mode,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        any_value(source_end_date) as source_end_date,
        any_value(service_date) as service_date,
        any_value(schedule_day_type) as schedule_day_type,
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

select counts.*, quantiles.median_delay_seconds
from counts
inner join quantiles using (grain_key)
