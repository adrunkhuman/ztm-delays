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
    select
        windows.window_type,
        windows.window_key,
        windows.source_end_date,
        arrivals.*,
        extract(hour from arrivals.hour_bracket at time zone 'Europe/Warsaw') as local_hour,
        mod(extract(hour from arrivals.hour_bracket at time zone 'Europe/Warsaw') + 20, 24) as service_hour_index
    from {{ ref('int_serving_stop_arrival') }} as arrivals
    inner join {{ ref('dim_serving_window_date') }} as windows
        on arrivals.service_date = windows.service_date
        and windows.source_end_date = date('{{ processing_date }}')
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
      and {{ serving_window_service_date_filter('arrivals.service_date', processing_date, processing_date) }}
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
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
        to_json_string(struct(entity_type, entity_id, mode, window_type, window_key, local_hour)) as grain_key
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
        min(service_date) as source_start_date,
        any_value(source_end_date) as source_end_date,
        any_value(local_hour) as local_hour,
        any_value(service_hour_index) as service_hour_index,
        format('%02d', any_value(local_hour)) as hour_bracket_label,
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
    counts.window_type,
    counts.window_key,
    counts.source_start_date,
    counts.source_end_date,
    counts.local_hour,
    counts.service_hour_index,
    counts.hour_bracket_label,
    counts.arrival_count,
    quantiles.median_delay_seconds,
    counts.arrival_count >= 3 as has_min_sample
from counts
inner join quantiles using (grain_key)
