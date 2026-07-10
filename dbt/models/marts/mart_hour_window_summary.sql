{% set processing_date = var("processing_date", "1970-01-01") %}

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
        *,
        extract(hour from hour_bracket at time zone 'Europe/Warsaw') as local_hour,
        mod(extract(hour from hour_bracket at time zone 'Europe/Warsaw') + 20, 24) as service_hour_index
    from {{ ref('int_serving_stop_arrival') }}
    where service_date between date_sub(date('{{ processing_date }}'), interval 60 day) and date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

windowed as (
    select 'day' as window_type, cast(service_date as string) as window_key, service_date as source_end_date, *
    from base
    where service_date = date('{{ processing_date }}')
    union all
    select 'month', format_date('%Y-%m', date('{{ processing_date }}')), date('{{ processing_date }}'), *
    from base
    where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)
    union all
    select 'weekdays', cast(date('{{ processing_date }}') as string), date('{{ processing_date }}'), *
    from base
    where schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')
    union all
    select 'weekend', cast(date('{{ processing_date }}') as string), date('{{ processing_date }}'), *
    from base
    where schedule_day_type in ('saturday', 'sunday_holiday')
),

entities as (
    select 'mode' as entity_type, mode as entity_id, * from windowed
    union all
    select 'line', line, * from windowed
    union all
    select 'stop_group', stop_group_id, * from windowed
    union all
    select 'stop_post', stop_id, * from windowed
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
