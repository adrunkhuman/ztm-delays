{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "window_type"],
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
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

keyed as (
    select
        *,
        to_json_string(struct(mode, window_type, window_key)) as grain_key,
        concat(gtfs_snapshot_id, '|', trip_id, '|', coalesce(vehicle_number, '')) as trip_key
    from base
),

counts as (
    select
        grain_key,
        any_value(mode) as mode,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        min(service_date) as source_start_date,
        any_value(source_end_date) as source_end_date,
        count(distinct service_date) as source_day_count,
        count(distinct line) as line_count,
        count(distinct stop_group_id) as stop_group_count,
        count(distinct stop_id) as stop_post_count,
        count(distinct trip_key) as trip_count,
        {{ serving_delay_count_columns() }}
    from keyed
    group by grain_key
),

quantiles as (
    select distinct
        grain_key,
        percentile_cont(delay_seconds, 0.5) over (partition by grain_key) as median_delay_seconds,
        percentile_cont(delay_seconds, 0.9) over (partition by grain_key) as p90_delay_seconds
    from keyed
)

select
    counts.mode,
    counts.window_type,
    counts.window_key,
    counts.source_start_date,
    counts.source_end_date,
    counts.source_day_count,
    counts.line_count,
    counts.stop_group_count,
    counts.stop_post_count,
    counts.arrival_count,
    counts.trip_count,
    counts.mean_delay_seconds,
    quantiles.median_delay_seconds,
    quantiles.p90_delay_seconds,
    quantiles.p90_delay_seconds - quantiles.median_delay_seconds as delay_spread_seconds,
    counts.early_count,
    counts.on_time_count,
    counts.late_count,
    counts.early_rate,
    counts.on_time_rate,
    counts.late_rate,
    counts.delay_histogram
from counts
inner join quantiles using (grain_key)
