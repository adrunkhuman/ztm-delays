{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "stop_group_id", "window_type"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with base as (
    select *, concat(gtfs_snapshot_id, '|', trip_id, '|', coalesce(vehicle_number, '')) as trip_key
    from {{ ref('int_serving_stop_arrival') }}
    where service_date between date_sub(date('{{ processing_date }}'), interval 60 day) and date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

windowed as (
    select 'day' as window_type, cast(service_date as string) as window_key, service_date as source_end_date, 'all_observed' as universe_type, * from base where service_date = date('{{ processing_date }}')
    union all
    select 'month', format_date('%Y-%m', date('{{ processing_date }}')), date('{{ processing_date }}'), 'all_observed', * from base where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)
    union all
    select 'weekdays', cast(date('{{ processing_date }}') as string), date('{{ processing_date }}'), 'all_observed', * from base where schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')
    union all
    select 'weekend', cast(date('{{ processing_date }}') as string), date('{{ processing_date }}'), 'all_observed', * from base where schedule_day_type in ('saturday', 'sunday_holiday')
),

zone1_windowed as (
    select windowed.* replace ('zone1_public' as universe_type)
    from windowed
    inner join {{ ref('int_serving_trip_universe') }} as universe
        on windowed.gtfs_snapshot_id = universe.gtfs_snapshot_id
        and windowed.gps_date = universe.processing_date
        and windowed.service_date = universe.service_date
        and windowed.trip_id = universe.trip_id
    where universe.processing_date between date_sub(date('{{ processing_date }}'), interval 60 day)
        and date('{{ var("max_gps_date", processing_date) }}')
      and universe.is_zone1_public_ranking_trip
),

all_windowed as (
    select * from windowed
    union all
    select * from zone1_windowed
),

keyed as (
    select *, to_json_string(struct(stop_group_id, mode, universe_type, window_type, window_key)) as grain_key
    from all_windowed
),

counts as (
    select
        grain_key,
        any_value(stop_group_id) as stop_group_id,
        any_value(stop_group_name) as stop_group_name,
        any_value(mode) as mode,
        any_value(universe_type) as universe_type,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        min(service_date) as source_start_date,
        any_value(source_end_date) as source_end_date,
        count(distinct service_date) as source_day_count,
        count(distinct stop_id) as stop_post_count,
        count(distinct line) as line_count,
        count(distinct trip_key) as trip_count,
        {{ serving_delay_count_columns() }}
    from keyed
    group by grain_key
),

quantiles as (
    select distinct grain_key,
        percentile_cont(delay_seconds, 0.5) over (partition by grain_key) as median_delay_seconds,
        percentile_cont(delay_seconds, 0.9) over (partition by grain_key) as p90_delay_seconds
    from keyed
)

select
    counts.stop_group_id,
    counts.stop_group_name,
    counts.mode,
    counts.universe_type,
    counts.window_type,
    counts.window_key,
    counts.source_start_date,
    counts.source_end_date,
    counts.source_day_count,
    counts.stop_post_count,
    counts.line_count,
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
