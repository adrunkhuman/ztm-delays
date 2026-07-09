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

with base as (
    select
        arrivals.*,
        versions.valid_from_date as schedule_version_start_date,
        concat(arrivals.gtfs_snapshot_id, '|', arrivals.trip_id, '|', coalesce(arrivals.vehicle_number, '')) as trip_key
    from {{ ref('fct_stop_arrival') }} as arrivals
    left join {{ ref('dim_schedule_version') }} as versions
        on arrivals.schedule_version_id = versions.schedule_version_id
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval 60 day) and date('{{ processing_date }}')
      and arrivals.trip_quality = 'complete'
      and arrivals.mode in ('bus', 'tram')
),

windowed as (
    select 'day' as window_type, cast(service_date as string) as window_key, service_date as source_end_date, 'all_observed' as universe_type, *
    from base
    where service_date = date('{{ processing_date }}')

    union all

    select 'month', format_date('%Y-%m', date('{{ processing_date }}')), date('{{ processing_date }}'), 'all_observed', *
    from base
    where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)

    union all

    select 'weekdays', cast(date('{{ processing_date }}') as string), date('{{ processing_date }}'), 'all_observed', *
    from base
    where schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')
      and service_date >= coalesce(schedule_version_start_date, date_sub(date('{{ processing_date }}'), interval 60 day))

    union all

    select 'weekend', cast(date('{{ processing_date }}') as string), date('{{ processing_date }}'), 'all_observed', *
    from base
    where schedule_day_type in ('saturday', 'sunday_holiday')
      and service_date >= coalesce(schedule_version_start_date, date_sub(date('{{ processing_date }}'), interval 60 day))
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
        and date('{{ processing_date }}')
      and universe.is_zone1_public_ranking_trip
),

all_windowed as (
    select * from windowed
    union all
    select * from zone1_windowed
),

keyed as (
    select *, to_json_string(struct(line, mode, universe_type, window_type, window_key)) as grain_key
    from all_windowed
),

counts as (
    select
        grain_key,
        any_value(line) as line,
        any_value(mode) as mode,
        any_value(route_short_name) as route_short_name,
        array_agg(trip_headsign order by trip_headsign limit 1)[offset(0)] as route_label,
        any_value(universe_type) as universe_type,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        min(service_date) as source_start_date,
        any_value(source_end_date) as source_end_date,
        count(distinct service_date) as source_day_count,
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
    counts.line,
    counts.mode,
    counts.route_short_name,
    counts.route_label,
    counts.universe_type,
    counts.window_type,
    counts.window_key,
    counts.source_start_date,
    counts.source_end_date,
    counts.source_day_count,
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
