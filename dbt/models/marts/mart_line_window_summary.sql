{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

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
        windows.window_type,
        windows.window_key,
        windows.source_end_date,
        'all_observed' as universe_type,
        arrivals.*,
        concat(arrivals.gtfs_snapshot_id, '|', arrivals.trip_id, '|', coalesce(arrivals.vehicle_number, '')) as trip_key
    from {{ ref('int_serving_stop_arrival') }} as arrivals
    inner join {{ ref('dim_serving_window_date') }} as windows
        on arrivals.service_date = windows.service_date
        and windows.source_end_date = date('{{ processing_date }}')
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
      and arrivals.trip_quality = 'complete'
      and arrivals.mode in ('bus', 'tram')
      and (
          windows.window_type in ('day', 'month')
          or exists (
              select 1
              from {{ ref('dim_schedule_version') }} as anchor_version
              where anchor_version.schedule_version_id = arrivals.schedule_version_id
                and date('{{ processing_date }}') between anchor_version.valid_from_date
                    and coalesce(anchor_version.valid_to_date, date '9999-12-31')
          )
      )
),

zone1_windowed as (
    select base.* replace ('zone1_public' as universe_type)
    from base
    inner join {{ ref('int_serving_trip_universe') }} as universe
        on base.gtfs_snapshot_id = universe.gtfs_snapshot_id
        and base.gps_date = universe.processing_date
        and base.service_date = universe.service_date
        and base.trip_id = universe.trip_id
    where universe.processing_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ var("max_gps_date", processing_date) }}')
      and universe.is_zone1_public_ranking_trip
),

all_windowed as (
    select * from base
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
