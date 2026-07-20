{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["entity_type", "mode", "entity_id", "window_type"],
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
        concat(
            arrivals.gtfs_snapshot_id,
            '|',
            arrivals.trip_id,
            '|',
            coalesce(arrivals.vehicle_number, '')
        ) as trip_key,
        coalesce(universe.is_zone1_public_ranking_trip, false) as is_zone1_public_ranking_trip,
        exists (
            select 1
            from {{ ref('dim_schedule_version') }} as anchor_version
            where anchor_version.schedule_version_id = arrivals.schedule_version_id
              and date('{{ processing_date }}') between anchor_version.valid_from_date
                  and coalesce(anchor_version.valid_to_date, date '9999-12-31')
        ) as is_anchor_schedule_version
    from {{ ref('int_serving_stop_arrival') }} as arrivals
    inner join {{ ref('dim_serving_window_date') }} as windows
        on arrivals.service_date = windows.service_date
        and windows.source_end_date = date('{{ processing_date }}')
    left join {{ ref('int_serving_trip_universe') }} as universe
        on arrivals.gtfs_snapshot_id = universe.gtfs_snapshot_id
        and arrivals.gps_date = universe.processing_date
        and arrivals.service_date = universe.service_date
        and arrivals.trip_id = universe.trip_id
        and universe.processing_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
            and date('{{ var("max_gps_date", processing_date) }}')
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
      and arrivals.trip_quality = 'complete'
      and arrivals.mode in ('bus', 'tram')
),

expanded as (
    select
        base.*,
        universe_type,
        entity.entity_type,
        entity.entity_id
    from base
    cross join unnest(
        if(
            is_zone1_public_ranking_trip,
            ['all_observed', 'zone1_public'],
            ['all_observed']
        )
    ) as universe_type
    cross join unnest([
        struct('mode' as entity_type, mode as entity_id),
        struct('line' as entity_type, line as entity_id),
        struct('stop_group' as entity_type, stop_group_id as entity_id),
        struct('stop_post' as entity_type, stop_id as entity_id)
    ]) as entity
    where not (entity.entity_type = 'mode' and universe_type = 'zone1_public')
      and (
          entity.entity_type != 'line'
          or window_type in ('day', 'month')
          or is_anchor_schedule_version
      )
),

keyed as (
    select
        *,
        to_json_string(struct(entity_type, entity_id, mode, universe_type, window_type, window_key)) as grain_key
    from expanded
),

counts as (
    select
        grain_key,
        any_value(entity_type) as entity_type,
        any_value(entity_id) as entity_id,
        any_value(mode) as mode,
        any_value(universe_type) as universe_type,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        min(service_date) as source_start_date,
        any_value(source_end_date) as source_end_date,
        count(distinct service_date) as source_day_count,
        any_value(route_short_name) as route_short_name,
        array_agg(trip_headsign ignore nulls order by trip_headsign limit 1)[safe_offset(0)] as route_label,
        any_value(stop_group_id) as stop_group_id,
        any_value(stop_group_name) as stop_group_name,
        any_value(stop_id) as stop_id,
        any_value({{ stop_post_code('stop_id') }}) as stop_post_code,
        any_value(stop_name) as stop_name,
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
    counts.* except (grain_key),
    quantiles.median_delay_seconds,
    quantiles.p90_delay_seconds,
    quantiles.p90_delay_seconds - quantiles.median_delay_seconds as delay_spread_seconds
from counts
inner join quantiles using (grain_key)
