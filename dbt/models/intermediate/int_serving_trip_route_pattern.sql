{% set processing_date = var("processing_date", "1970-01-01") %}
{% set scope = serving_route_pattern_rebuild_range() %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
    )
}}

with executions as (
    select *
    from {{ ref('int_serving_trip_execution') }}
    where service_date
    {% if is_incremental() %}
        = date('{{ processing_date }}')
    {% else %}
        between date_sub(date('{{ scope.start }}'), interval {{ lookback_days }} day) and date('{{ scope.end }}')
    {% endif %}
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

events as (
    select *
    from {{ ref('fct_expected_stop_event') }}
    where service_date
    {% if is_incremental() %}
        = date('{{ processing_date }}')
    {% else %}
        between date_sub(date('{{ scope.start }}'), interval {{ lookback_days }} day) and date('{{ scope.end }}')
    {% endif %}
),

profiles as (
    select
        executions.gtfs_snapshot_id,
        executions.gps_date,
        executions.service_date,
        executions.trip_id,
        executions.vehicle_number,
        executions.line,
        executions.mode,
        executions.route_short_name,
        executions.direction_id,
        executions.trip_headsign,
        executions.schedule_version_id,
        count(events.stop_sequence) > 0
            and coalesce(logical_and(events.are_passenger_boundaries_settled), false)
            and countif(events.stop_execution_class = 'unknown') = 0
            and countif(events.is_passenger_stop) > 0 as is_classified,
        array_agg(
            if(events.is_passenger_stop, struct(
                events.stop_sequence,
                events.stop_group_id,
                events.stop_id,
                events.stop_post_code,
                events.stop_name
            ), null) ignore nulls order by events.stop_sequence
        ) as passenger_stops
    from executions
    left join events
        on executions.gtfs_snapshot_id = events.gtfs_snapshot_id
        and executions.gps_date = events.gps_date
        and executions.service_date = events.service_date
        and executions.trip_id = events.trip_id
        and executions.vehicle_number = events.vehicle_number
    group by
        executions.gtfs_snapshot_id, executions.gps_date, executions.service_date,
        executions.trip_id, executions.vehicle_number, executions.line, executions.mode,
        executions.route_short_name, executions.direction_id, executions.trip_headsign,
        executions.schedule_version_id
),

normalized as (
    select
        * except (passenger_stops),
        -- Occurrences, not distinct groups: two calls at the same group remain two calls.
        array(
            select as struct call_offset + 1 as call_position, stop.*
            from unnest(if(is_classified, passenger_stops, [])) as stop with offset as call_offset
            order by call_offset
        ) as stops
    from profiles
)

select
    * except (is_classified),
    if(is_classified, 'classified', 'unclassified') as pattern_status,
    if(is_classified,
        to_hex(sha256(to_json_string(struct(
            mode, line, direction_id,
            array(select stop.stop_group_id from unnest(stops) as stop order by stop.call_position) as stop_groups
        )))),
        'unclassified'
    ) as route_pattern_id,
    array_length(stops) as stop_call_count,
    stops[safe_offset(0)].stop_name as origin_stop_name,
    stops[safe_offset(array_length(stops) - 1)].stop_name as destination_stop_name
from normalized
