{% set processing_date = var('processing_date', '1970-01-01') %}
{% set start = var('serving_rebuild_start_date', processing_date) %}
{% set end = var('serving_rebuild_end_date', processing_date) %}
{% set lookback_days = var('serving_window_lookback_days', 420) %}

with patterns as (
    select * from {{ ref('int_serving_trip_route_pattern') }}
    where service_date between date_sub(date('{{ start }}'), interval {{ lookback_days }} day) and date('{{ end }}')
),

violations as (
    select 'execution_mapping' as issue, count(*) as n
    from (
        select gtfs_snapshot_id, service_date, trip_id, vehicle_number
        from patterns
        group by gtfs_snapshot_id, service_date, trip_id, vehicle_number
        having count(*) != 1
    )
    union all
    select 'pattern_metadata', count(*)
    from patterns
    where route_pattern_id is null or pattern_status not in ('classified', 'unclassified')
        or (route_pattern_id = 'unclassified') != (pattern_status = 'unclassified')
        or stop_call_count != array_length(stops)
        or (pattern_status = 'classified' and (stop_call_count = 0 or origin_stop_name is null or destination_stop_name is null))
        or (pattern_status = 'unclassified' and (stop_call_count != 0 or origin_stop_name is not null or destination_stop_name is not null))
    union all
    select 'occurrence_order', count(*)
    from patterns cross join unnest(stops) as stop with offset as call_offset
    where stop.call_position != call_offset + 1 or stop.stop_group_id is null or stop.stop_id is null
    union all
    select 'missing_execution', count(*)
    from {{ ref('int_serving_trip_execution') }} as execution
    left join patterns
        on execution.gtfs_snapshot_id = patterns.gtfs_snapshot_id
        and execution.gps_date = patterns.gps_date
        and execution.service_date = patterns.service_date
        and execution.trip_id = patterns.trip_id
        and execution.vehicle_number = patterns.vehicle_number
    where execution.service_date between date_sub(date('{{ start }}'), interval {{ lookback_days }} day) and date('{{ end }}')
        and execution.trip_quality = 'complete' and execution.mode in ('bus', 'tram')
        and patterns.trip_id is null
)

select * from violations where n > 0
