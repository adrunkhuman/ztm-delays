{% set processing_date = var('processing_date', '1970-01-01') %}
{% set start = var('serving_rebuild_start_date', processing_date) %}
{% set end = var('serving_rebuild_end_date', processing_date) %}

with {{ serving_route_pattern_window_ctes() }},

courses as (
    select * from {{ ref('mart_line_course_window') }}
    where source_end_date between date('{{ start }}') and date('{{ end }}')
),

stops as (
    select * from {{ ref('mart_line_course_stop_window') }}
    where source_end_date between date('{{ start }}') and date('{{ end }}')
),

expected as (
    select line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date,
        count(*) as trip_count
    from eligible
    group by line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date
),

actual as (
    select line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date,
        count(*) as course_rows, sum(trip_count) as trip_count, any_value(stop_call_count) as stop_call_count
    from courses
    group by line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date
),

call_counts as (
    select line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date,
        count(*) as stop_rows, count(distinct call_position) as positions,
        min(call_position) as first_position, max(call_position) as last_position
    from stops
    group by line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date
),

violations as (
    select 'course_execution_counts' as issue, count(*) as n
    from expected full outer join actual
        on expected.line = actual.line and expected.mode = actual.mode
        and expected.direction_id is not distinct from actual.direction_id
        and expected.trip_headsign is not distinct from actual.trip_headsign
        and expected.route_pattern_id = actual.route_pattern_id
        and expected.window_type = actual.window_type and expected.window_key = actual.window_key
        and expected.source_end_date = actual.source_end_date
    where expected.trip_count is null or actual.trip_count is null
        or expected.trip_count != actual.trip_count or actual.course_rows != 1
    union all
    select 'scheduled_occurrences', count(*)
    from actual full outer join call_counts
        on actual.line = call_counts.line and actual.mode = call_counts.mode
        and actual.direction_id is not distinct from call_counts.direction_id
        and actual.trip_headsign is not distinct from call_counts.trip_headsign
        and actual.route_pattern_id = call_counts.route_pattern_id
        and actual.window_type = call_counts.window_type and actual.window_key = call_counts.window_key
        and actual.source_end_date = call_counts.source_end_date
    where actual.course_rows is null
        or coalesce(call_counts.stop_rows, 0) != actual.stop_call_count
        or (actual.stop_call_count > 0 and (
            call_counts.positions != actual.stop_call_count or call_counts.first_position != 1
            or call_counts.last_position != actual.stop_call_count
        ))
    union all
    select 'stop_samples', count(*)
    from stops
    where route_pattern_id is null or route_pattern_id = 'unclassified' or display_rank != call_position
        or arrival_count > trip_count or early_count + on_time_count + late_count != arrival_count
        or has_min_sample != (arrival_count >= 3)
        or (arrival_count = 0 and (
            mean_delay_seconds is not null or median_delay_seconds is not null or p90_delay_seconds is not null
            or early_rate is not null or on_time_rate is not null or late_rate is not null
            -- BigQuery cannot store a NULL array; an unobserved call has an empty histogram.
            or array_length(delay_histogram) > 0
        ))
)

select * from violations where n > 0
