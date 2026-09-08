{% macro schedule_fingerprints(trip_schedule_relation) %}
with line_timetables as (
    select
        processing_date,
        gtfs_snapshot_id,
        line,
        direction_id,
        schedule_day_type,
        string_agg(
            trip_timetable_signature,
            '\n'
            order by
                {{ warsaw_scheduled_timestamp('service_date', 'trip_start_seconds') }},
                {{ warsaw_scheduled_timestamp('service_date', 'trip_end_seconds') }},
                trip_timetable_signature
        ) as line_timetable_signature,
        count(*) as scheduled_trip_count
    from {{ trip_schedule_relation }}
    group by processing_date, gtfs_snapshot_id, line, direction_id, schedule_day_type
),

fingerprinted as (
    select
        processing_date,
        gtfs_snapshot_id,
        line,
        direction_id,
        schedule_day_type,
        to_hex(md5(line_timetable_signature)) as timetable_fingerprint,
        scheduled_trip_count
    from line_timetables
)
select * from fingerprinted
{% endmacro %}
