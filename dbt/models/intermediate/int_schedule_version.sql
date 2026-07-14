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
    from {{ ref('int_gtfs_trip_schedule_history') }}
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
),

changes as (
    select
        *,
        timetable_fingerprint != lag(timetable_fingerprint) over line_schedule_window
            or lag(timetable_fingerprint) over line_schedule_window is null as starts_new_version
    from fingerprinted
    window line_schedule_window as (
        partition by line, direction_id, schedule_day_type
        order by processing_date, gtfs_snapshot_id
    )
),

versioned as (
    select
        *,
        countif(starts_new_version) over (
            partition by line, direction_id, schedule_day_type
            order by processing_date, gtfs_snapshot_id
            rows between unbounded preceding and current row
        ) as version_group
    from changes
),

version_rows as (
    select
        line,
        direction_id,
        schedule_day_type,
        timetable_fingerprint,
        min(processing_date) as valid_from_date,
        array_agg(gtfs_snapshot_id order by processing_date, gtfs_snapshot_id limit 1)[offset(0)] as first_gtfs_snapshot_id,
        array_agg(gtfs_snapshot_id order by processing_date desc, gtfs_snapshot_id desc limit 1)[offset(0)] as last_gtfs_snapshot_id,
        min(processing_date) as first_processing_date,
        max(processing_date) as last_processing_date,
        max(scheduled_trip_count) as max_scheduled_trip_count
    from versioned
    group by line, direction_id, schedule_day_type, timetable_fingerprint, version_group
),

ranged_rows as (
    select
        to_hex(md5(to_json_string(struct(
            line as line,
            direction_id as direction_id,
            schedule_day_type as schedule_day_type,
            timetable_fingerprint as timetable_fingerprint,
            valid_from_date as valid_from_date
        )))) as schedule_version_id,
        line,
        direction_id,
        schedule_day_type,
        timetable_fingerprint,
        valid_from_date,
        date_sub(
            lead(valid_from_date) over (partition by line, direction_id, schedule_day_type order by valid_from_date),
            interval 1 day
        ) as valid_to_date,
        first_gtfs_snapshot_id,
        last_gtfs_snapshot_id,
        first_processing_date,
        last_processing_date,
        max_scheduled_trip_count
    from version_rows
)

select
    schedule_version_id,
    line,
    direction_id,
    schedule_day_type,
    timetable_fingerprint,
    valid_from_date,
    valid_to_date,
    first_gtfs_snapshot_id,
    last_gtfs_snapshot_id,
    first_processing_date,
    last_processing_date,
    max_scheduled_trip_count
from ranged_rows
