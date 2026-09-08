with ledger as (
    select * from {{ ref('int_schedule_fingerprint_daily') }}
), invalid_dates as (
    select processing_date
    from ledger
    group by processing_date
    having countif(is_date_marker) != 1
        or count(distinct gtfs_snapshot_id) > 1
), duplicate_lines as (
    select processing_date
    from ledger
    where not is_date_marker
    group by processing_date, line, direction_id, schedule_day_type
    having count(*) != 1
)
select processing_date from invalid_dates
union all
select processing_date from duplicate_lines
union all
select processing_date from ledger
where processing_date is null or is_date_marker is null
    or (not is_date_marker and (
        gtfs_snapshot_id is null or line is null or direction_id is null
        or schedule_day_type is null or timetable_fingerprint is null or scheduled_trip_count <= 0
        or scheduled_trip_count is null
    ))
    or (is_date_marker and (
        line is not null or direction_id is not null or schedule_day_type is not null
        or timetable_fingerprint is not null or scheduled_trip_count is not null
    ))
