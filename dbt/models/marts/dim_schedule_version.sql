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
from {{ ref('int_schedule_version') }}
