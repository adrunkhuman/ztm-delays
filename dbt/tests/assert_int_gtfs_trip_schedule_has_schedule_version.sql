{{ config(tags=['audit']) }}

select
    trip_schedule.gtfs_snapshot_id,
    trip_schedule.processing_date,
    trip_schedule.line,
    trip_schedule.direction_id,
    trip_schedule.schedule_day_type,
    trip_schedule.trip_id,
    count(dim_schedule_version.schedule_version_id) as matching_schedule_versions
from {{ ref('int_gtfs_trip_schedule') }} as trip_schedule
left join {{ ref('dim_schedule_version') }} as dim_schedule_version
    on trip_schedule.line = dim_schedule_version.line
    and trip_schedule.direction_id = dim_schedule_version.direction_id
    and trip_schedule.schedule_day_type = dim_schedule_version.schedule_day_type
    and trip_schedule.processing_date between dim_schedule_version.valid_from_date
    and coalesce(dim_schedule_version.valid_to_date, date '9999-12-31')
where trip_schedule.processing_date = date('{{ var("processing_date") }}')
group by
    trip_schedule.gtfs_snapshot_id,
    trip_schedule.processing_date,
    trip_schedule.line,
    trip_schedule.direction_id,
    trip_schedule.schedule_day_type,
    trip_schedule.trip_id
having matching_schedule_versions != 1
