select *
from {{ ref('fct_trip') }}
where service_date between date_sub(date('{{ var("processing_date", "1970-01-01") }}'), interval 1 day)
    and date('{{ var("processing_date", "1970-01-01") }}')
  and (
    gtfs_snapshot_id is null
    or gps_date is null
    or trip_id is null
    or vehicle_number is null
    or line is null
    or mode is null
    or trip_headsign is null
    or schedule_day_type is null
    or schedule_version_id is null
    or origin_stop_id is null
    or origin_stop_name is null
    or destination_stop_id is null
    or destination_stop_name is null
    or scheduled_start_time is null
    or scheduled_end_time is null
    or actual_start_time is null
    or actual_end_time is null
    or start_delay_seconds is null
    or end_delay_seconds is null
    or trip_quality is null
    or quality_flags is null
  )
