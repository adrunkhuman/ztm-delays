select *
from {{ ref('int_stop_arrivals') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
    gtfs_snapshot_id is null
    or line is null
    or brigade is null
    or vehicle_number is null
    or vehicle_type is null
    or service_date is null
    or day_type is null
    or trip_id is null
    or shape_id is null
    or service_id is null
    or direction_id is null
    or stop_id is null
    or stop_name is null
    or stop_sequence is null
    or pickup_type is null
    or drop_off_type is null
    or stop_service_class is null
    or scheduled_arrival_time is null
    or scheduled_departure_time is null
    or actual_arrival_time is null
    or arrival_delay_seconds is null
    or detection_method is null
    or stop_match_radius_m is null
    or stop_distance_m is null
    or prev_ping_distance_m is null
    or next_ping_distance_m is null
    or segment_start_time is null
    or segment_end_time is null
    or segment_duration_seconds is null
  )
