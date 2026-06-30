select *
from {{ ref('int_trip_summary') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
    gtfs_snapshot_id is null
    or service_date is null
    or trip_id is null
    or vehicle_number is null
    or line is null
    or brigade is null
    or vehicle_type is null
    or direction_id is null
    or service_id is null
    or trip_headsign is null
    or shape_id is null
    or schedule_day_type is null
    or schedule_version_id is null
    or scheduled_start_time is null
    or scheduled_end_time is null
    or actual_start_time is null
    or actual_end_time is null
    or start_delay_seconds is null
    or end_delay_seconds is null
    or stops_expected is null
    or stops_detected is null
    or detected_stop_ratio is null
    or first_detected_stop_sequence is null
    or last_detected_stop_sequence is null
    or max_stop_sequence_gap is null
    or max_ping_gap_seconds is null
    or max_speed_mps is null
    or is_first_stop_observed is null
    or is_last_stop_observed is null
    or has_non_monotonic_stop_progression is null
    or has_impossible_speed_jump is null
    or trip_quality is null
    or quality_flags is null
  )
