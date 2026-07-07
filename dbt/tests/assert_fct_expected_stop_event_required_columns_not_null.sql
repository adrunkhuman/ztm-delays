{% set test_service_date = var("publish_service_date", var("processing_date")) %}

select *
from {{ ref('fct_expected_stop_event') }}
where service_date = date('{{ test_service_date }}')
  and (
    gtfs_snapshot_id is null
    or gps_date is null
    or service_date is null
    or trip_id is null
    or vehicle_number is null
    or line is null
    or mode is null
    or vehicle_type is null
    or direction_id is null
    or trip_headsign is null
    or day_type is null
    or is_holiday is null
    or schedule_day_type is null
    or schedule_version_id is null
    or trip_quality is null
    or quality_flags is null
    or matching_method is null
    or matched_duty_chain_id is null
    or candidate_rank is null
    or matching_flags is null
    or stop_id is null
    or stop_group_id is null
    or stop_name is null
    or stop_lat is null
    or stop_lon is null
    or stop_group_name is null
    or stop_sequence is null
    or scheduled_arrival_time is null
    or scheduled_departure_time is null
    or hour_bracket is null
    or is_observed is null
    or is_match_uncertain is null
    or observation_status is null
  )
