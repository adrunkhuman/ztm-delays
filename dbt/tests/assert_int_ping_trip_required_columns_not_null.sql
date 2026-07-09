{{ config(tags=['audit']) }}

select *
from {{ ref('int_ping_trip') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
    line is null
    or brigade is null
    or lat is null
    or lon is null
    or gps_time is null
    or vehicle_number is null
    or vehicle_type is null
    or ingested_at is null
    or trip_id is null
    or shape_id is null
    or gtfs_snapshot_id is null
    or service_id is null
    or direction_id is null
    or service_date is null
    or day_type is null
    or trip_start_seconds is null
    or trip_end_seconds is null
    or gps_time_seconds is null
    or observed_line is null
    or matching_method is null
    or matched_duty_chain_id is null
    or matched_duty_chain_source is null
    or matched_duty_chain_source_id is null
    or matched_duty_trip_order is null
    or candidate_rank is null
    or candidate_count is null
    or line_match_candidate_count is null
    or timing_penalty_seconds is null
    or timing_score is null
    or duty_chain_continuity_score is null
    or matching_flags is null
  )
