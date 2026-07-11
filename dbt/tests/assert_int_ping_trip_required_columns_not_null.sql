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
    or execution_status is null
    or execution_confidence is null
    or execution_reason is null
    or execution_evidence is null
    or execution_status not in ('executed', 'uncertain')
    or execution_confidence not in ('high', 'medium', 'low')
    or (
        matching_method = 'settled_duty_execution'
        and execution_status != 'executed'
    )
    or (
        matching_method = 'uncertain_window_fallback'
        and (
            execution_status != 'uncertain'
            or execution_confidence != 'low'
            or execution_reason != 'window_fallback_unsettled'
            or execution_evidence != ['window_fallback_unsettled']
        )
    )
    or matching_method not in ('settled_duty_execution', 'uncertain_window_fallback')
    or (
        execution_status = 'executed'
        and (ownership_interval_start_time is null or ownership_interval_end_time is null)
    )
    or (
        execution_status = 'uncertain'
        and (ownership_interval_start_time is not null or ownership_interval_end_time is not null)
    )
    or candidate_rank is null
    or candidate_count is null
    or line_match_candidate_count is null
    or timing_penalty_seconds is null
    or timing_score is null
    or duty_chain_continuity_score is null
    or matching_flags is null
  )
