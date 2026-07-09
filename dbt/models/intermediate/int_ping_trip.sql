{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["line", "trip_id"],
        require_partition_filter=true,
    )
}}

{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}

with matching_thresholds as (
    select
        900 as pre_start_tolerance_seconds,
        1800 as post_end_tolerance_seconds,
        150.0 as handoff_origin_radius_m,
        150000.0 as delayed_tail_score_bonus,
        150000.0 as completed_handoff_score_penalty,
        60.0 as uncertain_score_margin
),

duty_segments as (
    select
        duty.service_date,
        duty.processing_date,
        duty.gtfs_snapshot_id,
        duty.duty_chain_id,
        duty.duty_chain_source,
        duty.duty_chain_source_id,
        duty.trip_order,
        duty.line,
        duty.mode,
        duty.brigade,
        duty.trip_id,
        duty.service_id,
        duty.direction_id,
        duty.shape_id,
        duty.trip_start_seconds,
        duty.trip_end_seconds,
        duty.scheduled_start_time,
        duty.scheduled_end_time,
        duty.previous_trip_id,
        duty.next_trip_id,
        duty.origin_stop_id,
        duty.destination_stop_id,
        duty.line_changed_from_previous,
        duty.line_changes_to_next,
        calendar_dates.day_type
    from {{ ref('int_gtfs_duty_chain') }} as duty
    inner join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on duty.service_id = calendar_dates.service_id
        and duty.service_date = calendar_dates.service_date
        and duty.gtfs_snapshot_id = calendar_dates.gtfs_snapshot_id
        and calendar_dates.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    where duty.processing_date = date('{{ var("processing_date") }}')
      and duty.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
      and duty.service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
        and date('{{ var("processing_date") }}')
      and not duty.is_malformed_duty_segment
),

duty_segments_with_handoff as (
    select
        duty.*,
        next_duty.origin_stop_id as next_origin_stop_id,
        next_origin_stop.stop_lat as next_origin_stop_lat,
        next_origin_stop.stop_lon as next_origin_stop_lon
    from duty_segments as duty
    left join duty_segments as next_duty
        on duty.gtfs_snapshot_id = next_duty.gtfs_snapshot_id
        and duty.service_date = next_duty.service_date
        and duty.duty_chain_id = next_duty.duty_chain_id
        and duty.next_trip_id = next_duty.trip_id
    left join {{ ref('stg_gtfs__stops') }} as next_origin_stop
        on next_duty.gtfs_snapshot_id = next_origin_stop.gtfs_snapshot_id
        and next_duty.origin_stop_id = next_origin_stop.stop_id
        and next_origin_stop.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
),

gps_pings as (
    select
        line as observed_line,
        brigade,
        lat,
        lon,
        gps_time,
        vehicle_number,
        vehicle_type,
        ingested_at,
        gps_date
    from {{ ref('stg_gps__pings') }}
    where gps_date = date('{{ var("processing_date") }}')
),

candidate_matches as (
    select
        gps.observed_line,
        gps.brigade,
        gps.lat,
        gps.lon,
        gps.gps_time,
        gps.vehicle_number,
        gps.vehicle_type,
        gps.ingested_at,
        gps.gps_date,
        duty.line,
        duty.mode,
        duty.trip_id,
        duty.shape_id,
        duty.gtfs_snapshot_id,
        duty.service_id,
        duty.direction_id,
        duty.service_date,
        duty.day_type,
        duty.trip_start_seconds,
        duty.trip_end_seconds,
        duty.scheduled_start_time,
        duty.scheduled_end_time,
        duty.duty_chain_id,
        duty.duty_chain_source,
        duty.duty_chain_source_id,
        duty.trip_order,
        duty.previous_trip_id,
        duty.next_trip_id,
        duty.line_changed_from_previous,
        duty.line_changes_to_next,
        case
            when duty.next_origin_stop_lat is null or duty.next_origin_stop_lon is null then null
            else st_distance(st_geogpoint(gps.lon, gps.lat), st_geogpoint(duty.next_origin_stop_lon, duty.next_origin_stop_lat))
        end as next_origin_distance_m,
        timestamp_diff(gps.gps_time, timestamp(duty.service_date, 'Europe/Warsaw'), second) as gps_time_seconds,
        gps.observed_line = duty.line as has_line_match,
        gps.gps_time between duty.scheduled_start_time and duty.scheduled_end_time as is_within_scheduled_window,
        greatest(
            0,
            timestamp_diff(duty.scheduled_start_time, gps.gps_time, second),
            timestamp_diff(gps.gps_time, duty.scheduled_end_time, second)
        ) as timing_penalty_seconds,
        gps.gps_time < duty.scheduled_start_time as is_pre_start,
        gps.gps_time > duty.scheduled_end_time as is_post_end
    from gps_pings as gps
    inner join duty_segments_with_handoff as duty
        on gps.brigade = duty.brigade
        and (
            (gps.vehicle_type = 1 and duty.mode = 'bus')
            or (gps.vehicle_type = 2 and duty.mode = 'tram')
        )
    cross join matching_thresholds
    where gps.gps_time between timestamp_sub(
            duty.scheduled_start_time,
            interval matching_thresholds.pre_start_tolerance_seconds second
        )
        and timestamp_add(duty.scheduled_end_time, interval matching_thresholds.post_end_tolerance_seconds second)
),

candidate_progression as (
    select
        candidates.*,
        coalesce(min(candidates.next_origin_distance_m) over (
            partition by candidates.gps_date, candidates.vehicle_number, candidates.duty_chain_id, candidates.trip_id
            order by candidates.gps_time
            rows between unbounded preceding and 1 preceding
        ) <= matching_thresholds.handoff_origin_radius_m, false) as has_reached_next_origin_before_ping
    from candidate_matches as candidates
    cross join matching_thresholds
),

vehicle_chain_evidence as (
    select
        gps_date,
        vehicle_number,
        duty_chain_id,
        countif(has_line_match) as chain_line_match_ping_count,
        count(distinct if(has_line_match, line, null)) as chain_line_match_count
    from candidate_progression
    group by gps_date, vehicle_number, duty_chain_id
),

segment_vehicle_evidence as (
    select
        gps_date,
        gtfs_snapshot_id,
        service_date,
        trip_id,
        count(distinct if(has_line_match, vehicle_number, null)) as segment_line_match_vehicle_count
    from candidate_progression
    group by gps_date, gtfs_snapshot_id, service_date, trip_id
),

candidate_counts as (
    select
        gps_date,
        vehicle_number,
        gps_time,
        count(*) as candidate_count,
        countif(has_line_match) as line_match_candidate_count
    from candidate_progression
    group by gps_date, vehicle_number, gps_time
),

scored as (
    select
        candidates.*,
        chain_evidence.chain_line_match_ping_count,
        chain_evidence.chain_line_match_count,
        segment_evidence.segment_line_match_vehicle_count,
        candidate_counts.candidate_count,
        candidate_counts.line_match_candidate_count,
        candidates.is_post_end
            and candidates.has_line_match
            and candidates.next_trip_id is not null
            and not candidates.line_changes_to_next
            and not candidates.has_reached_next_origin_before_ping as is_delayed_same_line_tail,
        candidates.is_post_end
            and candidates.has_line_match
            and candidates.next_trip_id is not null
            and not candidates.line_changes_to_next
            and candidates.has_reached_next_origin_before_ping
            and candidates.next_origin_distance_m > matching_thresholds.handoff_origin_radius_m
            as has_completed_same_line_handoff,
        greatest(0.0, 1.0 - safe_divide(candidates.timing_penalty_seconds, case
            when candidates.is_pre_start then matching_thresholds.pre_start_tolerance_seconds
            when candidates.is_post_end then matching_thresholds.post_end_tolerance_seconds
            else 1
        end)) as timing_score,
        coalesce(safe_divide(
            chain_evidence.chain_line_match_ping_count,
            max(chain_evidence.chain_line_match_ping_count) over (
                partition by candidates.gps_date, candidates.vehicle_number
            )
        ), 0.0) as duty_chain_continuity_score,
        cast(null as float64) as spatial_score,
        cast(null as float64) as progression_score,
        (
            if(candidates.has_line_match, 1000000.0, 0.0)
            + if(candidates.is_within_scheduled_window, 100000.0, 0.0)
            + if(
                candidates.is_post_end
                and candidates.has_line_match
                and candidates.next_trip_id is not null
                and not candidates.line_changes_to_next
                and not candidates.has_reached_next_origin_before_ping,
                matching_thresholds.delayed_tail_score_bonus,
                0.0
            )
            - if(
                candidates.is_post_end
                and candidates.has_line_match
                and candidates.next_trip_id is not null
                and not candidates.line_changes_to_next
                and candidates.has_reached_next_origin_before_ping
                and candidates.next_origin_distance_m > matching_thresholds.handoff_origin_radius_m,
                matching_thresholds.completed_handoff_score_penalty,
                0.0
            )
            + coalesce(chain_evidence.chain_line_match_ping_count, 0)
            + greatest(0.0, 1000.0 - candidates.timing_penalty_seconds)
        ) as candidate_score,
        matching_thresholds.uncertain_score_margin
    from candidate_progression as candidates
    inner join vehicle_chain_evidence as chain_evidence
        on candidates.gps_date = chain_evidence.gps_date
        and candidates.vehicle_number = chain_evidence.vehicle_number
        and candidates.duty_chain_id = chain_evidence.duty_chain_id
    inner join segment_vehicle_evidence as segment_evidence
        on candidates.gps_date = segment_evidence.gps_date
        and candidates.gtfs_snapshot_id = segment_evidence.gtfs_snapshot_id
        and candidates.service_date = segment_evidence.service_date
        and candidates.trip_id = segment_evidence.trip_id
    inner join candidate_counts
        on candidates.gps_date = candidate_counts.gps_date
        and candidates.vehicle_number = candidate_counts.vehicle_number
        and candidates.gps_time = candidate_counts.gps_time
    cross join matching_thresholds
),

ranked as (
    select
        *,
        row_number() over candidate_window as candidate_rank,
        lead(candidate_score) over candidate_window as next_candidate_score
    from scored
    window candidate_window as (
        partition by vehicle_number, gps_time
        order by
            candidate_score desc,
            timing_penalty_seconds,
            scheduled_start_time desc,
            trip_id
    )
),

selected_matches as (
    select
        *,
        array_concat(
            if(is_pre_start, ['early_origin_censored'], []),
            if(is_post_end, ['late_tail_censored'], []),
            if(is_delayed_same_line_tail, ['delayed_same_line_tail'], []),
            if(candidate_count > 1, ['candidate_overlap'], []),
            if(is_post_end and candidate_count > 1, ['likely_previous_trip_tail'], []),
            if(segment_line_match_vehicle_count > 1, ['likely_vehicle_swap'], []),
            if(
                next_candidate_score is not null
                and candidate_score - next_candidate_score < uncertain_score_margin,
                ['uncertain_assignment'],
                []
            )
        ) as matching_flags
    from ranked
    where candidate_rank = 1
)

select
    line,
    brigade,
    lat,
    lon,
    gps_time,
    vehicle_number,
    vehicle_type,
    ingested_at,
    gps_date,
    trip_id,
    shape_id,
    gtfs_snapshot_id,
    service_id,
    direction_id,
    service_date,
    day_type,
    trip_start_seconds,
    trip_end_seconds,
    gps_time_seconds,
    observed_line,
    'settled_duty_chain' as matching_method,
    duty_chain_id as matched_duty_chain_id,
    duty_chain_source as matched_duty_chain_source,
    duty_chain_source_id as matched_duty_chain_source_id,
    trip_order as matched_duty_trip_order,
    candidate_rank,
    candidate_count,
    line_match_candidate_count,
    timing_penalty_seconds,
    timing_score,
    duty_chain_continuity_score,
    spatial_score,
    progression_score,
    matching_flags
from selected_matches
