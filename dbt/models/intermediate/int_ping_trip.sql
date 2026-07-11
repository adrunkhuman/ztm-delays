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

with execution_courses as (
    select
        execution.*,
        calendar_dates.day_type
    from {{ ref('int_duty_execution') }} as execution
    inner join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on execution.service_id = calendar_dates.service_id
        and execution.service_date = calendar_dates.service_date
        and execution.gtfs_snapshot_id = calendar_dates.gtfs_snapshot_id
        and calendar_dates.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    where execution.processing_date = date('{{ var("processing_date") }}')
      and execution.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
),

settled_executions as (
    select *
    from execution_courses
    where execution_status = 'executed'
      and vehicle_number is not null
      and ownership_interval_start_time is not null
      and ownership_interval_end_time is not null
),

settled_candidates as (
    select
        gps.line as observed_line,
        gps.brigade,
        gps.lat,
        gps.lon,
        gps.gps_time,
        gps.vehicle_number,
        gps.vehicle_type,
        gps.ingested_at,
        gps.gps_date,
        execution.line,
        execution.trip_id,
        execution.shape_id,
        execution.gtfs_snapshot_id,
        execution.service_id,
        execution.direction_id,
        execution.service_date,
        execution.day_type,
        execution.trip_start_seconds,
        execution.trip_end_seconds,
        execution.scheduled_start_time,
        execution.scheduled_end_time,
        execution.duty_chain_id,
        execution.duty_chain_source,
        execution.duty_chain_source_id,
        execution.trip_order,
        execution.execution_status,
        execution.confidence as execution_confidence,
        execution.execution_reason,
        execution.execution_evidence,
        execution.ownership_interval_start_time,
        execution.ownership_interval_end_time,
        timestamp_diff(gps.gps_time, timestamp(execution.service_date, 'Europe/Warsaw'), second)
            as gps_time_seconds,
        greatest(
            0,
            timestamp_diff(execution.scheduled_start_time, gps.gps_time, second),
            timestamp_diff(gps.gps_time, execution.scheduled_end_time, second)
        ) as timing_penalty_seconds,
        count(*) over (partition by gps.vehicle_number, gps.gps_time) as candidate_count,
        countif(gps.line = execution.line) over (partition by gps.vehicle_number, gps.gps_time)
            as line_match_candidate_count
    from {{ ref('stg_gps__pings') }} as gps
    inner join settled_executions as execution
        on gps.vehicle_number = execution.vehicle_number
        and gps.brigade = execution.brigade
        and gps.gps_time between execution.ownership_interval_start_time and execution.ownership_interval_end_time
        and gps.gps_date = execution.processing_date
        and (
            (gps.vehicle_type = 1 and execution.mode = 'bus')
            or (gps.vehicle_type = 2 and execution.mode = 'tram')
        )
    where gps.gps_date = date('{{ var("processing_date") }}')
),

settled_ranked as (
    select
        *,
        row_number() over (
            partition by vehicle_number, gps_time
            order by ownership_interval_start_time desc, trip_order desc, trip_id
        ) as candidate_rank
    from settled_candidates
),

settled_ping_keys as (
    -- Any settled interval owns the ping, even if overlapping intervals need ranking.
    select distinct vehicle_number, gps_time
    from settled_candidates
),

unsettled_candidates as (
    select
        gps.line as observed_line,
        gps.brigade,
        gps.lat,
        gps.lon,
        gps.gps_time,
        gps.vehicle_number,
        gps.vehicle_type,
        gps.ingested_at,
        gps.gps_date,
        execution.line,
        execution.trip_id,
        execution.shape_id,
        execution.gtfs_snapshot_id,
        execution.service_id,
        execution.direction_id,
        execution.service_date,
        execution.day_type,
        execution.trip_start_seconds,
        execution.trip_end_seconds,
        execution.scheduled_start_time,
        execution.scheduled_end_time,
        execution.duty_chain_id,
        execution.duty_chain_source,
        execution.duty_chain_source_id,
        execution.trip_order,
        timestamp_diff(gps.gps_time, timestamp(execution.service_date, 'Europe/Warsaw'), second)
            as gps_time_seconds,
        greatest(
            0,
            timestamp_diff(execution.scheduled_start_time, gps.gps_time, second),
            timestamp_diff(gps.gps_time, execution.scheduled_end_time, second)
        ) as timing_penalty_seconds,
        count(*) over (partition by gps.vehicle_number, gps.gps_time) as candidate_count,
        countif(gps.line = execution.line) over (partition by gps.vehicle_number, gps.gps_time)
            as line_match_candidate_count
    from {{ ref('stg_gps__pings') }} as gps
    inner join execution_courses as execution
        on gps.brigade = execution.brigade
        and gps.gps_time between timestamp_sub(execution.scheduled_start_time, interval 15 minute)
            and timestamp_add(execution.scheduled_end_time, interval 30 minute)
        and gps.gps_date = execution.processing_date
        and execution.execution_status != 'executed'
        and (
            (gps.vehicle_type = 1 and execution.mode = 'bus')
            or (gps.vehicle_type = 2 and execution.mode = 'tram')
        )
    left join settled_ping_keys
        on gps.vehicle_number = settled_ping_keys.vehicle_number
        and gps.gps_time = settled_ping_keys.gps_time
    where gps.gps_date = date('{{ var("processing_date") }}')
      and settled_ping_keys.gps_time is null
),

unsettled_ranked as (
    select
        *,
        row_number() over (
            partition by vehicle_number, gps_time
            order by
                if(observed_line = line, 0, 1),
                timing_penalty_seconds,
                scheduled_start_time desc,
                trip_order desc,
                trip_id
        ) as candidate_rank
    from unsettled_candidates
),

settled_matches as (
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
        'settled_duty_execution' as matching_method,
        duty_chain_id as matched_duty_chain_id,
        duty_chain_source as matched_duty_chain_source,
        duty_chain_source_id as matched_duty_chain_source_id,
        trip_order as matched_duty_trip_order,
        execution_status,
        execution_confidence,
        execution_reason,
        execution_evidence,
        ownership_interval_start_time,
        ownership_interval_end_time,
        candidate_rank,
        candidate_count,
        line_match_candidate_count,
        timing_penalty_seconds,
        1.0 as timing_score,
        1.0 as duty_chain_continuity_score,
        cast(null as float64) as spatial_score,
        cast(null as float64) as progression_score,
        array_concat(
            execution_evidence,
            if(observed_line != line, ['observed_line_mismatch'], []),
            if(candidate_count > 1, ['overlapping_execution_intervals'], [])
        ) as matching_flags
    from settled_ranked
    where candidate_rank = 1
),

unsettled_matches as (
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
        'uncertain_window_fallback' as matching_method,
        duty_chain_id as matched_duty_chain_id,
        duty_chain_source as matched_duty_chain_source,
        duty_chain_source_id as matched_duty_chain_source_id,
        trip_order as matched_duty_trip_order,
        'uncertain' as execution_status,
        'low' as execution_confidence,
        'window_fallback_unsettled' as execution_reason,
        ['window_fallback_unsettled'] as execution_evidence,
        cast(null as timestamp) as ownership_interval_start_time,
        cast(null as timestamp) as ownership_interval_end_time,
        candidate_rank,
        candidate_count,
        line_match_candidate_count,
        timing_penalty_seconds,
        greatest(0.0, 1.0 - safe_divide(timing_penalty_seconds, 1800.0)) as timing_score,
        0.0 as duty_chain_continuity_score,
        cast(null as float64) as spatial_score,
        cast(null as float64) as progression_score,
        array_concat(
            ['window_fallback_unsettled'],
            if(observed_line != line, ['observed_line_mismatch'], []),
            if(candidate_count > 1, ['uncertain_window_overlap'], [])
        ) as matching_flags
    from unsettled_ranked
    where candidate_rank = 1
)

select * from settled_matches
union all
select * from unsettled_matches
