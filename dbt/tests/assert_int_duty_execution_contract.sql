{{ config(tags=['audit']) }}

with invalid_rows as (
    select
        'invalid_execution_status' as issue_type,
        count(*) as violation_count
    from {{ ref('int_duty_execution') }}
    where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
      and execution_status not in ('executed', 'missed', 'skipped', 'short_turned', 'vehicle_swap', 'uncertain')

    union all

    select
        'invalid_confidence',
        count(*)
    from {{ ref('int_duty_execution') }}
    where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
      and confidence not in ('high', 'medium', 'low')

    union all

    select
        'invalid_execution_evidence',
        count(*)
    from {{ ref('int_duty_execution') }}
    cross join unnest(execution_evidence) as evidence
    where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
      and evidence not in (
          'origin_departure_destination_progression',
          'multiple_vehicles',
          'adjacent_course_executions',
           'next_course_origin_before_destination',
          'line_observation_without_terminal_progression',
          'passenger_boundaries_unknown',
          'line_brigade_fallback'
      )

    union all

    select
        'invalid_execution_reason',
        count(*)
    from {{ ref('int_duty_execution') }}
    where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
      and execution_reason not in (
          'terminal_progression',
          'no_line_observation',
          'adjacent_courses_terminal_progression',
          'multiple_vehicles_terminal_progression',
           'next_course_origin_before_destination',
          'passenger_boundaries_unknown',
          'terminal_progression_incomplete'
      )

    union all

    select
        'invalid_ownership_bounds',
        count(*)
    from {{ ref('int_duty_execution') }}
    where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
      and (
          (execution_status = 'executed' and (
              vehicle_number is null
              or ownership_interval_start_time is null
              or ownership_interval_end_time is null
              or ownership_interval_start_time > ownership_interval_end_time
              or ownership_interval_start_time > source_ping_start_time
              or ownership_interval_end_time < source_ping_end_time
          ))
          or (execution_status != 'executed' and (
              ownership_interval_start_time is not null
              or ownership_interval_end_time is not null
          ))
      )
),

duplicate_courses as (
    select
        'duplicate_scheduled_course' as issue_type,
        count(*) as violation_count
    from (
        select
            service_date,
            processing_date,
            gtfs_snapshot_id,
            duty_chain_id,
            trip_id
        from {{ ref('int_duty_execution') }}
        where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
        group by service_date, processing_date, gtfs_snapshot_id, duty_chain_id, trip_id
        having count(*) > 1
    )
),

overlapping_vehicle_ownership_intervals as (
    select
        'overlapping_vehicle_ownership_intervals' as issue_type,
        count(*) as violation_count
    from (
        select
            ownership_interval_start_time,
            lag(ownership_interval_end_time) over (
                partition by
                    service_date,
                    processing_date,
                    gtfs_snapshot_id,
                    duty_chain_id,
                    vehicle_number,
                    vehicle_type
                order by ownership_interval_start_time, trip_order, trip_id
            ) as previous_ownership_interval_end_time
        from {{ ref('int_duty_execution') }}
        where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
          and execution_status = 'executed'
    )
    where ownership_interval_start_time <= previous_ownership_interval_end_time
),

duplicate_vehicle_traversal_endpoints as (
    select
        'duplicate_vehicle_traversal_endpoints' as issue_type,
        count(*) as violation_count
    from (
        select
            service_date,
            processing_date,
            gtfs_snapshot_id,
            duty_chain_id,
            vehicle_number,
            vehicle_type,
            ownership_interval_end_time
        from {{ ref('int_duty_execution') }}
        where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
          and execution_status = 'executed'
        group by
            service_date,
            processing_date,
            gtfs_snapshot_id,
            duty_chain_id,
            vehicle_number,
            vehicle_type,
            ownership_interval_end_time
        having count(*) > 1
    )
)

select issue_type, violation_count
from invalid_rows
where violation_count > 0

union all

select issue_type, violation_count
from duplicate_courses
where violation_count > 0

union all

select issue_type, violation_count
from overlapping_vehicle_ownership_intervals
where violation_count > 0

union all

select issue_type, violation_count
from duplicate_vehicle_traversal_endpoints
where violation_count > 0
