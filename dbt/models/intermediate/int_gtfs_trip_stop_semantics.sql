{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}
{% set terminal_movement_max_m = var("technical_terminal_movement_max_m", 250) %}

with duty_trips as (
    select
        service_date,
        processing_date,
        gtfs_snapshot_id,
        trip_id,
        duty_chain_id,
        duty_chain_source,
        duty_chain_source_id,
        trip_order,
        previous_trip_id,
        next_trip_id,
        layover_from_previous_seconds,
        layover_to_next_seconds,
        is_depot_segment
    from {{ ref('int_gtfs_duty_chain') }}
    where gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
),

stop_rows as (
    select
        duty_trips.*,
        stop_times.stop_id,
        substr(stop_times.stop_id, 1, 4) as stop_group_id,
        stop_times.stop_sequence,
        stop_times.arrival_time_seconds,
        stop_times.departure_time_seconds,
        stop_times.pickup_type,
        stop_times.drop_off_type,
        stop_times.stop_service_class,
        stops.stop_lat,
        stops.stop_lon,
        stop_times.stop_service_class != 'not_in_passenger_service' as is_gtfs_passenger_eligible,
        min(stop_times.stop_sequence) over trip_window as first_operational_stop_sequence,
        max(stop_times.stop_sequence) over trip_window as last_operational_stop_sequence,
        countif(stop_times.stop_service_class != 'not_in_passenger_service') over trip_window
            as gtfs_passenger_eligible_stop_count,
        countif(stop_times.stop_service_class != 'not_in_passenger_service') over (
            partition by duty_trips.gtfs_snapshot_id, duty_trips.service_date, duty_trips.trip_id
            order by stop_times.stop_sequence
            rows between unbounded preceding and 1 preceding
        ) as passenger_eligible_stops_before,
        countif(stop_times.stop_service_class != 'not_in_passenger_service') over (
            partition by duty_trips.gtfs_snapshot_id, duty_trips.service_date, duty_trips.trip_id
            order by stop_times.stop_sequence
            rows between 1 following and unbounded following
        ) as passenger_eligible_stops_after,
        lag(stop_times.stop_id) over trip_order_window as previous_stop_id,
        lead(stop_times.stop_id) over trip_order_window as next_stop_id,
        lag(substr(stop_times.stop_id, 1, 4)) over trip_order_window as previous_stop_group_id,
        lead(substr(stop_times.stop_id, 1, 4)) over trip_order_window as next_stop_group_id,
        lag(stops.stop_lat) over trip_order_window as previous_stop_lat,
        lag(stops.stop_lon) over trip_order_window as previous_stop_lon,
        lead(stops.stop_lat) over trip_order_window as next_stop_lat,
        lead(stops.stop_lon) over trip_order_window as next_stop_lon
    from duty_trips
    inner join {{ ref('stg_gtfs__stop_times') }} as stop_times
        on duty_trips.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
        and duty_trips.trip_id = stop_times.trip_id
        and stop_times.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    inner join {{ ref('stg_gtfs__stops') }} as stops
        on stop_times.gtfs_snapshot_id = stops.gtfs_snapshot_id
        and stop_times.stop_id = stops.stop_id
        and stops.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    window
        trip_window as (
            partition by duty_trips.gtfs_snapshot_id, duty_trips.service_date, duty_trips.trip_id
        ),
        trip_order_window as (
            partition by duty_trips.gtfs_snapshot_id, duty_trips.service_date, duty_trips.trip_id
            order by stop_times.stop_sequence
        )
),

trip_terminals as (
    select
        gtfs_snapshot_id,
        service_date,
        trip_id,
        array_agg(stop_id order by stop_sequence limit 1)[offset(0)] as origin_stop_id,
        array_agg(stop_id order by stop_sequence desc limit 1)[offset(0)] as destination_stop_id
    from stop_rows
    group by gtfs_snapshot_id, service_date, trip_id
),

handoff_evidence as (
    select
        stop_rows.*,
        previous_trip.destination_stop_id as previous_trip_destination_stop_id,
        next_trip.origin_stop_id as next_trip_origin_stop_id,
        case
            when previous_stop_lat is null or previous_stop_lon is null or stop_lat is null or stop_lon is null then null
            else st_distance(
                st_geogpoint(previous_stop_lon, previous_stop_lat),
                st_geogpoint(stop_lon, stop_lat)
            )
        end as distance_from_previous_stop_m,
        case
            when next_stop_lat is null or next_stop_lon is null or stop_lat is null or stop_lon is null then null
            else st_distance(
                st_geogpoint(stop_lon, stop_lat),
                st_geogpoint(next_stop_lon, next_stop_lat)
            )
        end as distance_to_next_stop_m
    from stop_rows
    left join trip_terminals as previous_trip
        on stop_rows.gtfs_snapshot_id = previous_trip.gtfs_snapshot_id
        and stop_rows.service_date = previous_trip.service_date
        and stop_rows.previous_trip_id = previous_trip.trip_id
    left join trip_terminals as next_trip
        on stop_rows.gtfs_snapshot_id = next_trip.gtfs_snapshot_id
        and stop_rows.service_date = next_trip.service_date
        and stop_rows.next_trip_id = next_trip.trip_id
),

boundary_evidence as (
    select
        *,
        stop_sequence = first_operational_stop_sequence
            and stop_id = previous_trip_destination_stop_id
            and next_stop_id is not null
            and next_stop_id != stop_id
            and next_stop_group_id = stop_group_id as has_prefix_handoff_shape,
        stop_sequence = last_operational_stop_sequence
            and stop_id = next_trip_origin_stop_id
            and previous_stop_id is not null
            and previous_stop_id != stop_id
            and previous_stop_group_id = stop_group_id as has_suffix_handoff_shape
    from handoff_evidence
),

classified as (
    select
        *,
        case
            when is_depot_segment or gtfs_passenger_eligible_stop_count = 0 then 'technical_trip'
            when not is_gtfs_passenger_eligible and coalesce(passenger_eligible_stops_before, 0) = 0
                then 'technical_prefix'
            when not is_gtfs_passenger_eligible and coalesce(passenger_eligible_stops_after, 0) = 0
                then 'technical_suffix'
            when not is_gtfs_passenger_eligible then 'unknown'
            when has_prefix_handoff_shape
                and duty_chain_source = 'block_id'
                and layover_from_previous_seconds >= 0
                and distance_to_next_stop_m <= {{ terminal_movement_max_m }}
                then 'technical_prefix'
            when has_suffix_handoff_shape
                and duty_chain_source = 'block_id'
                and layover_to_next_seconds >= 0
                and distance_from_previous_stop_m <= {{ terminal_movement_max_m }}
                then 'technical_suffix'
            when has_prefix_handoff_shape or has_suffix_handoff_shape then 'unknown'
            else 'passenger'
        end as stop_execution_class
    from boundary_evidence
),

with_trip_settlement as (
    select
        *,
        countif(
            stop_execution_class = 'unknown'
            and stop_sequence in (first_operational_stop_sequence, last_operational_stop_sequence)
        ) over trip_window = 0 as are_passenger_boundaries_settled,
        min(if(stop_execution_class = 'passenger', stop_sequence, null)) over trip_window
            as candidate_first_passenger_stop_sequence,
        max(if(stop_execution_class = 'passenger', stop_sequence, null)) over trip_window
            as candidate_last_passenger_stop_sequence
    from classified
    window trip_window as (
        partition by gtfs_snapshot_id, service_date, trip_id
    )
)

select
    gtfs_snapshot_id,
    service_date,
    processing_date,
    trip_id,
    stop_id,
    stop_group_id,
    stop_sequence,
    arrival_time_seconds,
    departure_time_seconds,
    pickup_type,
    drop_off_type,
    stop_service_class,
    duty_chain_id,
    duty_chain_source,
    duty_chain_source_id,
    trip_order,
    previous_trip_id,
    next_trip_id,
    stop_execution_class,
    case
        when stop_execution_class = 'unknown' then 'low'
        else 'high'
    end as classification_confidence,
    case
        when is_depot_segment then 'depot_segment'
        when gtfs_passenger_eligible_stop_count = 0 then 'no_gtfs_passenger_stops'
        when not is_gtfs_passenger_eligible and stop_execution_class = 'technical_prefix'
            then 'explicit_non_passenger_prefix'
        when not is_gtfs_passenger_eligible and stop_execution_class = 'technical_suffix'
            then 'explicit_non_passenger_suffix'
        when not is_gtfs_passenger_eligible then 'explicit_internal_non_passenger'
        when stop_execution_class = 'technical_prefix' then 'adjacent_duty_origin_handoff'
        when stop_execution_class = 'technical_suffix' then 'adjacent_duty_destination_handoff'
        when stop_execution_class = 'unknown' then 'ambiguous_terminal_handoff'
        else 'gtfs_passenger_stop'
    end as classification_reason,
    case
        when is_depot_segment then ['depot_terminal_name']
        when gtfs_passenger_eligible_stop_count = 0 then ['explicit_non_passenger_service']
        when not is_gtfs_passenger_eligible then ['explicit_non_passenger_service']
        when stop_execution_class in ('technical_prefix', 'technical_suffix') then [
            'adjacent_trip_exact_stop_post',
            'block_id_duty_chain',
            'non_negative_layover',
            'same_stop_group_terminal_movement',
            'short_terminal_movement'
        ]
        when stop_execution_class = 'unknown' then ['incomplete_terminal_handoff_evidence']
        else ['gtfs_passenger_eligible']
    end as classification_evidence,
    stop_execution_class = 'passenger' and are_passenger_boundaries_settled as is_passenger_stop,
    are_passenger_boundaries_settled,
    if(are_passenger_boundaries_settled, candidate_first_passenger_stop_sequence, null)
        as first_passenger_stop_sequence,
    if(are_passenger_boundaries_settled, candidate_last_passenger_stop_sequence, null)
        as last_passenger_stop_sequence
from with_trip_settlement
