{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partition_by={"field": "processing_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["duty_chain_id", "trip_id"],
        require_partition_filter=true,
    )
}}

{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}
{% set terminal_radius_m = var("duty_execution_terminal_radius_m", 250) %}
{% set terminal_visit_gap_seconds = var("duty_execution_terminal_visit_gap_seconds", 180) %}
{% set duty_execution_unit_test = var("duty_execution_unit_test", false) %}
{% set duty_execution_unit_test_output_course_count = var("duty_execution_unit_test_output_course_count", 12) %}

{% if duty_execution_unit_test %}
with duty_segments as (
{% else %}
with recursive duty_segments as (
{% endif %}
    select
        service_date,
        processing_date,
        gtfs_snapshot_id,
        duty_chain_id,
        duty_chain_source,
        duty_chain_source_id,
        trip_order,
        line,
        route_short_name,
        mode,
        brigade,
        trip_id,
        service_id,
        direction_id,
        shape_id,
        trip_start_seconds,
        trip_end_seconds,
        scheduled_start_time,
        scheduled_end_time,
        previous_trip_id,
        next_trip_id,
        layover_from_previous_seconds,
        layover_to_next_seconds,
        line_changed_from_previous,
        line_changes_to_next
    from {{ ref('int_gtfs_duty_chain') }}
    where processing_date = date('{{ var("processing_date") }}')
      and gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
      and mode in ('bus', 'tram')
      and not is_malformed_duty_segment
),

gps_pings as (
    select *
    from {{ ref('stg_gps__pings') }}
    where gps_date = date('{{ var("processing_date") }}')
),

terminal_coordinates as (
    select
        duty_segments.service_date,
        duty_segments.processing_date,
        duty_segments.gtfs_snapshot_id,
        duty_segments.duty_chain_id,
        duty_segments.trip_id,
        logical_and(stop_semantics.are_passenger_boundaries_settled) as are_passenger_boundaries_settled,
        array_agg(
            if(
                stop_semantics.is_passenger_stop
                or (
                    not stop_semantics.are_passenger_boundaries_settled
                    and stop_semantics.stop_execution_class in ('passenger', 'unknown')
                ),
                struct(stop_semantics.stop_sequence, stops.stop_lat, stops.stop_lon),
                null
            ) ignore nulls
            order by stop_semantics.stop_sequence
            limit 1
        )[safe_offset(0)] as origin_terminal,
        array_agg(
            if(
                stop_semantics.is_passenger_stop
                or (
                    not stop_semantics.are_passenger_boundaries_settled
                    and stop_semantics.stop_execution_class in ('passenger', 'unknown')
                ),
                struct(stop_semantics.stop_sequence, stops.stop_lat, stops.stop_lon),
                null
            ) ignore nulls
            order by stop_semantics.stop_sequence desc
            limit 1
        )[safe_offset(0)] as destination_terminal
    from duty_segments
    left join {{ ref('int_gtfs_trip_stop_semantics') }} as stop_semantics
        on duty_segments.gtfs_snapshot_id = stop_semantics.gtfs_snapshot_id
        and duty_segments.service_date = stop_semantics.service_date
        and duty_segments.processing_date = stop_semantics.processing_date
        and duty_segments.trip_id = stop_semantics.trip_id
    left join {{ ref('stg_gtfs__stops') }} as stops
        on stop_semantics.gtfs_snapshot_id = stops.gtfs_snapshot_id
        and stop_semantics.stop_id = stops.stop_id
        and stops.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    group by
        duty_segments.service_date,
        duty_segments.processing_date,
        duty_segments.gtfs_snapshot_id,
        duty_segments.duty_chain_id,
        duty_segments.trip_id
),

course_terminals_base as (
    select
        duty_segments.*,
        coalesce(terminal_coordinates.are_passenger_boundaries_settled, false)
            as are_passenger_boundaries_settled,
        terminal_coordinates.origin_terminal.stop_sequence as first_passenger_stop_sequence,
        terminal_coordinates.destination_terminal.stop_sequence as last_passenger_stop_sequence,
        terminal_coordinates.origin_terminal.stop_lat as origin_stop_lat,
        terminal_coordinates.origin_terminal.stop_lon as origin_stop_lon,
        terminal_coordinates.destination_terminal.stop_lat as destination_stop_lat,
        terminal_coordinates.destination_terminal.stop_lon as destination_stop_lon
    from duty_segments
    left join terminal_coordinates
        on duty_segments.gtfs_snapshot_id = terminal_coordinates.gtfs_snapshot_id
        and duty_segments.service_date = terminal_coordinates.service_date
        and duty_segments.processing_date = terminal_coordinates.processing_date
        and duty_segments.duty_chain_id = terminal_coordinates.duty_chain_id
        and duty_segments.trip_id = terminal_coordinates.trip_id
),

terminal_patterns as (
    select
        distinct service_date,
        processing_date,
        gtfs_snapshot_id,
        duty_chain_id,
        line,
        brigade,
        mode,
        origin_stop_lat,
        origin_stop_lon,
        destination_stop_lat,
        destination_stop_lon,
        to_json_string(struct(
            service_date,
            processing_date,
            gtfs_snapshot_id,
            duty_chain_id,
            line,
            brigade,
            mode,
            origin_stop_lat,
            origin_stop_lon,
            destination_stop_lat,
            destination_stop_lon
        )) as terminal_pattern_id
    from course_terminals_base
    where origin_stop_lat is not null
      and origin_stop_lon is not null
      and destination_stop_lat is not null
      and destination_stop_lon is not null
),

course_terminals as (
    select
        course_terminals_base.*,
        terminal_patterns.terminal_pattern_id
    from course_terminals_base
    left join terminal_patterns
        on course_terminals_base.service_date = terminal_patterns.service_date
        and course_terminals_base.processing_date = terminal_patterns.processing_date
        and course_terminals_base.gtfs_snapshot_id = terminal_patterns.gtfs_snapshot_id
        and course_terminals_base.duty_chain_id = terminal_patterns.duty_chain_id
        and course_terminals_base.line = terminal_patterns.line
        and course_terminals_base.brigade = terminal_patterns.brigade
        and course_terminals_base.mode = terminal_patterns.mode
        and course_terminals_base.origin_stop_lat = terminal_patterns.origin_stop_lat
        and course_terminals_base.origin_stop_lon = terminal_patterns.origin_stop_lon
        and course_terminals_base.destination_stop_lat = terminal_patterns.destination_stop_lat
        and course_terminals_base.destination_stop_lon = terminal_patterns.destination_stop_lon
),

terminal_pattern_gps_pings as (
    -- Repeated scheduled courses share a terminal pattern, so calculate distances once per pattern and vehicle.
    select
        terminal_patterns.*,
        gps.vehicle_number,
        gps.vehicle_type,
        gps.gps_time,
        gps.lat,
        gps.lon
    from terminal_patterns
    inner join gps_pings as gps
        on terminal_patterns.brigade = gps.brigade
        and terminal_patterns.line = gps.line
        and (
            (terminal_patterns.mode = 'bus' and gps.vehicle_type = 1)
            or (terminal_patterns.mode = 'tram' and gps.vehicle_type = 2)
        )
),

line_observations as (
    select
        course_terminals.service_date,
        course_terminals.processing_date,
        course_terminals.gtfs_snapshot_id,
        course_terminals.duty_chain_id,
        course_terminals.trip_id,
        terminal_pattern_gps_pings.vehicle_number,
        terminal_pattern_gps_pings.vehicle_type,
        count(*) as line_ping_count
    from course_terminals
    inner join terminal_pattern_gps_pings
        on course_terminals.terminal_pattern_id = terminal_pattern_gps_pings.terminal_pattern_id
    group by
        course_terminals.service_date,
        course_terminals.processing_date,
        course_terminals.gtfs_snapshot_id,
        course_terminals.duty_chain_id,
        course_terminals.trip_id,
        terminal_pattern_gps_pings.vehicle_number,
        terminal_pattern_gps_pings.vehicle_type
),

line_observation_summary as (
    select
        service_date,
        processing_date,
        gtfs_snapshot_id,
        duty_chain_id,
        trip_id,
        sum(line_ping_count) as line_ping_count
    from line_observations
    group by service_date, processing_date, gtfs_snapshot_id, duty_chain_id, trip_id
),

terminal_ping_states as (
    select
        *,
        case
            when is_origin_hit and is_destination_hit then 'origin_destination'
            when is_origin_hit then 'origin'
            when is_destination_hit then 'destination'
            else 'outside'
        end as terminal_state
    from (
        select
            terminal_pattern_gps_pings.*,
            st_distance(
                st_geogpoint(terminal_pattern_gps_pings.lon, terminal_pattern_gps_pings.lat),
                st_geogpoint(terminal_pattern_gps_pings.origin_stop_lon, terminal_pattern_gps_pings.origin_stop_lat)
            )
                <= {{ terminal_radius_m }} as is_origin_hit,
            st_distance(
                st_geogpoint(terminal_pattern_gps_pings.lon, terminal_pattern_gps_pings.lat),
                st_geogpoint(terminal_pattern_gps_pings.destination_stop_lon, terminal_pattern_gps_pings.destination_stop_lat)
            )
                <= {{ terminal_radius_m }} as is_destination_hit
        from terminal_pattern_gps_pings
    )
),

ordered_terminal_ping_states as (
    select
        *,
        lag(gps_time) over terminal_ping_window as previous_gps_time,
        lag(terminal_state) over terminal_ping_window as previous_terminal_state
    from terminal_ping_states
    window terminal_ping_window as (
        partition by
            terminal_pattern_id,
            vehicle_number,
            vehicle_type
        order by gps_time
    )
),

terminal_ping_episodes as (
    select
        *,
        sum(
            if(
                previous_terminal_state is null
                or terminal_state != previous_terminal_state
                or timestamp_diff(gps_time, previous_gps_time, second) > {{ terminal_visit_gap_seconds }},
                1,
                0
            )
        ) over (
            partition by
                terminal_pattern_id,
                vehicle_number,
                vehicle_type
            order by gps_time
            rows between unbounded preceding and current row
        ) as terminal_visit_id
    from ordered_terminal_ping_states
),

terminal_hits as (
    select *
    from terminal_ping_episodes
    where terminal_state != 'outside'
),

terminal_visit_episodes as (
    select
        terminal_pattern_id,
        vehicle_number,
        vehicle_type,
        terminal_visit_id,
        logical_or(is_origin_hit) as is_origin_visit,
        logical_or(is_destination_hit) as is_destination_visit,
        min(gps_time) as visit_start_time,
        max(gps_time) as visit_end_time
    from terminal_hits
    group by
        terminal_pattern_id,
        vehicle_number,
        vehicle_type,
        terminal_visit_id
),

origin_visit_context as (
    select
        *,
        last_value(if(is_origin_hit, terminal_visit_id, null) ignore nulls) over (
            partition by
                terminal_pattern_id,
                vehicle_number,
                vehicle_type
            order by gps_time
            rows between unbounded preceding and current row
        ) as origin_terminal_visit_id
    from terminal_ping_episodes
),

departure_events as (
    select
        terminal_pattern_id,
        vehicle_number,
        vehicle_type,
        origin_terminal_visit_id as terminal_visit_id,
        gps_time as departure_event_time
    from origin_visit_context
    where not is_origin_hit
      and origin_terminal_visit_id is not null
    qualify row_number() over (
        partition by
            terminal_pattern_id,
            vehicle_number,
            vehicle_type,
            origin_terminal_visit_id
        order by gps_time
    ) = 1
),

pattern_traversal_candidates as (
    select
        origin_visits.terminal_pattern_id,
        origin_visits.vehicle_number,
        origin_visits.vehicle_type,
        origin_visits.visit_start_time as origin_event_time,
        departure_events.departure_event_time,
        destination_visits.visit_start_time as destination_event_time
    from terminal_visit_episodes as origin_visits
    inner join departure_events
        on origin_visits.terminal_pattern_id = departure_events.terminal_pattern_id
        and origin_visits.vehicle_number = departure_events.vehicle_number
        and origin_visits.vehicle_type = departure_events.vehicle_type
        and origin_visits.terminal_visit_id = departure_events.terminal_visit_id
    inner join terminal_visit_episodes as destination_visits
        on origin_visits.terminal_pattern_id = destination_visits.terminal_pattern_id
        and origin_visits.vehicle_number = destination_visits.vehicle_number
        and origin_visits.vehicle_type = destination_visits.vehicle_type
        and destination_visits.is_destination_visit
        and destination_visits.visit_start_time >= departure_events.departure_event_time
    where origin_visits.is_origin_visit
    qualify row_number() over (
        partition by
            origin_visits.terminal_pattern_id,
            origin_visits.vehicle_number,
            origin_visits.vehicle_type,
            origin_visits.terminal_visit_id
        order by destination_visits.visit_start_time, destination_visits.terminal_visit_id
    ) = 1
),

all_traversal_candidates as (
    select
        course_terminals.service_date,
        course_terminals.processing_date,
        course_terminals.gtfs_snapshot_id,
        course_terminals.duty_chain_id,
        course_terminals.duty_chain_source_id,
        course_terminals.trip_order,
        course_terminals.trip_id,
        course_terminals.scheduled_start_time,
        course_terminals.scheduled_end_time,
        course_terminals.brigade,
        course_terminals.mode,
        course_terminals.line,
        pattern_traversal_candidates.vehicle_number,
        pattern_traversal_candidates.vehicle_type,
        pattern_traversal_candidates.origin_event_time,
        pattern_traversal_candidates.departure_event_time,
        pattern_traversal_candidates.destination_event_time
    from course_terminals
    inner join pattern_traversal_candidates
        on course_terminals.terminal_pattern_id = pattern_traversal_candidates.terminal_pattern_id
),

traversal_candidates as (
    -- A physical traversal may appear in both the overnight and current service-date schedules.
    select
        candidates.*,
        format(
            '%020d|%s|%s|%s',
            abs(timestamp_diff(candidates.origin_event_time, candidates.scheduled_start_time, second)),
            cast(candidates.origin_event_time as string),
            cast(candidates.destination_event_time as string),
            candidates.traversal_id
        ) as candidate_order_key
    from (
        select
            * except (service_day_candidate_rank),
        to_json_string(struct(
            vehicle_number,
            vehicle_type,
            origin_event_time,
            departure_event_time,
            destination_event_time
            )) as traversal_id
        from (
            select
                *,
                row_number() over (
                    partition by
                        vehicle_number,
                        brigade,
                        vehicle_type,
                        duty_chain_source_id,
                        trip_order,
                        origin_event_time,
                        destination_event_time
                    order by
                        abs(timestamp_diff(origin_event_time, scheduled_start_time, second)),
                        scheduled_start_time,
                        service_date,
                        duty_chain_id,
                        trip_id
                ) as service_day_candidate_rank
            from all_traversal_candidates
        )
        where service_day_candidate_rank = 1
    ) as candidates
),

traversal_candidate_groups as (
    select
        service_date,
        processing_date,
        gtfs_snapshot_id,
        duty_chain_id,
        trip_id,
        vehicle_number,
        vehicle_type,
        array_agg(struct(
            traversal_id,
            origin_event_time,
            departure_event_time,
            destination_event_time,
            candidate_order_key
        )) as candidates
    from traversal_candidates
    group by
        service_date,
        processing_date,
        gtfs_snapshot_id,
        duty_chain_id,
        trip_id,
        vehicle_number,
        vehicle_type
),

ordered_courses as (
    select
        course_terminals.*,
        row_number() over (
            partition by service_date, processing_date, gtfs_snapshot_id, duty_chain_id
            order by trip_order, trip_id
        ) as course_index
    from course_terminals
),

duty_vehicles as (
    select distinct
        course_terminals.service_date,
        course_terminals.processing_date,
        course_terminals.gtfs_snapshot_id,
        course_terminals.duty_chain_id,
        terminal_hits.vehicle_number,
        terminal_hits.vehicle_type
    from course_terminals
    inner join terminal_hits
        on course_terminals.terminal_pattern_id = terminal_hits.terminal_pattern_id
),

{% if duty_execution_unit_test %}
allocation_state_1 as (
{% else %}
allocation_state as (
    -- A null candidate deliberately skips a course while retaining the last
    -- confirmed endpoint for later courses.
{% endif %}
    select
        courses.service_date,
        courses.processing_date,
        courses.gtfs_snapshot_id,
        courses.duty_chain_id,
        courses.trip_id,
        courses.course_index,
        vehicles.vehicle_number,
        vehicles.vehicle_type,
        cast(null as timestamp) as progression_start_time,
        candidates.destination_event_time as previous_selected_destination_event_time,
        if(
            candidates.traversal_id is null,
            cast([] as array<string>),
            [candidates.traversal_id]
        ) as used_traversal_ids,
        candidates.traversal_id,
        candidates.origin_event_time,
        candidates.departure_event_time,
        candidates.destination_event_time
    from ordered_courses as courses
    inner join duty_vehicles as vehicles
        on courses.service_date = vehicles.service_date
        and courses.processing_date = vehicles.processing_date
        and courses.gtfs_snapshot_id = vehicles.gtfs_snapshot_id
        and courses.duty_chain_id = vehicles.duty_chain_id
    left join traversal_candidates as candidates
        on courses.service_date = candidates.service_date
        and courses.processing_date = candidates.processing_date
        and courses.gtfs_snapshot_id = candidates.gtfs_snapshot_id
        and courses.duty_chain_id = candidates.duty_chain_id
        and courses.trip_id = candidates.trip_id
        and vehicles.vehicle_number = candidates.vehicle_number
        and vehicles.vehicle_type = candidates.vehicle_type
    where courses.course_index = 1
    qualify row_number() over (
        partition by
            courses.service_date,
            courses.processing_date,
            courses.gtfs_snapshot_id,
            courses.duty_chain_id,
            vehicles.vehicle_number,
            vehicles.vehicle_type
        order by
            if(candidates.traversal_id is null, 1, 0),
            abs(timestamp_diff(candidates.origin_event_time, courses.scheduled_start_time, second)),
            candidates.origin_event_time,
            candidates.destination_event_time,
            candidates.traversal_id
    ) = 1
{% if duty_execution_unit_test %}
),
{% set allocation_steps = range(2, duty_execution_unit_test_output_course_count + 1) %}
{% else %}
    union all
{% set allocation_steps = [none] %}
{% endif %}
{% for course_index in allocation_steps %}
{% if duty_execution_unit_test %}
allocation_state_{{ course_index }} as (
{% endif %}
    select
        next_courses.service_date,
        next_courses.processing_date,
        next_courses.gtfs_snapshot_id,
        next_courses.duty_chain_id,
        next_courses.trip_id,
        next_courses.course_index,
        allocation_state.vehicle_number,
        allocation_state.vehicle_type,
        allocation_state.previous_selected_destination_event_time as progression_start_time,
        coalesce(
            candidates.destination_event_time,
            allocation_state.previous_selected_destination_event_time
        ) as previous_selected_destination_event_time,
        if(
            candidates.traversal_id is null,
            allocation_state.used_traversal_ids,
            array_concat(allocation_state.used_traversal_ids, [candidates.traversal_id])
        ) as used_traversal_ids,
        candidates.traversal_id,
        candidates.origin_event_time,
        candidates.departure_event_time,
        candidates.destination_event_time
    from {% if duty_execution_unit_test %}allocation_state_{{ course_index - 1 }}{% else %}allocation_state{% endif %} as allocation_state
    inner join ordered_courses as next_courses
        on allocation_state.service_date = next_courses.service_date
        and allocation_state.processing_date = next_courses.processing_date
        and allocation_state.gtfs_snapshot_id = next_courses.gtfs_snapshot_id
        and allocation_state.duty_chain_id = next_courses.duty_chain_id
        and next_courses.course_index = {% if duty_execution_unit_test %}{{ course_index }}{% else %}allocation_state.course_index + 1{% endif %}
    left join traversal_candidate_groups as candidate_group
        on next_courses.service_date = candidate_group.service_date
        and next_courses.processing_date = candidate_group.processing_date
        and next_courses.gtfs_snapshot_id = candidate_group.gtfs_snapshot_id
        and next_courses.duty_chain_id = candidate_group.duty_chain_id
        and next_courses.trip_id = candidate_group.trip_id
        and allocation_state.vehicle_number = candidate_group.vehicle_number
        and allocation_state.vehicle_type = candidate_group.vehicle_type
    left join unnest(candidate_group.candidates) as candidates
        on (
            allocation_state.previous_selected_destination_event_time is null
            or candidates.origin_event_time >= allocation_state.previous_selected_destination_event_time
        )
        and candidates.traversal_id not in unnest(allocation_state.used_traversal_ids)
    left join unnest(candidate_group.candidates) as better_candidates
        on (
            allocation_state.previous_selected_destination_event_time is null
            or better_candidates.origin_event_time >= allocation_state.previous_selected_destination_event_time
        )
        and better_candidates.traversal_id not in unnest(allocation_state.used_traversal_ids)
        and better_candidates.candidate_order_key < candidates.candidate_order_key
    where better_candidates.traversal_id is null
{% if duty_execution_unit_test %}
),
{% else %}
),
{% endif %}
{% endfor %}
{% if duty_execution_unit_test %}
allocation_state as (
    select * from allocation_state_1
    {% for course_index in range(2, duty_execution_unit_test_output_course_count + 1) %}
    union all
    select * from allocation_state_{{ course_index }}
    {% endfor %}
),
{% endif %}

settled_sequence as (
    select
        * except (previous_selected_destination_event_time, used_traversal_ids, traversal_id)
    from allocation_state
),

settled_execution_intervals as (
    select
        settled_sequence.*,
        greatest(
            origin_event_time,
            coalesce(timestamp_add(progression_start_time, interval 1 microsecond), origin_event_time)
        ) as ownership_interval_start_time,
        destination_event_time as ownership_interval_end_time
    from settled_sequence
    where destination_event_time is not null
),

progressed_candidates as (
    select
        settled_execution_intervals.*,
        count(gps.gps_time) as source_ping_count,
        min(gps.gps_time) as source_ping_start_time,
        max(gps.gps_time) as source_ping_end_time
    from settled_execution_intervals
    inner join course_terminals
        on settled_execution_intervals.gtfs_snapshot_id = course_terminals.gtfs_snapshot_id
        and settled_execution_intervals.service_date = course_terminals.service_date
        and settled_execution_intervals.processing_date = course_terminals.processing_date
        and settled_execution_intervals.duty_chain_id = course_terminals.duty_chain_id
        and settled_execution_intervals.trip_id = course_terminals.trip_id
    left join gps_pings as gps
        on settled_execution_intervals.vehicle_number = gps.vehicle_number
        and settled_execution_intervals.vehicle_type = gps.vehicle_type
        and course_terminals.brigade = gps.brigade
        and course_terminals.line = gps.line
        and gps.gps_time between settled_execution_intervals.ownership_interval_start_time and settled_execution_intervals.ownership_interval_end_time
    group by
        settled_execution_intervals.service_date,
        settled_execution_intervals.processing_date,
        settled_execution_intervals.gtfs_snapshot_id,
        settled_execution_intervals.duty_chain_id,
        settled_execution_intervals.trip_id,
        settled_execution_intervals.course_index,
        settled_execution_intervals.vehicle_number,
        settled_execution_intervals.vehicle_type,
        settled_execution_intervals.progression_start_time,
        settled_execution_intervals.origin_event_time,
        settled_execution_intervals.departure_event_time,
        settled_execution_intervals.destination_event_time,
        settled_execution_intervals.ownership_interval_start_time,
        settled_execution_intervals.ownership_interval_end_time
),

course_observations as (
    select
        service_date,
        processing_date,
        gtfs_snapshot_id,
        duty_chain_id,
        trip_id,
        count(*) as settled_vehicle_count,
        array_agg(vehicle_number order by vehicle_number limit 1)[safe_offset(0)] as observed_vehicle_number,
        min(source_ping_start_time) as observed_start_time,
        max(source_ping_end_time) as observed_end_time,
        sum(source_ping_count) as source_ping_count,
        min(ownership_interval_start_time) as ownership_interval_start_time,
        max(ownership_interval_end_time) as ownership_interval_end_time
    from progressed_candidates
    group by service_date, processing_date, gtfs_snapshot_id, duty_chain_id, trip_id
),

short_turn_evidence as (
    select
        current_course.service_date,
        current_course.processing_date,
        current_course.gtfs_snapshot_id,
        current_course.duty_chain_id,
        current_course.trip_id,
        count(distinct current_course.vehicle_number) as short_turn_vehicle_count,
        array_agg(distinct current_course.vehicle_number order by current_course.vehicle_number limit 1)[safe_offset(0)]
            as short_turn_vehicle_number
    from settled_sequence as current_course
    inner join settled_sequence as next_course
        on current_course.service_date = next_course.service_date
        and current_course.processing_date = next_course.processing_date
        and current_course.gtfs_snapshot_id = next_course.gtfs_snapshot_id
        and current_course.duty_chain_id = next_course.duty_chain_id
        and current_course.vehicle_number = next_course.vehicle_number
        and current_course.vehicle_type = next_course.vehicle_type
        and current_course.course_index + 1 = next_course.course_index
    where current_course.destination_event_time is null
      and next_course.destination_event_time is not null
        and exists (
            select 1
            from departure_events as departure
            inner join course_terminals as departure_course
                on departure.terminal_pattern_id = departure_course.terminal_pattern_id
            where departure_course.service_date = current_course.service_date
              and departure_course.processing_date = current_course.processing_date
              and departure_course.gtfs_snapshot_id = current_course.gtfs_snapshot_id
              and departure_course.duty_chain_id = current_course.duty_chain_id
              and departure_course.trip_id = current_course.trip_id
              and departure.vehicle_number = current_course.vehicle_number
            and departure.vehicle_type = current_course.vehicle_type
            and (
                current_course.progression_start_time is null
                or departure.departure_event_time >= current_course.progression_start_time
            )
            and departure.departure_event_time < next_course.origin_event_time
      )
      and not exists (
          select 1
          from all_traversal_candidates as traversal
          where traversal.service_date = current_course.service_date
            and traversal.processing_date = current_course.processing_date
            and traversal.gtfs_snapshot_id = current_course.gtfs_snapshot_id
            and traversal.duty_chain_id = current_course.duty_chain_id
            and traversal.trip_id = current_course.trip_id
            and traversal.vehicle_number = current_course.vehicle_number
            and traversal.vehicle_type = current_course.vehicle_type
            and traversal.destination_event_time < next_course.origin_event_time
      )
    group by
        current_course.service_date,
        current_course.processing_date,
        current_course.gtfs_snapshot_id,
        current_course.duty_chain_id,
        current_course.trip_id
),

course_context as (
    select
        course_terminals.*,
        course_terminals.origin_stop_lat is not null
            and course_terminals.destination_stop_lat is not null as has_terminal_coordinates,
        coalesce(course_observations.settled_vehicle_count, 0) as settled_vehicle_count,
        course_observations.observed_vehicle_number,
        course_observations.observed_start_time,
        course_observations.observed_end_time,
        coalesce(course_observations.source_ping_count, 0) as source_ping_count,
        course_observations.ownership_interval_start_time,
        course_observations.ownership_interval_end_time,
        coalesce(short_turn_evidence.short_turn_vehicle_count, 0) as short_turn_vehicle_count,
        short_turn_evidence.short_turn_vehicle_number,
        coalesce(line_observation_summary.line_ping_count, 0) > 0 as has_line_observation,
        coalesce(previous_course.settled_vehicle_count, 0) = 1
            and coalesce(next_course.settled_vehicle_count, 0) = 1
            and previous_course.observed_vehicle_number = next_course.observed_vehicle_number
            as has_adjacent_same_vehicle_execution,
        coalesce(previous_course.settled_vehicle_count, 0) = 1
            and coalesce(next_course.settled_vehicle_count, 0) = 1
            and previous_course.observed_vehicle_number != next_course.observed_vehicle_number
            as has_adjacent_different_vehicle_execution
    from course_terminals
    left join course_observations
        on course_terminals.gtfs_snapshot_id = course_observations.gtfs_snapshot_id
        and course_terminals.service_date = course_observations.service_date
        and course_terminals.processing_date = course_observations.processing_date
        and course_terminals.duty_chain_id = course_observations.duty_chain_id
        and course_terminals.trip_id = course_observations.trip_id
    left join line_observation_summary
        on course_terminals.gtfs_snapshot_id = line_observation_summary.gtfs_snapshot_id
        and course_terminals.service_date = line_observation_summary.service_date
        and course_terminals.processing_date = line_observation_summary.processing_date
        and course_terminals.duty_chain_id = line_observation_summary.duty_chain_id
        and course_terminals.trip_id = line_observation_summary.trip_id
    left join short_turn_evidence
        on course_terminals.gtfs_snapshot_id = short_turn_evidence.gtfs_snapshot_id
        and course_terminals.service_date = short_turn_evidence.service_date
        and course_terminals.processing_date = short_turn_evidence.processing_date
        and course_terminals.duty_chain_id = short_turn_evidence.duty_chain_id
        and course_terminals.trip_id = short_turn_evidence.trip_id
    left join course_observations as previous_course
        on course_terminals.gtfs_snapshot_id = previous_course.gtfs_snapshot_id
        and course_terminals.service_date = previous_course.service_date
        and course_terminals.processing_date = previous_course.processing_date
        and course_terminals.duty_chain_id = previous_course.duty_chain_id
        and course_terminals.previous_trip_id = previous_course.trip_id
    left join course_observations as next_course
        on course_terminals.gtfs_snapshot_id = next_course.gtfs_snapshot_id
        and course_terminals.service_date = next_course.service_date
        and course_terminals.processing_date = next_course.processing_date
        and course_terminals.duty_chain_id = next_course.duty_chain_id
        and course_terminals.next_trip_id = next_course.trip_id
),

classified as (
    select
        *,
        case
            when settled_vehicle_count > 1 then 'vehicle_swap'
            when settled_vehicle_count = 1 and not are_passenger_boundaries_settled then 'uncertain'
            when settled_vehicle_count = 1 then 'executed'
            when short_turn_vehicle_count = 1 and are_passenger_boundaries_settled then 'short_turned'
            when has_adjacent_different_vehicle_execution then 'vehicle_swap'
            when has_adjacent_same_vehicle_execution then 'skipped'
            when has_line_observation then 'uncertain'
            else 'missed'
        end as execution_status
    from course_context
)

select
    service_date,
    processing_date,
    gtfs_snapshot_id,
    duty_chain_id,
    duty_chain_source,
    duty_chain_source_id,
    trip_order,
    line,
    route_short_name,
    mode,
    brigade,
    trip_id,
    service_id,
    direction_id,
    shape_id,
    trip_start_seconds,
    trip_end_seconds,
    scheduled_start_time,
    scheduled_end_time,
    previous_trip_id,
    next_trip_id,
    layover_from_previous_seconds,
    layover_to_next_seconds,
    line_changed_from_previous,
    line_changes_to_next,
    has_terminal_coordinates as has_stop_semantics,
    are_passenger_boundaries_settled,
    first_passenger_stop_sequence,
    last_passenger_stop_sequence,
    if(
        settled_vehicle_count = 1,
        observed_vehicle_number,
        if(execution_status = 'short_turned', short_turn_vehicle_number, null)
    ) as vehicle_number,
    observed_start_time,
    observed_end_time,
    if(execution_status in ('executed', 'vehicle_swap', 'uncertain'), observed_start_time, null) as source_ping_start_time,
    if(execution_status in ('executed', 'vehicle_swap', 'uncertain'), observed_end_time, null) as source_ping_end_time,
    source_ping_count,
    execution_status,
    case
        when execution_status in ('vehicle_swap', 'uncertain') then 'low'
        when execution_status = 'skipped' then 'high'
        when execution_status = 'missed' then 'medium'
        when duty_chain_source = 'line_brigade' then 'low'
        else 'high'
    end as confidence,
    case
        when execution_status = 'vehicle_swap' then 'multiple_vehicles_terminal_progression'
        when execution_status = 'skipped' then 'adjacent_courses_terminal_progression'
        when execution_status = 'missed' then 'no_line_observation'
        when execution_status = 'short_turned' then 'next_course_origin_before_destination'
        when execution_status = 'uncertain' and not are_passenger_boundaries_settled then 'passenger_boundaries_unknown'
        when execution_status = 'uncertain' then 'terminal_progression_incomplete'
        else 'terminal_progression'
    end as execution_reason,
    array_concat(
        if(settled_vehicle_count > 0, ['origin_departure_destination_progression'], []),
        if(settled_vehicle_count > 1 or has_adjacent_different_vehicle_execution, ['multiple_vehicles'], []),
        if(has_adjacent_same_vehicle_execution, ['adjacent_course_executions'], []),
        if(short_turn_vehicle_count > 0, ['next_course_origin_before_destination'], []),
        if(has_line_observation and settled_vehicle_count = 0, ['line_observation_without_terminal_progression'], []),
        if(not are_passenger_boundaries_settled, ['passenger_boundaries_unknown'], []),
        if(duty_chain_source = 'line_brigade', ['line_brigade_fallback'], [])
    ) as execution_evidence,
    if(execution_status = 'executed', ownership_interval_start_time, null) as ownership_interval_start_time,
    if(execution_status = 'executed', ownership_interval_end_time, null) as ownership_interval_end_time
from classified
