{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}

with scheduled_trips as (
    select
        schedule.service_date,
        schedule.processing_date,
        schedule.schedule_day_type,
        schedule.schedule_service_ids,
        schedule.gtfs_snapshot_id,
        schedule.line,
        routes.route_short_name,
        routes.mode,
        schedule.direction_id,
        schedule.trip_id,
        schedule.service_id,
        schedule.trip_headsign,
        schedule.shape_id,
        trips.block_id,
        trips.block_short_name,
        trips.brigade,
        coalesce(
            trips.block_id,
            concat(schedule.line, ':', trips.brigade)
        ) as duty_chain_source_id,
        case
            when trips.block_id is not null then 'block_id'
            else 'line_brigade'
        end as duty_chain_source,
        schedule.trip_start_seconds,
        schedule.trip_end_seconds,
        schedule.stop_count,
        schedule.passenger_stop_count,
        schedule.is_public_service_segment
    from {{ ref('int_gtfs_trip_schedule') }} as schedule
    inner join {{ ref('stg_gtfs__trips') }} as trips
        on schedule.gtfs_snapshot_id = trips.gtfs_snapshot_id
        and schedule.trip_id = trips.trip_id
        and trips.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    left join {{ ref('stg_gtfs__routes') }} as routes
        on schedule.gtfs_snapshot_id = routes.gtfs_snapshot_id
        and schedule.line = routes.route_id
    where schedule.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
),

service_day_trips as (
    select distinct
        service_date,
        schedule_day_type,
        schedule_service_ids,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_id,
        service_id,
        trip_headsign,
        shape_id,
        block_id,
        block_short_name,
        brigade,
        duty_chain_source_id,
        duty_chain_source,
        trip_start_seconds,
        trip_end_seconds,
        stop_count,
        passenger_stop_count,
        is_public_service_segment
    from scheduled_trips
),

trip_terminal_stops as (
    select
        service_day_trips.gtfs_snapshot_id,
        service_day_trips.trip_id,
        array_agg(stop_times.stop_id order by stop_times.stop_sequence limit 1)[offset(0)] as origin_stop_id,
        array_agg(stop_times.stop_id order by stop_times.stop_sequence desc limit 1)[offset(0)] as destination_stop_id,
        min(stop_times.stop_sequence) as first_stop_sequence,
        max(stop_times.stop_sequence) as last_stop_sequence
    from service_day_trips
    inner join {{ ref('stg_gtfs__stop_times') }} as stop_times
        on service_day_trips.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
        and service_day_trips.trip_id = stop_times.trip_id
        and stop_times.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    group by service_day_trips.gtfs_snapshot_id, service_day_trips.trip_id
),

trip_bounds as (
    select
        service_day_trips.*,
        trip_terminal_stops.origin_stop_id,
        origin_stop.stop_name as origin_stop_name,
        trip_terminal_stops.destination_stop_id,
        destination_stop.stop_name as destination_stop_name,
        trip_terminal_stops.first_stop_sequence,
        trip_terminal_stops.last_stop_sequence,
        {{ warsaw_scheduled_timestamp('service_day_trips.service_date', 'service_day_trips.trip_start_seconds') }}
            as scheduled_start_time,
        {{ warsaw_scheduled_timestamp('service_day_trips.service_date', 'service_day_trips.trip_end_seconds') }}
            as scheduled_end_time
    from service_day_trips
    left join trip_terminal_stops
        on service_day_trips.gtfs_snapshot_id = trip_terminal_stops.gtfs_snapshot_id
        and service_day_trips.trip_id = trip_terminal_stops.trip_id
    left join {{ ref('stg_gtfs__stops') }} as origin_stop
        on service_day_trips.gtfs_snapshot_id = origin_stop.gtfs_snapshot_id
        and trip_terminal_stops.origin_stop_id = origin_stop.stop_id
        and origin_stop.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    left join {{ ref('stg_gtfs__stops') }} as destination_stop
        on service_day_trips.gtfs_snapshot_id = destination_stop.gtfs_snapshot_id
        and trip_terminal_stops.destination_stop_id = destination_stop.stop_id
        and destination_stop.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
),

classified_trip_bounds as (
    select
        *,
        regexp_contains(coalesce(origin_stop_name, ''), r'(?i)(^R-[0-9]+\s+Zajezdnia|^Zajezdnia|\sZajezdnia)')
            and passenger_stop_count = 0
            as is_depot_pull_out,
        regexp_contains(coalesce(destination_stop_name, ''), r'(?i)(^R-[0-9]+\s+Zajezdnia|^Zajezdnia|\sZajezdnia)')
            and passenger_stop_count = 0
            as is_depot_pull_in
    from trip_bounds
),

ordered as (
    select
        *,
        to_hex(md5(to_json_string(struct(
            gtfs_snapshot_id as gtfs_snapshot_id,
            service_date as service_date,
            duty_chain_source as duty_chain_source,
            duty_chain_source_id as duty_chain_source_id
        )))) as duty_chain_id,
        row_number() over duty_chain_window as trip_order,
        lag(trip_id) over duty_chain_window as previous_trip_id,
        lead(trip_id) over duty_chain_window as next_trip_id,
        lag(line) over duty_chain_window as previous_line,
        lead(line) over duty_chain_window as next_line,
        lag(trip_end_seconds) over duty_chain_window as previous_trip_end_seconds,
        lead(trip_start_seconds) over duty_chain_window as next_trip_start_seconds
    from classified_trip_bounds
    window duty_chain_window as (
        partition by gtfs_snapshot_id, service_date, duty_chain_source, duty_chain_source_id
        order by trip_start_seconds, trip_end_seconds, trip_id
    )
)

select
    ordered.service_date,
    scheduled_trips.processing_date,
    ordered.schedule_day_type,
    ordered.schedule_service_ids,
    ordered.gtfs_snapshot_id,
    ordered.duty_chain_id,
    ordered.duty_chain_source,
    ordered.duty_chain_source_id,
    ordered.trip_order,
    ordered.line,
    ordered.route_short_name,
    ordered.mode,
    ordered.block_id,
    ordered.block_short_name,
    ordered.brigade,
    ordered.trip_id,
    ordered.service_id,
    ordered.direction_id,
    ordered.shape_id,
    ordered.trip_headsign,
    ordered.trip_start_seconds,
    ordered.trip_end_seconds,
    ordered.scheduled_start_time,
    ordered.scheduled_end_time,
    ordered.stop_count,
    ordered.passenger_stop_count,
    ordered.origin_stop_id,
    ordered.origin_stop_name,
    ordered.destination_stop_id,
    ordered.destination_stop_name,
    ordered.is_depot_pull_out,
    ordered.is_depot_pull_in,
    not ordered.is_public_service_segment as is_depot_segment,
    ordered.is_public_service_segment,
    ordered.first_stop_sequence,
    ordered.last_stop_sequence,
    cast(null as float64) as scheduled_distance_meters,
    ordered.previous_trip_id,
    ordered.next_trip_id,
    case
        when ordered.previous_trip_id is null then null
        else ordered.trip_start_seconds - ordered.previous_trip_end_seconds
    end as layover_from_previous_seconds,
    case
        when ordered.next_trip_id is null then null
        else ordered.next_trip_start_seconds - ordered.trip_end_seconds
    end as layover_to_next_seconds,
    ordered.previous_trip_id is not null and ordered.line != ordered.previous_line as line_changed_from_previous,
    ordered.next_trip_id is not null and ordered.line != ordered.next_line as line_changes_to_next,
    ordered.previous_trip_id is not null
        and ordered.trip_start_seconds < ordered.previous_trip_end_seconds as overlaps_previous_trip,
    ordered.trip_end_seconds < ordered.trip_start_seconds as has_negative_duration,
    ordered.stop_count < 2 or ordered.origin_stop_id is null or ordered.destination_stop_id is null as has_missing_stops,
    ordered.previous_trip_id is not null and ordered.trip_start_seconds < ordered.previous_trip_end_seconds
        or ordered.trip_end_seconds < ordered.trip_start_seconds
        or ordered.stop_count < 2
        or ordered.origin_stop_id is null
        or ordered.destination_stop_id is null as is_malformed_duty_segment
from scheduled_trips
inner join ordered
    on scheduled_trips.gtfs_snapshot_id = ordered.gtfs_snapshot_id
    and scheduled_trips.service_date = ordered.service_date
    and scheduled_trips.trip_id = ordered.trip_id
