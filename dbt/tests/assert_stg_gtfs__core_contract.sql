with grouped_stop_times as (
    select
        gtfs_snapshot_id,
        trip_id,
        stop_sequence,
        count(*) as row_count,
        countif(
            trip_id is null
            or stop_id is null
            or stop_sequence is null
            or arrival_time_seconds is null
            or departure_time_seconds is null
            or pickup_type is null
            or drop_off_type is null
            or stop_service_class is null
            or gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(pickup_type not in (0, 1, 2, 3)) as pickup_type_violations,
        countif(drop_off_type not in (0, 1, 2, 3)) as drop_off_type_violations,
        countif(stop_service_class not in ('regular', 'request', 'not_in_passenger_service')) as stop_service_class_violations
    from {{ ref('stg_gtfs__stop_times') }}
    where gtfs_snapshot_id = '{{ var("gtfs_snapshot_id") }}'
    group by gtfs_snapshot_id, trip_id, stop_sequence
),

stop_times_contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(pickup_type_violations) as pickup_type_violations,
        sum(drop_off_type_violations) as drop_off_type_violations,
        sum(stop_service_class_violations) as stop_service_class_violations,
        countif(row_count > 1) as duplicate_trip_stop_sequence_violations
    from grouped_stop_times
),

grouped_trips as (
    select
        gtfs_snapshot_id,
        trip_id,
        count(*) as row_count,
        countif(
            trip_id is null
            or line is null
            or service_id is null
            or direction_id is null
            or brigade is null
            or shape_id is null
            or gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(direction_id not in (0, 1)) as direction_id_violations
    from {{ ref('stg_gtfs__trips') }}
    group by gtfs_snapshot_id, trip_id
),

trips_contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(direction_id_violations) as direction_id_violations,
        countif(row_count > 1) as duplicate_trip_id_violations
    from grouped_trips
),

grouped_stops as (
    select
        gtfs_snapshot_id,
        stop_id,
        count(*) as row_count,
        countif(
            stop_id is null
            or stop_name is null
            or stop_lat is null
            or stop_lon is null
            or gtfs_snapshot_id is null
        ) as required_field_violations
    from {{ ref('stg_gtfs__stops') }}
    group by gtfs_snapshot_id, stop_id
),

stops_contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        countif(row_count > 1) as duplicate_stop_id_violations
    from grouped_stops
),

grouped_routes as (
    select
        gtfs_snapshot_id,
        route_id,
        count(*) as row_count,
        countif(
            route_id is null
            or route_short_name is null
            or route_type is null
            or mode is null
            or gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(route_type not in (0, 1, 2, 3)) as route_type_violations,
        countif(mode not in ('tram', 'metro', 'rail', 'bus')) as mode_violations
    from {{ ref('stg_gtfs__routes') }}
    group by gtfs_snapshot_id, route_id
),

routes_contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(route_type_violations) as route_type_violations,
        sum(mode_violations) as mode_violations,
        countif(row_count > 1) as duplicate_route_id_violations
    from grouped_routes
),

grouped_calendar_dates as (
    select
        gtfs_snapshot_id,
        service_id,
        service_date,
        count(*) as row_count,
        countif(
            service_id is null
            or service_date is null
            or exception_type is null
            or day_type is null
            or gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(exception_type != 1) as exception_type_violations,
        countif(day_type not in ('weekday', 'weekend')) as day_type_violations
    from {{ ref('stg_gtfs__calendar_dates') }}
    group by gtfs_snapshot_id, service_id, service_date
),

calendar_dates_contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(exception_type_violations) as exception_type_violations,
        sum(day_type_violations) as day_type_violations,
        countif(row_count > 1) as duplicate_service_date_violations
    from grouped_calendar_dates
)

select 'stg_gtfs__stop_times' as table_name, issue_type, violation_count
from stop_times_contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('pickup_type_accepted_values' as issue_type, pickup_type_violations as violation_count),
    struct('drop_off_type_accepted_values' as issue_type, drop_off_type_violations as violation_count),
    struct('stop_service_class_accepted_values' as issue_type, stop_service_class_violations as violation_count),
    struct('unique_trip_stop_sequence' as issue_type, duplicate_trip_stop_sequence_violations as violation_count)
])
where violation_count > 0

union all

select 'stg_gtfs__trips' as table_name, issue_type, violation_count
from trips_contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('direction_id_accepted_values' as issue_type, direction_id_violations as violation_count),
    struct('unique_trip_id' as issue_type, duplicate_trip_id_violations as violation_count)
])
where violation_count > 0

union all

select 'stg_gtfs__stops' as table_name, issue_type, violation_count
from stops_contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('unique_stop_id' as issue_type, duplicate_stop_id_violations as violation_count)
])
where violation_count > 0

union all

select 'stg_gtfs__routes' as table_name, issue_type, violation_count
from routes_contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('route_type_accepted_values' as issue_type, route_type_violations as violation_count),
    struct('mode_accepted_values' as issue_type, mode_violations as violation_count),
    struct('unique_route_id' as issue_type, duplicate_route_id_violations as violation_count)
])
where violation_count > 0

union all

select 'stg_gtfs__calendar_dates' as table_name, issue_type, violation_count
from calendar_dates_contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('exception_type_accepted_values' as issue_type, exception_type_violations as violation_count),
    struct('day_type_accepted_values' as issue_type, day_type_violations as violation_count),
    struct('unique_service_date' as issue_type, duplicate_service_date_violations as violation_count)
])
where violation_count > 0
