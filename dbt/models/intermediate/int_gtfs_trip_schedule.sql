with selected_snapshot as (
    select snapshot_id as gtfs_snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_id = '{{ var("gtfs_snapshot_id") }}'
    limit 1
),

date_bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date
    from {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
    inner join selected_snapshot
        on calendar_dates.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
),

processing_date_spine as (
    select processing_date
    from date_bounds,
        unnest(generate_date_array(min_service_date, max_service_date)) as processing_date
),

processing_service_dates as (
    select
        processing_date_spine.processing_date,
        service_date
    from processing_date_spine
    cross join unnest([
        date_sub(processing_date_spine.processing_date, interval 1 day),
        processing_date_spine.processing_date
    ]) as service_date
    where service_date between (select min_service_date from date_bounds)
        and (select max_service_date from date_bounds)
),

calendar_dates as (
    select
        calendar_dates.service_id,
        processing_service_dates.processing_date,
        calendar_dates.service_date,
        calendar_dates.gtfs_snapshot_id,
        safe_cast(regexp_extract(calendar_dates.service_id, r'^(\d{4}-\d{2}-\d{2}):') as date) as schedule_pattern_date,
        regexp_extract(calendar_dates.service_id, r'(?:^|:)(Pc|Pt|Sb|Nd)[A-Za-z]*$') as schedule_token
    from processing_service_dates
    inner join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on processing_service_dates.service_date = calendar_dates.service_date
    inner join selected_snapshot
        on calendar_dates.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
),

calendar_schedule_classes as (
    select
        service_id,
        processing_date,
        service_date,
        gtfs_snapshot_id,
        case
            when schedule_token = 'Nd' then 'sunday_holiday'
            when schedule_token = 'Sb' then 'saturday'
            when schedule_token = 'Pt' then 'friday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 2 then 'monday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 3 then 'tuesday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 4 then 'wednesday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 5 then 'thursday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 6 then 'friday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 7 then 'saturday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 1 then 'sunday_holiday'
            when schedule_token = 'Pc' then 'weekday'
            else 'unknown'
        end as schedule_class
    from calendar_dates
),

active_trip_services as (
    select
        calendar_schedule_classes.service_date,
        calendar_schedule_classes.processing_date,
        calendar_schedule_classes.gtfs_snapshot_id,
        trips.line,
        trips.direction_id,
        trips.service_id,
        calendar_schedule_classes.schedule_class
    from {{ ref('stg_gtfs__trips') }} as trips
    inner join calendar_schedule_classes
        on trips.service_id = calendar_schedule_classes.service_id
        and trips.gtfs_snapshot_id = calendar_schedule_classes.gtfs_snapshot_id
),

line_direction_schedule_classes as (
    select
        service_date,
        processing_date,
        gtfs_snapshot_id,
        line,
        direction_id,
        string_agg(distinct schedule_class, ', ' order by schedule_class) as schedule_day_types,
        string_agg(distinct service_id, ', ' order by service_id) as schedule_service_ids,
        count(distinct if(schedule_class != 'weekday', schedule_class, null)) as specific_schedule_class_count,
        max(if(schedule_class != 'weekday', schedule_class, null)) as specific_schedule_class,
        count(distinct schedule_class) as schedule_class_count,
        max(schedule_class) as any_schedule_class
    from active_trip_services
    group by service_date, processing_date, gtfs_snapshot_id, line, direction_id
),

active_trips as (
    select
        calendar_dates.service_date,
        calendar_dates.processing_date,
        case
            when line_direction_schedule_classes.specific_schedule_class_count = 1
                then line_direction_schedule_classes.specific_schedule_class
            when line_direction_schedule_classes.specific_schedule_class_count > 1 then 'mixed'
            when line_direction_schedule_classes.schedule_class_count = 1
                then line_direction_schedule_classes.any_schedule_class
            when line_direction_schedule_classes.schedule_class_count > 1 then 'mixed'
            else 'unknown'
        end as schedule_day_type,
        line_direction_schedule_classes.schedule_service_ids,
        trips.gtfs_snapshot_id,
        trips.line,
        trips.direction_id,
        trips.trip_id,
        trips.service_id,
        trips.trip_headsign,
        trips.shape_id
    from {{ ref('stg_gtfs__trips') }} as trips
    inner join calendar_dates
        on trips.service_id = calendar_dates.service_id
        and trips.gtfs_snapshot_id = calendar_dates.gtfs_snapshot_id
    inner join line_direction_schedule_classes
        on calendar_dates.service_date = line_direction_schedule_classes.service_date
        and calendar_dates.processing_date = line_direction_schedule_classes.processing_date
        and calendar_dates.gtfs_snapshot_id = line_direction_schedule_classes.gtfs_snapshot_id
        and trips.line = line_direction_schedule_classes.line
        and trips.direction_id = line_direction_schedule_classes.direction_id
),

trip_stop_times as (
    select
        active_trips.service_date,
        active_trips.processing_date,
        active_trips.schedule_day_type,
        active_trips.schedule_service_ids,
        active_trips.gtfs_snapshot_id,
        active_trips.line,
        active_trips.direction_id,
        active_trips.trip_id,
        active_trips.service_id,
        active_trips.trip_headsign,
        active_trips.shape_id,
        stop_times.stop_sequence,
        stop_times.stop_id,
        stop_times.arrival_time_seconds,
        stop_times.departure_time_seconds
    from active_trips
    inner join {{ ref('stg_gtfs__stop_times') }} as stop_times
        on active_trips.trip_id = stop_times.trip_id
        and active_trips.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
),

trip_schedules as (
    select
        service_date,
        processing_date,
        schedule_day_type,
        schedule_service_ids,
        gtfs_snapshot_id,
        line,
        direction_id,
        trip_id,
        service_id,
        trip_headsign,
        shape_id,
        min(least(
            coalesce(arrival_time_seconds, departure_time_seconds),
            coalesce(departure_time_seconds, arrival_time_seconds)
        )) as trip_start_seconds,
        max(greatest(
            coalesce(arrival_time_seconds, departure_time_seconds),
            coalesce(departure_time_seconds, arrival_time_seconds)
        )) as trip_end_seconds,
        count(*) as stop_count,
        string_agg(stop_id, ' | ' order by stop_sequence) as ordered_stop_ids,
        string_agg(cast(arrival_time_seconds as string), ' | ' order by stop_sequence) as ordered_arrival_time_seconds,
        string_agg(
            format('%06d:%s:%08d', stop_sequence, stop_id, arrival_time_seconds),
            ' | '
            order by stop_sequence
        ) as trip_timetable_signature
    from trip_stop_times
    group by
        service_date,
        processing_date,
        schedule_day_type,
        schedule_service_ids,
        gtfs_snapshot_id,
        line,
        direction_id,
        trip_id,
        service_id,
        trip_headsign,
        shape_id
)

select
    service_date,
    processing_date,
    schedule_day_type,
    schedule_service_ids,
    gtfs_snapshot_id,
    line,
    direction_id,
    trip_id,
    service_id,
    trip_headsign,
    shape_id,
    trip_start_seconds,
    trip_end_seconds,
    stop_count,
    ordered_stop_ids,
    ordered_arrival_time_seconds,
    trip_timetable_signature
from trip_schedules
where timestamp_add(timestamp(service_date, 'Europe/Warsaw'), interval trip_end_seconds second)
    >= timestamp(processing_date, 'Europe/Warsaw')
  and timestamp_add(timestamp(service_date, 'Europe/Warsaw'), interval trip_start_seconds second)
    < timestamp(date_add(processing_date, interval 1 day), 'Europe/Warsaw')
