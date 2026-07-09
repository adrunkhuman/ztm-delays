{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "processing_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["gtfs_snapshot_id", "line", "trip_id"],
        require_partition_filter=true,
    )
}}

with scheduled_trips as (
    select
        service_date,
        processing_date,
        schedule_day_type,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_id,
        trip_headsign,
        is_public_service_segment,
        origin_stop_id,
        destination_stop_id,
        stop_count
    from {{ ref('int_gtfs_duty_chain') }}
    where mode in ('bus', 'tram')
      and processing_date = date('{{ var("processing_date") }}')
),

trip_stops as (
    select
        scheduled_trips.service_date,
        scheduled_trips.processing_date,
        scheduled_trips.schedule_day_type,
        scheduled_trips.gtfs_snapshot_id,
        scheduled_trips.line,
        scheduled_trips.route_short_name,
        scheduled_trips.mode,
        scheduled_trips.direction_id,
        scheduled_trips.trip_id,
        scheduled_trips.trip_headsign,
        scheduled_trips.is_public_service_segment,
        scheduled_trips.origin_stop_id,
        scheduled_trips.destination_stop_id,
        scheduled_trips.stop_count,
        string_agg(stop_times.stop_id, '|' order by stop_times.stop_sequence) as ordered_stop_ids,
        countif(coalesce(stops.effective_zone_id, '') != '1') as non_zone1_stop_count,
        array_agg(distinct coalesce(stops.effective_zone_id, '') order by coalesce(stops.effective_zone_id, '')) as zone_ids
    from scheduled_trips
    inner join {{ ref('stg_gtfs__stop_times') }} as stop_times
        on scheduled_trips.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
        and scheduled_trips.trip_id = stop_times.trip_id
    left join {{ ref('stg_gtfs__stops') }} as stops
        on scheduled_trips.gtfs_snapshot_id = stops.gtfs_snapshot_id
        and stop_times.stop_id = stops.stop_id
    group by
        scheduled_trips.service_date,
        scheduled_trips.processing_date,
        scheduled_trips.schedule_day_type,
        scheduled_trips.gtfs_snapshot_id,
        scheduled_trips.line,
        scheduled_trips.route_short_name,
        scheduled_trips.mode,
        scheduled_trips.direction_id,
        scheduled_trips.trip_id,
        scheduled_trips.trip_headsign,
        scheduled_trips.is_public_service_segment,
        scheduled_trips.origin_stop_id,
        scheduled_trips.destination_stop_id,
        scheduled_trips.stop_count
),

terminal_counts as (
    select
        gtfs_snapshot_id,
        line,
        direction_id,
        schedule_day_type,
        origin_stop_id,
        destination_stop_id,
        count(*) as terminal_pair_trip_count
    from trip_stops
    where is_public_service_segment
    group by gtfs_snapshot_id, line, direction_id, schedule_day_type, origin_stop_id, destination_stop_id
),

terminal_ranks as (
    select
        *,
        row_number() over (
            partition by gtfs_snapshot_id, line, direction_id, schedule_day_type
            order by terminal_pair_trip_count desc, origin_stop_id, destination_stop_id
        ) as terminal_pair_rank
    from terminal_counts
),

classified as (
    select
        trip_stops.*,
        coalesce(terminal_ranks.terminal_pair_trip_count, 0) as terminal_pair_trip_count,
        coalesce(terminal_ranks.terminal_pair_rank, 999999) as terminal_pair_rank,
        exists(
            select 1
            from trip_stops as longer_trip
            where longer_trip.gtfs_snapshot_id = trip_stops.gtfs_snapshot_id
              and longer_trip.line = trip_stops.line
              and longer_trip.direction_id = trip_stops.direction_id
              and longer_trip.schedule_day_type = trip_stops.schedule_day_type
              and longer_trip.is_public_service_segment
              and longer_trip.stop_count > trip_stops.stop_count
              and strpos(concat('|', longer_trip.ordered_stop_ids, '|'), concat('|', trip_stops.ordered_stop_ids, '|')) > 0
        ) and coalesce(terminal_ranks.terminal_pair_rank, 999999) > 1 as is_short_turn_part_trip
    from trip_stops
    left join terminal_ranks
        on trip_stops.gtfs_snapshot_id = terminal_ranks.gtfs_snapshot_id
        and trip_stops.line = terminal_ranks.line
        and trip_stops.direction_id = terminal_ranks.direction_id
        and trip_stops.schedule_day_type = terminal_ranks.schedule_day_type
        and trip_stops.origin_stop_id = terminal_ranks.origin_stop_id
        and trip_stops.destination_stop_id = terminal_ranks.destination_stop_id
)

select
    *,
    non_zone1_stop_count = 0 as is_zone1_only,
    is_public_service_segment
        and not is_short_turn_part_trip
        and non_zone1_stop_count = 0 as is_zone1_public_ranking_trip
from classified
