with selected_snapshot as (
    select snapshot_id as gtfs_snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_id = '{{ var("gtfs_snapshot_id") }}'
    limit 1
),

stop_service as (
    select distinct
        stop_times.stop_id,
        stop_times.gtfs_snapshot_id,
        trips.line,
        routes.mode,
        trips.direction_id
    from {{ ref('stg_gtfs__stop_times') }} as stop_times
    inner join {{ ref('stg_gtfs__trips') }} as trips
        on stop_times.trip_id = trips.trip_id
        and stop_times.gtfs_snapshot_id = trips.gtfs_snapshot_id
    inner join {{ ref('stg_gtfs__routes') }} as routes
        on trips.line = routes.route_id
        and trips.gtfs_snapshot_id = routes.gtfs_snapshot_id
    inner join selected_snapshot
        on stop_times.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
),

served_by as (
    select
        stop_id,
        gtfs_snapshot_id,
        string_agg(distinct line, ', ' order by line) as lines_served,
        string_agg(distinct mode, ', ' order by mode) as modes_served,
        string_agg(distinct cast(direction_id as string), ', ' order by cast(direction_id as string)) as directions_served
    from stop_service
    group by stop_id, gtfs_snapshot_id
)

select
    stops.stop_id,
    substr(stops.stop_id, 1, 4) as stop_group_id,
    {{ stop_post_code('stops.stop_id') }} as stop_post_code,
    stops.stop_name,
    stops.stop_code,
    stops.zone_id,
    stops.effective_zone_id,
    stops.stop_name_stem,
    stops.town_name,
    stops.stop_lat,
    stops.stop_lon,
    coalesce(served_by.lines_served, '') as lines_served,
    coalesce(served_by.modes_served, '') as modes_served,
    coalesce(served_by.directions_served, '') as directions_served,
    stops.gtfs_snapshot_id
from {{ ref('stg_gtfs__stops') }} as stops
inner join selected_snapshot
    on stops.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
left join served_by
    on stops.stop_id = served_by.stop_id
    and stops.gtfs_snapshot_id = served_by.gtfs_snapshot_id
