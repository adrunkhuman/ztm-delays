with selected_snapshot as (
    select snapshot_id as gtfs_snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_id = '{{ var("gtfs_snapshot_id") }}'
    limit 1
),

routes as (
    select
        routes.route_id as line,
        routes.route_short_name,
        routes.route_type,
        routes.mode,
        routes.gtfs_snapshot_id
    from {{ ref('stg_gtfs__routes') }} as routes
    inner join selected_snapshot
        on routes.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
),

headsigns_by_direction as (
    select
        trips.line,
        trips.gtfs_snapshot_id,
        trips.direction_id,
        string_agg(distinct trips.trip_headsign, ' | ' order by trips.trip_headsign) as headsigns
    from {{ ref('stg_gtfs__trips') }} as trips
    inner join selected_snapshot
        on trips.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
    where trips.trip_headsign is not null
    group by trips.line, trips.gtfs_snapshot_id, trips.direction_id
),

headsigns as (
    select
        line,
        gtfs_snapshot_id,
        max(if(direction_id = 0, headsigns, null)) as direction_0_headsigns,
        max(if(direction_id = 1, headsigns, null)) as direction_1_headsigns
    from headsigns_by_direction
    group by line, gtfs_snapshot_id
)

select
    routes.line,
    routes.mode,
    routes.route_short_name,
    cast(null as string) as route_long_name,
    headsigns.direction_0_headsigns,
    headsigns.direction_1_headsigns,
    routes.mode in ('bus', 'tram') as has_live_gps,
    routes.gtfs_snapshot_id
from routes
left join headsigns
    on routes.line = headsigns.line
    and routes.gtfs_snapshot_id = headsigns.gtfs_snapshot_id
