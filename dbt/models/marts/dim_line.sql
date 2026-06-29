with loaded_snapshots as (
    select distinct gtfs_snapshot_id
    from {{ ref('stg_gtfs__routes') }}
),

-- One loaded snapshot governs each Warsaw-local service date: latest snapshot from the prior local date.
governing_snapshots as (
    select
        snapshots.snapshot_id as gtfs_snapshot_id,
        snapshots.snapshot_timestamp,
        date_add(date(snapshots.snapshot_timestamp, 'Europe/Warsaw'), interval 1 day) as valid_from_date
    from {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
    inner join loaded_snapshots
        on snapshots.snapshot_id = loaded_snapshots.gtfs_snapshot_id
    qualify row_number() over (
        partition by date_add(date(snapshots.snapshot_timestamp, 'Europe/Warsaw'), interval 1 day)
        order by snapshots.snapshot_timestamp desc, snapshots.snapshot_id desc
    ) = 1
),

entities as (
    select distinct route_id as line
    from {{ ref('stg_gtfs__routes') }}
),

-- Keep missing entity states so removals close SCD ranges instead of merging across gaps.
line_snapshots as (
    select
        entities.line,
        routes.route_short_name,
        cast(null as string) as route_long_name,
        routes.mode,
        routes.mode in ('bus', 'tram') as has_live_gps,
        governing_snapshots.gtfs_snapshot_id,
        governing_snapshots.snapshot_timestamp,
        governing_snapshots.valid_from_date,
        routes.route_id is not null as is_present,
        if(
            routes.route_id is null,
            '__missing__',
            to_hex(md5(to_json_string(struct(
                routes.route_short_name as route_short_name,
                cast(null as string) as route_long_name,
                routes.mode as mode,
                routes.mode in ('bus', 'tram') as has_live_gps
            ))))
        ) as attribute_hash
    from entities
    cross join governing_snapshots
    left join {{ ref('stg_gtfs__routes') }} as routes
        on entities.line = routes.route_id
        and governing_snapshots.gtfs_snapshot_id = routes.gtfs_snapshot_id
),

changes as (
    select
        *,
        attribute_hash != lag(attribute_hash) over (
            partition by line
            order by snapshot_timestamp, gtfs_snapshot_id
        ) or lag(attribute_hash) over (
            partition by line
            order by snapshot_timestamp, gtfs_snapshot_id
        ) is null as starts_new_version
    from line_snapshots
),

versioned as (
    select
        *,
        countif(starts_new_version) over (
            partition by line
            order by snapshot_timestamp, gtfs_snapshot_id
            rows between unbounded preceding and current row
        ) as version_group
    from changes
),

scd_rows as (
    select
        line,
        route_short_name,
        route_long_name,
        mode,
        has_live_gps,
        is_present,
        attribute_hash,
        min(valid_from_date) as valid_from_date,
        array_agg(gtfs_snapshot_id order by snapshot_timestamp, gtfs_snapshot_id limit 1)[offset(0)] as first_gtfs_snapshot_id,
        array_agg(gtfs_snapshot_id order by snapshot_timestamp desc, gtfs_snapshot_id desc limit 1)[offset(0)] as last_gtfs_snapshot_id
    from versioned
    group by
        line,
        route_short_name,
        route_long_name,
        mode,
        has_live_gps,
        is_present,
        attribute_hash,
        version_group
),

ranged_rows as (
    select
        line,
        route_short_name,
        route_long_name,
        mode,
        has_live_gps,
        is_present,
        attribute_hash,
        valid_from_date,
        date_sub(lead(valid_from_date) over (partition by line order by valid_from_date), interval 1 day) as valid_to_date,
        first_gtfs_snapshot_id,
        last_gtfs_snapshot_id
    from scd_rows
)

select
    line,
    route_short_name,
    route_long_name,
    mode,
    has_live_gps,
    attribute_hash,
    valid_from_date,
    valid_to_date,
    first_gtfs_snapshot_id,
    last_gtfs_snapshot_id
from ranged_rows
where is_present
