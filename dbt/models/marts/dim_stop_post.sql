with loaded_snapshots as (
    select distinct gtfs_snapshot_id
    from {{ ref('stg_gtfs__stops') }}
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
    select distinct stop_id
    from {{ ref('stg_gtfs__stops') }}
),

-- Keep missing entity states so removals close SCD ranges instead of merging across gaps.
stop_snapshots as (
    select
        entities.stop_id,
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
        governing_snapshots.gtfs_snapshot_id,
        governing_snapshots.snapshot_timestamp,
        governing_snapshots.valid_from_date,
        stops.stop_id is not null as is_present,
        if(
            stops.stop_id is null,
            '__missing__',
            to_hex(md5(to_json_string(struct(
                substr(stops.stop_id, 1, 4) as stop_group_id,
                {{ stop_post_code('stops.stop_id') }} as stop_post_code,
                stops.stop_name as stop_name,
                stops.stop_code as stop_code,
                stops.zone_id as zone_id,
                stops.effective_zone_id as effective_zone_id,
                stops.stop_name_stem as stop_name_stem,
                stops.town_name as town_name,
                stops.stop_lat as stop_lat,
                stops.stop_lon as stop_lon
            ))))
        ) as attribute_hash
    from entities
    cross join governing_snapshots
    left join {{ ref('stg_gtfs__stops') }} as stops
        on entities.stop_id = stops.stop_id
        and governing_snapshots.gtfs_snapshot_id = stops.gtfs_snapshot_id
),

changes as (
    select
        *,
        attribute_hash != lag(attribute_hash) over (
            partition by stop_id
            order by snapshot_timestamp, gtfs_snapshot_id
        ) or lag(attribute_hash) over (
            partition by stop_id
            order by snapshot_timestamp, gtfs_snapshot_id
        ) is null as starts_new_version
    from stop_snapshots
),

versioned as (
    select
        *,
        countif(starts_new_version) over (
            partition by stop_id
            order by snapshot_timestamp, gtfs_snapshot_id
            rows between unbounded preceding and current row
        ) as version_group
    from changes
),

scd_rows as (
    select
        stop_id,
        stop_group_id,
        stop_post_code,
        stop_name,
        stop_code,
        zone_id,
        effective_zone_id,
        stop_name_stem,
        town_name,
        stop_lat,
        stop_lon,
        is_present,
        attribute_hash,
        min(valid_from_date) as valid_from_date,
        array_agg(gtfs_snapshot_id order by snapshot_timestamp, gtfs_snapshot_id limit 1)[offset(0)] as first_gtfs_snapshot_id,
        array_agg(gtfs_snapshot_id order by snapshot_timestamp desc, gtfs_snapshot_id desc limit 1)[offset(0)] as last_gtfs_snapshot_id
    from versioned
    group by
        stop_id,
        stop_group_id,
        stop_post_code,
        stop_name,
        stop_code,
        zone_id,
        effective_zone_id,
        stop_name_stem,
        town_name,
        stop_lat,
        stop_lon,
        is_present,
        attribute_hash,
        version_group
),

ranged_rows as (
    select
        stop_id,
        stop_group_id,
        stop_post_code,
        stop_name,
        stop_code,
        zone_id,
        effective_zone_id,
        stop_name_stem,
        town_name,
        stop_lat,
        stop_lon,
        is_present,
        attribute_hash,
        valid_from_date,
        date_sub(lead(valid_from_date) over (partition by stop_id order by valid_from_date), interval 1 day) as valid_to_date,
        first_gtfs_snapshot_id,
        last_gtfs_snapshot_id
    from scd_rows
)

select
    stop_id,
    stop_group_id,
    stop_post_code,
    stop_name,
    stop_code,
    zone_id,
    effective_zone_id,
    stop_name_stem,
    town_name,
    stop_lat,
    stop_lon,
    attribute_hash,
    valid_from_date,
    valid_to_date,
    first_gtfs_snapshot_id,
    last_gtfs_snapshot_id
from ranged_rows
where is_present
