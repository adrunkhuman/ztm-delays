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

posts as (
    select
        stops.stop_id,
        substr(stops.stop_id, 1, 4) as stop_group_id,
        stops.stop_name,
        stops.stop_lat,
        stops.stop_lon,
        stops.gtfs_snapshot_id,
        governing_snapshots.snapshot_timestamp,
        governing_snapshots.valid_from_date
    from {{ ref('stg_gtfs__stops') }} as stops
    inner join governing_snapshots
        on stops.gtfs_snapshot_id = governing_snapshots.gtfs_snapshot_id
),

entities as (
    select distinct stop_group_id
    from posts
),

group_names as (
    select
        stop_group_id,
        gtfs_snapshot_id,
        stop_name,
        count(*) as post_count_for_name
    from posts
    group by stop_group_id, gtfs_snapshot_id, stop_name
),

canonical_names as (
    select
        stop_group_id,
        gtfs_snapshot_id,
        stop_name as stop_group_name
    from group_names
    qualify row_number() over (
        partition by stop_group_id, gtfs_snapshot_id
        order by post_count_for_name desc, stop_name
    ) = 1
),

group_snapshots as (
    select
        posts.stop_group_id,
        canonical_names.stop_group_name,
        string_agg(distinct posts.stop_name, ' | ' order by posts.stop_name) as stop_group_names,
        avg(posts.stop_lat) as centroid_lat,
        avg(posts.stop_lon) as centroid_lon,
        count(*) as stop_post_count,
        posts.gtfs_snapshot_id,
        any_value(posts.snapshot_timestamp) as snapshot_timestamp,
        any_value(posts.valid_from_date) as valid_from_date,
        to_hex(md5(to_json_string(struct(
            canonical_names.stop_group_name as stop_group_name,
            string_agg(distinct posts.stop_name, ' | ' order by posts.stop_name) as stop_group_names,
            avg(posts.stop_lat) as centroid_lat,
            avg(posts.stop_lon) as centroid_lon,
            count(*) as stop_post_count
        )))) as attribute_hash
    from posts
    inner join canonical_names
        on posts.stop_group_id = canonical_names.stop_group_id
        and posts.gtfs_snapshot_id = canonical_names.gtfs_snapshot_id
    group by posts.stop_group_id, canonical_names.stop_group_name, posts.gtfs_snapshot_id
),

-- Keep missing entity states so removals close SCD ranges instead of merging across gaps.
group_states as (
    select
        entities.stop_group_id,
        group_snapshots.stop_group_name,
        group_snapshots.stop_group_names,
        group_snapshots.centroid_lat,
        group_snapshots.centroid_lon,
        group_snapshots.stop_post_count,
        governing_snapshots.gtfs_snapshot_id,
        governing_snapshots.snapshot_timestamp,
        governing_snapshots.valid_from_date,
        group_snapshots.stop_group_id is not null as is_present,
        coalesce(group_snapshots.attribute_hash, '__missing__') as attribute_hash
    from entities
    cross join governing_snapshots
    left join group_snapshots
        on entities.stop_group_id = group_snapshots.stop_group_id
        and governing_snapshots.gtfs_snapshot_id = group_snapshots.gtfs_snapshot_id
),

changes as (
    select
        *,
        attribute_hash != lag(attribute_hash) over (
            partition by stop_group_id
            order by snapshot_timestamp, gtfs_snapshot_id
        ) or lag(attribute_hash) over (
            partition by stop_group_id
            order by snapshot_timestamp, gtfs_snapshot_id
        ) is null as starts_new_version
    from group_states
),

versioned as (
    select
        *,
        countif(starts_new_version) over (
            partition by stop_group_id
            order by snapshot_timestamp, gtfs_snapshot_id
            rows between unbounded preceding and current row
        ) as version_group
    from changes
),

scd_rows as (
    select
        stop_group_id,
        stop_group_name,
        stop_group_names,
        centroid_lat,
        centroid_lon,
        stop_post_count,
        is_present,
        attribute_hash,
        min(valid_from_date) as valid_from_date,
        array_agg(gtfs_snapshot_id order by snapshot_timestamp, gtfs_snapshot_id limit 1)[offset(0)] as first_gtfs_snapshot_id,
        array_agg(gtfs_snapshot_id order by snapshot_timestamp desc, gtfs_snapshot_id desc limit 1)[offset(0)] as last_gtfs_snapshot_id
    from versioned
    group by
        stop_group_id,
        stop_group_name,
        stop_group_names,
        centroid_lat,
        centroid_lon,
        stop_post_count,
        is_present,
        attribute_hash,
        version_group
),

ranged_rows as (
    select
        stop_group_id,
        stop_group_name,
        stop_group_names,
        centroid_lat,
        centroid_lon,
        stop_post_count,
        is_present,
        attribute_hash,
        valid_from_date,
        date_sub(lead(valid_from_date) over (partition by stop_group_id order by valid_from_date), interval 1 day) as valid_to_date,
        first_gtfs_snapshot_id,
        last_gtfs_snapshot_id
    from scd_rows
)

select
    stop_group_id,
    stop_group_name,
    stop_group_names,
    centroid_lat,
    centroid_lon,
    stop_post_count,
    attribute_hash,
    valid_from_date,
    valid_to_date,
    first_gtfs_snapshot_id,
    last_gtfs_snapshot_id
from ranged_rows
where is_present
