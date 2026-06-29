with posts as (
    select
        stop_id,
        stop_group_id,
        stop_name,
        stop_lat,
        stop_lon,
        lines_served,
        modes_served,
        directions_served,
        gtfs_snapshot_id
    from {{ ref('dim_stop_post_current') }}
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

lines as (
    select distinct
        stop_group_id,
        gtfs_snapshot_id,
        line
    from posts,
        unnest(split(lines_served, ', ')) as line
    where line != ''
),

modes as (
    select distinct
        stop_group_id,
        gtfs_snapshot_id,
        mode
    from posts,
        unnest(split(modes_served, ', ')) as mode
    where mode != ''
),

directions as (
    select distinct
        stop_group_id,
        gtfs_snapshot_id,
        direction_id
    from posts,
        unnest(split(directions_served, ', ')) as direction_id
    where direction_id != ''
),

served_by as (
    select
        stop_groups.stop_group_id,
        stop_groups.gtfs_snapshot_id,
        coalesce(string_agg(distinct lines.line, ', ' order by lines.line), '') as lines_served,
        coalesce(string_agg(distinct modes.mode, ', ' order by modes.mode), '') as modes_served,
        coalesce(string_agg(distinct directions.direction_id, ', ' order by directions.direction_id), '') as directions_served
    from (select distinct stop_group_id, gtfs_snapshot_id from posts) as stop_groups
    left join lines
        on stop_groups.stop_group_id = lines.stop_group_id
        and stop_groups.gtfs_snapshot_id = lines.gtfs_snapshot_id
    left join modes
        on stop_groups.stop_group_id = modes.stop_group_id
        and stop_groups.gtfs_snapshot_id = modes.gtfs_snapshot_id
    left join directions
        on stop_groups.stop_group_id = directions.stop_group_id
        and stop_groups.gtfs_snapshot_id = directions.gtfs_snapshot_id
    group by stop_groups.stop_group_id, stop_groups.gtfs_snapshot_id
)

select
    posts.stop_group_id,
    canonical_names.stop_group_name,
    string_agg(distinct posts.stop_name, ' | ' order by posts.stop_name) as stop_group_names,
    avg(posts.stop_lat) as centroid_lat,
    avg(posts.stop_lon) as centroid_lon,
    count(*) as stop_post_count,
    served_by.lines_served,
    served_by.modes_served,
    served_by.directions_served,
    posts.gtfs_snapshot_id
from posts
inner join canonical_names
    on posts.stop_group_id = canonical_names.stop_group_id
    and posts.gtfs_snapshot_id = canonical_names.gtfs_snapshot_id
inner join served_by
    on posts.stop_group_id = served_by.stop_group_id
    and posts.gtfs_snapshot_id = served_by.gtfs_snapshot_id
group by
    posts.stop_group_id,
    canonical_names.stop_group_name,
    served_by.lines_served,
    served_by.modes_served,
    served_by.directions_served,
    posts.gtfs_snapshot_id
