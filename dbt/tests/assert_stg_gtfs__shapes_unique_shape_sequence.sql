select gtfs_snapshot_id, shape_id, shape_pt_sequence
from {{ ref('stg_gtfs__shapes') }}
group by gtfs_snapshot_id, shape_id, shape_pt_sequence
having count(*) > 1
