select shape_id, shape_pt_sequence
from {{ ref('stg_gtfs_shapes') }}
group by shape_id, shape_pt_sequence
having count(*) > 1
