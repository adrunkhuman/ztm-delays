select
    facts.service_date,
    facts.gps_date,
    facts.source_gps_date,
    facts.gtfs_snapshot_id,
    processing_snapshots.gtfs_snapshot_id as expected_gtfs_snapshot_id,
    facts.trip_id,
    facts.vehicle_number,
    facts.stop_sequence
from {{ ref('fct_stop_arrival') }} as facts
left join {{ ref('int_gtfs_processing_snapshot') }} as processing_snapshots
    on facts.source_gps_date = processing_snapshots.processing_date
where facts.service_date = date('{{ var("publish_service_date", var("processing_date")) }}')
  and facts.gtfs_snapshot_id != coalesce(processing_snapshots.gtfs_snapshot_id, '__missing_snapshot_mapping__')
