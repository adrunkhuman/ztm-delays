select
    facts.service_date,
    facts.gps_date,
    facts.source_gps_date,
    facts.gtfs_snapshot_id,
    '{{ var("gtfs_snapshot_id") }}' as expected_gtfs_snapshot_id,
    facts.trip_id,
    facts.vehicle_number,
    facts.stop_sequence
from {{ ref('fct_stop_arrival') }} as facts
where facts.service_date = date('{{ var("publish_service_date", var("processing_date")) }}')
  and facts.gtfs_snapshot_id != '{{ var("gtfs_snapshot_id") }}'
