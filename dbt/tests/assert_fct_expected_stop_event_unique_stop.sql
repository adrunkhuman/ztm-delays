{% set test_service_date = var("publish_service_date", var("processing_date")) %}

select gtfs_snapshot_id, service_date, matched_duty_chain_id, trip_id, vehicle_number, stop_sequence
from {{ ref('fct_expected_stop_event') }}
where service_date = date('{{ test_service_date }}')
group by gtfs_snapshot_id, service_date, matched_duty_chain_id, trip_id, vehicle_number, stop_sequence
having count(*) > 1
