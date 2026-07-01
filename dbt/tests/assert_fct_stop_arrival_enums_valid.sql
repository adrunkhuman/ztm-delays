{% set test_service_date = var("publish_service_date", var("processing_date")) %}

select *
from {{ ref('fct_stop_arrival') }}
where service_date = date('{{ test_service_date }}')
  and (
    mode not in ('bus', 'tram', 'metro', 'rail')
    or vehicle_type not in (1, 2)
    or direction_id not in (0, 1)
    or trip_quality not in ('complete', 'partial', 'broken')
    or detection_method not in ('segment_within_50m')
  )
