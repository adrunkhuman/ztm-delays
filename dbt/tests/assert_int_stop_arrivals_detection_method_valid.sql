select detection_method
from {{ ref('int_stop_arrivals') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and detection_method not in ('segment_within_75m', 'segment_within_250m')
