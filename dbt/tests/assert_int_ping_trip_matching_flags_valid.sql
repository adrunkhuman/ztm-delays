select gps_date, vehicle_number, gps_time, flag
from {{ ref('int_ping_trip') }}
cross join unnest(matching_flags) as flag
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and flag not in (
    'early_origin_censored',
    'late_tail_censored',
    'delayed_same_line_tail',
    'candidate_overlap',
    'likely_previous_trip_tail',
    'likely_vehicle_swap',
    'uncertain_assignment'
  )
