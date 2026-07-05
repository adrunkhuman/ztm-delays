select *
from {{ ref('agg_service_coverage') }}
where scheduled_start_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
  and (
      mode not in ('bus', 'tram')
      or expected_trip_count <= 0
      or observed_trip_count < 0
      or complete_trip_count < 0
      or partial_trip_count < 0
      or observed_trip_count != complete_trip_count + partial_trip_count
      or expected_service_minutes < 0
      or observed_service_minutes < 0
      or service_coverage_ratio < 0
      or service_coverage_ratio > 1
      or scheduled_start_date != date(service_hour, 'Europe/Warsaw')
      or extract(minute from service_hour at time zone 'Europe/Warsaw') != 0
      or extract(second from service_hour at time zone 'Europe/Warsaw') != 0
      or service_hour_end != timestamp_add(service_hour, interval 1 hour)
      or is_settled_hour != (service_hour_end < timestamp_sub(current_timestamp(), interval 90 minute))
  )
