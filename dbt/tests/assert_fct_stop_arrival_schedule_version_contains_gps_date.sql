select
    facts.service_date,
    facts.gps_date,
    facts.source_gps_date,
    facts.schedule_version_id,
    facts.trip_id,
    facts.vehicle_number,
    facts.stop_sequence
from {{ ref('fct_stop_arrival') }} as facts
left join {{ ref('dim_schedule_version') }} as schedule_version
    on facts.schedule_version_id = schedule_version.schedule_version_id
where facts.service_date between date('{{ var("aggregation_start_date", var("publish_service_date", var("processing_date"))) }}')
    and date('{{ var("processing_date") }}')
  and (
      schedule_version.schedule_version_id is null
      or facts.gps_date not between schedule_version.valid_from_date
          and coalesce(schedule_version.valid_to_date, date '9999-12-31')
  )
