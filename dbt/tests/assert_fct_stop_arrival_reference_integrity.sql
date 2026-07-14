select
    'selected_snapshot' as issue_type,
    facts.service_date,
    facts.gps_date,
    facts.source_gps_date,
    facts.trip_id,
    facts.vehicle_number,
    facts.stop_sequence,
    cast(facts.gtfs_snapshot_id as string) as observed_value,
    cast(processing_snapshots.gtfs_snapshot_id as string) as expected_value
from {{ ref('fct_stop_arrival') }} as facts
left join {{ ref('int_gtfs_processing_snapshot') }} as processing_snapshots
    on facts.gps_date = processing_snapshots.processing_date
where facts.service_date = date('{{ var("publish_service_date", var("processing_date")) }}')
  and facts.gtfs_snapshot_id != coalesce(processing_snapshots.gtfs_snapshot_id, '__missing_snapshot_mapping__')

union all

select
    'schedule_version_range' as issue_type,
    facts.service_date,
    facts.gps_date,
    facts.source_gps_date,
    facts.trip_id,
    facts.vehicle_number,
    facts.stop_sequence,
    cast(facts.schedule_version_id as string) as observed_value,
    cast(null as string) as expected_value
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
