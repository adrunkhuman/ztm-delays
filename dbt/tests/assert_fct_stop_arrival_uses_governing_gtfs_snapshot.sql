with fact_dates as (
    select distinct service_date
    from {{ ref('fct_stop_arrival') }}
    where service_date between date('{{ var("aggregation_start_date", var("publish_service_date", var("processing_date"))) }}')
        and date('{{ var("processing_date") }}')
),

governing_snapshots as (
    select
        fact_dates.service_date,
        snapshots.snapshot_id as expected_gtfs_snapshot_id
    from fact_dates
    inner join {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
        on date(snapshots.snapshot_timestamp, 'Europe/Warsaw') < fact_dates.service_date
    qualify row_number() over (
        partition by fact_dates.service_date
        order by snapshots.snapshot_timestamp desc, snapshots.snapshot_id desc
    ) = 1
)

select
    facts.service_date,
    facts.gps_date,
    facts.source_gps_date,
    facts.gtfs_snapshot_id,
    governing_snapshots.expected_gtfs_snapshot_id,
    facts.trip_id,
    facts.vehicle_number,
    facts.stop_sequence
from {{ ref('fct_stop_arrival') }} as facts
left join governing_snapshots
    on facts.service_date = governing_snapshots.service_date
where facts.service_date between date('{{ var("aggregation_start_date", var("publish_service_date", var("processing_date"))) }}')
    and date('{{ var("processing_date") }}')
  and (
      governing_snapshots.expected_gtfs_snapshot_id is null
      or facts.gtfs_snapshot_id != governing_snapshots.expected_gtfs_snapshot_id
  )
