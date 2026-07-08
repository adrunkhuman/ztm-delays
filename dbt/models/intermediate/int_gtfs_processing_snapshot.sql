{{ config(materialized='table') }}

with snapshot_service_dates as (
    select distinct
        snapshots.snapshot_id as gtfs_snapshot_id,
        snapshots.snapshot_timestamp,
        calendar_dates.service_date
    from {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
    inner join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on snapshots.snapshot_id = calendar_dates.gtfs_snapshot_id
),

covered_processing_dates as (
    select
        current_service.gtfs_snapshot_id,
        current_service.snapshot_timestamp,
        current_service.service_date as processing_date
    from snapshot_service_dates as current_service
    inner join snapshot_service_dates as prior_service
        on current_service.gtfs_snapshot_id = prior_service.gtfs_snapshot_id
        and date_sub(current_service.service_date, interval 1 day) = prior_service.service_date
),

ranked as (
    select
        processing_date,
        gtfs_snapshot_id,
        snapshot_timestamp,
        row_number() over (
            partition by processing_date
            order by snapshot_timestamp desc, gtfs_snapshot_id desc
        ) as snapshot_rank
    from covered_processing_dates
)

select
    processing_date,
    gtfs_snapshot_id,
    snapshot_timestamp
from ranked
where snapshot_rank = 1
