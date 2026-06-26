with valid_snapshot as (
    select snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_timestamp <= timestamp(date('{{ var("processing_date") }}'), 'Europe/Warsaw')
    order by snapshot_timestamp desc, snapshot_id desc
    limit 1
)

select
    cast(service_id as string) as service_id,
    cast(date as date) as service_date,
    cast(exception_type as int64) as exception_type,
    case
        when extract(dayofweek from cast(date as date)) in (1, 7) then 'weekend'
        else 'weekday'
    end as day_type,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_calendar_dates') }}
inner join valid_snapshot
    on gtfs_snapshot_id = valid_snapshot.snapshot_id
where cast(exception_type as int64) = 1
