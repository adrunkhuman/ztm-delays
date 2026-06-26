with valid_snapshot as (
    select snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_timestamp <= timestamp(date('{{ var("processing_date") }}'), 'Europe/Warsaw')
    order by snapshot_timestamp desc, snapshot_id desc
    limit 1
)

select
    cast(trip_id as string) as trip_id,
    cast(stop_id as string) as stop_id,
    cast(stop_sequence as int64) as stop_sequence,
    safe_cast(split(cast(arrival_time as string), ':')[safe_offset(0)] as int64) * 3600
        + safe_cast(split(cast(arrival_time as string), ':')[safe_offset(1)] as int64) * 60
        + safe_cast(split(cast(arrival_time as string), ':')[safe_offset(2)] as int64) as arrival_time_seconds,
    safe_cast(split(cast(departure_time as string), ':')[safe_offset(0)] as int64) * 3600
        + safe_cast(split(cast(departure_time as string), ':')[safe_offset(1)] as int64) * 60
        + safe_cast(split(cast(departure_time as string), ':')[safe_offset(2)] as int64) as departure_time_seconds,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_stop_times') }}
inner join valid_snapshot
    on gtfs_snapshot_id = valid_snapshot.snapshot_id
