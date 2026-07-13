{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["line"],
        require_partition_filter=true,
    )
}}

with source as (
    select
        cast(Lines as string) as line,
        coalesce(nullif(regexp_replace(cast(Brigade as string), r'^0+', ''), ''), '0') as brigade,
        safe_cast(Lat as float64) as lat,
        safe_cast(Lon as float64) as lon,
        cast(Time as timestamp) as gps_time,
        cast(VehicleNumber as string) as vehicle_number,
        cast(vehicle_type as int64) as vehicle_type,
        cast(ingested_at as timestamp) as ingested_at,
        date(cast(Time as timestamp), 'Europe/Warsaw') as gps_date
    from {{ source('raw', 'raw_gps_pings') }}
    where cast(Time as timestamp) >= timestamp(date('{{ var("processing_date") }}'), 'Europe/Warsaw')
      and cast(Time as timestamp) < timestamp(date_add(date('{{ var("processing_date") }}'), interval 1 day), 'Europe/Warsaw')
      and regexp_contains(cast(Brigade as string), r'^\d+$')
      and regexp_contains(cast(VehicleNumber as string), r'^\d+$')
      and safe_cast(Lat as float64) between 51.0 and 53.5
      and safe_cast(Lon as float64) between 19.5 and 22.5
),

deduplicated as (
    select
        line,
        brigade,
        lat,
        lon,
        gps_time,
        vehicle_number,
        vehicle_type,
        ingested_at,
        gps_date
    from source
    qualify row_number() over (
        partition by vehicle_type, vehicle_number, gps_time
        order by ingested_at desc
    ) = 1
)

select
    line,
    brigade,
    lat,
    lon,
    gps_time,
    vehicle_number,
    vehicle_type,
    ingested_at,
    gps_date
from deduplicated
