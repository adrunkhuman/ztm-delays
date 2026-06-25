{{
    config(
        materialized='incremental',
        partition_by={"field": "gps_date", "data_type": "date"},
        cluster_by=["line"],
    )
}}

with source as (
    select
        cast(Lines as string) as line,
        coalesce(nullif(regexp_replace(cast(Brigade as string), r'^0+', ''), ''), '0') as brigade,
        cast(Lat as float64) as lat,
        cast(Lon as float64) as lon,
        cast(Time as timestamp) as gps_time,
        cast(VehicleNumber as string) as vehicle_number,
        cast(vehicle_type as int64) as vehicle_type,
        cast(ingested_at as timestamp) as ingested_at,
        date(cast(Time as timestamp)) as gps_date
    from {{ source('raw', 'raw_gps_pings') }}
    where date(cast(Time as timestamp)) = date('{{ var("processing_date") }}')
      and regexp_contains(cast(Brigade as string), r'^\d+$')
      and regexp_contains(cast(VehicleNumber as string), r'^\d+$')
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by vehicle_number, gps_time
        order by ingested_at desc
    ) = 1
)

select *
from deduplicated
