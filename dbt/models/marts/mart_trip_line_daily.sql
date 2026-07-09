{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

select
    service_date,
    mode,
    line,
    any_value(route_short_name) as route_short_name,
    count(*) as trip_count,
    row_number() over (partition by service_date, mode order by safe_cast(line as int64), line) as line_display_rank
from {{ ref('fct_trip') }}
where service_date = date('{{ processing_date }}')
  and trip_quality = 'complete'
  and mode in ('bus', 'tram')
group by service_date, mode, line
