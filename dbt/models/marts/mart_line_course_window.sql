{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with trips as (
    select windows.window_type, windows.window_key, windows.source_end_date, executions.*
    from {{ ref('int_serving_trip_execution') }} as executions
    inner join {{ ref('dim_serving_window_date') }} as windows
        on executions.service_date = windows.service_date
        and windows.source_end_date = date('{{ processing_date }}')
    where executions.service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
      and (
          windows.window_type in ('day', 'month')
          or exists (
              select 1
              from {{ ref('dim_schedule_version') }} as anchor_version
              where anchor_version.schedule_version_id = executions.schedule_version_id
                and date('{{ processing_date }}') between anchor_version.valid_from_date
                    and coalesce(anchor_version.valid_to_date, date '9999-12-31')
          )
      )
),

grouped as (
    select
        line,
        mode,
        any_value(route_short_name) as route_short_name,
        direction_id,
        trip_headsign,
        'all_observed' as universe_type,
        window_type,
        window_key,
        source_end_date,
        count(*) as trip_count
    from trips
    group by line, mode, direction_id, trip_headsign, window_type, window_key, source_end_date
)

select
    *,
    row_number() over (partition by line, mode, universe_type, window_type, window_key order by trip_count desc, direction_id, trip_headsign) as course_rank
from grouped
