{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with {{ serving_route_pattern_window_ctes() }},

grouped as (
    select
        line,
        mode,
        any_value(route_short_name) as route_short_name,
        direction_id,
        trip_headsign,
        route_pattern_id,
        any_value(pattern_status) as pattern_status,
        array_agg(origin_stop_name ignore nulls order by service_date desc, gtfs_snapshot_id desc, trip_id limit 1)[safe_offset(0)] as origin_stop_name,
        array_agg(destination_stop_name ignore nulls order by service_date desc, gtfs_snapshot_id desc, trip_id limit 1)[safe_offset(0)] as destination_stop_name,
        any_value(stop_call_count) as stop_call_count,
        array_agg(distinct service_date order by service_date) as observed_service_dates,
        'all_observed' as universe_type,
        window_type,
        window_key,
        source_end_date,
        count(*) as trip_count
    from eligible
    group by line, mode, direction_id, trip_headsign, route_pattern_id, window_type, window_key, source_end_date
)

select
    *,
    row_number() over (
        partition by line, mode, universe_type, window_type, window_key, source_end_date
        order by trip_count desc, direction_id, trip_headsign, route_pattern_id
    ) as course_rank
from grouped
