{% set processing_date = var("processing_date", "1970-01-01") %}
{% set scope = serving_route_pattern_rebuild_range() %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line", "stop_group_id"],
        require_partition_filter=true,
    )
}}

-- Window marts re-read up to 120 service dates every night; they read these narrow rows instead of the wide
-- arrivals. Long identifiers become 64-bit fingerprints: trip_key keeps the snapshot|trip|vehicle identity the marts
-- count, universe_trip_key and schedule_version_key are matched against fingerprints of the same columns. The ranking
-- universe is still joined when windows are built: overnight arrivals belong to the next GPS date's universe.
select
    arrivals.service_date,
    arrivals.gps_date,
    farm_fingerprint(concat(
        arrivals.gtfs_snapshot_id, '|', arrivals.trip_id, '|', coalesce(arrivals.vehicle_number, '')
    )) as trip_key,
    farm_fingerprint(concat(arrivals.gtfs_snapshot_id, '|', arrivals.trip_id)) as universe_trip_key,
    farm_fingerprint(arrivals.schedule_version_id) as schedule_version_key,
    arrivals.mode,
    arrivals.line,
    arrivals.route_short_name,
    arrivals.direction_id,
    arrivals.trip_headsign,
    arrivals.stop_group_id,
    arrivals.stop_group_name,
    arrivals.stop_id,
    arrivals.stop_name,
    arrivals.stop_sequence,
    arrivals.schedule_day_type,
    arrivals.hour_bracket,
    arrivals.delay_seconds
from {{ ref('int_serving_stop_arrival') }} as arrivals
where arrivals.service_date between date('{{ scope.start }}') and date('{{ scope.end }}')
  and arrivals.trip_quality = 'complete'
  and arrivals.mode in ('bus', 'tram')
