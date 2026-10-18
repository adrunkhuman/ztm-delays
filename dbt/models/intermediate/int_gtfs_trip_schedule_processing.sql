{{ config(materialized='ephemeral') }}
-- The history spine for one processing date and its governing snapshot. Ephemeral so each caller
-- compiles it with its own vars; literal snapshot bounds prune the partitioned GTFS sources.
{% set mapping_sql %}
    select * from {{ ref('int_gtfs_processing_snapshot') }}
    where processing_date = date('{{ var("processing_date") }}')
{% endset %}
{{ gtfs_trip_schedule_history(mapping_sql, [var("gtfs_snapshot_id")]) }}
