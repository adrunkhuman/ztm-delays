-- Explicit dependencies also apply to an empty plan (which must not read raw history).
-- depends_on: {{ ref('int_gtfs_processing_snapshot') }}
-- depends_on: {{ ref('stg_gtfs__calendar_dates') }}
-- depends_on: {{ ref('stg_gtfs__trips') }}
-- depends_on: {{ ref('stg_gtfs__stop_times') }}
{% set plan = schedule_ledger_plan() %}
-- schedule_ledger_plan: {{ tojson(plan) }}
{% set snapshots = [] %}
{% for item in plan %}
    {% if item['gtfs_snapshot_id'] is not none and item['gtfs_snapshot_id'] not in snapshots %}
        {% do snapshots.append(item['gtfs_snapshot_id']) %}
    {% endif %}
{% endfor %}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'processing_date', 'data_type': 'date'},
    cluster_by=['line', 'direction_id', 'schedule_day_type'],
    on_schema_change='ignore',
    full_refresh=false
) }}

{% if plan %}
{% set mapping_sql %}
    {% for item in plan %}
    select date('{{ item['processing_date'] }}') as processing_date,
        {% if item['gtfs_snapshot_id'] is none %}cast(null as string){% else %}'{{ item['gtfs_snapshot_id'] }}'{% endif %} as gtfs_snapshot_id
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endset %}
with trip_history as (
    {{ gtfs_trip_schedule_history(mapping_sql, snapshots) }}
), fingerprints as (
    {{ schedule_fingerprints('trip_history') }}
), processing_dates as ({{ mapping_sql }})
select *, false as is_date_marker from fingerprints
union all
-- A marker records successful expansion even when no lines exist. It is never a version row.
select processing_date, gtfs_snapshot_id, cast(null as string) as line,
    cast(null as int64) as direction_id, cast(null as string) as schedule_day_type,
    cast(null as string) as timetable_fingerprint, cast(null as int64) as scheduled_trip_count,
    true as is_date_marker
from processing_dates
{% else %}
select cast(null as date) as processing_date, cast(null as string) as gtfs_snapshot_id,
    cast(null as string) as line, cast(null as int64) as direction_id,
    cast(null as string) as schedule_day_type, cast(null as string) as timetable_fingerprint,
    cast(null as int64) as scheduled_trip_count, false as is_date_marker
from unnest(cast([] as array<int64>)) as empty_relation
where false
{% endif %}
