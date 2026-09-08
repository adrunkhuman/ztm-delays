-- depends_on: {{ ref('stg_gtfs__calendar_dates') }}
-- depends_on: {{ ref('stg_gtfs__trips') }}
-- depends_on: {{ ref('stg_gtfs__stop_times') }}
-- depends_on: {{ ref('int_schedule_fingerprint_daily') }}
-- Compile with an explicit schedule_ledger_plan, at most schedule_ledger_max_dates.
-- This is a paid, bounded equivalence check ONLY AFTER separate execution approval.
{% set plan = schedule_ledger_plan() %}
{% set snapshots = [] %}
{% for item in plan %}
    {% if item['gtfs_snapshot_id'] is not none and item['gtfs_snapshot_id'] not in snapshots %}
        {% do snapshots.append(item['gtfs_snapshot_id']) %}
    {% endif %}
{% endfor %}
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
), expected as (
    {{ schedule_fingerprints('trip_history') }}
), actual as (
    select processing_date, gtfs_snapshot_id, line, direction_id, schedule_day_type,
        timetable_fingerprint, scheduled_trip_count
    from {{ ref('int_schedule_fingerprint_daily') }}
    where not is_date_marker and processing_date in (
        {% for item in plan %}date('{{ item['processing_date'] }}'){% if not loop.last %}, {% endif %}{% endfor %}
    )
), expected_only as (
    select * from expected except distinct select * from actual
), actual_only as (
    select * from actual except distinct select * from expected
)
select 'expected_only' as issue, * from expected_only
union all
select 'actual_only' as issue, * from actual_only
{% else %}
select 'empty verification plan' as issue
{% endif %}
