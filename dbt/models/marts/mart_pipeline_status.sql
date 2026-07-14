{% set processing_date = var("processing_date", "1970-01-01") %}
{% set aggregation_start_date = var("aggregation_start_date", processing_date) %}
{% set start_date = modules.datetime.datetime.strptime(aggregation_start_date, "%Y-%m-%d").date() %}
{% set end_date = modules.datetime.datetime.strptime(processing_date, "%Y-%m-%d").date() %}
{% set partition_dates = [] %}
{% for day_offset in range((end_date - start_date).days + 1) %}
    {% do partition_dates.append("date('" ~ (start_date + modules.datetime.timedelta(days=day_offset)).isoformat() ~ "')") %}
{% endfor %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=partition_dates,
        cluster_by=["mode"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with status_spine as (
    select
        gps_date as service_date,
        vehicle_type,
        mode,
        expected_hours,
        present_hours,
        missing_hours,
        first_observed_time,
        last_observed_time,
        completeness_ratio,
        is_complete_day,
        gps_row_count,
        max_vehicle_count,
        mean_hourly_coverage_ratio,
        min_hourly_coverage_ratio,
        max_gap_seconds
    from {{ ref('mart_day_completeness') }}
    where gps_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
),

staged_pings as (
    select
        gps_date as service_date,
        vehicle_type,
        case vehicle_type
            when 1 then 'bus'
            when 2 then 'tram'
        end as mode,
        count(*) as pings_total
    from {{ ref('stg_gps__pings') }}
    where gps_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
    group by service_date, vehicle_type, mode
),

trips as (
    select
        gps_date as service_date,
        vehicle_type,
        mode,
        count(*) as trips_observed,
        countif(trip_quality = 'complete') as trips_complete,
        countif(trip_quality = 'partial') as trips_partial,
        countif(trip_quality = 'broken') as trips_broken
    from {{ ref('fct_trip') }}
    where service_date between date_sub(date('{{ aggregation_start_date }}'), interval 1 day)
        and date('{{ processing_date }}')
      and gps_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
    group by service_date, vehicle_type, mode
),

service_coverage as (
    select
        scheduled_start_date as service_date,
        mode,
        sum(expected_trip_count) as expected_trips,
        sum(observed_trip_count) as observed_trips,
        sum(expected_service_minutes) as expected_service_minutes,
        sum(observed_service_minutes) as observed_service_minutes,
        count(distinct schedule_version_id) as schedule_versions_active
    from {{ ref('agg_service_coverage') }}
    where scheduled_start_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
      and is_settled_hour
    group by service_date, mode
),

stop_arrivals as (
    select
        gps_date as service_date,
        vehicle_type,
        case vehicle_type
            when 1 then 'bus'
            when 2 then 'tram'
        end as mode,
        count(*) as stop_arrivals_count
    from {{ ref('fct_stop_arrival') }}
    where service_date between date_sub(date('{{ aggregation_start_date }}'), interval 1 day)
        and date('{{ processing_date }}')
      and gps_date between date('{{ aggregation_start_date }}')
        and date('{{ processing_date }}')
    group by service_date, vehicle_type, mode
),

latest_gtfs_snapshot as (
    select
        snapshot_id as latest_gtfs_snapshot_id,
        snapshot_timestamp as latest_gtfs_snapshot_at,
        timestamp_diff(current_timestamp(), snapshot_timestamp, hour) as gtfs_snapshot_age_hours
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    qualify row_number() over (order by snapshot_timestamp desc) = 1
)

select
    status_spine.service_date,
    status_spine.vehicle_type,
    status_spine.mode,
    status_spine.expected_hours,
    status_spine.present_hours,
    status_spine.missing_hours,
    status_spine.first_observed_time,
    status_spine.last_observed_time,
    status_spine.completeness_ratio,
    status_spine.is_complete_day,
    status_spine.gps_row_count,
    status_spine.max_vehicle_count,
    status_spine.mean_hourly_coverage_ratio,
    status_spine.min_hourly_coverage_ratio,
    status_spine.max_gap_seconds,
    coalesce(staged_pings.pings_total, 0) as pings_total,
    coalesce(trips.trips_observed, 0) as trips_observed,
    coalesce(trips.trips_complete, 0) as trips_complete,
    coalesce(trips.trips_partial, 0) as trips_partial,
    coalesce(trips.trips_broken, 0) as trips_broken,
    coalesce(safe_divide(trips.trips_broken, trips.trips_observed), 0.0) as broken_rate,
    coalesce(service_coverage.expected_trips, 0) as expected_trips,
    coalesce(service_coverage.observed_trips, 0) as observed_trips,
    safe_divide(service_coverage.observed_trips, service_coverage.expected_trips) as service_coverage_ratio,
    coalesce(service_coverage.expected_service_minutes, 0.0) as expected_service_minutes,
    coalesce(service_coverage.observed_service_minutes, 0.0) as observed_service_minutes,
    coalesce(stop_arrivals.stop_arrivals_count, 0) as stop_arrivals_count,
    latest_gtfs_snapshot.latest_gtfs_snapshot_id,
    latest_gtfs_snapshot.latest_gtfs_snapshot_at,
    latest_gtfs_snapshot.gtfs_snapshot_age_hours,
    coalesce(service_coverage.schedule_versions_active, 0) as schedule_versions_active,
    least(
        coalesce(status_spine.completeness_ratio, 1.0),
        coalesce(safe_divide(service_coverage.observed_trips, service_coverage.expected_trips), 1.0)
    ) as health_ratio,
    case
        when status_spine.completeness_ratio is null and service_coverage.observed_trips is null then 'no data'
        when least(
            coalesce(status_spine.completeness_ratio, 1.0),
            coalesce(safe_divide(service_coverage.observed_trips, service_coverage.expected_trips), 1.0)
        ) >= 0.9 then 'good'
        when least(
            coalesce(status_spine.completeness_ratio, 1.0),
            coalesce(safe_divide(service_coverage.observed_trips, service_coverage.expected_trips), 1.0)
        ) >= 0.7 then 'usable'
        when least(
            coalesce(status_spine.completeness_ratio, 1.0),
            coalesce(safe_divide(service_coverage.observed_trips, service_coverage.expected_trips), 1.0)
        ) > 0 then 'patchy'
        else 'missing'
    end as health_label,
    row_number() over (partition by status_spine.mode order by status_spine.service_date desc) as status_rank_desc,
    cast(null as timestamp) as last_export_at,
    current_timestamp() as status_generated_at
from status_spine
left join staged_pings
    on status_spine.service_date = staged_pings.service_date
    and status_spine.mode = staged_pings.mode
left join trips
    on status_spine.service_date = trips.service_date
    and status_spine.mode = trips.mode
left join service_coverage
    on status_spine.service_date = service_coverage.service_date
    and status_spine.mode = service_coverage.mode
left join stop_arrivals
    on status_spine.service_date = stop_arrivals.service_date
    and status_spine.mode = stop_arrivals.mode
cross join latest_gtfs_snapshot
