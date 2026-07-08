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

with trips as (
    select *
    from {{ ref('fct_trip') }}
    where service_date = date('{{ processing_date }}')
      and mode in ('bus', 'tram')
),

profiles as (
    select
        service_date,
        trip_id,
        vehicle_number,
        array_agg(delay_seconds ignore nulls order by stop_sequence) as delay_profile
    from {{ ref('fct_expected_stop_event') }}
    where service_date = date('{{ processing_date }}')
      and observation_status = 'observed'
    group by service_date, trip_id, vehicle_number
),

profile_scores as (
    select
        *,
        (
            select max(abs(next_delay - delay))
            from unnest(delay_profile) as delay with offset pos
            join unnest(delay_profile) as next_delay with offset next_pos
                on next_pos = pos + 1
        ) as erratic_score
    from profiles
),

joined as (
    select
        trips.*,
        coalesce(trips.trip_headsign, trips.destination_stop_name, trips.route_short_name) as route_label,
        coalesce(profile_scores.delay_profile, cast([] as array<float64>)) as delay_profile,
        coalesce(profile_scores.erratic_score, 0) as erratic_score
    from trips
    left join profile_scores
        on trips.service_date = profile_scores.service_date
        and trips.trip_id = profile_scores.trip_id
        and trips.vehicle_number = profile_scores.vehicle_number
)

select
    service_date,
    gps_date,
    trip_id,
    vehicle_number,
    line,
    route_short_name,
    mode,
    brigade,
    direction_id,
    trip_headsign,
    route_label,
    origin_stop_name,
    destination_stop_name,
    scheduled_start_time,
    scheduled_end_time,
    actual_start_time,
    actual_end_time,
    start_delay_seconds,
    end_delay_seconds,
    stops_expected,
    stops_detected,
    trip_quality,
    delay_profile,
    erratic_score,
    row_number() over (partition by service_date, mode, line order by scheduled_start_time, trip_id, vehicle_number) as departure_rank,
    row_number() over (partition by service_date, mode, line order by end_delay_seconds desc, scheduled_start_time) as line_end_delay_rank,
    row_number() over (partition by service_date, mode, line order by erratic_score desc, scheduled_start_time) as line_erratic_rank,
    if(trip_quality = 'complete', row_number() over (partition by service_date, mode, trip_quality order by abs(end_delay_seconds) desc, end_delay_seconds desc, scheduled_start_time), null) as landing_worst_rank,
    if(trip_quality = 'complete', row_number() over (partition by service_date, mode, trip_quality order by abs(end_delay_seconds), scheduled_start_time), null) as landing_best_rank,
    if(trip_quality = 'complete', row_number() over (partition by service_date, mode, trip_quality order by erratic_score desc, end_delay_seconds desc), null) as landing_erratic_rank
from joined
