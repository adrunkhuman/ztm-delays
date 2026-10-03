-- Per-month stop-to-stop delay change, one JSON record per row (avoids BigQuery's row-size limit).
-- Adjacency is established BEFORE filtering observations, so a missing stop is never bridged.
-- Net change is downstream signed arrival delay minus upstream signed arrival delay.
-- Daytime requires BOTH scheduled endpoints in [06:00, 22:00), Europe/Warsaw,
-- on the same local calendar date. Disjoint buckets preserve all-day statistics.
WITH expected AS (
  SELECT gtfs_snapshot_id, service_date, trip_id, vehicle_number,
    shape_id, line, mode, direction_id,
    stop_id, stop_sequence, stop_name, stop_post_code, stop_lat, stop_lon,
    is_observed, delay_seconds, scheduled_arrival_time, actual_arrival_time,
    IF(EXTRACT(DAYOFWEEK FROM service_date) IN (1, 7), 'weekend', 'weekday') AS period
  FROM `{{ fct_expected_stop_event }}`
  WHERE service_date BETWEEN @month_start AND @month_end
    AND trip_quality = 'complete' AND mode IN ('bus', 'tram')
), ordered AS (
  SELECT *, LAG(STRUCT(stop_id, stop_sequence, stop_name, stop_post_code,
      stop_lat, stop_lon, is_observed, delay_seconds, scheduled_arrival_time,
      actual_arrival_time)) OVER (
      PARTITION BY gtfs_snapshot_id, service_date, trip_id, vehicle_number
      ORDER BY stop_sequence) AS previous
  FROM expected
), candidates AS (
  SELECT *,
    IF(COALESCE(
      DATE(scheduled_arrival_time, 'Europe/Warsaw') = DATE(previous.scheduled_arrival_time, 'Europe/Warsaw')
      AND TIME(scheduled_arrival_time, 'Europe/Warsaw') >= TIME '06:00:00'
      AND TIME(scheduled_arrival_time, 'Europe/Warsaw') < TIME '22:00:00'
      AND TIME(previous.scheduled_arrival_time, 'Europe/Warsaw') >= TIME '06:00:00'
      AND TIME(previous.scheduled_arrival_time, 'Europe/Warsaw') < TIME '22:00:00', FALSE),
      'daytime', 'outside') AS time_window,
    COALESCE(is_observed AND previous.is_observed
      AND delay_seconds IS NOT NULL AND previous.delay_seconds IS NOT NULL, FALSE) AS has_endpoints,
    COALESCE(stop_lat BETWEEN -90 AND 90 AND stop_lon BETWEEN -180 AND 180
      AND previous.stop_lat BETWEEN -90 AND 90 AND previous.stop_lon BETWEEN -180 AND 180
      AND NOT IS_NAN(stop_lat) AND NOT IS_NAN(stop_lon)
      AND NOT IS_NAN(previous.stop_lat) AND NOT IS_NAN(previous.stop_lon)
      AND NOT IS_INF(stop_lat) AND NOT IS_INF(stop_lon)
      AND NOT IS_INF(previous.stop_lat) AND NOT IS_INF(previous.stop_lon), FALSE) AS has_coordinates,
    COALESCE(actual_arrival_time >= previous.actual_arrival_time
      AND scheduled_arrival_time >= previous.scheduled_arrival_time, FALSE) AS has_ordered_times,
    shape_id IS NOT NULL AND shape_id != '' AS has_shape_id,
    delay_seconds - previous.delay_seconds AS delta_seconds,
    TIMESTAMP_DIFF(actual_arrival_time, previous.actual_arrival_time, SECOND) AS actual_elapsed_seconds,
    TIMESTAMP_DIFF(scheduled_arrival_time, previous.scheduled_arrival_time, SECOND) AS scheduled_elapsed_seconds
  FROM ordered
  WHERE previous IS NOT NULL
), classified AS (
  SELECT *,
    COALESCE(ABS(delta_seconds - (actual_elapsed_seconds - scheduled_elapsed_seconds)) <= 1, FALSE) AS has_consistent_delta
  FROM candidates
), usable AS (
  SELECT * FROM classified
  WHERE has_endpoints AND has_coordinates AND has_ordered_times
    AND has_shape_id AND has_consistent_delta
), segments AS (
  SELECT period, time_window, gtfs_snapshot_id, shape_id, line, mode, direction_id,
    previous.stop_id AS from_stop_id, stop_id AS to_stop_id,
    previous.stop_sequence AS from_stop_sequence, stop_sequence AS to_stop_sequence,
    ANY_VALUE(previous.stop_name) AS from_stop_name, ANY_VALUE(stop_name) AS to_stop_name,
    ANY_VALUE(previous.stop_post_code) AS from_stop_post_code, ANY_VALUE(stop_post_code) AS to_stop_post_code,
    ANY_VALUE(previous.stop_lat) AS from_lat, ANY_VALUE(previous.stop_lon) AS from_lon,
    ANY_VALUE(stop_lat) AS to_lat, ANY_VALUE(stop_lon) AS to_lon,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT service_date) AS observed_days,
    ARRAY_AGG(DISTINCT CAST(service_date AS STRING) ORDER BY CAST(service_date AS STRING)) AS observed_service_dates,
    SUM(delta_seconds) AS sum_delta_seconds,
    SUM(GREATEST(delta_seconds, 0)) AS sum_gain_seconds,
    SUM(GREATEST(-delta_seconds, 0)) AS sum_recovery_seconds,
    COUNTIF(delta_seconds > 0) AS gain_count,
    COUNTIF(delta_seconds < 0) AS recovery_count,
    COUNTIF(delta_seconds = 0) AS unchanged_count,
    AVG(previous.delay_seconds) AS mean_from_delay_seconds,
    AVG(delay_seconds) AS mean_to_delay_seconds,
    AVG(scheduled_elapsed_seconds) AS mean_scheduled_elapsed_seconds,
    AVG(actual_elapsed_seconds) AS mean_actual_elapsed_seconds
  FROM usable
  GROUP BY period, time_window, gtfs_snapshot_id, shape_id, line, mode, direction_id,
    from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence
), coverage AS (
  SELECT service_date, period,
    COUNT(*) AS candidate_pairs,
    COUNTIF(has_endpoints AND has_coordinates AND has_ordered_times AND has_shape_id AND has_consistent_delta) AS usable_pairs,
    COUNTIF(NOT has_endpoints) AS missing_endpoint_count,
    COUNTIF(NOT has_coordinates) AS invalid_coordinate_count,
    COUNTIF(NOT has_ordered_times) AS invalid_time_count,
    COUNTIF(NOT has_shape_id) AS missing_shape_id_count,
    COUNTIF(NOT has_consistent_delta) AS inconsistent_delta_count,
    COUNTIF(time_window = 'daytime') AS daytime_candidate_pairs,
    COUNTIF(time_window = 'daytime' AND has_endpoints AND has_coordinates AND has_ordered_times AND has_shape_id AND has_consistent_delta) AS daytime_usable_pairs,
    COUNTIF(time_window = 'outside') AS outside_candidate_pairs,
    COUNTIF(time_window = 'outside' AND has_endpoints AND has_coordinates AND has_ordered_times AND has_shape_id AND has_consistent_delta) AS outside_usable_pairs,
    COUNTIF(time_window = 'daytime' AND NOT has_endpoints) AS daytime_missing_endpoint_count,
    COUNTIF(time_window = 'daytime' AND NOT has_coordinates) AS daytime_invalid_coordinate_count,
    COUNTIF(time_window = 'daytime' AND NOT has_ordered_times) AS daytime_invalid_time_count,
    COUNTIF(time_window = 'daytime' AND NOT has_shape_id) AS daytime_missing_shape_id_count,
    COUNTIF(time_window = 'daytime' AND NOT has_consistent_delta) AS daytime_inconsistent_delta_count
  FROM classified
  GROUP BY service_date, period
)
-- One record per result row avoids BigQuery's maximum-row-size limit.
SELECT 'segment' AS kind, TO_JSON_STRING(record) AS payload FROM segments AS record
UNION ALL
SELECT 'coverage' AS kind, TO_JSON_STRING(record) AS payload FROM coverage AS record;
