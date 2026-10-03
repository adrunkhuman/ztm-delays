-- Pool identical GTFS geometry + ordered stop patterns across snapshot/shape IDs.
-- Statistics come from segment_statistics.sql's result table. Route geometry is rebuilt
-- from ordered raw points: BigQuery GEOGRAPHY normalization loses repeated traversal.
WITH records AS (
  SELECT kind, PARSE_JSON(payload, wide_number_mode => 'round') AS record
  FROM `{{ segment_statistics }}`
), segments AS (
  SELECT
    JSON_VALUE(record, '$.period') AS period,
    JSON_VALUE(record, '$.time_window') AS time_window,
    JSON_VALUE(record, '$.gtfs_snapshot_id') AS gtfs_snapshot_id,
    JSON_VALUE(record, '$.shape_id') AS shape_id,
    JSON_VALUE(record, '$.line') AS line,
    JSON_VALUE(record, '$.mode') AS mode,
    SAFE_CAST(JSON_VALUE(record, '$.direction_id') AS INT64) AS direction_id,
    JSON_VALUE(record, '$.from_stop_id') AS from_stop_id,
    JSON_VALUE(record, '$.to_stop_id') AS to_stop_id,
    CAST(JSON_VALUE(record, '$.from_stop_sequence') AS INT64) AS from_stop_sequence,
    CAST(JSON_VALUE(record, '$.to_stop_sequence') AS INT64) AS to_stop_sequence,
    JSON_VALUE(record, '$.from_stop_name') AS from_stop_name,
    JSON_VALUE(record, '$.to_stop_name') AS to_stop_name,
    JSON_VALUE(record, '$.from_stop_post_code') AS from_stop_post_code,
    JSON_VALUE(record, '$.to_stop_post_code') AS to_stop_post_code,
    CAST(JSON_VALUE(record, '$.from_lat') AS FLOAT64) AS from_lat,
    CAST(JSON_VALUE(record, '$.from_lon') AS FLOAT64) AS from_lon,
    CAST(JSON_VALUE(record, '$.to_lat') AS FLOAT64) AS to_lat,
    CAST(JSON_VALUE(record, '$.to_lon') AS FLOAT64) AS to_lon,
    CAST(JSON_VALUE(record, '$.observation_count') AS INT64) AS observation_count,
    JSON_VALUE_ARRAY(record, '$.observed_service_dates') AS observed_service_dates,
    CAST(JSON_VALUE(record, '$.sum_delta_seconds') AS INT64) AS sum_delta_seconds,
    CAST(JSON_VALUE(record, '$.sum_gain_seconds') AS INT64) AS sum_gain_seconds,
    CAST(JSON_VALUE(record, '$.sum_recovery_seconds') AS INT64) AS sum_recovery_seconds,
    CAST(JSON_VALUE(record, '$.gain_count') AS INT64) AS gain_count,
    CAST(JSON_VALUE(record, '$.recovery_count') AS INT64) AS recovery_count,
    CAST(JSON_VALUE(record, '$.unchanged_count') AS INT64) AS unchanged_count,
    CAST(JSON_VALUE(record, '$.mean_from_delay_seconds') AS FLOAT64) AS mean_from_delay_seconds,
    CAST(JSON_VALUE(record, '$.mean_to_delay_seconds') AS FLOAT64) AS mean_to_delay_seconds,
    CAST(JSON_VALUE(record, '$.mean_scheduled_elapsed_seconds') AS FLOAT64) AS mean_scheduled_elapsed_seconds,
    CAST(JSON_VALUE(record, '$.mean_actual_elapsed_seconds') AS FLOAT64) AS mean_actual_elapsed_seconds,
    TO_JSON_STRING(STRUCT(
      JSON_VALUE(record, '$.gtfs_snapshot_id'), JSON_VALUE(record, '$.shape_id'),
      JSON_VALUE(record, '$.line'), JSON_VALUE(record, '$.direction_id'))) AS source_key
  FROM records WHERE kind = 'segment'
), used_shape_keys AS (
  SELECT DISTINCT JSON_VALUE(record, '$.gtfs_snapshot_id') AS gtfs_snapshot_id,
    JSON_VALUE(record, '$.shape_id') AS shape_id
  FROM records WHERE kind = 'segment'
), raw_shape_points AS (
  SELECT raw.gtfs_snapshot_id, raw.shape_id,
    SAFE_CAST(raw.shape_pt_sequence AS INT64) AS sequence,
    SAFE_CAST(raw.shape_pt_lon AS FLOAT64) AS lon,
    SAFE_CAST(raw.shape_pt_lat AS FLOAT64) AS lat
  FROM `{{ raw_gtfs_shapes }}` AS raw
  INNER JOIN used_shape_keys USING (gtfs_snapshot_id, shape_id)
  -- A literal snapshot list prunes the gtfs_snapshot_id clusters; the join alone scans every snapshot.
  WHERE raw.gtfs_snapshot_id IN UNNEST(@snapshot_ids)
), shape_geometry AS (
  SELECT gtfs_snapshot_id, shape_id,
    CONCAT('{"type":"LineString","coordinates":[',
      STRING_AGG(FORMAT('[%.8f,%.8f]', lon, lat), ',' ORDER BY sequence), ']}') AS geometry
  FROM raw_shape_points
  WHERE lat BETWEEN 51.0 AND 53.5 AND lon BETWEEN 19.5 AND 22.5
  GROUP BY gtfs_snapshot_id, shape_id
  HAVING COUNT(*) >= 2
), stops AS (
  SELECT DISTINCT source_key, from_stop_sequence AS sequence, from_stop_id AS stop_id,
    from_lat AS lat, from_lon AS lon FROM segments
  UNION DISTINCT
  SELECT DISTINCT source_key, to_stop_sequence, to_stop_id, to_lat, to_lon FROM segments
), patterns AS (
  SELECT source_key, TO_JSON_STRING(ARRAY_AGG(STRUCT(sequence, stop_id, lat, lon)
    ORDER BY sequence, stop_id, lat, lon)) AS pattern
  FROM stops GROUP BY source_key
), geometry_mapping AS (
  SELECT DISTINCT segments.source_key,
    TO_HEX(SHA256(CONCAT(
      COALESCE(shape_geometry.geometry, CONCAT('missing:', segments.gtfs_snapshot_id, ':', segments.shape_id)),
      '|', patterns.pattern))) AS pooled_shape_id,
    shape_geometry.geometry
  FROM segments
  INNER JOIN patterns USING (source_key)
  LEFT JOIN shape_geometry USING (gtfs_snapshot_id, shape_id)
), assigned AS (
  SELECT segments.* EXCEPT(gtfs_snapshot_id, shape_id), geometry_mapping.pooled_shape_id
  FROM segments INNER JOIN geometry_mapping USING (source_key)
), aggregated AS (
  SELECT period, time_window, '__identical_geometry_and_pattern__' AS gtfs_snapshot_id,
    pooled_shape_id AS shape_id, line, mode, direction_id,
    from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence,
    ANY_VALUE(from_stop_name) AS from_stop_name, ANY_VALUE(to_stop_name) AS to_stop_name,
    ANY_VALUE(from_stop_post_code) AS from_stop_post_code, ANY_VALUE(to_stop_post_code) AS to_stop_post_code,
    ANY_VALUE(from_lat) AS from_lat, ANY_VALUE(from_lon) AS from_lon,
    ANY_VALUE(to_lat) AS to_lat, ANY_VALUE(to_lon) AS to_lon,
    SUM(observation_count) AS observation_count,
    SUM(sum_delta_seconds) AS sum_delta_seconds,
    SUM(sum_gain_seconds) AS sum_gain_seconds,
    SUM(sum_recovery_seconds) AS sum_recovery_seconds,
    SUM(gain_count) AS gain_count, SUM(recovery_count) AS recovery_count,
    SUM(unchanged_count) AS unchanged_count,
    SUM(mean_from_delay_seconds * observation_count) / SUM(observation_count) AS mean_from_delay_seconds,
    SUM(mean_to_delay_seconds * observation_count) / SUM(observation_count) AS mean_to_delay_seconds,
    SUM(mean_scheduled_elapsed_seconds * observation_count) / SUM(observation_count) AS mean_scheduled_elapsed_seconds,
    SUM(mean_actual_elapsed_seconds * observation_count) / SUM(observation_count) AS mean_actual_elapsed_seconds,
    ARRAY_CONCAT_AGG(observed_service_dates) AS dates
  FROM assigned
  GROUP BY period, time_window, pooled_shape_id, line, mode, direction_id,
    from_stop_id, to_stop_id, from_stop_sequence, to_stop_sequence
), pooled_segments AS (
  SELECT * EXCEPT(dates),
    -- Unique days across pooled rows; the per-row date lists themselves would double the download.
    ARRAY_LENGTH(ARRAY(SELECT DISTINCT day FROM UNNEST(dates) AS day)) AS observed_days
  FROM aggregated
), pooled_shapes AS (
  SELECT DISTINCT '__identical_geometry_and_pattern__' AS gtfs_snapshot_id,
    pooled_shape_id AS shape_id, geometry
  FROM geometry_mapping WHERE geometry IS NOT NULL
)
SELECT 'segment' AS kind, TO_JSON_STRING(record) AS payload FROM pooled_segments AS record
UNION ALL
SELECT 'shape' AS kind, TO_JSON_STRING(record) AS payload FROM pooled_shapes AS record
UNION ALL
SELECT kind, TO_JSON_STRING(record) AS payload FROM records WHERE kind = 'coverage';
