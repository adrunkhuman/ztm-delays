select *
from {{ ref('agg_time_period') }}
where (period_start_date is null or {{ period_partition_filter() }})
  and (
      period_type is null
      or period_id is null
      or period_start_date is null
      or source_start_date is null
      or source_end_date is null
      or is_partial_period is null
      or day_class_type is null
      or day_class is null
      or mode is null
      or hour_bracket is null
      or n <= 0
      or mean_delay_seconds is null
      or p10_delay_seconds is null
      or median_delay_seconds is null
      or p50_delay_seconds is null
      or p90_delay_seconds is null
      or on_time_rate is null
      or delay_histogram is null
      or gtfs_snapshot_ids is null
      or array_length(gtfs_snapshot_ids) = 0
      or (period_type = 'schedule_version' and (line is null or direction_id is null or schedule_version_id is null))
      or (period_type = 'month' and (line is not null or direction_id is not null or schedule_version_id is not null))
  )
