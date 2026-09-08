-- Retained as the independent full-history migration/audit reference; not a nightly dependency.
{{ gtfs_trip_schedule_history('select * from ' ~ ref('int_gtfs_processing_snapshot')) }}
