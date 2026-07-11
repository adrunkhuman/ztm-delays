"""Versioned Arrow boundary schemas."""

import pyarrow as pa

RAW_GPS_SCHEMA_VERSION = "raw-gps-v1"
NORMALIZED_GPS_SCHEMA_VERSION = "normalized-gps-v1"
SCHEDULE_SCHEMA_VERSION = "schedule-v1"
SEMANTICS_SCHEMA_VERSION = "stop-semantics-v2"
EXECUTION_SCHEMA_VERSION = "duty-execution-v1"
MANIFEST_VERSION = 1

RAW_GPS_SCHEMA = pa.schema(
    [
        pa.field("Lines", pa.string()),
        pa.field("Brigade", pa.string()),
        pa.field("Lat", pa.float64()),
        pa.field("Lon", pa.float64()),
        pa.field("Time", pa.timestamp("us", tz="UTC")),
        pa.field("VehicleNumber", pa.string()),
        pa.field("vehicle_type", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)
NORMALIZED_GPS_SCHEMA = pa.schema(
    [
        pa.field("line", pa.string()),
        pa.field("brigade", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("gps_time", pa.timestamp("us", tz="UTC")),
        pa.field("vehicle_number", pa.string()),
        pa.field("vehicle_type", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("gps_date", pa.date32()),
    ]
)

TRAVERSAL_EVIDENCE_SCHEMA = pa.schema(
    [
        pa.field("service_date", pa.date32()),
        pa.field("processing_date", pa.date32()),
        pa.field("gtfs_snapshot_id", pa.string()),
        pa.field("duty_chain_id", pa.string()),
        pa.field("trip_id", pa.string()),
        pa.field("vehicle_number", pa.string()),
        pa.field("vehicle_type", pa.int64()),
        pa.field("candidate_kind", pa.string()),
        pa.field("traversal_id", pa.string()),
        pa.field("origin_event_time", pa.timestamp("us", tz="UTC")),
        pa.field("departure_event_time", pa.timestamp("us", tz="UTC")),
        pa.field("destination_event_time", pa.timestamp("us", tz="UTC")),
        pa.field("source_ping_start_time", pa.timestamp("us", tz="UTC")),
        pa.field("source_ping_end_time", pa.timestamp("us", tz="UTC")),
        pa.field("source_ping_count", pa.int64()),
    ]
)

DUTY_EXECUTION_SCHEMA = pa.schema(
    [
        pa.field("service_date", pa.date32()),
        pa.field("processing_date", pa.date32()),
        pa.field("gtfs_snapshot_id", pa.string()),
        pa.field("duty_chain_id", pa.string()),
        pa.field("duty_chain_source", pa.string()),
        pa.field("duty_chain_source_id", pa.string()),
        pa.field("trip_order", pa.int64()),
        pa.field("trip_id", pa.string()),
        pa.field("line", pa.string()),
        pa.field("brigade", pa.string()),
        pa.field("mode", pa.string()),
        pa.field("scheduled_start_time", pa.timestamp("us", tz="UTC")),
        pa.field("scheduled_end_time", pa.timestamp("us", tz="UTC")),
        pa.field("are_passenger_boundaries_settled", pa.bool_()),
        pa.field("has_terminal_coordinates", pa.bool_()),
        pa.field("vehicle_number", pa.string()),
        pa.field("vehicle_type", pa.int64()),
        pa.field("execution_status", pa.string()),
        pa.field("confidence", pa.string()),
        pa.field("execution_reason", pa.string()),
        pa.field("execution_evidence", pa.list_(pa.string())),
        pa.field("competing_candidate_count", pa.int64()),
        pa.field("source_ping_start_time", pa.timestamp("us", tz="UTC")),
        pa.field("source_ping_end_time", pa.timestamp("us", tz="UTC")),
        pa.field("source_ping_count", pa.int64()),
        pa.field("ownership_interval_start_time", pa.timestamp("us", tz="UTC")),
        pa.field("ownership_interval_end_time", pa.timestamp("us", tz="UTC")),
    ]
)
