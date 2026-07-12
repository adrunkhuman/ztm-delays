"""Versioned Arrow boundary schemas."""

import pyarrow as pa

RAW_GPS_SCHEMA_VERSION = "raw-gps-v1"
NORMALIZED_GPS_SCHEMA_VERSION = "normalized-gps-v1"
SCHEDULE_SCHEMA_VERSION = "schedule-v1"
SEMANTICS_SCHEMA_VERSION = "stop-semantics-v2"
EXECUTION_SCHEMA_VERSION = "duty-execution-v1"
OPERATIONAL_CROSSING_SCHEMA_VERSION = "operational-stop-crossing-v1"
PASSENGER_ARRIVAL_SCHEMA_VERSION = "passenger-stop-arrival-v1"
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

STOP_CROSSING_SCHEMA = pa.schema(
    [
        pa.field("gtfs_snapshot_id", pa.string()),
        pa.field("service_date", pa.date32()),
        pa.field("processing_date", pa.date32()),
        pa.field("duty_chain_id", pa.string()),
        pa.field("trip_id", pa.string()),
        pa.field("line", pa.string()),
        pa.field("brigade", pa.string()),
        pa.field("mode", pa.string()),
        pa.field("vehicle_number", pa.string()),
        pa.field("vehicle_type", pa.int64()),
        pa.field("stop_id", pa.string()),
        pa.field("stop_group_id", pa.string()),
        pa.field("stop_sequence", pa.int64()),
        pa.field("pickup_type", pa.int64()),
        pa.field("drop_off_type", pa.int64()),
        pa.field("stop_service_class", pa.string()),
        pa.field("stop_execution_class", pa.string()),
        pa.field("classification_confidence", pa.string()),
        pa.field("classification_reason", pa.string()),
        pa.field("classification_evidence", pa.list_(pa.string())),
        pa.field("are_passenger_boundaries_settled", pa.bool_()),
        pa.field("is_passenger_stop", pa.bool_()),
        pa.field("scheduled_arrival_time", pa.timestamp("us", tz="UTC")),
        pa.field("scheduled_departure_time", pa.timestamp("us", tz="UTC")),
        pa.field("actual_arrival_time", pa.timestamp("us", tz="UTC")),
        pa.field("arrival_delay_seconds", pa.int64()),
        pa.field("segment_start_time", pa.timestamp("us", tz="UTC")),
        pa.field("segment_end_time", pa.timestamp("us", tz="UTC")),
        pa.field("segment_duration_seconds", pa.int64()),
        pa.field("segment_distance_m", pa.float64()),
        pa.field("segment_start_distance_m", pa.float64()),
        pa.field("segment_end_distance_m", pa.float64()),
        pa.field("stop_match_radius_m", pa.float64()),
        pa.field("detection_method", pa.string()),
        pa.field("alignment_confidence", pa.string()),
        pa.field("alignment_evidence", pa.list_(pa.string())),
    ]
)

# Passenger arrivals intentionally retain the operational schema. Consumers can switch
# from this adapter to fct_stop_arrival without losing direct-crossing diagnostics.
PASSENGER_STOP_ARRIVAL_SCHEMA = STOP_CROSSING_SCHEMA
