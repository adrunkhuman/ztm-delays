"""Versioned Arrow boundary schemas."""

import pyarrow as pa

RAW_GPS_SCHEMA_VERSION = "raw-gps-v1"
NORMALIZED_GPS_SCHEMA_VERSION = "normalized-gps-v1"
SCHEDULE_SCHEMA_VERSION = "schedule-v1"
SEMANTICS_SCHEMA_VERSION = "stop-semantics-v1"
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
