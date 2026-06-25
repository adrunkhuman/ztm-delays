from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypedDict, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import storage

if TYPE_CHECKING:
    from collections.abc import Callable

API_URL = "https://dane.um.warszawa.pl/api/action/get_ztm_lokalizacja_pojazdow"
WARSAW_TZ = ZoneInfo("Europe/Warsaw")
LOGGER = logging.getLogger(__name__)

SCHEMA = pa.schema(
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

VEHICLE_TYPES = {
    "1": (1, "bus"),
    "bus": (1, "bus"),
    "buses": (1, "bus"),
    "2": (2, "tram"),
    "tram": (2, "tram"),
    "trams": (2, "tram"),
}


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded from environment variables."""

    api_token: str
    vehicle_type_id: int
    vehicle_type_name: str
    gcs_bucket: str
    gcs_prefix: str
    poll_interval_seconds: float
    api_timeout_seconds: float


class GpsRow(TypedDict):
    """Parsed GPS row matching the raw Parquet schema before file-level ingestion fields."""

    Lines: str
    Brigade: str
    Lat: float
    Lon: float
    Time: datetime
    VehicleNumber: str
    vehicle_type: int


def main() -> int:
    """Run the GPS poller daemon."""
    _configure_logging()
    config = _load_config()
    stop_requested = _build_signal_handler()

    LOGGER.info("starting GPS poller", extra={"vehicle_type": config.vehicle_type_name})

    session = requests.Session()
    storage_client = storage.Client()
    bucket = storage_client.bucket(config.gcs_bucket)
    buffers: dict[datetime, list[GpsRow]] = defaultdict(list)

    while not stop_requested():
        loop_started = time.monotonic()
        current_hour = _hour_key(datetime.now(WARSAW_TZ))

        try:
            rows = _poll_api(session, config)
            for row in rows:
                buffers[_hour_key(row["Time"].astimezone(WARSAW_TZ))].append(row)
            LOGGER.info("poll succeeded", extra={"rows": len(rows), "vehicle_type": config.vehicle_type_name})
        except requests.RequestException:
            LOGGER.exception("API request failed")
        except json.JSONDecodeError:
            LOGGER.exception("API returned malformed JSON")
        except ValueError:
            LOGGER.exception("API returned invalid payload")

        _flush_closed_hours(bucket, config, buffers, current_hour)
        _sleep_remaining(config.poll_interval_seconds, loop_started, stop_requested)

    _flush_closed_hours(bucket, config, buffers, _hour_key(datetime.now(WARSAW_TZ)))
    LOGGER.info("poller stopped")
    return 0


def _configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )


def _load_config() -> Config:
    vehicle_type = os.environ.get("VEHICLE_TYPE", "").strip().lower()
    if vehicle_type not in VEHICLE_TYPES:
        raise RuntimeError("VEHICLE_TYPE must be one of: bus, tram, 1, 2")

    api_token = os.environ.get("WARSAW_API_TOKEN", "").strip()
    if not api_token:
        raise RuntimeError("WARSAW_API_TOKEN is required")

    vehicle_type_id, vehicle_type_name = VEHICLE_TYPES[vehicle_type]
    return Config(
        api_token=api_token,
        vehicle_type_id=vehicle_type_id,
        vehicle_type_name=vehicle_type_name,
        gcs_bucket=os.getenv("GCS_BUCKET", "ztm-analytics-bucket"),
        gcs_prefix=os.getenv("GCS_PREFIX", "raw/gps").strip("/"),
        poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "10")),
        api_timeout_seconds=float(os.getenv("API_TIMEOUT_SECONDS", "5")),
    )


def _build_signal_handler() -> Callable[[], bool]:
    stopped = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    return lambda: stopped


def _poll_api(session: requests.Session, config: Config) -> list[GpsRow]:
    response = session.post(
        API_URL,
        headers={"Authorization": config.api_token},
        json={"type": config.vehicle_type_id},
        timeout=config.api_timeout_seconds,
    )
    response.raise_for_status()

    payload: object = response.json()
    records = _extract_records(payload)
    if not records:
        LOGGER.warning("API returned no records", extra={"vehicle_type": config.vehicle_type_name})
        return []

    parsed_rows = []
    for record in records:
        row = _parse_record(record, config.vehicle_type_id)
        if row is not None:
            parsed_rows.append(row)
    return parsed_rows


def _extract_records(payload: object) -> list[dict[str, object]]:
    records = payload.get("result", payload) if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise TypeError("expected API payload result to be a list")
    return [cast("dict[str, object]", record) for record in records if isinstance(record, dict)]


def _parse_record(record: dict[str, object], vehicle_type_id: int) -> GpsRow | None:
    try:
        gps_time = _parse_warsaw_time(str(record["Time"]))
        return {
            "Lines": str(record["Lines"]),
            "Brigade": str(record["Brigade"]),
            "Lat": float(str(record["Lat"])),
            "Lon": float(str(record["Lon"])),
            "Time": gps_time,
            "VehicleNumber": str(record["VehicleNumber"]),
            "vehicle_type": vehicle_type_id,
        }
    except KeyError, TypeError, ValueError:
        LOGGER.warning("skipping invalid record", extra={"record_keys": sorted(record)})
        return None


def _parse_warsaw_time(value: str) -> datetime:
    local_time = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=WARSAW_TZ)
    return local_time.astimezone(UTC)


def _hour_key(value: datetime) -> datetime:
    return value.astimezone(WARSAW_TZ).replace(minute=0, second=0, microsecond=0)


def _flush_closed_hours(
    bucket: storage.Bucket,
    config: Config,
    buffers: dict[datetime, list[GpsRow]],
    current_hour: datetime,
) -> None:
    closed_hours = sorted(buffer_hour for buffer_hour in buffers if buffer_hour < current_hour)
    for buffer_hour in closed_hours:
        rows = buffers.pop(buffer_hour)
        if not rows:
            continue
        _upload_hour(bucket, config, buffer_hour, rows)


def _upload_hour(
    bucket: storage.Bucket,
    config: Config,
    buffer_hour: datetime,
    rows: list[GpsRow],
) -> None:
    ingested_at = datetime.now(UTC)
    upload_rows = [row | {"ingested_at": ingested_at} for row in rows]
    table = pa.Table.from_pylist(upload_rows, schema=SCHEMA)

    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression="snappy")
    parquet_buffer.seek(0)

    path = (
        f"{config.gcs_prefix}/vehicle_type={config.vehicle_type_name}/"
        f"date={buffer_hour:%Y-%m-%d}/hour={buffer_hour:%H}.parquet"
    )
    bucket.blob(path).upload_from_file(parquet_buffer, content_type="application/octet-stream")
    LOGGER.info("uploaded hourly parquet", extra={"gcs_path": f"gs://{config.gcs_bucket}/{path}", "rows": len(rows)})


def _sleep_remaining(interval_seconds: float, loop_started: float, stop_requested: Callable[[], bool]) -> None:
    remaining = interval_seconds - (time.monotonic() - loop_started)
    deadline = time.monotonic() + max(0.0, remaining)
    while not stop_requested() and time.monotonic() < deadline:
        time.sleep(min(0.5, deadline - time.monotonic()))


if __name__ == "__main__":
    raise SystemExit(main())
