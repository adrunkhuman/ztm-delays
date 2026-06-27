from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, TypedDict, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.api_core.exceptions import GoogleAPIError, PreconditionFailed
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


@dataclass(frozen=True)
class VehicleType:
    """Warsaw ZTM API vehicle type identifier."""

    id: int
    name: str


VEHICLE_TYPES = (VehicleType(1, "bus"), VehicleType(2, "tram"))


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded from environment variables."""

    api_token: str
    vehicle_types: tuple[VehicleType, ...]
    gcs_bucket: str
    gcs_prefix: str
    poll_interval_seconds: float
    api_timeout_seconds: float
    api_proxy: str | None
    max_ping_age_seconds: float
    future_ping_tolerance_seconds: float
    partial_flush_interval_seconds: float
    flush_lag_seconds: float
    run_once: bool
    no_upload: bool


@dataclass(frozen=True)
class UploadContext:
    """Upload dependencies for one vehicle type partition."""

    bucket: storage.Bucket
    config: Config
    vehicle_type_name: str


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
    config = _load_config(_parse_args())
    stop_requested = _build_signal_handler()

    vehicle_type_names = ",".join(vehicle_type.name for vehicle_type in config.vehicle_types)
    LOGGER.info(
        "starting GPS poller vehicle_types=%s run_once=%s no_upload=%s",
        vehicle_type_names,
        config.run_once,
        config.no_upload,
    )

    session = requests.Session()
    bucket = None if config.no_upload else storage.Client().bucket(config.gcs_bucket)
    buffers: dict[str, dict[datetime, list[GpsRow]]] = {
        vehicle_type.name: defaultdict(list) for vehicle_type in config.vehicle_types
    }
    last_partial_flush = 0.0

    while not stop_requested():
        loop_started = time.monotonic()

        for vehicle_type in config.vehicle_types:
            _poll_vehicle_type(session, config, vehicle_type, buffers[vehicle_type.name])

        if config.no_upload:
            buffered_rows = sum(len(rows) for vehicle_buffers in buffers.values() for rows in vehicle_buffers.values())
            LOGGER.info("upload disabled buffered_rows=%d", buffered_rows)
        else:
            now = datetime.now(WARSAW_TZ)
            if loop_started - last_partial_flush >= config.partial_flush_interval_seconds:
                flush_before = now - timedelta(seconds=config.flush_lag_seconds)
                if _flush_vehicle_buffers(cast("storage.Bucket", bucket), config, buffers, flush_before=flush_before):
                    last_partial_flush = loop_started

        if config.run_once:
            break

        _sleep_remaining(config.poll_interval_seconds, loop_started, stop_requested)

    if not config.no_upload and not _flush_vehicle_buffers(
        cast("storage.Bucket", bucket), config, buffers, flush_all=True
    ):
        LOGGER.error("poller stopped with buffered rows after failed shutdown flush")
        return 1
    LOGGER.info("poller stopped")
    return 0


def _configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )


def _parse_args() -> Namespace:
    parser = ArgumentParser(description="Poll Warsaw ZTM GPS data and write hourly Parquet files to GCS.")
    parser.add_argument("--once", action="store_true", help="poll once and exit")
    parser.add_argument("--no-upload", action="store_true", help="do not initialize GCS or upload files")
    return parser.parse_args()


def _load_config(args: Namespace) -> Config:
    api_token = os.environ.get("ZTM_API_TOKEN", os.environ.get("WARSAW_API_TOKEN", "")).strip()
    if not api_token:
        raise RuntimeError("ZTM_API_TOKEN is required")

    poll_interval_seconds = _positive_float_env("POLL_INTERVAL_SECONDS", "10")
    api_timeout_seconds = _positive_float_env("API_TIMEOUT_SECONDS", "5")
    max_ping_age_seconds = _positive_float_env("MAX_PING_AGE_SECONDS", "300")
    future_ping_tolerance_seconds = _positive_float_env("FUTURE_PING_TOLERANCE_SECONDS", "60")
    partial_flush_interval_seconds = _positive_float_env("PARTIAL_FLUSH_INTERVAL_SECONDS", "900")
    flush_lag_seconds = _positive_float_env("FLUSH_LAG_SECONDS", str(max_ping_age_seconds))
    api_proxy = os.getenv("ZTM_API_PROXY", "").strip() or None

    return Config(
        api_token=api_token,
        vehicle_types=VEHICLE_TYPES,
        gcs_bucket=os.getenv("GCS_BUCKET", "ztm-analytics-bucket"),
        gcs_prefix=os.getenv("GCS_PREFIX", "raw/gps").strip("/"),
        poll_interval_seconds=poll_interval_seconds,
        api_timeout_seconds=api_timeout_seconds,
        api_proxy=api_proxy,
        max_ping_age_seconds=max_ping_age_seconds,
        future_ping_tolerance_seconds=future_ping_tolerance_seconds,
        partial_flush_interval_seconds=partial_flush_interval_seconds,
        flush_lag_seconds=flush_lag_seconds,
        run_once=args.once,
        no_upload=args.no_upload,
    )


def _positive_float_env(name: str, default: str) -> float:
    try:
        value = float(os.getenv(name, default))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive number") from exc

    if value <= 0:
        raise RuntimeError(f"{name} must be a positive number")
    return value


def _build_signal_handler() -> Callable[[], bool]:
    stopped = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    return lambda: stopped


def _poll_vehicle_type(
    session: requests.Session,
    config: Config,
    vehicle_type: VehicleType,
    buffers: dict[datetime, list[GpsRow]],
) -> None:
    try:
        rows = _poll_api(session, config, vehicle_type)
        rows, dropped_stale, dropped_future = _filter_fresh_rows(rows, datetime.now(UTC), config)
        for row in rows:
            buffers[_hour_key(row["Time"].astimezone(WARSAW_TZ))].append(row)
        LOGGER.info(
            "poll succeeded vehicle_type=%s accepted_rows=%d dropped_stale=%d dropped_future=%d",
            vehicle_type.name,
            len(rows),
            dropped_stale,
            dropped_future,
        )
    except requests.RequestException:
        LOGGER.exception("API request failed vehicle_type=%s", vehicle_type.name)
    except json.JSONDecodeError:
        LOGGER.exception("API returned malformed JSON vehicle_type=%s", vehicle_type.name)
    except (TypeError, ValueError):
        LOGGER.exception("API returned invalid payload vehicle_type=%s", vehicle_type.name)


def _poll_api(session: requests.Session, config: Config, vehicle_type: VehicleType) -> list[GpsRow]:
    proxies = {"http": config.api_proxy, "https": config.api_proxy} if config.api_proxy else None
    response = session.post(
        API_URL,
        headers={"Authorization": config.api_token},
        json={"type": vehicle_type.id},
        timeout=config.api_timeout_seconds,
        proxies=proxies,
    )
    response.raise_for_status()

    payload: object = response.json()
    records = _extract_records(payload)
    if not records:
        LOGGER.warning("API returned no records vehicle_type=%s", vehicle_type.name)
        return []

    parsed_rows = []
    for record in records:
        row = _parse_record(record, vehicle_type.id)
        if row is not None:
            parsed_rows.append(row)
    return parsed_rows


def _filter_fresh_rows(rows: list[GpsRow], now: datetime, config: Config) -> tuple[list[GpsRow], int, int]:
    min_time = now - timedelta(seconds=config.max_ping_age_seconds)
    max_time = now + timedelta(seconds=config.future_ping_tolerance_seconds)
    fresh_rows = []
    dropped_stale = 0
    dropped_future = 0

    for row in rows:
        ping_time = row["Time"]
        if ping_time < min_time:
            dropped_stale += 1
            continue
        if ping_time > max_time:
            dropped_future += 1
            continue
        fresh_rows.append(row)

    return fresh_rows, dropped_stale, dropped_future


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
    except (KeyError, TypeError, ValueError):
        LOGGER.warning("skipping invalid record record_keys=%s", sorted(record))
        return None


def _parse_warsaw_time(value: str) -> datetime:
    local_time = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=WARSAW_TZ)
    return local_time.astimezone(UTC)


def _hour_key(value: datetime) -> datetime:
    return value.astimezone(WARSAW_TZ).replace(minute=0, second=0, microsecond=0)


def _flush_vehicle_buffers(
    bucket: storage.Bucket,
    config: Config,
    buffers: dict[str, dict[datetime, list[GpsRow]]],
    *,
    flush_before: datetime | None = None,
    flush_all: bool = False,
) -> bool:
    flush_succeeded = True
    for vehicle_type in config.vehicle_types:
        upload_context = UploadContext(bucket, config, vehicle_type.name)
        flush_succeeded = (
            _flush_buffered_rows(
                upload_context, buffers[vehicle_type.name], flush_before=flush_before, flush_all=flush_all
            )
            and flush_succeeded
        )
    return flush_succeeded


def _flush_buffered_rows(
    upload_context: UploadContext,
    buffers: dict[datetime, list[GpsRow]],
    *,
    flush_before: datetime | None = None,
    flush_all: bool = False,
) -> bool:
    if not flush_all and flush_before is None:
        raise ValueError("flush_before is required unless flush_all is true")
    cutoff = flush_before

    flush_succeeded = True
    hours_to_flush = sorted(buffers)
    for buffer_hour in hours_to_flush:
        rows = buffers[buffer_hour]
        if not rows:
            buffers.pop(buffer_hour)
            continue
        if flush_all:
            rows_to_upload = rows
            remaining_rows: list[GpsRow] = []
        else:
            if cutoff is None:
                raise ValueError("flush_before is required unless flush_all is true")
            rows_to_upload = [row for row in rows if row["Time"].astimezone(WARSAW_TZ) <= cutoff]
            remaining_rows = [row for row in rows if row["Time"].astimezone(WARSAW_TZ) > cutoff]

        if not rows_to_upload:
            continue

        try:
            _upload_hour(upload_context, buffer_hour, rows_to_upload)
        except (GoogleAPIError, OSError, pa.ArrowException):
            LOGGER.exception("failed to upload hourly parquet hour=%s", buffer_hour.isoformat())
            flush_succeeded = False
            continue
        if remaining_rows:
            buffers[buffer_hour] = remaining_rows
        else:
            buffers.pop(buffer_hour)
    return flush_succeeded


def _upload_hour(
    upload_context: UploadContext,
    buffer_hour: datetime,
    rows: list[GpsRow],
) -> None:
    config = upload_context.config
    ingested_at = datetime.now(UTC)
    upload_rows = [row | {"ingested_at": ingested_at} for row in rows]
    table = pa.Table.from_pylist(upload_rows, schema=SCHEMA)

    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression="snappy")
    parquet_buffer.seek(0)

    path = (
        f"{config.gcs_prefix}/vehicle_type={upload_context.vehicle_type_name}/"
        f"date={buffer_hour:%Y-%m-%d}/hour={buffer_hour:%H}/"
        f"part-{_rows_digest(rows)}.parquet"
    )
    try:
        upload_context.bucket.blob(path).upload_from_file(
            parquet_buffer,
            content_type="application/octet-stream",
            if_generation_match=0,
        )
    except PreconditionFailed:
        # Deterministic path + create-only upload makes duplicate retry uploads a success.
        LOGGER.info("hourly parquet already exists gcs_path=gs://%s/%s rows=%d", config.gcs_bucket, path, len(rows))
        return
    LOGGER.info("uploaded hourly parquet gcs_path=gs://%s/%s rows=%d", config.gcs_bucket, path, len(rows))


def _rows_digest(rows: list[GpsRow]) -> str:
    """Stable content hash for idempotent part filenames."""
    digest = sha256()
    for row in sorted(
        rows,
        key=lambda item: (
            item["VehicleNumber"],
            item["Time"],
            item["Lines"],
            item["Brigade"],
            item["Lat"],
            item["Lon"],
            item["vehicle_type"],
        ),
    ):
        digest.update(row["VehicleNumber"].encode())
        digest.update(b"\0")
        digest.update(row["Time"].isoformat().encode())
        digest.update(b"\0")
        digest.update(row["Lines"].encode())
        digest.update(b"\0")
        digest.update(row["Brigade"].encode())
        digest.update(b"\0")
        digest.update(repr(row["Lat"]).encode())
        digest.update(b"\0")
        digest.update(repr(row["Lon"]).encode())
        digest.update(b"\0")
        digest.update(str(row["vehicle_type"]).encode())
        digest.update(b"\n")
    return digest.hexdigest()[:24]


def _sleep_remaining(interval_seconds: float, loop_started: float, stop_requested: Callable[[], bool]) -> None:
    remaining = interval_seconds - (time.monotonic() - loop_started)
    deadline = time.monotonic() + max(0.0, remaining)
    while not stop_requested() and time.monotonic() < deadline:
        time.sleep(min(0.5, deadline - time.monotonic()))


if __name__ == "__main__":
    raise SystemExit(main())
