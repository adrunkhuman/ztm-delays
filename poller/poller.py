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
from pathlib import Path
from socket import gethostname
from typing import TYPE_CHECKING, TypedDict, cast
from urllib.parse import urlsplit
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
DEFAULT_SPOOL_MAX_BYTES = 100 * 1024 * 1024
SPOOL_FILE_NAME = "buffers.json"

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
    """Runtime poller settings from env and CLI."""

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
    heartbeat_gcs_path: str
    heartbeat_interval_seconds: float
    require_polish_egress: bool
    egress_check_url: str
    spool_dir: Path
    spool_max_bytes: int
    run_once: bool
    no_upload: bool


@dataclass(frozen=True)
class UploadContext:
    """Upload dependencies for one vehicle type partition."""

    bucket: storage.Bucket
    config: Config
    vehicle_type_name: str


@dataclass(frozen=True)
class PollResult:
    """Outcome of one API poll attempt for heartbeat reporting."""

    vehicle_type_name: str
    attempted_at: datetime
    succeeded: bool
    accepted_rows: int = 0
    dropped_stale: int = 0
    dropped_future: int = 0
    error_type: str | None = None


@dataclass
class PollState:
    """Latest poll state retained in memory for heartbeat reporting."""

    vehicle_type_name: str
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_accepted_rows: int = 0
    last_dropped_stale_rows: int = 0
    last_dropped_future_rows: int = 0
    consecutive_failures: int = 0
    last_error_type: str | None = None


class GpsRow(TypedDict):
    """Parsed GPS row matching the raw Parquet schema before file-level ingestion fields."""

    Lines: str
    Brigade: str
    Lat: float
    Lon: float
    Time: datetime
    VehicleNumber: str
    vehicle_type: int


@dataclass
class RuntimeState:
    """Mutable state shared across poll loop iterations."""

    session: requests.Session
    bucket: storage.Bucket | None
    config: Config
    buffers: dict[str, dict[datetime, list[GpsRow]]]
    poll_states: dict[str, PollState]
    stop_requested: Callable[[], bool]
    last_partial_flush: float = 0.0
    last_heartbeat: float = 0.0


def main() -> int:
    """Run until signaled unless --once is set."""
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
    if config.require_polish_egress:
        _assert_polish_egress(session, config)

    bucket = None if config.no_upload else storage.Client().bucket(config.gcs_bucket)
    buffers = _empty_buffers(config) if config.no_upload else _load_spool(config)
    if not config.no_upload:
        _prepare_spool(config)
    poll_states = {vehicle_type.name: PollState(vehicle_type.name) for vehicle_type in config.vehicle_types}
    runtime = RuntimeState(
        session=session,
        bucket=cast("storage.Bucket | None", bucket),
        config=config,
        buffers=buffers,
        poll_states=poll_states,
        stop_requested=stop_requested,
        last_heartbeat=-config.heartbeat_interval_seconds,
    )

    try:
        _run_poll_loop(runtime)
    except Exception:
        if not config.no_upload:
            _flush_shutdown(cast("storage.Bucket", bucket), config, buffers)
        raise

    if not config.no_upload and not _flush_shutdown(cast("storage.Bucket", bucket), config, buffers):
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
    heartbeat_interval_seconds = _positive_float_env("POLLER_HEARTBEAT_INTERVAL_SECONDS", "60")
    api_proxy = os.getenv("ZTM_API_PROXY", "").strip() or None
    heartbeat_gcs_path = os.getenv("POLLER_HEARTBEAT_GCS_PATH", "health/poller/latest.json").strip("/")
    require_polish_egress = _bool_env("POLLER_REQUIRE_POLISH_EGRESS", default=False)
    egress_check_url = os.getenv("POLLER_EGRESS_CHECK_URL", "https://ipinfo.io/json").strip()
    spool_dir = Path(os.getenv("POLLER_SPOOL_DIR", "/var/lib/ztm-poller-spool")).expanduser()
    spool_max_bytes = _positive_int_env("POLLER_SPOOL_MAX_BYTES", str(DEFAULT_SPOOL_MAX_BYTES))
    if require_polish_egress and not api_proxy:
        raise RuntimeError("POLLER_REQUIRE_POLISH_EGRESS requires ZTM_API_PROXY")
    if require_polish_egress:
        egress_check_url = _validate_egress_check_url(egress_check_url)

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
        heartbeat_gcs_path=heartbeat_gcs_path,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        require_polish_egress=require_polish_egress,
        egress_check_url=egress_check_url,
        spool_dir=spool_dir,
        spool_max_bytes=spool_max_bytes,
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


def _positive_int_env(name: str, default: str) -> int:
    try:
        value = int(os.getenv(name, default))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc

    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _bool_env(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean")


def _validate_egress_check_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RuntimeError("POLLER_EGRESS_CHECK_URL must be an HTTPS URL with a hostname")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise RuntimeError("POLLER_EGRESS_CHECK_URL must not contain credentials, query, or fragment")
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
) -> PollResult:
    attempted_at = datetime.now(UTC)
    try:
        rows = _poll_api(session, config, vehicle_type)
        rows, dropped_stale, dropped_future = _filter_fresh_rows(rows, attempted_at, config)
        for row in rows:
            buffers[_hour_key(row["Time"].astimezone(WARSAW_TZ))].append(row)
        LOGGER.info(
            "poll succeeded vehicle_type=%s accepted_rows=%d dropped_stale=%d dropped_future=%d",
            vehicle_type.name,
            len(rows),
            dropped_stale,
            dropped_future,
        )
        return PollResult(
            vehicle_type_name=vehicle_type.name,
            attempted_at=attempted_at,
            succeeded=True,
            accepted_rows=len(rows),
            dropped_stale=dropped_stale,
            dropped_future=dropped_future,
        )
    except requests.RequestException:
        LOGGER.exception("API request failed vehicle_type=%s", vehicle_type.name)
        return PollResult(vehicle_type.name, attempted_at, succeeded=False, error_type="request_error")
    except json.JSONDecodeError:
        LOGGER.exception("API returned malformed JSON vehicle_type=%s", vehicle_type.name)
        return PollResult(vehicle_type.name, attempted_at, succeeded=False, error_type="malformed_json")
    except (TypeError, ValueError):
        LOGGER.exception("API returned invalid payload vehicle_type=%s", vehicle_type.name)
        return PollResult(vehicle_type.name, attempted_at, succeeded=False, error_type="invalid_payload")


def _poll_vehicle_types(
    session: requests.Session,
    config: Config,
    buffers: dict[str, dict[datetime, list[GpsRow]]],
    poll_states: dict[str, PollState],
) -> int:
    accepted_rows = 0
    for vehicle_type in config.vehicle_types:
        result = _poll_vehicle_type(session, config, vehicle_type, buffers[vehicle_type.name])
        _update_poll_state(poll_states[vehicle_type.name], result)
        accepted_rows += result.accepted_rows
        if result.accepted_rows and not config.no_upload:
            _save_spool(config, buffers)
    return accepted_rows


def _run_poll_loop(runtime: RuntimeState) -> None:
    config = runtime.config
    while not runtime.stop_requested():
        loop_started = time.monotonic()
        _poll_vehicle_types(runtime.session, config, runtime.buffers, runtime.poll_states)
        if config.no_upload:
            buffered_rows = sum(
                len(rows) for vehicle_buffers in runtime.buffers.values() for rows in vehicle_buffers.values()
            )
            LOGGER.info("upload disabled buffered_rows=%d", buffered_rows)
        else:
            _handle_uploads(runtime, loop_started)

        if config.run_once:
            return
        _sleep_remaining(config.poll_interval_seconds, loop_started, runtime.stop_requested)


def _handle_uploads(runtime: RuntimeState, loop_started: float) -> None:
    config = runtime.config
    bucket = cast("storage.Bucket", runtime.bucket)
    now = datetime.now(WARSAW_TZ)
    if loop_started - runtime.last_partial_flush >= config.partial_flush_interval_seconds:
        flush_before = now - timedelta(seconds=config.flush_lag_seconds)
        flush_succeeded = _flush_vehicle_buffers(bucket, config, runtime.buffers, flush_before=flush_before)
        _save_spool(config, runtime.buffers)
        if flush_succeeded:
            runtime.last_partial_flush = loop_started
    if loop_started - runtime.last_heartbeat >= config.heartbeat_interval_seconds and _write_heartbeat(
        bucket, config, runtime.poll_states
    ):
        runtime.last_heartbeat = loop_started


def _update_poll_state(state: PollState, result: PollResult) -> None:
    state.last_attempt_at = result.attempted_at
    if result.succeeded:
        state.last_success_at = result.attempted_at
        state.last_accepted_rows = result.accepted_rows
        state.last_dropped_stale_rows = result.dropped_stale
        state.last_dropped_future_rows = result.dropped_future
        state.consecutive_failures = 0
        state.last_error_type = None
        return
    state.consecutive_failures += 1
    state.last_error_type = result.error_type


def _assert_polish_egress(session: requests.Session, config: Config) -> None:
    proxies = {"http": config.api_proxy, "https": config.api_proxy} if config.api_proxy else None
    try:
        response = session.get(config.egress_check_url, timeout=config.api_timeout_seconds, proxies=proxies)
        response.raise_for_status()
        payload: object = response.json()
    except (requests.RequestException, json.JSONDecodeError) as exc:
        raise RuntimeError("failed to verify Polish egress before polling") from exc

    match payload:
        case {"country": str(country_value)}:
            pass
        case _:
            raise RuntimeError("egress check returned invalid payload")

    country = country_value.strip().upper()
    if country != "PL":
        raise RuntimeError(f"poller egress country is {country or 'unknown'}, expected PL")

    LOGGER.info("verified Polish egress hostname=%s country=%s", urlsplit(config.egress_check_url).hostname, country)


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


def _empty_buffers(config: Config) -> dict[str, dict[datetime, list[GpsRow]]]:
    return {vehicle_type.name: defaultdict(list) for vehicle_type in config.vehicle_types}


def _spool_path(config: Config) -> Path:
    return config.spool_dir / SPOOL_FILE_NAME


def _prepare_spool(config: Config) -> None:
    config.spool_dir.mkdir(parents=True, exist_ok=True)
    if not os.access(config.spool_dir, os.W_OK):
        raise RuntimeError(f"poller spool directory is not writable path={config.spool_dir}")


def _load_spool(config: Config) -> dict[str, dict[datetime, list[GpsRow]]]:
    buffers = _empty_buffers(config)
    spool_path = _spool_path(config)
    if not spool_path.exists():
        return buffers
    if spool_path.stat().st_size > config.spool_max_bytes:
        raise RuntimeError(f"poller spool exceeds POLLER_SPOOL_MAX_BYTES path={spool_path}")

    payload = json.loads(spool_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise RuntimeError("poller spool has unsupported format")
    vehicle_payload = payload.get("vehicle_types")
    if not isinstance(vehicle_payload, dict):
        raise TypeError("poller spool is missing vehicle_types")

    vehicle_names = {vehicle_type.name for vehicle_type in config.vehicle_types}
    restored_rows = 0
    for vehicle_type_name, hour_payload in vehicle_payload.items():
        if vehicle_type_name not in vehicle_names or not isinstance(hour_payload, dict):
            continue
        for hour_value, rows_payload in hour_payload.items():
            if not isinstance(rows_payload, list):
                raise TypeError("poller spool hour payload must be a list")
            hour = datetime.fromisoformat(hour_value)
            rows = [_gps_row_from_spool(row_payload) for row_payload in rows_payload]
            buffers[vehicle_type_name][hour].extend(rows)
            restored_rows += len(rows)
    LOGGER.info("loaded poller spool path=%s rows=%d", spool_path, restored_rows)
    return buffers


def _save_spool(config: Config, buffers: dict[str, dict[datetime, list[GpsRow]]]) -> None:
    spool_path = _spool_path(config)
    buffered_rows = sum(len(rows) for vehicle_buffers in buffers.values() for rows in vehicle_buffers.values())
    if buffered_rows == 0:
        if spool_path.exists():
            spool_path.unlink()
            LOGGER.info("removed empty poller spool path=%s", spool_path)
        return

    payload = {
        "version": 1,
        "vehicle_types": {
            vehicle_type_name: {
                hour.isoformat(): [_gps_row_to_spool(row) for row in rows]
                for hour, rows in sorted(vehicle_buffers.items())
                if rows
            }
            for vehicle_type_name, vehicle_buffers in sorted(buffers.items())
        },
    }
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    if len(data) > config.spool_max_bytes:
        raise RuntimeError("poller spool snapshot exceeds POLLER_SPOOL_MAX_BYTES")

    config.spool_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = spool_path.with_suffix(".tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(spool_path)
    LOGGER.info("saved poller spool path=%s rows=%d bytes=%d", spool_path, buffered_rows, len(data))


def _flush_shutdown(bucket: storage.Bucket, config: Config, buffers: dict[str, dict[datetime, list[GpsRow]]]) -> bool:
    if not _flush_vehicle_buffers(bucket, config, buffers, flush_all=True):
        _save_spool(config, buffers)
        LOGGER.error("poller stopped with buffered rows after failed shutdown flush")
        return False
    _save_spool(config, buffers)
    return True


def _gps_row_to_spool(row: GpsRow) -> dict[str, object]:
    return {
        "Lines": row["Lines"],
        "Brigade": row["Brigade"],
        "Lat": row["Lat"],
        "Lon": row["Lon"],
        "Time": row["Time"].astimezone(UTC).isoformat(),
        "VehicleNumber": row["VehicleNumber"],
        "vehicle_type": row["vehicle_type"],
    }


def _gps_row_from_spool(payload: object) -> GpsRow:
    if not isinstance(payload, dict):
        raise TypeError("poller spool row must be an object")
    row_payload = cast("dict[str, object]", payload)
    return {
        "Lines": str(row_payload["Lines"]),
        "Brigade": str(row_payload["Brigade"]),
        "Lat": float(cast("str | float", row_payload["Lat"])),
        "Lon": float(cast("str | float", row_payload["Lon"])),
        "Time": datetime.fromisoformat(str(row_payload["Time"])).astimezone(UTC),
        "VehicleNumber": str(row_payload["VehicleNumber"]),
        "vehicle_type": int(cast("str | int", row_payload["vehicle_type"])),
    }


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


def _write_heartbeat(bucket: storage.Bucket, config: Config, poll_states: dict[str, PollState]) -> bool:
    payload = _heartbeat_payload(config, poll_states, datetime.now(UTC))
    data = json.dumps(payload, sort_keys=True).encode()
    try:
        bucket.blob(config.heartbeat_gcs_path).upload_from_string(data, content_type="application/json")
    except (GoogleAPIError, OSError):
        LOGGER.exception(
            "failed to upload poller heartbeat gcs_path=gs://%s/%s", config.gcs_bucket, config.heartbeat_gcs_path
        )
        return False
    LOGGER.info(
        "uploaded poller heartbeat gcs_path=gs://%s/%s status=%s",
        config.gcs_bucket,
        config.heartbeat_gcs_path,
        payload["status"],
    )
    return True


def _heartbeat_payload(config: Config, poll_states: dict[str, PollState], updated_at: datetime) -> dict[str, object]:
    return {
        "updated_at": _isoformat_utc(updated_at),
        "status": _heartbeat_status(poll_states),
        "poller_hostname": gethostname(),
        "poll_interval_seconds": config.poll_interval_seconds,
        "heartbeat_interval_seconds": config.heartbeat_interval_seconds,
        "gcs_prefix": config.gcs_prefix,
        "vehicle_types": {
            name: {
                "last_attempt_at": _isoformat_utc(state.last_attempt_at),
                "last_success_at": _isoformat_utc(state.last_success_at),
                "last_accepted_rows": state.last_accepted_rows,
                "last_dropped_stale_rows": state.last_dropped_stale_rows,
                "last_dropped_future_rows": state.last_dropped_future_rows,
                "consecutive_failures": state.consecutive_failures,
                "last_error_type": state.last_error_type,
            }
            for name, state in sorted(poll_states.items())
        },
    }


def _heartbeat_status(poll_states: dict[str, PollState]) -> str:
    states = list(poll_states.values())
    if any(state.last_attempt_at is None for state in states):
        return "starting"
    failed_states = [state for state in states if state.consecutive_failures > 0]
    if not failed_states:
        return "ok"
    if len(failed_states) == len(states):
        return "down"
    return "degraded"


def _isoformat_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


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
