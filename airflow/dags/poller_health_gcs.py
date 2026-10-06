"""GCS reads shared by the poller-health DAGs.

DAG files must not import each other: Airflow would register the imported
file's DAG a second time under the importing file.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from poller_health import SUMMARY_MAX_BYTES, decode_json, iso, timestamp, validate_summary

if TYPE_CHECKING:
    from google.cloud import storage

LOGGER = logging.getLogger(__name__)
HEARTBEAT_PATH = "health/poller/latest.json"
SUMMARY_PREFIX = "health/poller/hourly"
HEARTBEAT_MAX_BYTES = 16 * 1024


def configured_path(name: str, default: str) -> str:
    """Normalize object keys/prefixes, failing explicitly for empty configuration."""
    path = os.getenv(name, default).strip().strip("/")
    if not path.strip():
        raise ValueError(f"{name} must not be empty")
    return path


def summary_path(hour: datetime) -> str:
    """Use the collector's configurable prefix with the unchanged UTC hour suffix."""
    prefix = configured_path("POLLER_HEALTH_GCS_PREFIX", SUMMARY_PREFIX)
    return f"{prefix}/{hour.astimezone(UTC):%Y-%m-%d/%H}.json"


def read_object(bucket: storage.Bucket, path: str, limit: int) -> tuple[dict[str, Any] | None, int]:
    """Read only a bounded JSON object with a generation-consistent byte range."""
    blob = bucket.blob(path)
    try:
        blob.reload()
        if blob.size is None or blob.size > limit:
            raise ValueError("health object too large or missing size")
        data = blob.download_as_bytes(start=0, end=limit, if_generation_match=blob.generation, raw_download=True)
    except NotFound:
        return None, 0
    return decode_json(data, limit), int(blob.generation)


def read_summary(bucket: storage.Bucket, hour: datetime) -> tuple[dict[str, Any] | None, str]:
    """Malformed telemetry is a monitoring gap, never evidence of zero service."""
    path = summary_path(hour)
    try:
        data, _ = read_object(bucket, path, SUMMARY_MAX_BYTES)
        return (validate_summary(data, hour), "") if data is not None else (None, "summary_absent")
    except ValueError:
        LOGGER.warning("Invalid poller summary for %s", iso(hour))
        return None, "invalid_summary"


def collection_start(bucket: storage.Bucket) -> str | None:
    """Older heartbeats have no collection marker; absence is not an outage."""
    path = configured_path("POLLER_HEARTBEAT_GCS_PATH", HEARTBEAT_PATH)
    try:
        heartbeat, _ = read_object(bucket, path, HEARTBEAT_MAX_BYTES)
        value = heartbeat.get("collection_started_at") if heartbeat else None
        return iso(timestamp(value)) if value else None
    except ValueError:
        LOGGER.warning("Invalid poller collection marker")
        return None


def first_collection_start(bucket: storage.Bucket) -> str | None:
    """Rollout fallback: inspect one retained summary, never list the GPS archive."""
    prefix = configured_path("POLLER_HEALTH_GCS_PREFIX", SUMMARY_PREFIX) + "/"
    first = next(iter(bucket.list_blobs(prefix=prefix, max_results=1)), None)
    if first is None or not first.name.startswith(prefix):
        return None
    suffix = first.name.removeprefix(prefix)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}/\d{2}\.json", suffix):
        return None
    try:
        hour = datetime.strptime(suffix, "%Y-%m-%d/%H.json").replace(tzinfo=UTC)
    except ValueError:
        return None
    source, _ = read_summary(bucket, hour)
    return source["collection_started_at"] if source else None
