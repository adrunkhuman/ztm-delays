"""Bounded matcher execution, validation, and atomic publication."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path, PurePosixPath
from typing import Any, cast
from urllib.parse import urlparse

from google.api_core.exceptions import Conflict, NotFound, PreconditionFailed
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_INT_DATASET,
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
    BIGQUERY_MATCHER_INPUT_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    RAW_GPS_PREFIX,
    RAW_GTFS_PREFIX,
    matcher_input_inventory_digest,
)

LOGGER = logging.getLogger(__name__)

RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
VEHICLE_TYPES = ("bus", "tram")
ARTIFACT_SCHEMA_VERSIONS = {
    "reconstruction_trip_facts": "reconstruction-trip-facts-v2",
    "reconstruction_stop_arrivals": "reconstruction-stop-arrivals-v2",
    "reconstruction_expected_stop_events": "reconstruction-expected-stop-events-v2",
    "stop_semantics": "stop-semantics-v3",
    "trip_universe": "trip-universe-v1",
}
IDENTIFIER_PATTERN = re.compile(r"[^a-z0-9_]+")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
DATASET_ID_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,1023}\Z")
VALIDATION_MEMORY_LIMIT = "320MB"
VALIDATION_TEMP_LIMIT = "20GB"
VALIDATION_BATCH_SIZE = 8_192
DEFAULT_MAX_GPS_OBJECTS = 5_000
DEFAULT_MAX_GPS_BYTES = 20 * 1024**3
DEFAULT_MIN_FREE_DISK_BYTES = 5 * 1024**3
DEFAULT_MAX_MARKER_BYTES = 20 * 1024**2
DEFAULT_MAX_RSS_BYTES = 3 * 1024**3
DEFAULT_MAX_PUBLICATION_BYTES = 5 * 1024**3
DEFAULT_STAGING_RETENTION_DAYS = 3
DEFAULT_INTERMEDIATE_MARKER_RETENTION_DAYS = 3
DEFAULT_PUBLISHED_MARKER_RETENTION_DAYS = 30
PUBLICATION_JOB_VERSION = "v2"
IMMUTABLE_RUN_IDENTITY_VERSION = "matcher-run-identity-v1"
HISTORICAL_RUN_ID_PREFIX = "matcher-historical-correction__"
STABLE_INPUT_TABLES = {
    "trip": "reconstruction_trip_facts",
    "stop_arrival": "reconstruction_stop_arrivals",
    "expected_stop_event": "reconstruction_expected_stop_events",
    "stop_semantics": "reconstruction_stop_semantics",
}


@dataclass(frozen=True)
class FieldSpec:
    """A stable Parquet-to-BigQuery field mapping."""

    name: str
    bigquery_type: str
    repeated: bool = False


@dataclass(frozen=True)
class ArtifactSpec:
    """One matcher artifact loaded into a run-scoped table."""

    key: str
    filename: str
    table_suffix: str
    fields: tuple[FieldSpec, ...]
    grain: tuple[str, ...]
    source_date_field: str
    partition_field: str = "gps_date"
    quality_field: str | None = "trip_quality"
    status_field: str | None = None
    delay_field: str | None = None


def _fields(*values: tuple[str, str] | tuple[str, str, bool]) -> tuple[FieldSpec, ...]:
    return tuple(FieldSpec(*value) for value in values)


# These schemas are deliberately local adapter schemas, not warehouse fact schemas.
ARTIFACTS = (
    ArtifactSpec(
        "trip",
        "reconstruction_trip_facts.parquet",
        "trip",
        _fields(
            ("gtfs_snapshot_id", "STRING"),
            ("processing_date", "DATE"),
            ("gps_date", "DATE"),
            ("service_date", "DATE"),
            ("trip_id", "STRING"),
            ("vehicle_number", "STRING"),
            ("line", "STRING"),
            ("brigade", "STRING"),
            ("mode", "STRING"),
            ("scheduled_start_time", "TIMESTAMP"),
            ("scheduled_end_time", "TIMESTAMP"),
            ("actual_start_time", "TIMESTAMP"),
            ("actual_end_time", "TIMESTAMP"),
            ("start_delay_seconds", "INTEGER"),
            ("end_delay_seconds", "INTEGER"),
            ("passenger_stops_expected", "INTEGER"),
            ("passenger_stops_detected", "INTEGER"),
            ("detected_stop_ratio", "FLOAT"),
            ("optional_passenger_stops_expected", "INTEGER"),
            ("optional_passenger_stops_detected", "INTEGER"),
            ("first_detected_stop_sequence", "INTEGER"),
            ("last_detected_stop_sequence", "INTEGER"),
            ("max_stop_sequence_gap", "INTEGER"),
            ("max_ping_gap_seconds", "INTEGER"),
            ("max_speed_mps", "FLOAT"),
            ("is_first_stop_observed", "BOOLEAN"),
            ("is_last_stop_observed", "BOOLEAN"),
            ("has_non_monotonic_stop_progression", "BOOLEAN"),
            ("has_impossible_speed_jump", "BOOLEAN"),
            ("has_stale_stop_progression", "BOOLEAN"),
            ("trip_quality", "STRING"),
            ("quality_flags", "STRING", True),
            ("service_observation_class", "STRING"),
            ("service_observation_flags", "STRING", True),
            ("is_zone1_public_ranking_trip", "BOOLEAN"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id", "vehicle_number"),
        "gps_date",
        delay_field="end_delay_seconds",
    ),
    ArtifactSpec(
        "stop_arrival",
        "reconstruction_stop_arrivals.parquet",
        "stop_arrival",
        _fields(
            ("gtfs_snapshot_id", "STRING"),
            ("processing_date", "DATE"),
            ("gps_date", "DATE"),
            ("source_gps_date", "DATE"),
            ("service_date", "DATE"),
            ("trip_id", "STRING"),
            ("vehicle_number", "STRING"),
            ("line", "STRING"),
            ("brigade", "STRING"),
            ("mode", "STRING"),
            ("stop_id", "STRING"),
            ("stop_group_id", "STRING"),
            ("stop_sequence", "INTEGER"),
            ("pickup_type", "INTEGER"),
            ("drop_off_type", "INTEGER"),
            ("stop_service_class", "STRING"),
            ("scheduled_arrival_time", "TIMESTAMP"),
            ("scheduled_departure_time", "TIMESTAMP"),
            ("actual_arrival_time", "TIMESTAMP"),
            ("delay_seconds", "INTEGER"),
            ("detection_method", "STRING"),
            ("stop_match_radius_m", "FLOAT"),
            ("stop_distance_m", "FLOAT"),
            ("prev_ping_distance_m", "FLOAT"),
            ("next_ping_distance_m", "FLOAT"),
            ("segment_start_time", "TIMESTAMP"),
            ("segment_end_time", "TIMESTAMP"),
            ("segment_duration_seconds", "INTEGER"),
            ("alignment_confidence", "STRING"),
            ("alignment_evidence", "STRING", True),
            ("trip_quality", "STRING"),
            ("quality_flags", "STRING", True),
            ("service_observation_class", "STRING"),
            ("service_observation_flags", "STRING", True),
            ("is_zone1_public_ranking_trip", "BOOLEAN"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id", "vehicle_number", "stop_sequence"),
        "source_gps_date",
        delay_field="delay_seconds",
    ),
    ArtifactSpec(
        "expected_stop_event",
        "reconstruction_expected_stop_events.parquet",
        "expected_stop_event",
        _fields(
            ("gtfs_snapshot_id", "STRING"),
            ("processing_date", "DATE"),
            ("gps_date", "DATE"),
            ("source_gps_date", "DATE"),
            ("service_date", "DATE"),
            ("trip_id", "STRING"),
            ("vehicle_number", "STRING"),
            ("line", "STRING"),
            ("brigade", "STRING"),
            ("mode", "STRING"),
            ("stop_id", "STRING"),
            ("stop_group_id", "STRING"),
            ("stop_sequence", "INTEGER"),
            ("pickup_type", "INTEGER"),
            ("drop_off_type", "INTEGER"),
            ("stop_service_class", "STRING"),
            ("scheduled_arrival_time", "TIMESTAMP"),
            ("scheduled_departure_time", "TIMESTAMP"),
            ("observation_status", "STRING"),
            ("actual_arrival_time", "TIMESTAMP"),
            ("delay_seconds", "INTEGER"),
            ("uncertainty_evidence", "STRING", True),
            ("trip_quality", "STRING"),
            ("quality_flags", "STRING", True),
            ("service_observation_class", "STRING"),
            ("service_observation_flags", "STRING", True),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id", "vehicle_number", "stop_sequence"),
        "source_gps_date",
        status_field="observation_status",
        delay_field="delay_seconds",
    ),
    ArtifactSpec(
        "stop_semantics",
        "stop_semantics.parquet",
        "stop_semantics",
        _fields(
            ("gtfs_snapshot_id", "STRING"),
            ("service_date", "DATE"),
            ("processing_date", "DATE"),
            ("trip_id", "STRING"),
            ("stop_id", "STRING"),
            ("stop_group_id", "STRING"),
            ("stop_lat", "FLOAT"),
            ("stop_lon", "FLOAT"),
            ("zone_id", "STRING"),
            ("effective_zone_id", "STRING"),
            ("stop_sequence", "INTEGER"),
            ("arrival_time_seconds", "INTEGER"),
            ("departure_time_seconds", "INTEGER"),
            ("pickup_type", "INTEGER"),
            ("drop_off_type", "INTEGER"),
            ("stop_service_class", "STRING"),
            ("duty_chain_id", "STRING"),
            ("duty_chain_source", "STRING"),
            ("duty_chain_source_id", "STRING"),
            ("trip_order", "INTEGER"),
            ("previous_trip_id", "STRING"),
            ("next_trip_id", "STRING"),
            ("stop_execution_class", "STRING"),
            ("classification_confidence", "STRING"),
            ("classification_reason", "STRING"),
            ("classification_evidence", "STRING", True),
            ("is_passenger_stop", "BOOLEAN"),
            ("are_passenger_boundaries_settled", "BOOLEAN"),
            ("first_passenger_stop_sequence", "INTEGER"),
            ("last_passenger_stop_sequence", "INTEGER"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id", "stop_sequence"),
        "processing_date",
        partition_field="processing_date",
        quality_field=None,
    ),
)
TRIP_UNIVERSE_FIELDS = _fields(
    ("gtfs_snapshot_id", "STRING"),
    ("processing_date", "DATE"),
    ("service_date", "DATE"),
    ("duty_chain_id", "STRING"),
    ("trip_id", "STRING"),
    ("line", "STRING"),
    ("mode", "STRING"),
    ("direction_id", "INTEGER"),
    ("origin_stop_id", "STRING"),
    ("destination_stop_id", "STRING"),
    ("ordered_stop_ids", "STRING"),
    ("stop_count", "INTEGER"),
    ("non_zone1_stop_count", "INTEGER"),
    ("is_public_service_segment", "BOOLEAN"),
    ("is_public_passenger_segment", "BOOLEAN"),
    ("terminal_pair_trip_count", "INTEGER"),
    ("terminal_pair_rank", "INTEGER"),
    ("is_short_turn_part_trip", "BOOLEAN"),
    ("is_zone1_only", "BOOLEAN"),
    ("is_zone1_public_ranking_trip", "BOOLEAN"),
)


@dataclass(frozen=True)
class MatcherConfig:
    """Runtime configuration for the matcher publication pipeline."""

    enabled: bool
    staging_dataset: str | None
    input_dataset: str
    workspace_root: Path
    command: tuple[str, ...]
    project_dir: Path | None
    timeout_seconds: int
    marker_prefix: str
    keep_workspace: bool = False
    max_gps_objects: int = DEFAULT_MAX_GPS_OBJECTS
    max_gps_bytes: int = DEFAULT_MAX_GPS_BYTES
    min_free_disk_bytes: int = DEFAULT_MIN_FREE_DISK_BYTES
    max_marker_bytes: int = DEFAULT_MAX_MARKER_BYTES
    max_rss_bytes: int = DEFAULT_MAX_RSS_BYTES
    max_publication_bytes: int = DEFAULT_MAX_PUBLICATION_BYTES
    staging_retention_days: int = DEFAULT_STAGING_RETENTION_DAYS
    intermediate_marker_retention_days: int = DEFAULT_INTERMEDIATE_MARKER_RETENTION_DAYS
    published_marker_retention_days: int = DEFAULT_PUBLISHED_MARKER_RETENTION_DAYS

    @classmethod
    def from_env(cls) -> MatcherConfig:
        """Read the matcher environment contract."""
        project_dir = os.getenv("MATCHER_PROJECT_DIR", "/opt/airflow/matcher").strip()
        try:
            command = tuple(
                shlex.split(os.getenv("MATCHER_COMMAND", "uv run --locked --project /opt/airflow/matcher ztm-matcher"))
            )
        except ValueError as error:
            raise ValueError("MATCHER_COMMAND has invalid shell-style quoting") from error
        return cls(
            enabled=_env_bool("MATCHER_ENABLED", False),
            staging_dataset=os.getenv("BIGQUERY_MATCHER_STAGING_DATASET", "").strip() or None,
            input_dataset=os.getenv("BIGQUERY_MATCHER_INPUT_DATASET", BIGQUERY_MATCHER_INPUT_DATASET).strip()
            or BIGQUERY_MATCHER_INPUT_DATASET,
            workspace_root=Path(os.getenv("MATCHER_WORKSPACE_ROOT", "/opt/airflow/matcher-work")),
            command=command,
            project_dir=Path(project_dir) if project_dir else None,
            timeout_seconds=_env_positive_int("MATCHER_TIMEOUT_SECONDS", 45 * 60),
            marker_prefix=os.getenv("MATCHER_GCS_PREFIX", "matcher/runs").strip(),
            keep_workspace=_env_bool("MATCHER_KEEP_WORKSPACE", False),
            max_gps_objects=_env_positive_int("MATCHER_MAX_GPS_OBJECTS", DEFAULT_MAX_GPS_OBJECTS),
            max_gps_bytes=_env_positive_int("MATCHER_MAX_GPS_BYTES", DEFAULT_MAX_GPS_BYTES),
            min_free_disk_bytes=_env_positive_int("MATCHER_MIN_FREE_DISK_BYTES", DEFAULT_MIN_FREE_DISK_BYTES),
            max_marker_bytes=_env_positive_int("MATCHER_MAX_MARKER_BYTES", DEFAULT_MAX_MARKER_BYTES),
            max_rss_bytes=_env_positive_int("MATCHER_MAX_RSS_BYTES", DEFAULT_MAX_RSS_BYTES),
            max_publication_bytes=_env_positive_int("MATCHER_MAX_PUBLICATION_BYTES", DEFAULT_MAX_PUBLICATION_BYTES),
            staging_retention_days=_env_positive_int("MATCHER_STAGING_RETENTION_DAYS", DEFAULT_STAGING_RETENTION_DAYS),
            intermediate_marker_retention_days=_env_positive_int(
                "MATCHER_INTERMEDIATE_MARKER_RETENTION_DAYS", DEFAULT_INTERMEDIATE_MARKER_RETENTION_DAYS
            ),
            published_marker_retention_days=_env_positive_int(
                "MATCHER_PUBLISHED_MARKER_RETENTION_DAYS", DEFAULT_PUBLISHED_MARKER_RETENTION_DAYS
            ),
        )

    def validate(self) -> None:
        """Reject incomplete or overlapping dataset configuration before execution."""
        if not self.command:
            raise ValueError("MATCHER_COMMAND must not be empty")
        if self.project_dir is None:
            raise ValueError("MATCHER_PROJECT_DIR is required")
        command_projects = _command_projects(self.command)
        if len(command_projects) != 1 or Path(command_projects[0]) != self.project_dir:
            raise ValueError("MATCHER_COMMAND --project must match MATCHER_PROJECT_DIR")
        if not self.enabled:
            return
        if not self.staging_dataset:
            raise ValueError("BIGQUERY_MATCHER_STAGING_DATASET is required when MATCHER_ENABLED=true")
        _validate_dataset_id("BIGQUERY_MATCHER_STAGING_DATASET", self.staging_dataset)
        _validate_dataset_id("BIGQUERY_MATCHER_INPUT_DATASET", self.input_dataset)
        reserved_datasets = {
            BIGQUERY_RAW_DATASET.casefold(),
            BIGQUERY_INT_DATASET.casefold(),
            BIGQUERY_MARTS_DATASET.casefold(),
        }
        if self.staging_dataset.casefold() in reserved_datasets or self.input_dataset.casefold() in reserved_datasets:
            raise ValueError("Matcher staging and input datasets must not use raw, int, or marts datasets")
        if self.staging_dataset.casefold() == self.input_dataset.casefold():
            raise ValueError("BIGQUERY_MATCHER_STAGING_DATASET and BIGQUERY_MATCHER_INPUT_DATASET must differ")
        if not self.marker_prefix:
            raise ValueError("MATCHER_GCS_PREFIX must not be empty")
        _strict_posix_name(self.marker_prefix)
        if not self.workspace_root.is_absolute():
            raise ValueError("MATCHER_WORKSPACE_ROOT must be absolute")
        for name, value in (
            ("MATCHER_MAX_GPS_OBJECTS", self.max_gps_objects),
            ("MATCHER_MAX_GPS_BYTES", self.max_gps_bytes),
            ("MATCHER_MIN_FREE_DISK_BYTES", self.min_free_disk_bytes),
            ("MATCHER_MAX_MARKER_BYTES", self.max_marker_bytes),
            ("MATCHER_MAX_RSS_BYTES", self.max_rss_bytes),
            ("MATCHER_MAX_PUBLICATION_BYTES", self.max_publication_bytes),
            ("MATCHER_STAGING_RETENTION_DAYS", self.staging_retention_days),
            ("MATCHER_INTERMEDIATE_MARKER_RETENTION_DAYS", self.intermediate_marker_retention_days),
            ("MATCHER_PUBLISHED_MARKER_RETENTION_DAYS", self.published_marker_retention_days),
        ):
            if value < 1:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class GcsObject:
    """Immutable object inventory retained in run metadata."""

    name: str
    generation: str | None
    size: int | None
    md5_hash: str | None
    crc32c: str | None


@dataclass(frozen=True)
class ArtifactValidation:
    """Validated local artifact information used for loading and the validated marker."""

    path: Path
    rows: int
    sha256: str
    bytes: int
    service_dates: tuple[str, ...]
    repeated_nonempty: tuple[tuple[str, int], ...] = ()
    modes: tuple[str, ...] = ()


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() == "true"


def _validate_dataset_id(name: str, value: str) -> None:
    """BigQuery dataset IDs are identifiers, not table expressions or project paths."""
    if not DATASET_ID_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a BigQuery dataset ID without project qualification or backticks")


def _command_projects(command: tuple[str, ...]) -> tuple[str, ...]:
    """Return explicitly supplied uv project paths without interpreting a shell command."""
    projects = []
    for index, argument in enumerate(command):
        if argument == "--project" and index + 1 < len(command):
            projects.append(command[index + 1])
        elif argument.startswith("--project="):
            projects.append(argument.removeprefix("--project="))
    return tuple(projects)


def _env_positive_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default)).strip()
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _run_id(value: str) -> str:
    normalized = IDENTIFIER_PATTERN.sub("_", value.lower()).strip("_") or "run"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"run_{normalized[:59]}_{digest}"


def _validated_sha256(value: str) -> str:
    if not isinstance(value, str) or not SHA256_PATTERN.fullmatch(value):
        raise ValueError("Artifact SHA-256 must be a lowercase 64-character hexadecimal digest")
    return value


def _table_id(dataset: str, run_id: str, spec: ArtifactSpec, artifact_sha256: str) -> str:
    digest = _validated_sha256(artifact_sha256)
    return f"{GCP_PROJECT}.{dataset}.matcher_run_{spec.table_suffix}_{_run_id(run_id)}_{digest}"


def _load_job_id(run_id: str, spec: ArtifactSpec, artifact_sha256: str) -> str:
    run_digest = hashlib.sha256(_run_id(run_id).encode("utf-8")).hexdigest()[:16]
    artifact_digest = hashlib.sha256(_validated_sha256(artifact_sha256).encode("ascii")).hexdigest()[:16]
    return f"matcher_load_{spec.table_suffix}_{run_digest}_{artifact_digest}"


def _gps_input_dates(processing_date: str, include_prior_gps: bool) -> tuple[str, ...]:
    """Return the explicit Warsaw GPS dates for one reconstruction run."""
    current = date.fromisoformat(processing_date)
    return tuple(
        item.isoformat() for item in ((current - timedelta(days=1), current) if include_prior_gps else (current,))
    )


def _gps_prefixes(processing_date: str, include_prior_gps: bool) -> list[str]:
    return [
        f"{RAW_GPS_PREFIX}/vehicle_type={mode}/date={input_date}/"
        for input_date in _gps_input_dates(processing_date, include_prior_gps)
        for mode in VEHICLE_TYPES
    ]


def _strict_posix_name(name: str) -> PurePosixPath:
    """Accept only normalized relative GCS object names."""
    path = PurePosixPath(name)
    if (
        not name
        or path == PurePosixPath(".")
        or "\\" in name
        or path.is_absolute()
        or ".." in path.parts
        or path.as_posix() != name
    ):
        raise ValueError(f"Unsafe GCS object name: {name}")
    return path


def _strict_posix_prefix(prefix: str) -> str:
    normalized = prefix.removesuffix("/")
    _strict_posix_name(normalized)
    return normalized


def _gcs_relative_name(name: str, prefix: str) -> PurePosixPath:
    normalized_prefix = _strict_posix_prefix(prefix)
    _strict_posix_name(name)
    expected_prefix = f"{normalized_prefix}/"
    if not name.startswith(expected_prefix):
        raise ValueError(f"GCS object is outside expected prefix {normalized_prefix}: {name}")
    relative = PurePosixPath(name.removeprefix(expected_prefix))
    if relative == PurePosixPath("."):
        raise ValueError(f"GCS object is not below expected prefix {normalized_prefix}: {name}")
    return relative


def _contained_destination(root: Path, relative: PurePosixPath) -> Path:
    destination = root.joinpath(*relative.parts)
    if not destination.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"GCS object destination escapes workspace: {relative}")
    return destination


def _list_gps_objects(bucket: Any, processing_date: str, include_prior_gps: bool) -> list[GcsObject]:
    objects: dict[str, GcsObject] = {}
    for prefix in _gps_prefixes(processing_date, include_prior_gps):
        for blob in bucket.list_blobs(prefix=prefix):
            name = str(blob.name)
            if not (name.endswith(".parquet") and "/part-" in name):
                continue
            _gcs_relative_name(name, prefix)
            item = GcsObject(
                name,
                str(getattr(blob, "generation", "")) or None,
                int(blob.size) if getattr(blob, "size", None) is not None else None,
                getattr(blob, "md5_hash", None),
                getattr(blob, "crc32c", None),
            )
            if name in objects and objects[name] != item:
                raise RuntimeError(f"Matcher GPS inventory has conflicting object metadata: {name}")
            objects[name] = item
    if not objects:
        raise RuntimeError(
            f"No bus/tram GPS part objects found for {_gps_input_dates(processing_date, include_prior_gps)}"
        )
    if any(
        item.generation is None or item.size is None or not (item.md5_hash or item.crc32c) for item in objects.values()
    ):
        raise RuntimeError("Matcher GPS inventory requires object generation, size, and hash metadata")
    return sorted(objects.values(), key=lambda item: item.name)


def _snapshot_gcs_path(client: Any, snapshot_id: str) -> str:
    query = f"""
        select gcs_path
        from `{RAW_GTFS_SNAPSHOTS_TABLE}`
        where snapshot_id = @snapshot_id
        limit 2
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)]
    )
    rows = list(client.query(query, job_config=config).result())
    if len(rows) != 1 or not getattr(rows[0], "gcs_path", None):
        raise RuntimeError(f"No exact GCS path found for GTFS snapshot {snapshot_id}")
    return str(rows[0].gcs_path)


def _gcs_uri_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise ValueError(f"GTFS snapshot path is not a GCS URI: {uri}")
    name = parsed.path.removeprefix("/")
    _gcs_relative_name(name, RAW_GTFS_PREFIX)
    return parsed.netloc, name


def _enforce_input_bounds(config: MatcherConfig, objects: list[GcsObject], gtfs_size: int, workspace: Path) -> None:
    if len(objects) > config.max_gps_objects:
        raise RuntimeError(f"Matcher GPS object count exceeds limit: {len(objects)} > {config.max_gps_objects}")
    gps_bytes = sum(item.size or 0 for item in objects)
    if gps_bytes > config.max_gps_bytes:
        raise RuntimeError(f"Matcher GPS input bytes exceed limit: {gps_bytes} > {config.max_gps_bytes}")
    free_bytes = shutil.disk_usage(workspace).free
    required_bytes = gps_bytes + gtfs_size + config.min_free_disk_bytes
    if free_bytes < required_bytes:
        raise RuntimeError(f"Matcher workspace free disk is below input plus reserve: {free_bytes} < {required_bytes}")


def _download_inputs(
    client: Any,
    config: MatcherConfig,
    processing_date: str,
    include_prior_gps: bool,
    snapshot_id: str,
    snapshot_uri: str,
    workspace: Path,
    expected_input_inventory_digest: str | None = None,
) -> tuple[list[dict[str, object]], dict[str, object], Path, Path]:
    gps_root = workspace / "gps"
    gps_bucket = client.bucket(GCS_BUCKET)
    objects = _list_gps_objects(gps_bucket, processing_date, include_prior_gps)
    gtfs_bucket_name, gtfs_name = _gcs_uri_parts(snapshot_uri)
    gtfs_bucket = client.bucket(gtfs_bucket_name)
    gtfs_blob = gtfs_bucket.get_blob(gtfs_name)
    if gtfs_blob is None:
        raise RuntimeError(f"Pinned GTFS ZIP no longer exists: {snapshot_uri}")
    gtfs_inventory = asdict(
        GcsObject(
            gtfs_name,
            str(getattr(gtfs_blob, "generation", "")) or None,
            int(gtfs_blob.size) if getattr(gtfs_blob, "size", None) is not None else None,
            getattr(gtfs_blob, "md5_hash", None),
            getattr(gtfs_blob, "crc32c", None),
        )
    )
    if (
        gtfs_inventory["generation"] is None
        or gtfs_inventory["size"] is None
        or not (gtfs_inventory["md5_hash"] or gtfs_inventory["crc32c"])
    ):
        raise RuntimeError("Pinned GTFS ZIP inventory requires object generation, size, and hash metadata")
    actual_inventory_digest = matcher_input_inventory_digest(
        processing_date=processing_date,
        snapshot_id=snapshot_id,
        snapshot_gcs_path=snapshot_uri,
        include_prior_gps=include_prior_gps,
        input_dates=_gps_input_dates(processing_date, include_prior_gps),
        gtfs_object=gtfs_inventory,
        gps_objects=[asdict(item) for item in objects],
    )
    _require_expected_inventory_digest(expected_input_inventory_digest, actual_inventory_digest)
    _enforce_input_bounds(config, objects, int(gtfs_inventory["size"]), workspace)
    inventory = []
    for item in objects:
        destination = _contained_destination(gps_root, _gcs_relative_name(item.name, RAW_GPS_PREFIX))
        destination.parent.mkdir(parents=True, exist_ok=True)
        gps_bucket.blob(item.name, generation=item.generation).download_to_filename(destination)
        inventory.append(asdict(item))

    gtfs_path = workspace / "gtfs" / "snapshot.zip"
    gtfs_path.parent.mkdir(parents=True, exist_ok=True)
    gtfs_bucket.blob(gtfs_name, generation=gtfs_inventory["generation"]).download_to_filename(gtfs_path)
    return inventory, gtfs_inventory, gps_root, gtfs_path


def _matcher_argv(
    config: MatcherConfig,
    processing_date: str,
    snapshot_id: str,
    include_prior_gps: bool,
    gps_root: Path,
    gtfs_zip: Path,
    output: Path,
) -> list[str]:
    return [
        *config.command,
        "prepare",
        "--processing-date",
        processing_date,
        "--include-prior-gps" if include_prior_gps else "--no-include-prior-gps",
        "--snapshot-id",
        snapshot_id,
        "--gps-root",
        str(gps_root),
        "--gtfs-zip",
        str(gtfs_zip),
        "--output-dir",
        str(output),
        "--threads",
        "2",
        "--alignment-workers",
        "1",
        "--memory-limit",
        "384MB",
        "--temp-limit",
        "20GB",
    ]


def _invoke_matcher(argv: list[str], config: MatcherConfig) -> None:
    if config.project_dir is not None and not config.project_dir.is_dir():
        raise RuntimeError(f"MATCHER_PROJECT_DIR is not mounted: {config.project_dir}")
    subprocess.run(  # noqa: S603
        argv,
        cwd=config.project_dir,
        check=True,
        timeout=config.timeout_seconds,
        shell=False,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_arrow_type(field: FieldSpec, pa: Any) -> Any:
    base = {
        "STRING": pa.string(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "INTEGER": pa.int64(),
        "FLOAT": pa.float64(),
        "BOOLEAN": pa.bool_(),
    }[field.bigquery_type]
    return pa.list_(base) if field.repeated else base


def _validation_connection(duckdb: Any, temp_directory: Path) -> Any:
    temp_directory.mkdir(parents=True, exist_ok=False)
    connection = duckdb.connect()
    temp_directory_sql = temp_directory.as_posix().replace("'", "''")
    connection.execute(f"set memory_limit = '{VALIDATION_MEMORY_LIMIT}'")
    connection.execute(f"set temp_directory = '{temp_directory_sql}'")
    connection.execute(f"set max_temp_directory_size = '{VALIDATION_TEMP_LIMIT}'")
    connection.execute("set threads = 2")
    connection.execute("set preserve_insertion_order = false")
    return connection


def _query_scalar(connection: Any, query: str, parameters: list[object]) -> object:
    row = connection.execute(query, parameters).fetchone()
    if row is None:
        raise RuntimeError("DuckDB validation query returned no row")
    return row[0]


def _query_count(connection: Any, query: str, parameters: list[object]) -> int:
    value = _query_scalar(connection, query, parameters)
    if not isinstance(value, int):
        raise TypeError("DuckDB validation count is not an integer")
    return value


def _validate_artifact_relationships(artifacts: dict[str, ArtifactValidation]) -> None:
    """Require the published adapters to describe one internally complete trip set."""
    try:
        duckdb = import_module("duckdb")
    except ImportError as exc:
        raise RuntimeError("duckdb is required for matcher artifact validation") from exc

    temp_directory = artifacts["trip"].path.parent / ".validation-relationships"
    connection = _validation_connection(duckdb, temp_directory)
    paths = {key: str(artifacts[key].path) for key in ("trip", "stop_semantics", "expected_stop_event", "stop_arrival")}
    try:
        violations = connection.execute(
            """
            with trips as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                from read_parquet(?)
            ), semantics as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id,
                    countif(are_passenger_boundaries_settled and is_passenger_stop) as passenger_stop_count
                from read_parquet(?)
                group by all
            ), events as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number,
                    count(*) as event_count
                from read_parquet(?)
                group by all
            ), arrivals as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence
                from read_parquet(?)
            ), event_stops as (
                select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence
                from read_parquet(?)
            )
            select
                (select count(*) from trips t left join semantics s using (
                    gtfs_snapshot_id, processing_date, service_date, trip_id
                ) where s.trip_id is null) as trips_without_semantics,
                (select count(*) from trips t inner join semantics s using (
                    gtfs_snapshot_id, processing_date, service_date, trip_id
                ) left join events e using (
                    gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                ) where s.passenger_stop_count != coalesce(e.event_count, 0)) as trips_with_incomplete_events,
                (select count(*) from events e left join trips t using (
                    gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                ) where t.trip_id is null) as events_without_trip,
                (select count(*) from arrivals a left join trips t using (
                    gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                ) where t.trip_id is null) as arrivals_without_trip,
                (select count(*) from arrivals a left join event_stops e using (
                    gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number, stop_sequence
                ) where e.trip_id is null) as arrivals_without_event
            """,
            [
                paths["trip"],
                paths["stop_semantics"],
                paths["expected_stop_event"],
                paths["stop_arrival"],
                paths["expected_stop_event"],
            ],
        ).fetchone()
    finally:
        connection.close()
        shutil.rmtree(temp_directory, ignore_errors=True)
    if violations is None:
        raise RuntimeError("Matcher relationship validation returned no row")
    names = (
        "trips_without_semantics",
        "trips_with_incomplete_events",
        "events_without_trip",
        "arrivals_without_trip",
        "arrivals_without_event",
    )
    failures = {name: int(count) for name, count in zip(names, violations, strict=True) if count}
    if failures:
        raise RuntimeError(f"Matcher artifacts have inconsistent trip relationships: {failures}")


def _inspect_artifact(path: Path, spec: ArtifactSpec, processing_date: str, snapshot_id: str) -> ArtifactValidation:
    try:
        duckdb = import_module("duckdb")
        pa = import_module("pyarrow")
        pq = import_module("pyarrow.parquet")
    except ImportError as exc:
        raise RuntimeError("duckdb and pyarrow are required for matcher artifact validation") from exc

    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    expected_names = [field.name for field in spec.fields]
    if schema.names != expected_names:
        raise RuntimeError(f"Unexpected schema columns in {path.name}")
    for field, expected in zip(schema, spec.fields, strict=True):
        if field.type != _expected_arrow_type(expected, pa):
            raise RuntimeError(f"Unexpected schema type for {path.name}.{field.name}: {field.type}")

    batch_rows = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=VALIDATION_BATCH_SIZE))
    temp_directory = path.parent / f".validation-{spec.key}"
    connection = _validation_connection(duckdb, temp_directory)
    try:
        rows = _query_count(connection, "select count(*) from read_parquet(?)", [str(path)])
        lineage_fields = tuple(dict.fromkeys(("processing_date", spec.partition_field)))
        lineage_predicate = " or ".join(f"cast({field} as varchar) != ?" for field in lineage_fields)
        bad_lineage = _query_count(
            connection,
            f"""
            select count(*)
            from read_parquet(?)
            where {lineage_predicate}
               or gtfs_snapshot_id != ?
            """,
            [str(path), *(processing_date for _ in lineage_fields), snapshot_id],
        )
        duplicate_grains = _query_count(
            connection,
            f"""
            select count(*)
            from (
                select {", ".join(spec.grain)}
                from read_parquet(?)
                group by {", ".join(spec.grain)}
                having count(*) > 1
            )
            """,
            [str(path)],
        )
        null_grains = _query_count(
            connection,
            f"select count(*) from read_parquet(?) where {' or '.join(f'{field} is null' for field in spec.grain)}",
            [str(path)],
        )
        service_dates = tuple(
            str(row[0])
            for row in connection.execute(
                """
                select cast(service_date as varchar)
                from read_parquet(?)
                group by 1
                order by 1
                limit 3
                """,
                [str(path)],
            ).fetchall()
        )
        repeated_nonempty = tuple(
            (field.name, count)
            for field in spec.fields
            if field.repeated
            and (
                count := _query_count(
                    connection,
                    f"select count(*) from read_parquet(?) where array_length({field.name}) > 0",
                    [str(path)],
                )
            )
        )
        modes = (
            tuple(
                str(row[0])
                for row in connection.execute(
                    "select mode from read_parquet(?) group by 1 order by 1",
                    [str(path)],
                ).fetchall()
            )
            if any(field.name == "mode" for field in spec.fields)
            else ()
        )
    finally:
        connection.close()
        shutil.rmtree(temp_directory, ignore_errors=True)
    if batch_rows != rows:
        raise RuntimeError(f"Parquet batch row count mismatch in {path.name}")
    if bad_lineage:
        fields = " and ".join(lineage_fields)
        raise RuntimeError(f"Lineage mismatch in {path.name}: {fields} must equal the request")
    if duplicate_grains:
        raise RuntimeError(f"Duplicate {spec.key} grain in {path.name}")
    if null_grains:
        raise RuntimeError(f"Null {spec.key} grain in {path.name}")
    return ArtifactValidation(
        path,
        rows,
        _sha256(path),
        path.stat().st_size,
        tuple(sorted(service_dates)),
        repeated_nonempty,
        modes,
    )


def _validate_outputs(
    output: Path, processing_date: str, snapshot_id: str, include_prior_gps: bool
) -> tuple[dict[str, ArtifactValidation], dict[str, object]]:
    manifest_path = output / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError("Matcher did not publish manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("processing_date") != processing_date or manifest.get("snapshot_id") != snapshot_id:
        raise RuntimeError("Matcher manifest processing date or snapshot does not match the request")
    config = manifest.get("config")
    if not isinstance(config, dict) or config.get("include_prior_gps") is not include_prior_gps:
        raise RuntimeError("Matcher manifest input-date policy does not match the request")
    schema_versions = manifest.get("schema_versions")
    outputs = manifest.get("outputs")
    if not isinstance(schema_versions, dict) or not isinstance(outputs, dict):
        raise TypeError("Matcher manifest has no schema/output inventory")
    required = {Path(spec.filename).stem for spec in ARTIFACTS} | {"trip_universe"}
    if required - schema_versions.keys() or required - outputs.keys():
        raise RuntimeError("Matcher manifest is missing required reconstruction artifacts or trip universe")
    for key in required:
        if schema_versions[key] != ARTIFACT_SCHEMA_VERSIONS[key]:
            raise RuntimeError(f"Matcher manifest schema version mismatch for {key}")

    metrics = _read_metrics(output)
    validated = {}
    for spec in ARTIFACTS:
        manifest_key = Path(spec.filename).stem
        artifact = _inspect_artifact(output / spec.filename, spec, processing_date, snapshot_id)
        expected_identity = outputs[manifest_key]
        if not isinstance(expected_identity, dict) or expected_identity.get("sha256") != artifact.sha256:
            raise RuntimeError(f"Matcher manifest hash mismatch for {spec.key}")
        if expected_identity.get("bytes") != artifact.bytes:
            raise RuntimeError(f"Matcher manifest byte count mismatch for {spec.key}")
        expected_rows = metrics.get(Path(spec.filename).stem)
        if spec.key != "stop_semantics" and (not isinstance(expected_rows, int) or expected_rows != artifact.rows):
            raise RuntimeError(f"Matcher metrics row count mismatch for {spec.key}")
        validated[spec.key] = artifact
    _validate_artifact_relationships(validated)
    universe = output / "trip_universe.parquet"
    universe_spec = ArtifactSpec(
        "trip_universe",
        universe.name,
        "trip_universe",
        TRIP_UNIVERSE_FIELDS,
        ("gtfs_snapshot_id", "service_date", "duty_chain_id", "trip_id"),
        "processing_date",
        partition_field="processing_date",
        quality_field=None,
    )
    universe_validation = _inspect_artifact(universe, universe_spec, processing_date, snapshot_id)
    universe_identity = outputs["trip_universe"]
    if not isinstance(universe_identity, dict) or universe_identity.get("sha256") != universe_validation.sha256:
        raise RuntimeError("Matcher trip universe is missing or differs from manifest")
    if universe_identity.get("bytes") != universe_validation.bytes:
        raise RuntimeError("Matcher trip universe differs from its manifest")
    validated["trip_universe"] = universe_validation
    return validated, manifest


def _load_config(spec: ArtifactSpec) -> Any:
    parquet_options = bigquery.ParquetOptions()
    parquet_options.enable_list_inference = True
    return bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(field.name, field.bigquery_type, mode="REPEATED" if field.repeated else "NULLABLE")
            for field in spec.fields
        ],
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        parquet_options=parquet_options,
    )


def _load_artifact(
    client: Any,
    dataset: str,
    run_id: str,
    spec: ArtifactSpec,
    artifact: ArtifactValidation,
    retention_days: int = DEFAULT_STAGING_RETENTION_DAYS,
) -> dict[str, str]:
    table = _table_identity(dataset, run_id, spec, artifact.sha256)
    table_id = table["table_id"]
    job_id = table["job_id"]
    with artifact.path.open("rb") as source:
        try:
            job = client.load_table_from_file(
                source, table_id, job_config=_load_config(spec), job_id=job_id, location=BIGQUERY_LOCATION
            )
        except Conflict:
            job = client.get_job(job_id, project=GCP_PROJECT, location=BIGQUERY_LOCATION)
    _verify_load_job(job, job_id, table_id, spec)
    _set_table_expiration(client, table_id, retention_days)
    _verify_loaded_repeated_fields(client, table_id, artifact)
    return table


def _set_table_expiration(client: Any, table_id: str, retention_days: int) -> None:
    table = client.get_table(table_id)
    table.expires = datetime.now(UTC) + timedelta(days=retention_days)
    client.update_table(table, ["expires"])


def _maintain_staging_table_retention(client: Any, config: MatcherConfig, now: datetime) -> tuple[int, int]:
    deleted_count = 0
    expiration_count = 0
    targets = (
        (config.staging_dataset or "", "matcher_run_"),
        (config.input_dataset, "matcher_run_stage_"),
    )
    cutoff = now - timedelta(days=config.staging_retention_days)
    for dataset, table_prefix in targets:
        dataset_id = f"{GCP_PROJECT}.{dataset}"
        for item in client.list_tables(dataset_id):
            table_name = str(getattr(item, "table_id", ""))
            if not table_name.startswith(table_prefix):
                continue
            table_id = f"{dataset_id}.{table_name}"
            table = client.get_table(table_id)
            created = getattr(table, "created", None)
            if not isinstance(created, datetime):
                continue
            if created < cutoff:
                client.delete_table(table_id, not_found_ok=True)
                deleted_count += 1
                continue
            desired_expiration = created + timedelta(days=config.staging_retention_days)
            expires = getattr(table, "expires", None)
            if isinstance(expires, datetime) and expires <= desired_expiration:
                continue
            table.expires = desired_expiration
            client.update_table(table, ["expires"])
            expiration_count += 1
    return deleted_count, expiration_count


def _maintain_staging_table_retention_best_effort(client: Any, config: MatcherConfig, now: datetime) -> None:
    try:
        deleted_count, expiration_count = _maintain_staging_table_retention(client, config, now)
    except Exception:
        LOGGER.exception("Failed to maintain matcher staging table retention")
        return
    LOGGER.info(
        "Maintained matcher staging table retention: deleted_tables=%d expiration_updates=%d",
        deleted_count,
        expiration_count,
    )


def _verify_loaded_repeated_fields(client: Any, table_id: str, artifact: ArtifactValidation) -> None:
    """Reject a load that silently discards non-empty Parquet LIST values."""
    if not artifact.repeated_nonempty:
        return
    expressions = ", ".join(f"countif(array_length({name}) > 0) as {name}" for name, _ in artifact.repeated_nonempty)
    rows = list(client.query(f"select {expressions} from `{table_id}`", location=BIGQUERY_LOCATION).result())
    if len(rows) != 1:
        raise RuntimeError(f"Matcher repeated-field validation returned no row: {table_id}")
    loaded = rows[0]
    mismatches = {
        name: {"parquet": expected, "bigquery": int(loaded[name])}
        for name, expected in artifact.repeated_nonempty
        if int(loaded[name]) != expected
    }
    if mismatches:
        raise RuntimeError(f"Matcher load lost repeated-field evidence in {table_id}: {mismatches}")


def _table_identity(dataset: str, run_id: str, spec: ArtifactSpec, artifact_sha256: str) -> dict[str, str]:
    """Return deterministic, content-addressed table and load-job identities."""
    return {
        "table_id": _table_id(dataset, run_id, spec, artifact_sha256),
        "job_id": _load_job_id(run_id, spec, artifact_sha256),
    }


def _table_reference_id(table: object) -> str:
    if isinstance(table, str):
        return table
    project = getattr(table, "project", None)
    dataset = getattr(table, "dataset_id", None)
    table_name = getattr(table, "table_id", None)
    if all(isinstance(value, str) and value for value in (project, dataset, table_name)):
        return f"{project}.{dataset}.{table_name}"
    return str(table)


def _verify_load_job(job: Any, job_id: str, table_id: str, spec: ArtifactSpec) -> None:
    """Accept a retried load only when its immutable job/table identity matches."""
    job.result()
    if getattr(job, "job_id", None) != job_id:
        raise RuntimeError(f"Matcher load job ID does not match artifact-bound ID: {job_id}")
    if getattr(job, "state", None) != "DONE" or getattr(job, "error_result", None) is not None:
        raise RuntimeError(f"Matcher load job did not complete successfully: {job_id}")
    if _table_reference_id(getattr(job, "destination", None)) != table_id:
        raise RuntimeError(f"Matcher load job destination does not match expected table: {job_id}")
    if (source_format := getattr(job, "source_format", None)) is not None and str(source_format).upper() != "PARQUET":
        raise RuntimeError(f"Matcher load job source format does not match expected artifact: {job_id}")
    if (write_disposition := getattr(job, "write_disposition", None)) is not None and str(
        write_disposition
    ).upper() != "WRITE_TRUNCATE":
        raise RuntimeError(f"Matcher load job write disposition does not match expected artifact: {job_id}")
    if (schema := getattr(job, "schema", None)) is not None and _schema_signature(list(schema)) != _expected_schema(
        spec
    ):
        raise RuntimeError(f"Matcher load job schema does not match expected artifact: {job_id}")


def _validated_name(config: MatcherConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/validated.json"


def _pending_name(config: MatcherConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/pending.json"


def _cleanup_stale_run_markers(
    client: Any,
    config: MatcherConfig,
    processing_date: str,
    run_id: str,
    now: datetime,
) -> tuple[int, int]:
    bucket = client.bucket(GCS_BUCKET)
    current_run_prefix = f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/"
    cutoffs = {
        "pending.json": now - timedelta(days=config.intermediate_marker_retention_days),
        "validated.json": now - timedelta(days=config.intermediate_marker_retention_days),
        "published.json": now - timedelta(days=config.published_marker_retention_days),
    }
    deleted_count = 0
    deleted_bytes = 0
    for blob in bucket.list_blobs(prefix=f"{config.marker_prefix}/processing_date="):
        marker_name = PurePosixPath(blob.name).name
        cutoff = cutoffs.get(marker_name)
        if (
            cutoff is None
            or blob.name.startswith(current_run_prefix)
            or blob.updated is None
            or blob.generation is None
            or blob.updated >= cutoff
        ):
            continue
        deleted_bytes += int(blob.size or 0)
        bucket.blob(blob.name).delete(if_generation_match=blob.generation)
        deleted_count += 1
    return deleted_count, deleted_bytes


def _cleanup_stale_run_markers_best_effort(
    client: Any,
    config: MatcherConfig,
    processing_date: str,
    run_id: str,
    now: datetime,
) -> None:
    try:
        deleted_count, deleted_bytes = _cleanup_stale_run_markers(client, config, processing_date, run_id, now)
    except Exception:
        LOGGER.exception("Failed to clean stale matcher run markers")
        return
    LOGGER.info(
        "Cleaned stale matcher run markers: deleted_objects=%d deleted_bytes=%d",
        deleted_count,
        deleted_bytes,
    )


def _json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, default=_json_value, separators=(",", ":")).encode("utf-8")


def _write_pending(
    client: Any, config: MatcherConfig, processing_date: str, run_id: str, pending: dict[str, object]
) -> str:
    name = _pending_name(config, processing_date, run_id)
    payload = _json_bytes(pending)
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(
            f"Matcher pending metadata exceeds configured size: {len(payload)} > {config.max_marker_bytes}"
        )
    client.bucket(GCS_BUCKET).blob(name).upload_from_string(payload, content_type="application/json")
    return f"gs://{GCS_BUCKET}/{name}"


def _read_pending(client: Any, config: MatcherConfig, processing_date: str, run_id: str) -> dict[str, object]:
    blob = client.bucket(GCS_BUCKET).blob(_pending_name(config, processing_date, run_id))
    payload = _read_bounded_blob(blob, config, "pending metadata")
    if payload is None:
        raise AssertionError("required pending metadata was unexpectedly absent")
    pending = json.loads(payload)
    if not isinstance(pending, dict):
        raise TypeError("Matcher pending metadata is not an object")
    return pending


def _read_validated_marker(
    client: Any, config: MatcherConfig, processing_date: str, run_id: str
) -> dict[str, object] | None:
    blob = client.bucket(GCS_BUCKET).blob(_validated_name(config, processing_date, run_id))
    payload = _read_bounded_blob(blob, config, "validated marker", missing_ok=True)
    if payload is None:
        return None
    marker = json.loads(payload)
    if not isinstance(marker, dict):
        raise TypeError("Matcher validated marker is not an object")
    return marker


def _read_bounded_blob(blob: Any, config: MatcherConfig, label: str, *, missing_ok: bool = False) -> bytes | None:
    """Read small JSON metadata only after checking its current object metadata."""
    try:
        if not blob.exists():
            if missing_ok:
                return None
            raise RuntimeError(f"Matcher {label} does not exist")
        blob.reload()
    except NotFound:
        if missing_ok:
            return None
        raise RuntimeError(f"Matcher {label} does not exist") from None
    size = getattr(blob, "size", None)
    if not isinstance(size, int) or size < 0:
        raise RuntimeError(f"Matcher {label} has no valid byte size")
    if size > config.max_marker_bytes:
        raise RuntimeError(f"Matcher {label} exceeds configured size: {size} > {config.max_marker_bytes}")
    payload = blob.download_as_bytes()
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(
            f"Matcher {label} exceeds configured size after metadata check: {len(payload)} > {config.max_marker_bytes}"
        )
    return payload


def _marker_is_identical(existing: bytes, payload: bytes) -> bool:
    if existing == payload:
        return True
    try:
        return json.loads(existing) == json.loads(payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        return False


def _write_validated_marker(
    client: Any, config: MatcherConfig, processing_date: str, run_id: str, marker: dict[str, object]
) -> str:
    name = _validated_name(config, processing_date, run_id)
    blob = client.bucket(GCS_BUCKET).blob(name)
    identity = _validated_marker_identity(marker)
    payload = _json_bytes(marker)
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(
            f"Matcher validated marker exceeds configured size: {len(payload)} > {config.max_marker_bytes}"
        )
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
    except PreconditionFailed as exc:
        existing = _read_bounded_blob(blob, config, "existing validated marker", missing_ok=True)
        if existing is None:
            raise RuntimeError(
                f"Matcher validated marker already exists with different immutable run content: gs://{GCS_BUCKET}/{name}"
            ) from exc
        try:
            existing_marker = json.loads(existing)
        except (TypeError, ValueError, UnicodeDecodeError) as error:
            raise RuntimeError(
                f"Matcher validated marker already exists with different immutable run content: gs://{GCS_BUCKET}/{name}"
            ) from error
        if not isinstance(existing_marker, dict) or _validated_marker_identity(existing_marker) != identity:
            raise RuntimeError(
                f"Matcher validated marker already exists with different immutable run content: gs://{GCS_BUCKET}/{name}"
            ) from exc
    return f"gs://{GCS_BUCKET}/{name}"


def _validation_issue(level: str, category: str, message: str, **details: object) -> dict[str, object]:
    return {"level": level, "category": category, "message": message, **details}


def _marker_diagnostics(metrics: dict[str, object]) -> dict[str, object]:
    return {
        name: metrics.get(name)
        for name in ("duty_execution_status_counts", "stop_alignment_missing_stops", "stop_alignment_ambiguous_trips")
    }


def _immutable_gcs_inventory(value: object, label: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise TypeError(f"Matcher immutable identity has no {label} inventory")
    inventory = []
    for item in value:
        if not isinstance(item, dict):
            raise TypeError(f"Matcher immutable identity has invalid {label} inventory")
        name, generation, size = item.get("name"), item.get("generation"), item.get("size")
        md5_hash, crc32c = item.get("md5_hash"), item.get("crc32c")
        if (
            not isinstance(name, str)
            or not isinstance(generation, str)
            or not isinstance(size, int)
            or not isinstance(md5_hash, str | type(None))
            or not isinstance(crc32c, str | type(None))
            or not (md5_hash or crc32c)
        ):
            raise TypeError(f"Matcher immutable identity has invalid {label} object metadata")
        inventory.append(
            {
                "name": name,
                "generation": generation,
                "size": size,
                "md5_hash": md5_hash,
                "crc32c": crc32c,
            }
        )
    return sorted(inventory, key=lambda item: str(item["name"]))


def _immutable_matcher_run_identity(pending: dict[str, object]) -> dict[str, object]:
    """Return the durable content identity, deliberately excluding runtime diagnostics."""
    processing_date, run_id, snapshot_id, snapshot_gcs_path = (
        pending.get("processing_date"),
        pending.get("run_id"),
        pending.get("snapshot_id"),
        pending.get("snapshot_gcs_path"),
    )
    include_prior_gps, gps_input_dates = pending.get("include_prior_gps"), pending.get("gps_input_dates")
    if not all(isinstance(value, str) and value for value in (processing_date, run_id, snapshot_id, snapshot_gcs_path)):
        raise TypeError("Matcher immutable identity is missing run, processing, or snapshot identifiers")
    if not isinstance(include_prior_gps, bool) or not isinstance(gps_input_dates, (list, tuple)):
        raise TypeError("Matcher immutable identity has invalid GPS input-date policy")
    expected_gps_dates = list(_gps_input_dates(processing_date, include_prior_gps))
    if list(gps_input_dates) != expected_gps_dates:
        raise RuntimeError("Matcher immutable identity GPS input-date policy does not match processing date")

    artifacts, tables = pending.get("artifacts"), pending.get("tables")
    if not isinstance(artifacts, dict) or not isinstance(tables, dict):
        raise TypeError("Matcher immutable identity has no artifact/table inventory")
    artifact_identity: dict[str, dict[str, object]] = {}
    artifact_schema_versions = {spec.key: ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem] for spec in ARTIFACTS} | {
        "trip_universe": ARTIFACT_SCHEMA_VERSIONS["trip_universe"]
    }
    specs = {spec.key: spec for spec in ARTIFACTS}
    for key, schema_version in sorted(artifact_schema_versions.items()):
        artifact = artifacts.get(key)
        if not isinstance(artifact, dict):
            raise TypeError(f"Matcher immutable identity is missing {key} artifact metadata")
        sha256, rows = artifact.get("sha256"), artifact.get("rows")
        if not isinstance(sha256, str) or not isinstance(rows, int) or artifact.get("schema_version") != schema_version:
            raise TypeError(f"Matcher immutable identity has invalid {key} artifact metadata")
        identity: dict[str, object] = {
            "sha256": _validated_sha256(sha256),
            "rows": rows,
            "schema_version": schema_version,
        }
        if specs.get(key) is not None:
            table = tables.get(key)
            if not isinstance(table, dict) or not all(
                isinstance(table.get(name), str) for name in ("table_id", "job_id")
            ):
                raise TypeError(f"Matcher immutable identity has invalid {key} table identity")
            identity["table"] = {"table_id": table["table_id"], "job_id": table["job_id"]}
            if not str(table["table_id"]).endswith(_validated_sha256(sha256)):
                raise RuntimeError(f"Matcher immutable identity table is not bound to {key} artifact content")
        artifact_identity[key] = identity

    gtfs_inventory = pending.get("gtfs_inventory")
    if not isinstance(gtfs_inventory, dict):
        raise TypeError("Matcher immutable identity has no GTFS inventory")
    return {
        "identity_contract_version": IMMUTABLE_RUN_IDENTITY_VERSION,
        "run_id": run_id,
        "processing_date": processing_date,
        "snapshot_id": snapshot_id,
        "snapshot_gcs_path": snapshot_gcs_path,
        "gps_input_policy": {"include_prior_gps": include_prior_gps, "gps_input_dates": expected_gps_dates},
        "gps_inventory": _immutable_gcs_inventory(pending.get("gps_inventory"), "GPS"),
        "gtfs_inventory": _immutable_gcs_inventory([gtfs_inventory], "GTFS")[0],
        "artifacts": artifact_identity,
    }


def _validated_marker_identity(marker: dict[str, object]) -> dict[str, object]:
    identity = _immutable_matcher_run_identity(marker)
    if marker.get("immutable_run_identity") != identity:
        raise RuntimeError("Matcher validated marker immutable run identity does not match its content")
    return identity


def _reject_conflicting_marker(
    client: Any, config: MatcherConfig, processing_date: str, run_id: str, pending: dict[str, object]
) -> None:
    """Fail before loading when an immutable marker belongs to different content."""
    marker = _read_validated_marker(client, config, processing_date, run_id)
    if marker is None:
        return
    if _validated_marker_identity(marker) != _immutable_matcher_run_identity(pending):
        name = _validated_name(config, processing_date, run_id)
        raise RuntimeError(
            f"Matcher validated marker already exists with different immutable run content: gs://{GCS_BUCKET}/{name}"
        )


def _reject_legacy_validated_marker(client: Any, config: MatcherConfig, processing_date: str, run_id: str) -> None:
    marker = _read_validated_marker(client, config, processing_date, run_id)
    if marker is None:
        return
    include_prior_gps = marker.get("include_prior_gps")
    gps_input_dates = marker.get("gps_input_dates")
    if (
        not isinstance(marker.get("immutable_run_identity"), dict)
        or not isinstance(include_prior_gps, bool)
        or not isinstance(gps_input_dates, list)
        or gps_input_dates != list(_gps_input_dates(processing_date, include_prior_gps))
    ):
        raise RuntimeError(
            "Legacy single-date matcher validated marker cannot identify two-date inputs; "
            "trigger controlled recovery with a new Airflow run ID"
        )


def _require_expected_inventory_digest(expected: str | None, actual: str) -> None:
    if expected is not None and actual != expected:
        raise RuntimeError("Matcher input inventory digest differs from approved historical plan")


def _require_historical_inventory_digest(run_id: str, expected: str | None) -> None:
    if run_id.startswith(HISTORICAL_RUN_ID_PREFIX) and (
        not isinstance(expected, str) or SHA256_PATTERN.fullmatch(expected) is None
    ):
        raise RuntimeError("Historical matcher runs require a valid expected_input_inventory_digest")


def _pending_tables(config: MatcherConfig, run_id: str, pending: dict[str, object]) -> dict[str, dict[str, str]]:
    artifacts = pending.get("artifacts")
    tables = pending.get("tables")
    if not isinstance(artifacts, dict) or not isinstance(tables, dict):
        raise TypeError("Matcher pending metadata has no artifact/table inventory")
    validated = {}
    for spec in ARTIFACTS:
        artifact = artifacts.get(spec.key)
        table = tables.get(spec.key)
        if not isinstance(artifact, dict) or not isinstance(table, dict):
            raise TypeError(f"Matcher pending metadata is missing {spec.key}")
        sha256 = artifact.get("sha256")
        if not isinstance(sha256, str):
            raise TypeError(f"Matcher pending metadata has no hash for {spec.key}")
        expected_table = _table_id(config.staging_dataset or "", run_id, spec, sha256)
        expected_job = _load_job_id(run_id, spec, sha256)
        if table.get("table_id") != expected_table or table.get("job_id") != expected_job:
            raise RuntimeError(f"Matcher pending table/job identity mismatch for {spec.key}")
        validated[spec.key] = {"table_id": expected_table, "job_id": expected_job}
    return validated


def run_matcher_load(
    processing_date: str,
    snapshot_id: str,
    run_id: str,
    *,
    include_prior_gps: bool,
    expected_input_inventory_digest: str | None = None,
    try_number: int = 1,
) -> dict[str, object]:
    """Run, validate, and load matcher artifacts before publication."""
    config = MatcherConfig.from_env()
    config.validate()
    _require_historical_inventory_digest(run_id, expected_input_inventory_digest)
    if not config.enabled:
        return {"enabled": False, "reason": "MATCHER_ENABLED is false"}

    run_workspace = config.workspace_root / _run_id(run_id)
    workspace = run_workspace / f"attempt-{try_number}"
    output = workspace / "output"
    _validate_run_workspace(config, workspace)
    try:
        # Cleanup is local only; loaded tables and run metadata retain durable retry evidence.
        shutil.rmtree(run_workspace, ignore_errors=True)
        workspace.mkdir(parents=True, exist_ok=False)
        bq_client = bigquery.Client(project=GCP_PROJECT)
        storage_client = storage.Client(project=GCP_PROJECT)
        _maintain_staging_table_retention_best_effort(bq_client, config, datetime.now(UTC))
        _cleanup_stale_run_markers_best_effort(storage_client, config, processing_date, run_id, datetime.now(UTC))
        _reject_legacy_validated_marker(storage_client, config, processing_date, run_id)
        snapshot_uri = _snapshot_gcs_path(bq_client, snapshot_id)
        gps_inventory, gtfs_inventory, gps_root, gtfs_zip = _download_inputs(
            storage_client,
            config,
            processing_date,
            include_prior_gps,
            snapshot_id,
            snapshot_uri,
            workspace,
            expected_input_inventory_digest,
        )
        actual_inventory_digest = matcher_input_inventory_digest(
            processing_date=processing_date,
            snapshot_id=snapshot_id,
            snapshot_gcs_path=snapshot_uri,
            include_prior_gps=include_prior_gps,
            input_dates=_gps_input_dates(processing_date, include_prior_gps),
            gtfs_object=gtfs_inventory,
            gps_objects=gps_inventory,
        )
        # Defend against a future downloader refactor that bypasses its pre-download check.
        _require_expected_inventory_digest(expected_input_inventory_digest, actual_inventory_digest)
        _invoke_matcher(
            _matcher_argv(config, processing_date, snapshot_id, include_prior_gps, gps_root, gtfs_zip, output), config
        )
        artifacts, manifest = _validate_outputs(output, processing_date, snapshot_id, include_prior_gps)
        pending = {
            "run_id": run_id,
            "processing_date": processing_date,
            "snapshot_id": snapshot_id,
            "snapshot_gcs_path": snapshot_uri,
            "include_prior_gps": include_prior_gps,
            "gps_input_dates": _gps_input_dates(processing_date, include_prior_gps),
            "input_inventory_digest": actual_inventory_digest,
            "gps_inventory": gps_inventory,
            "gtfs_inventory": gtfs_inventory,
            "artifacts": {
                key: {
                    "rows": item.rows,
                    "bytes": item.bytes,
                    "sha256": item.sha256,
                    "schema_version": (
                        ARTIFACT_SCHEMA_VERSIONS["trip_universe"]
                        if key == "trip_universe"
                        else ARTIFACT_SCHEMA_VERSIONS[
                            Path(next(spec.filename for spec in ARTIFACTS if spec.key == key)).stem
                        ]
                    ),
                    "service_dates": item.service_dates,
                    "modes": item.modes,
                }
                for key, item in artifacts.items()
            },
            "tables": {
                spec.key: _table_identity(config.staging_dataset or "", run_id, spec, artifacts[spec.key].sha256)
                for spec in ARTIFACTS
            },
            "metrics": manifest.get("metrics", _read_metrics(output)),
        }
        pending["immutable_run_identity"] = _immutable_matcher_run_identity(pending)
        _reject_conflicting_marker(storage_client, config, processing_date, run_id, pending)
        for spec in ARTIFACTS:
            _load_artifact(
                bq_client,
                config.staging_dataset or "",
                run_id,
                spec,
                artifacts[spec.key],
                config.staging_retention_days,
            )
        pending_uri = _write_pending(storage_client, config, processing_date, run_id, pending)
    except Exception:
        if not config.keep_workspace:
            shutil.rmtree(run_workspace, ignore_errors=True)
        raise
    if not config.keep_workspace:
        shutil.rmtree(run_workspace, ignore_errors=True)
    return {
        "enabled": True,
        "status": "loaded_pending",
        "processing_date": processing_date,
        "run_id": run_id,
        "pending_uri": pending_uri,
    }


def _validate_run_workspace(config: MatcherConfig, workspace: Path) -> None:
    """Do not let per-run cleanup delete the mounted matcher project."""
    if config.project_dir is not None and config.project_dir.resolve().is_relative_to(workspace.resolve()):
        raise ValueError("MATCHER_PROJECT_DIR must not be inside the run workspace or output")


def _read_metrics(output: Path) -> dict[str, object]:
    metrics_path = output / "metrics.json"
    return json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.is_file() else {}


def _json_value(value: object) -> object:
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return value


def _publication_table_id(
    dataset: str, processing_date: str, run_id: str, spec: ArtifactSpec, artifact_sha256: str
) -> str:
    digest = _validated_sha256(artifact_sha256)
    return f"{GCP_PROJECT}.{dataset}.matcher_run_stage_{spec.table_suffix}_{processing_date.replace('-', '')}_{_run_id(run_id)}_{digest[:16]}"


def _publication_job_id(
    action: str, processing_date: str, run_id: str, spec: ArtifactSpec, artifact_sha256: str
) -> str:
    """Return a retry-safe job ID that cannot collide across normalized run IDs."""
    digest = _validated_sha256(artifact_sha256)[:24]
    run_digest = hashlib.sha256(_run_id(run_id).encode("utf-8")).hexdigest()[:16]
    return (
        f"matcher_publish_{PUBLICATION_JOB_VERSION}_{action}_{spec.table_suffix}_"
        f"{processing_date.replace('-', '')}_{run_digest}_{digest}"
    )


def _publication_transaction_job_id(processing_date: str, run_id: str, artifacts: dict[str, dict[str, object]]) -> str:
    digests = ":".join(_validated_sha256(str(artifacts[key]["sha256"])) for key in sorted(STABLE_INPUT_TABLES))
    content_digest = hashlib.sha256(digests.encode("ascii")).hexdigest()[:24]
    run_digest = hashlib.sha256(_run_id(run_id).encode("utf-8")).hexdigest()[:16]
    return (
        f"matcher_publish_{PUBLICATION_JOB_VERSION}_replace_all_"
        f"{processing_date.replace('-', '')}_{run_digest}_{content_digest}"
    )


def _query_job(
    client: Any,
    query: str,
    job_id: str,
    max_bytes: int,
    parameters: list[Any] | None = None,
    destination: str | None = None,
) -> Any:
    """Run a bounded query and only reuse an API-visible identical job on conflict."""
    config = bigquery.QueryJobConfig(
        query_parameters=parameters or [],
        maximum_bytes_billed=max_bytes,
    )
    try:
        job = client.query(
            query,
            job_config=config,
            job_id=job_id,
            location=BIGQUERY_LOCATION,
            job_retry=None,
        )
    except Conflict:
        job = client.get_job(job_id, project=GCP_PROJECT, location=BIGQUERY_LOCATION)
    job.result()
    _verify_query_job_identity(job, query, job_id, max_bytes, parameters or [], destination)
    return job


def _query_parameters_match(actual: object, expected: list[Any]) -> bool:
    """Compare public query parameter fields without depending on client internals."""
    if actual is None or not isinstance(actual, (list, tuple)):
        return True
    actual_values = list(actual)
    if len(actual_values) != len(expected):
        return False
    return all(
        getattr(item, "name", None) == getattr(want, "name", None)
        and getattr(item, "type_", getattr(item, "type", None)) == getattr(want, "type_", getattr(want, "type", None))
        and getattr(item, "value", None) == getattr(want, "value", None)
        for item, want in zip(actual_values, expected, strict=True)
    )


def _verify_query_job_identity(
    job: Any, query: str, job_id: str, max_bytes: int, parameters: list[Any], destination: str | None = None
) -> None:
    """Reject recovery unless the server-visible job is this exact bounded query."""
    if getattr(job, "job_id", job_id) != job_id or getattr(job, "error_result", None) is not None:
        raise RuntimeError(f"Matcher publication query did not complete successfully: {job_id}")
    if getattr(job, "state", "DONE") != "DONE":
        raise RuntimeError(f"Matcher publication query is not complete: {job_id}")
    if (actual_query := getattr(job, "query", None)) is not None and actual_query != query:
        raise RuntimeError(f"Matcher publication query text does not match reused job: {job_id}")
    actual_destination = getattr(job, "destination", None)
    if destination is not None and actual_destination is not None and str(actual_destination) != destination:
        raise RuntimeError(f"Matcher publication query destination does not match reused job: {job_id}")
    if (location := getattr(job, "location", None)) is not None and location != BIGQUERY_LOCATION:
        raise RuntimeError(f"Matcher publication query location does not match reused job: {job_id}")
    if (actual_max_bytes := getattr(job, "maximum_bytes_billed", None)) is not None and actual_max_bytes != max_bytes:
        raise RuntimeError(f"Matcher publication query byte cap does not match reused job: {job_id}")
    configuration = getattr(job, "configuration", None)
    if configuration is not None and getattr(configuration, "maximum_bytes_billed", max_bytes) != max_bytes:
        raise RuntimeError(f"Matcher publication query byte cap does not match reused job: {job_id}")
    if configuration is not None and not _query_parameters_match(
        getattr(configuration, "query_parameters", None), parameters
    ):
        raise RuntimeError(f"Matcher publication query parameters do not match reused job: {job_id}")


def _publication_table_counts(
    client: Any, table_id: str, processing_date: str, spec: ArtifactSpec, job_id: str, max_bytes: int
) -> dict[str, int]:
    job = _query_job(
        client,
        f"""
        select
            count(*) as total_rows,
            countif({spec.partition_field} = @processing_date) as partition_rows,
            countif({spec.partition_field} is null or {spec.partition_field} != @processing_date) as wrong_partition_rows,
            countif(processing_date is null or processing_date != @processing_date) as wrong_processing_date_rows
        from `{table_id}`
        """,
        job_id,
        max_bytes,
        [bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))],
    )
    rows = list(job.result())
    if len(rows) != 1:
        raise RuntimeError("Matcher publication validation did not return one count row")
    row = rows[0]
    values = {
        key: row.get(key) if isinstance(row, dict) else getattr(row, key, None)
        for key in ("total_rows", "partition_rows", "wrong_partition_rows", "wrong_processing_date_rows")
    }
    if not all(isinstance(value, int) for value in values.values()):
        raise TypeError("Matcher publication validation returned invalid row counts")
    return cast("dict[str, int]", values)


def _stable_partition_counts(
    client: Any, table_id: str, processing_date: str, spec: ArtifactSpec, job_id: str, max_bytes: int
) -> dict[str, int]:
    """Validate only the stable partition being replaced, never retained history."""
    job = _query_job(
        client,
        f"""
        select
            count(*) as total_rows,
            countif(processing_date is null or processing_date != @processing_date) as wrong_processing_date_rows
        from `{table_id}`
        where {spec.partition_field} = @processing_date
        """,
        job_id,
        max_bytes,
        [bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))],
    )
    rows = list(job.result())
    if len(rows) != 1:
        raise RuntimeError("Matcher publication stable validation did not return one count row")
    row = rows[0]
    values = {
        key: row.get(key) if isinstance(row, dict) else getattr(row, key, None)
        for key in ("total_rows", "wrong_processing_date_rows")
    }
    if not all(isinstance(value, int) for value in values.values()):
        raise TypeError("Matcher publication stable validation returned invalid row counts")
    return cast("dict[str, int]", values)


def _require_exact_processing_partition(
    client: Any,
    table_id: str,
    processing_date: str,
    spec: ArtifactSpec,
    expected_rows: int,
    job_id: str,
    max_bytes: int,
) -> int:
    """Artifacts and stages must contain only the one requested processing partition."""
    counts = _publication_table_counts(client, table_id, processing_date, spec, job_id, max_bytes)
    if counts["total_rows"] != expected_rows or counts["partition_rows"] != expected_rows:
        raise RuntimeError(
            f"Matcher publication {table_id} row count does not equal its processing {spec.partition_field} partition"
        )
    if counts["wrong_partition_rows"] or counts["wrong_processing_date_rows"]:
        raise RuntimeError(f"Matcher publication {table_id} has rows outside processing_date={processing_date}")
    return counts["total_rows"]


def _require_stable_processing_partition(
    client: Any,
    table_id: str,
    processing_date: str,
    spec: ArtifactSpec,
    expected_rows: int,
    job_id: str,
    max_bytes: int,
) -> int:
    counts = _stable_partition_counts(client, table_id, processing_date, spec, job_id, max_bytes)
    if counts["total_rows"] != expected_rows or counts["wrong_processing_date_rows"]:
        raise RuntimeError(
            f"Matcher publication {table_id} stable {spec.partition_field} partition failed lineage validation"
        )
    return counts["total_rows"]


def _stable_partition_difference_counts(
    client: Any,
    stable_table: str,
    staged_table: str,
    processing_date: str,
    spec: ArtifactSpec,
    max_bytes: int,
) -> dict[str, int]:
    """Compare multisets via canonical JSON so repeated fields remain part of row equality."""
    columns = _column_list(spec)
    query = f"""
        with staged as (
            select row_json, count(*) as row_count
            from (
                select to_json_string(struct({columns})) as row_json
                from `{staged_table}`
                where {spec.partition_field} = @processing_date
            )
            group by row_json
        ), stable as (
            select row_json, count(*) as row_count
            from (
                select to_json_string(struct({columns})) as row_json
                from `{stable_table}`
                where {spec.partition_field} = @processing_date
            )
            group by row_json
        )
        select
            coalesce((
                select sum(row_count)
                from (select row_json, row_count from staged except distinct select row_json, row_count from stable)
            ), 0) as staged_only_rows,
            coalesce((
                select sum(row_count)
                from (select row_json, row_count from stable except distinct select row_json, row_count from staged)
            ), 0) as stable_only_rows
    """
    # Do not assign a reusable job ID or use query cache: this must observe current stable contents.
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))
        ],
        maximum_bytes_billed=max_bytes,
        use_query_cache=False,
    )
    job = client.query(query, job_config=config, location=BIGQUERY_LOCATION, job_retry=None)
    rows = list(job.result())
    if len(rows) != 1:
        raise RuntimeError("Matcher publication content validation did not return one row")
    row = rows[0]
    values = {
        key: row.get(key) if isinstance(row, dict) else getattr(row, key, None)
        for key in ("staged_only_rows", "stable_only_rows")
    }
    if not all(isinstance(value, int) for value in values.values()):
        raise TypeError("Matcher publication content validation returned invalid row differences")
    return cast("dict[str, int]", values)


def _require_stable_partition_equals_stage(
    client: Any,
    stable_table: str,
    staged_table: str,
    processing_date: str,
    spec: ArtifactSpec,
    max_bytes: int,
) -> None:
    differences = _stable_partition_difference_counts(
        client, stable_table, staged_table, processing_date, spec, max_bytes
    )
    if differences["staged_only_rows"] or differences["stable_only_rows"]:
        raise RuntimeError(
            f"Matcher publication stable {spec.partition_field} partition is not content-identical to staged {spec.key}: "
            f"{differences}"
        )


def _schema_signature(fields: list[Any]) -> tuple[tuple[str, str, str], ...]:
    return tuple((str(field.name), str(field.field_type).upper(), str(field.mode).upper()) for field in fields)


def _expected_schema(spec: ArtifactSpec) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (field.name, field.bigquery_type, "REPEATED" if field.repeated else "NULLABLE") for field in spec.fields
    )


def _stage_labels(spec: ArtifactSpec, artifact_sha256: str) -> dict[str, str]:
    return {
        "matcher_schema_version": ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem],
        "matcher_artifact_sha256": _validated_sha256(artifact_sha256)[:63],
    }


def _verify_table_contract(
    client: Any, table_id: str, spec: ArtifactSpec, labels: dict[str, str] | None = None
) -> None:
    table = client.get_table(table_id)
    if _schema_signature(list(getattr(table, "schema", []))) != _expected_schema(spec):
        raise RuntimeError(f"Matcher publication table schema does not exactly match {spec.key}: {table_id}")
    partitioning = getattr(table, "time_partitioning", None)
    if getattr(partitioning, "field", None) != spec.partition_field:
        raise RuntimeError(f"Matcher publication table must be partitioned by {spec.partition_field}: {table_id}")
    if labels is not None:
        actual_labels = getattr(table, "labels", None) or {}
        if {key: actual_labels.get(key) for key in labels} != labels:
            raise RuntimeError(
                f"Matcher publication stage table labels do not match immutable artifact identity: {table_id}"
            )


def _column_list(spec: ArtifactSpec) -> str:
    return ", ".join(f"`{field.name}`" for field in spec.fields)


def _publication_transaction_query(published: dict[str, dict[str, object]]) -> str:
    """Replace all stable partitions together; callers must preflight every stage first."""
    statements = ["begin transaction;"]
    for spec in ARTIFACTS:
        stable_table = str(published[spec.key]["stable_table"])
        staged_table = str(published[spec.key]["staged_table"])
        columns = _column_list(spec)
        statements.extend(
            [
                f"delete from `{stable_table}` where {spec.partition_field} = @processing_date;",
                f"insert into `{stable_table}` ({columns}) select {columns} from `{staged_table}` "
                f"where {spec.partition_field} = @processing_date;",
            ]
        )
    statements.append("commit transaction;")
    return "\n".join(statements)


def _stage_artifact(
    client: Any,
    source_table: str,
    staged_table: str,
    processing_date: str,
    run_id: str,
    spec: ArtifactSpec,
    artifact_sha256: str,
    max_bytes: int,
    retention_days: int = DEFAULT_STAGING_RETENTION_DAYS,
) -> None:
    """Create a stage only through its deterministic query job, then validate it."""
    columns = _column_list(spec)
    labels = _stage_labels(spec, artifact_sha256)
    label_sql = ", ".join(f'("{key}", "{value}")' for key, value in labels.items())
    _query_job(
        client,
        f"""
        create table `{staged_table}`
        partition by {spec.partition_field}
        options (
          expiration_timestamp=timestamp_add(current_timestamp(), interval {retention_days} day),
          labels=[{label_sql}]
        ) as
        select {columns}
        from `{source_table}`
        where {spec.partition_field} = @processing_date
        """,
        _publication_job_id("stage", processing_date, run_id, spec, artifact_sha256),
        max_bytes,
        [bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))],
        staged_table,
    )
    _verify_table_contract(client, staged_table, spec, labels)


def _published_name(config: MatcherConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/published.json"


def _published_marker_identity_payload(marker: dict[str, object]) -> dict[str, object]:
    stable_inputs = marker.get("stable_inputs")
    if not isinstance(stable_inputs, dict):
        raise TypeError("Matcher publication marker has no stable input identity")
    tables: dict[str, dict[str, object]] = {}
    for spec in ARTIFACTS:
        item = stable_inputs.get(spec.key)
        if not isinstance(item, dict):
            raise TypeError(f"Matcher publication marker has no {spec.key} stable identity")
        required = {name: item.get(name) for name in ("stable_table", "staged_table", "sha256", "rows")}
        if not isinstance(required["stable_table"], str) or not isinstance(required["staged_table"], str):
            raise TypeError(f"Matcher publication marker has invalid {spec.key} table identity")
        if not isinstance(required["rows"], int):
            raise TypeError(f"Matcher publication marker has invalid {spec.key} row identity")
        tables[spec.key] = required
    identity = {
        "identity_contract_version": "matcher-publication-identity-v1",
        "processing_date": marker.get("processing_date"),
        "run_id": marker.get("run_id"),
        "transaction_job_id": marker.get("transaction_job_id"),
        "stable_inputs": tables,
    }
    if not all(
        isinstance(identity[name], str) and identity[name]
        for name in ("processing_date", "run_id", "transaction_job_id")
    ):
        raise TypeError("Matcher publication marker has invalid publication identity")
    return identity


def _published_marker_identity(marker: dict[str, object]) -> dict[str, object]:
    identity = _published_marker_identity_payload(marker)
    if marker.get("publication_identity") != identity:
        raise RuntimeError("Matcher publication marker identity does not match its stable content")
    return identity


def _write_published_marker(
    client: Any, config: MatcherConfig, processing_date: str, run_id: str, marker: dict[str, object]
) -> str:
    name = _published_name(config, processing_date, run_id)
    blob = client.bucket(GCS_BUCKET).blob(name)
    identity = _published_marker_identity(marker)
    payload = _json_bytes(marker)
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(
            f"Matcher publication marker exceeds configured size: {len(payload)} > {config.max_marker_bytes}"
        )
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
    except PreconditionFailed as exc:
        existing = _read_bounded_blob(blob, config, "existing publication marker", missing_ok=True)
        if existing is None:
            raise RuntimeError(
                f"Matcher publication marker already exists with different content: gs://{GCS_BUCKET}/{name}"
            ) from exc
        try:
            existing_marker = json.loads(existing)
        except (TypeError, ValueError, UnicodeDecodeError) as error:
            raise RuntimeError(
                f"Matcher publication marker already exists with different content: gs://{GCS_BUCKET}/{name}"
            ) from error
        if not isinstance(existing_marker, dict) or _published_marker_identity(existing_marker) != identity:
            raise RuntimeError(
                f"Matcher publication marker already exists with different content: gs://{GCS_BUCKET}/{name}"
            ) from exc
    return f"gs://{GCS_BUCKET}/{name}"


def _delete_publication_stages_best_effort(client: Any, published: dict[str, dict[str, object]]) -> None:
    deleted_count = 0
    failed_count = 0
    for spec in ARTIFACTS:
        table_id = str(published[spec.key]["staged_table"])
        try:
            client.delete_table(table_id, not_found_ok=True)
        except Exception:
            failed_count += 1
            LOGGER.exception("Failed to delete matcher publication staging table: %s", table_id)
            continue
        deleted_count += 1
    LOGGER.info(
        "Deleted matcher publication staging tables: deleted_tables=%d failed_tables=%d",
        deleted_count,
        failed_count,
    )


def publish_staged_artifacts(
    processing_date: str,
    run_id: str,
) -> dict[str, object]:
    """Atomically publish prevalidated run tables into stable input partitions."""
    config = MatcherConfig.from_env()
    config.validate()
    if not config.enabled:
        raise RuntimeError("Matcher publication requires MATCHER_ENABLED=true")
    date.fromisoformat(processing_date)

    storage_client = storage.Client(project=GCP_PROJECT)
    marker = _read_validated_marker(storage_client, config, processing_date, run_id)
    if marker is None:
        raise RuntimeError("Matcher publication requires a validated marker")
    if marker.get("processing_date") != processing_date or marker.get("run_id") != run_id:
        raise RuntimeError("Matcher publication marker does not match this processing date and run")
    _validated_marker_identity(marker)
    run_tables = _pending_tables(config, run_id, marker)
    artifacts = marker.get("artifacts")
    if not isinstance(artifacts, dict):
        raise TypeError("Matcher publication marker has no artifact inventory")

    # Complete every source/stage check before inspecting or changing stable inputs.
    # A stage failure therefore cannot delete either retained stable partition.
    bq_client = bigquery.Client(project=GCP_PROJECT)
    _maintain_staging_table_retention_best_effort(bq_client, config, datetime.now(UTC))
    _ensure_stable_input_tables(bq_client, config.input_dataset)
    published: dict[str, dict[str, object]] = {}
    for spec in ARTIFACTS:
        artifact = artifacts.get(spec.key)
        if (
            not isinstance(artifact, dict)
            or not isinstance(artifact.get("sha256"), str)
            or not isinstance(artifact.get("rows"), int)
        ):
            raise TypeError(f"Matcher publication marker has invalid {spec.key} artifact metadata")
        sha256, expected_rows = str(artifact.get("sha256")), int(cast("int", artifact.get("rows")))
        source_table = run_tables[spec.key]["table_id"]
        staged_table = _publication_table_id(config.input_dataset, processing_date, run_id, spec, sha256)
        stable_table = f"{GCP_PROJECT}.{config.input_dataset}.{STABLE_INPUT_TABLES[spec.key]}"
        source_rows = _require_exact_processing_partition(
            bq_client,
            source_table,
            processing_date,
            spec,
            expected_rows,
            _publication_job_id("source_validate", processing_date, run_id, spec, sha256),
            config.max_publication_bytes,
        )
        _stage_artifact(
            bq_client,
            source_table,
            staged_table,
            processing_date,
            run_id,
            spec,
            sha256,
            config.max_publication_bytes,
            config.staging_retention_days,
        )
        staged_rows = _require_exact_processing_partition(
            bq_client,
            staged_table,
            processing_date,
            spec,
            expected_rows,
            _publication_job_id("stage_validate", processing_date, run_id, spec, sha256),
            config.max_publication_bytes,
        )
        published[spec.key] = {
            "source_table": source_table,
            "staged_table": staged_table,
            "stable_table": stable_table,
            "rows": staged_rows,
            "source_rows": source_rows,
            "sha256": sha256,
            "job_ids": {
                action: _publication_job_id(action, processing_date, run_id, spec, sha256)
                for action in ("source_validate", "stage", "stage_validate")
            },
        }

    for spec in ARTIFACTS:
        _verify_table_contract(bq_client, str(published[spec.key]["stable_table"]), spec)

    pre_counts = {
        spec.key: _stable_partition_counts(
            bq_client,
            str(published[spec.key]["stable_table"]),
            processing_date,
            spec,
            _publication_job_id("precount", processing_date, run_id, spec, str(published[spec.key]["sha256"])),
            config.max_publication_bytes,
        )
        for spec in ARTIFACTS
    }
    transaction_job_id = _publication_transaction_job_id(processing_date, run_id, published)
    _query_job(
        bq_client,
        _publication_transaction_query(published),
        transaction_job_id,
        config.max_publication_bytes,
        [bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))],
    )
    try:
        for spec in ARTIFACTS:
            stable_rows = _require_stable_processing_partition(
                bq_client,
                str(published[spec.key]["stable_table"]),
                processing_date,
                spec,
                cast("int", published[spec.key]["rows"]),
                _publication_job_id("postvalidate", processing_date, run_id, spec, str(published[spec.key]["sha256"])),
                config.max_publication_bytes,
            )
            _require_stable_partition_equals_stage(
                bq_client,
                str(published[spec.key]["stable_table"]),
                str(published[spec.key]["staged_table"]),
                processing_date,
                spec,
                config.max_publication_bytes,
            )
            published[spec.key]["rows"] = stable_rows
            published[spec.key]["job_ids"] = cast("dict[str, str]", published[spec.key]["job_ids"]) | {
                "precount": _publication_job_id(
                    "precount", processing_date, run_id, spec, str(published[spec.key]["sha256"])
                ),
                "postvalidate": _publication_job_id(
                    "postvalidate", processing_date, run_id, spec, str(published[spec.key]["sha256"])
                ),
            }
    except Exception:
        LOGGER.exception(
            "Matcher publication transaction committed but post-commit validation failed; marker remains absent. "
            "Restore the externally captured pre-publication partition copies if rollback is required; "
            "this function records counts only: %s",
            pre_counts,
        )
        raise
    marker_uri = _write_published_marker(
        storage_client,
        config,
        processing_date,
        run_id,
        {
            "processing_date": processing_date,
            "run_id": run_id,
            "stable_inputs": published,
            "transaction_job_id": transaction_job_id,
            "pre_publication_partition_counts": pre_counts,
            "rollback_boundary": (
                "The four-table transaction is committed before post-validation. If post-validation fails, "
                "the marker is absent but the transaction is not rolled back; restore the externally "
                "captured pre-publication partition copies."
            ),
        }
        | {
            "publication_identity": {
                "identity_contract_version": "matcher-publication-identity-v1",
                "processing_date": processing_date,
                "run_id": run_id,
                "transaction_job_id": transaction_job_id,
                "stable_inputs": {
                    spec.key: {
                        name: published[spec.key][name] for name in ("stable_table", "staged_table", "sha256", "rows")
                    }
                    for spec in ARTIFACTS
                },
            }
        },
    )
    _delete_publication_stages_best_effort(bq_client, published)
    _cleanup_stale_run_markers_best_effort(storage_client, config, processing_date, run_id, datetime.now(UTC))
    return {"enabled": True, "status": "published", "marker_uri": marker_uri, "stable_inputs": published}


def _bigquery_schema(spec: ArtifactSpec) -> list[Any]:
    return [
        bigquery.SchemaField(
            field.name,
            field.bigquery_type,
            mode="REPEATED" if field.repeated else "NULLABLE",
        )
        for field in spec.fields
    ]


def _ensure_stable_input_tables(client: Any, dataset: str) -> None:
    """Idempotently create the derived matcher-input dataset and stable tables."""
    dataset_id = f"{GCP_PROJECT}.{dataset}"
    dataset_resource = bigquery.Dataset(dataset_id)
    dataset_resource.location = BIGQUERY_LOCATION
    client.create_dataset(dataset_resource, exists_ok=True)
    existing_dataset = client.get_dataset(dataset_id)
    location = getattr(existing_dataset, "location", BIGQUERY_LOCATION)
    if location != BIGQUERY_LOCATION:
        raise RuntimeError(f"Matcher input dataset must use {BIGQUERY_LOCATION}: {dataset_id}")

    for spec in ARTIFACTS:
        table_id = f"{dataset_id}.{STABLE_INPUT_TABLES[spec.key]}"
        table = bigquery.Table(table_id, schema=_bigquery_schema(spec))
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=spec.partition_field,
            require_partition_filter=True,
        )
        table.labels = {
            "managed_by": "python_matcher",
            "matcher_schema_version": ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem],
        }
        client.create_table(table, exists_ok=True)
        _verify_table_contract(client, table_id, spec)


def _validate_publication_invariants(pending: dict[str, object], config: MatcherConfig) -> dict[str, object]:
    """Reject artifacts that are incomplete, unbounded, or missing required execution evidence."""
    issues: list[dict[str, object]] = []
    artifacts = pending.get("artifacts")
    metrics = pending.get("metrics")
    if not isinstance(artifacts, dict) or not isinstance(metrics, dict):
        raise TypeError("Matcher pending metadata is incomplete")
    for spec in ARTIFACTS:
        artifact = artifacts.get(spec.key)
        rows = artifact.get("rows") if isinstance(artifact, dict) else None
        if not isinstance(rows, int) or rows <= 0:
            issues.append(_validation_issue("fail", "artifact", f"{spec.key} artifact is empty", rows=rows))
        if any(field.name == "mode" for field in spec.fields):
            modes = artifact.get("modes") if isinstance(artifact, dict) else None
            if not isinstance(modes, (list, tuple)) or set(modes) != {"bus", "tram"}:
                issues.append(
                    _validation_issue(
                        "fail", "artifact", f"{spec.key} artifact is missing a transport mode", modes=modes
                    )
                )

    trip_artifact = artifacts.get("trip")
    trip_rows = trip_artifact.get("rows") if isinstance(trip_artifact, dict) else None
    accepted_fact_executions = metrics.get("accepted_fact_executions")
    if not isinstance(accepted_fact_executions, int) or trip_rows != accepted_fact_executions:
        issues.append(
            _validation_issue(
                "fail",
                "artifact",
                "trip facts do not match eligible accepted executions",
                trip_rows=trip_rows,
                accepted_fact_executions=accepted_fact_executions,
            )
        )

    peak_rss = metrics.get("peak_rss_bytes")
    swapping = metrics.get("swapping_observed")
    if not isinstance(peak_rss, int) or peak_rss > config.max_rss_bytes:
        issues.append(
            _validation_issue(
                "fail",
                "resource",
                "peak RSS is missing or exceeds configured bound",
                peak_rss_bytes=peak_rss,
                peak_rss_bytes_max=config.max_rss_bytes,
            )
        )
    if swapping is not False:
        issues.append(
            _validation_issue("fail", "resource", "swapping was observed or not measured", swapping_observed=swapping)
        )
    return {
        "status": "fail" if issues else "pass",
        "validation_contract_version": "matcher-invariants-v1",
        "resource_bounds": {
            "peak_rss_bytes": peak_rss,
            "peak_rss_bytes_max": config.max_rss_bytes,
            "swapping_observed": swapping,
            "swapping_observed_must_be": False,
        },
        "issues": issues,
    }


def publish_matcher_artifacts(
    processing_date: str, run_id: str, pending_context: dict[str, object]
) -> dict[str, object]:
    """Validate a loaded run and atomically publish its four stable partitions."""
    if pending_context.get("status") != "loaded_pending":
        raise RuntimeError("Matcher publication requires a successful load")
    if pending_context.get("processing_date") != processing_date or pending_context.get("run_id") != run_id:
        raise RuntimeError("Matcher pending context does not match this DAG run")
    config = MatcherConfig.from_env()
    config.validate()
    storage_client = storage.Client(project=GCP_PROJECT)
    pending = _read_pending(storage_client, config, processing_date, run_id)
    if pending.get("processing_date") != processing_date or pending.get("run_id") != run_id:
        raise RuntimeError("Matcher pending metadata does not match this DAG run")
    _pending_tables(config, run_id, pending)
    existing_marker = _read_validated_marker(storage_client, config, processing_date, run_id)
    if existing_marker is not None:
        if _validated_marker_identity(existing_marker) != _immutable_matcher_run_identity(pending):
            raise RuntimeError("Matcher validated marker already exists with different immutable run content")
        validation = existing_marker.get("validation")
        if not isinstance(validation, dict) or validation.get("status") != "pass":
            raise RuntimeError("Matcher validated marker does not record a passing validation")
        marker_uri = f"gs://{GCS_BUCKET}/{_validated_name(config, processing_date, run_id)}"
    else:
        validation = _validate_publication_invariants(pending, config)
        if validation["status"] != "pass":
            raise RuntimeError(f"Matcher publication invariants failed: {validation['issues']}")
        marker = pending | {
            "immutable_run_identity": _immutable_matcher_run_identity(pending),
            "validation_contract_version": "matcher-invariants-v1",
            "validation": validation,
            "diagnostics": _marker_diagnostics(cast("dict[str, object]", pending["metrics"])),
        }
        marker_uri = _write_validated_marker(storage_client, config, processing_date, run_id, marker)
    published = publish_staged_artifacts(processing_date, run_id)
    if published.get("enabled") is not True or published.get("status") != "published":
        raise RuntimeError("Matcher publication is incomplete")
    return published | {"validated_marker_uri": marker_uri, "validation": validation}
