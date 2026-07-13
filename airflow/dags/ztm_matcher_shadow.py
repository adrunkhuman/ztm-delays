"""Isolated, opt-in matcher reconstruction runs for comparison only.

This module intentionally has no dependency on canonical dbt publication.  A
successful marker is the only signal that a shadow run is complete.
"""

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
from datetime import date, datetime, timedelta
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
)

LOGGER = logging.getLogger(__name__)

RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
CANONICAL_FACT_TABLES = {
    "trip": f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.fct_trip",
    "stop_arrival": f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.fct_stop_arrival",
    "expected_stop_event": f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.fct_expected_stop_event",
}
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
DEFAULT_MAX_COMPARISON_BYTES = 5 * 1024**3
DEFAULT_MAX_MARKER_BYTES = 20 * 1024**2
COMPARISON_CONTRACT_VERSION = "matcher-shadow-comparison-v4"
DEFAULT_GATE_CURRENT_RETENTION_MIN = 0.75
DEFAULT_GATE_PRIOR_RETENTION_MIN = 0.40
DEFAULT_GATE_COMPLETE_RATE_DROP_MAX = 0.15
DEFAULT_GATE_EXPECTED_RATE_DELTA_MAX = 0.15
DEFAULT_GATE_DELAY_PERCENTILE_RATIO_MAX = 2.0
DEFAULT_GATE_DELAY_TAIL_DELTA_MAX = 0.15
DEFAULT_GATE_MATERIAL_LINE_ROWS = 20
DEFAULT_GATE_PEAK_RSS_BYTES = 2 * 1024**3
MAX_MANUAL_SWAP_EXCEPTION_BYTES = 64 * 1024**2
DEFAULT_MAX_PROMOTION_BYTES = 5 * 1024**3
COMPARISON_DATE_COUNT = 2
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
    """One matcher artifact loaded as a dedicated shadow table."""

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


# These schemas are deliberately local adapter schemas, not canonical fact schemas.
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
COMPARISON_ARTIFACTS = tuple(spec for spec in ARTIFACTS if spec.key in CANONICAL_FACT_TABLES)
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
class ShadowConfig:
    """Runtime-only shadow settings; no environment is changed by this module."""

    enabled: bool
    strict: bool
    dataset: str | None
    workspace_root: Path
    command: tuple[str, ...]
    project_dir: Path | None
    timeout_seconds: int
    marker_prefix: str
    keep_workspace: bool = False
    max_gps_objects: int = DEFAULT_MAX_GPS_OBJECTS
    max_gps_bytes: int = DEFAULT_MAX_GPS_BYTES
    min_free_disk_bytes: int = DEFAULT_MIN_FREE_DISK_BYTES
    max_comparison_bytes: int = DEFAULT_MAX_COMPARISON_BYTES
    max_marker_bytes: int = DEFAULT_MAX_MARKER_BYTES

    @classmethod
    def from_env(cls) -> ShadowConfig:
        """Read the shadow-only runtime configuration."""
        project_dir = os.getenv("MATCHER_SHADOW_PROJECT_DIR", "/opt/airflow/matcher").strip()
        try:
            command = tuple(
                shlex.split(
                    os.getenv("MATCHER_SHADOW_COMMAND", "uv run --locked --project /opt/airflow/matcher ztm-matcher")
                )
            )
        except ValueError as error:
            raise ValueError("MATCHER_SHADOW_COMMAND has invalid shell-style quoting") from error
        return cls(
            enabled=_env_bool("MATCHER_SHADOW_ENABLED", False),
            strict=_env_bool("MATCHER_SHADOW_STRICT", False),
            dataset=os.getenv("BIGQUERY_MATCHER_SHADOW_DATASET", "").strip() or None,
            workspace_root=Path(os.getenv("MATCHER_SHADOW_WORKSPACE_ROOT", "/opt/airflow/matcher-shadow")),
            command=command,
            project_dir=Path(project_dir) if project_dir else None,
            timeout_seconds=_env_positive_int("MATCHER_SHADOW_TIMEOUT_SECONDS", 45 * 60),
            marker_prefix=os.getenv("MATCHER_SHADOW_GCS_PREFIX", "shadow/matcher").strip(),
            keep_workspace=_env_bool("MATCHER_SHADOW_KEEP_WORKSPACE", False),
            max_gps_objects=_env_positive_int("MATCHER_SHADOW_MAX_GPS_OBJECTS", DEFAULT_MAX_GPS_OBJECTS),
            max_gps_bytes=_env_positive_int("MATCHER_SHADOW_MAX_GPS_BYTES", DEFAULT_MAX_GPS_BYTES),
            min_free_disk_bytes=_env_positive_int("MATCHER_SHADOW_MIN_FREE_DISK_BYTES", DEFAULT_MIN_FREE_DISK_BYTES),
            max_comparison_bytes=_env_positive_int("MATCHER_SHADOW_MAX_COMPARISON_BYTES", DEFAULT_MAX_COMPARISON_BYTES),
            max_marker_bytes=_env_positive_int("MATCHER_SHADOW_MAX_MARKER_BYTES", DEFAULT_MAX_MARKER_BYTES),
        )

    def validate(self) -> None:
        """Reject enabled configurations that could reach canonical datasets."""
        if not self.command:
            raise ValueError("MATCHER_SHADOW_COMMAND must not be empty")
        if self.project_dir is None:
            raise ValueError("MATCHER_SHADOW_PROJECT_DIR is required")
        command_projects = _command_projects(self.command)
        if len(command_projects) != 1 or Path(command_projects[0]) != self.project_dir:
            raise ValueError("MATCHER_SHADOW_COMMAND --project must match MATCHER_SHADOW_PROJECT_DIR")
        if not self.enabled:
            return
        if not self.dataset:
            raise ValueError("BIGQUERY_MATCHER_SHADOW_DATASET is required when MATCHER_SHADOW_ENABLED=true")
        _validate_dataset_id("BIGQUERY_MATCHER_SHADOW_DATASET", self.dataset)
        forbidden = {value.casefold() for value in (BIGQUERY_RAW_DATASET, BIGQUERY_INT_DATASET, BIGQUERY_MARTS_DATASET)}
        if self.dataset.casefold() in forbidden:
            raise ValueError("BIGQUERY_MATCHER_SHADOW_DATASET must not name a canonical raw, int, or marts dataset")
        if not self.marker_prefix:
            raise ValueError("MATCHER_SHADOW_GCS_PREFIX must not be empty")
        _strict_posix_name(self.marker_prefix)
        if not self.workspace_root.is_absolute():
            raise ValueError("MATCHER_SHADOW_WORKSPACE_ROOT must be absolute")
        for name, value in (
            ("MATCHER_SHADOW_MAX_GPS_OBJECTS", self.max_gps_objects),
            ("MATCHER_SHADOW_MAX_GPS_BYTES", self.max_gps_bytes),
            ("MATCHER_SHADOW_MIN_FREE_DISK_BYTES", self.min_free_disk_bytes),
            ("MATCHER_SHADOW_MAX_COMPARISON_BYTES", self.max_comparison_bytes),
            ("MATCHER_SHADOW_MAX_MARKER_BYTES", self.max_marker_bytes),
        ):
            if value < 1:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class GcsObject:
    """Immutable object inventory retained in the shadow marker."""

    name: str
    generation: str | None
    size: int | None
    md5_hash: str | None
    crc32c: str | None


@dataclass(frozen=True)
class ArtifactValidation:
    """Validated local artifact information used for loading and the commit marker."""

    path: Path
    rows: int
    sha256: str
    bytes: int
    service_dates: tuple[str, ...]


@dataclass(frozen=True)
class CutoverConfig:
    """Manual-only configuration for promoting a validated shadow run."""

    enabled: bool
    input_dataset: str
    max_promotion_bytes: int

    @classmethod
    def from_env(cls) -> CutoverConfig:
        """Read the manual cutover settings without enabling a promotion."""
        return cls(
            enabled=_env_bool("MATCHER_CUTOVER_ENABLED", False),
            input_dataset=os.getenv("BIGQUERY_MATCHER_INPUT_DATASET", BIGQUERY_MATCHER_INPUT_DATASET).strip()
            or BIGQUERY_MATCHER_INPUT_DATASET,
            max_promotion_bytes=_env_positive_int("MATCHER_CUTOVER_MAX_BYTES", DEFAULT_MAX_PROMOTION_BYTES),
        )

    def validate(self, shadow: ShadowConfig) -> None:
        """Reject an enabled cutover unless its isolated shadow prerequisites hold."""
        _validate_dataset_id("BIGQUERY_MATCHER_INPUT_DATASET", self.input_dataset)
        datasets = (BIGQUERY_RAW_DATASET, BIGQUERY_INT_DATASET, BIGQUERY_MARTS_DATASET, shadow.dataset)
        forbidden = {value.casefold() for value in datasets if value}
        if self.input_dataset.casefold() in forbidden:
            raise ValueError("BIGQUERY_MATCHER_INPUT_DATASET must not name a shadow, raw, int, or marts dataset")
        if not self.enabled:
            return
        if not shadow.enabled:
            raise ValueError("MATCHER_CUTOVER_ENABLED requires MATCHER_SHADOW_ENABLED=true")


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


def _env_nonnegative_float(name: str, default: float) -> float:
    value = os.getenv(name, str(default)).strip()
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a non-negative number") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be a non-negative number")
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
    return f"{GCP_PROJECT}.{dataset}.matcher_shadow_{spec.table_suffix}_{_run_id(run_id)}_{digest}"


def _load_job_id(run_id: str, spec: ArtifactSpec, artifact_sha256: str) -> str:
    run_digest = hashlib.sha256(_run_id(run_id).encode("utf-8")).hexdigest()[:16]
    artifact_digest = hashlib.sha256(_validated_sha256(artifact_sha256).encode("ascii")).hexdigest()[:16]
    return f"matcher_shadow_load_{spec.table_suffix}_{run_digest}_{artifact_digest}"


def _gps_prefixes(processing_date: str) -> list[str]:
    return [f"{RAW_GPS_PREFIX}/vehicle_type={mode}/date={processing_date}/" for mode in VEHICLE_TYPES]


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


def _list_gps_objects(bucket: Any, processing_date: str) -> list[GcsObject]:
    objects = []
    for prefix in _gps_prefixes(processing_date):
        for blob in bucket.list_blobs(prefix=prefix):
            name = str(blob.name)
            if not (name.endswith(".parquet") and "/part-" in name):
                continue
            _gcs_relative_name(name, prefix)
            objects.append(
                GcsObject(
                    name,
                    str(getattr(blob, "generation", "")) or None,
                    int(blob.size) if getattr(blob, "size", None) is not None else None,
                    getattr(blob, "md5_hash", None),
                    getattr(blob, "crc32c", None),
                )
            )
    if not objects:
        raise RuntimeError(f"No bus/tram GPS part objects found for {processing_date}")
    if any(item.generation is None or item.size is None or not (item.md5_hash or item.crc32c) for item in objects):
        raise RuntimeError("Shadow GPS inventory requires object generation, size, and hash metadata")
    return sorted(objects, key=lambda item: item.name)


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


def _enforce_input_bounds(config: ShadowConfig, objects: list[GcsObject], gtfs_size: int, workspace: Path) -> None:
    if len(objects) > config.max_gps_objects:
        raise RuntimeError(f"Shadow GPS object count exceeds limit: {len(objects)} > {config.max_gps_objects}")
    gps_bytes = sum(item.size or 0 for item in objects)
    if gps_bytes > config.max_gps_bytes:
        raise RuntimeError(f"Shadow GPS input bytes exceed limit: {gps_bytes} > {config.max_gps_bytes}")
    free_bytes = shutil.disk_usage(workspace).free
    required_bytes = gps_bytes + gtfs_size + config.min_free_disk_bytes
    if free_bytes < required_bytes:
        raise RuntimeError(f"Shadow workspace free disk is below input plus reserve: {free_bytes} < {required_bytes}")


def _download_inputs(
    client: Any, config: ShadowConfig, processing_date: str, snapshot_uri: str, workspace: Path
) -> tuple[list[dict[str, object]], dict[str, object], Path, Path]:
    gps_root = workspace / "gps"
    gps_bucket = client.bucket(GCS_BUCKET)
    objects = _list_gps_objects(gps_bucket, processing_date)
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
    _enforce_input_bounds(config, objects, int(gtfs_inventory["size"]), workspace)
    inventory = []
    for item in objects:
        destination = _contained_destination(gps_root, _gcs_relative_name(item.name, RAW_GPS_PREFIX))
        destination.parent.mkdir(parents=True, exist_ok=True)
        gps_bucket.blob(item.name, generation=item.generation).download_to_filename(destination)
        inventory.append(asdict(item))

    gtfs_path = workspace / "gtfs" / "snapshot.zip"
    gtfs_path.parent.mkdir(parents=True, exist_ok=True)
    gtfs_blob.download_to_filename(gtfs_path)
    return inventory, gtfs_inventory, gps_root, gtfs_path


def _matcher_argv(
    config: ShadowConfig, processing_date: str, snapshot_id: str, gps_root: Path, gtfs_zip: Path, output: Path
) -> list[str]:
    return [
        *config.command,
        "prepare",
        "--processing-date",
        processing_date,
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


def _invoke_matcher(argv: list[str], config: ShadowConfig) -> None:
    if config.project_dir is not None and not config.project_dir.is_dir():
        raise RuntimeError(f"MATCHER_SHADOW_PROJECT_DIR is not mounted: {config.project_dir}")
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


def _inspect_artifact(path: Path, spec: ArtifactSpec, processing_date: str, snapshot_id: str) -> ArtifactValidation:
    try:
        duckdb = import_module("duckdb")
        pa = import_module("pyarrow")
        pq = import_module("pyarrow.parquet")
    except ImportError as exc:
        raise RuntimeError("duckdb and pyarrow are required for matcher shadow validation") from exc

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
    return ArtifactValidation(path, rows, _sha256(path), path.stat().st_size, tuple(sorted(service_dates)))


def _validate_outputs(
    output: Path, processing_date: str, snapshot_id: str
) -> tuple[dict[str, ArtifactValidation], dict[str, object]]:
    manifest_path = output / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError("Matcher did not publish manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("processing_date") != processing_date or manifest.get("snapshot_id") != snapshot_id:
        raise RuntimeError("Matcher manifest processing date or snapshot does not match the shadow request")
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

    expected_dates = {processing_date, (date.fromisoformat(processing_date) - timedelta(days=1)).isoformat()}
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
        if not expected_dates.issubset(artifact.service_dates):
            raise RuntimeError(f"Matcher {spec.key} lacks current/prior service-date evidence")
        expected_rows = metrics.get(Path(spec.filename).stem)
        if spec.key != "stop_semantics" and (not isinstance(expected_rows, int) or expected_rows != artifact.rows):
            raise RuntimeError(f"Matcher metrics row count mismatch for {spec.key}")
        validated[spec.key] = artifact
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
    if universe_identity.get("bytes") != universe_validation.bytes or not expected_dates.issubset(
        universe_validation.service_dates
    ):
        raise RuntimeError("Matcher trip universe lacks expected current/prior evidence")
    validated["trip_universe"] = universe_validation
    return validated, manifest


def _load_config(spec: ArtifactSpec) -> Any:
    return bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(field.name, field.bigquery_type, mode="REPEATED" if field.repeated else "NULLABLE")
            for field in spec.fields
        ],
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )


def _load_artifact(
    client: Any, dataset: str, run_id: str, spec: ArtifactSpec, artifact: ArtifactValidation
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
    return table


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
        raise RuntimeError(f"Shadow load job ID does not match artifact-bound ID: {job_id}")
    if getattr(job, "state", None) != "DONE" or getattr(job, "error_result", None) is not None:
        raise RuntimeError(f"Shadow load job did not complete successfully: {job_id}")
    if _table_reference_id(getattr(job, "destination", None)) != table_id:
        raise RuntimeError(f"Shadow load job destination does not match expected table: {job_id}")
    if (source_format := getattr(job, "source_format", None)) is not None and str(source_format).upper() != "PARQUET":
        raise RuntimeError(f"Shadow load job source format does not match expected artifact: {job_id}")
    if (write_disposition := getattr(job, "write_disposition", None)) is not None and str(
        write_disposition
    ).upper() != "WRITE_TRUNCATE":
        raise RuntimeError(f"Shadow load job write disposition does not match expected artifact: {job_id}")
    if (schema := getattr(job, "schema", None)) is not None and _schema_signature(list(schema)) != _expected_schema(
        spec
    ):
        raise RuntimeError(f"Shadow load job schema does not match expected artifact: {job_id}")


def _comparison_query(shadow_tables: dict[str, dict[str, str]]) -> str:
    sections = []
    for spec in COMPARISON_ARTIFACTS:
        shadow = shadow_tables[spec.key]["table_id"]
        canonical = CANONICAL_FACT_TABLES[spec.key]
        source_date = "gps_date" if spec.key == "trip" else "source_gps_date"
        delay = "end_delay_seconds" if spec.key == "trip" else "delay_seconds"
        status = "cast(null as string)" if spec.key != "expected_stop_event" else "fact.observation_status"
        grain = ", ".join(f"fact.{field}" for field in spec.grain)
        for source, table in (("shadow", shadow), ("canonical", canonical)):
            trip_table = shadow_tables["trip"]["table_id"] if source == "shadow" else CANONICAL_FACT_TABLES["trip"]
            cohort = f"""
                select
                    trip.gtfs_snapshot_id,
                    trip.gps_date,
                    trip.service_date,
                    trip.trip_id,
                    trip.vehicle_number,
                    trip.line,
                    trip.mode,
                    trip.trip_quality,
                    trip.scheduled_end_time,
                    trip.end_delay_seconds
                from `{trip_table}` as trip
                where trip.gps_date = @processing_date
                  and trip.gtfs_snapshot_id = @gtfs_snapshot_id
                  and trip.service_date in unnest(@service_dates)
                  and (
                      trip.service_date = @processing_date
                      or (
                          trip.service_date = date_sub(@processing_date, interval 1 day)
                          and trip.scheduled_end_time >= timestamp(@processing_date, 'Europe/Warsaw')
                      )
                  )
            """
            if spec.key == "trip":
                fact_source = "from cohort as fact"
                fact_filter = ""
            else:
                fact_source = f"""
                    from `{table}` as fact
                    inner join cohort
                        on fact.gtfs_snapshot_id = cohort.gtfs_snapshot_id
                        and fact.gps_date = cohort.gps_date
                        and fact.service_date = cohort.service_date
                        and fact.trip_id = cohort.trip_id
                        and fact.vehicle_number = cohort.vehicle_number
                """
                # Canonical stop facts require this partition predicate. Keep the
                # processing lineage separate from source_gps_date diagnostics.
                fact_filter = """
                    where fact.service_date in unnest(@service_dates)
                      and fact.gps_date = @processing_date
                """
            sections.append(
                f"""(
                with cohort as ({cohort})
                select
                    '{spec.key}' as artifact,
                    '{source}' as source,
                    fact.service_date,
                    fact.mode,
                    fact.line,
                    fact.gtfs_snapshot_id,
                    fact.trip_quality,
                    {status} as observation_status,
                    count(*) as row_count,
                    count(distinct to_json_string(struct({grain}))) as distinct_grains,
                    avg(fact.{delay}) as avg_delay_seconds,
                    approx_quantiles(fact.{delay}, 100)[offset(50)] as delay_p50_seconds,
                    approx_quantiles(fact.{delay}, 100)[offset(90)] as delay_p90_seconds,
                    approx_quantiles(fact.{delay}, 100)[offset(95)] as delay_p95_seconds,
                    countif(abs(fact.{delay}) > 3600) as abs_delay_over_3600_count,
                    countif({status} = 'uncertain') as uncertain_count,
                    countif({status} = 'missed') as missed_count,
                    countif(fact.{source_date} != fact.service_date) as overnight_rows,
                    count(distinct fact.{source_date}) as source_date_count,
                    array_agg(
                        distinct cast(fact.{source_date} as string) ignore nulls
                        order by cast(fact.{source_date} as string)
                    ) as source_dates
                {fact_source}
                {fact_filter}
                group by
                    artifact,
                    source,
                    fact.service_date,
                    fact.mode,
                    fact.line,
                    fact.gtfs_snapshot_id,
                    fact.trip_quality,
                    observation_status
                )"""
            )
    return " union all ".join(sections)


def _comparison_report(
    client: Any,
    processing_date: str,
    snapshot_id: str,
    shadow_tables: dict[str, dict[str, str]],
    max_comparison_bytes: int,
) -> dict[str, object]:
    if not snapshot_id.strip():
        raise ValueError("Matcher shadow comparison requires a nonempty snapshot ID")
    dates = [date.fromisoformat(processing_date) - timedelta(days=1), date.fromisoformat(processing_date)]
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("service_dates", "DATE", dates),
            bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date)),
            bigquery.ScalarQueryParameter("gtfs_snapshot_id", "STRING", snapshot_id),
        ],
        maximum_bytes_billed=max_comparison_bytes,
    )
    rows = [
        {key: _json_value(value) for key, value in (dict(row.items()) if hasattr(row, "items") else dict(row)).items()}
        for row in client.query(_comparison_query(shadow_tables), job_config=config).result()
    ]
    # BigQuery does not preserve result order without ORDER BY; markers must be rerun-stable.
    rows.sort(key=lambda row: _json_bytes(row).decode("utf-8"))
    grouped: dict[tuple[object, ...], dict[str, dict[str, object]]] = {}
    for row in rows:
        key = tuple(
            row[field]
            for field in (
                "artifact",
                "service_date",
                "mode",
                "line",
                "gtfs_snapshot_id",
                "trip_quality",
                "observation_status",
            )
        )
        grouped.setdefault(key, {})[str(row["source"])] = row
    differences = [
        {"group": list(key), "shadow": values.get("shadow"), "canonical": values.get("canonical")}
        for key, values in grouped.items()
        if _comparison_payload(values.get("shadow")) != _comparison_payload(values.get("canonical"))
    ]
    differences.sort(key=lambda difference: _json_bytes(difference).decode("utf-8"))
    return {
        "comparison_contract_version": COMPARISON_CONTRACT_VERSION,
        "service_dates": [item.isoformat() for item in dates],
        "aggregates": rows,
        "differences": differences,
    }


def _comparison_payload(row: dict[str, object] | None) -> dict[str, object] | None:
    """Exclude the query-only source discriminator from aggregate equality."""
    return None if row is None else {key: value for key, value in row.items() if key != "source"}


def _marker_name(config: ShadowConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/commit.json"


def _pending_name(config: ShadowConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/pending.json"


def _json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, default=_json_value, separators=(",", ":")).encode("utf-8")


def _write_pending(
    client: Any, config: ShadowConfig, processing_date: str, run_id: str, pending: dict[str, object]
) -> str:
    name = _pending_name(config, processing_date, run_id)
    client.bucket(GCS_BUCKET).blob(name).upload_from_string(_json_bytes(pending), content_type="application/json")
    return f"gs://{GCS_BUCKET}/{name}"


def _read_pending(client: Any, config: ShadowConfig, processing_date: str, run_id: str) -> dict[str, object]:
    blob = client.bucket(GCS_BUCKET).blob(_pending_name(config, processing_date, run_id))
    payload = _read_bounded_blob(blob, config, "pending metadata")
    if payload is None:
        raise AssertionError("required pending metadata was unexpectedly absent")
    pending = json.loads(payload)
    if not isinstance(pending, dict):
        raise TypeError("Matcher shadow pending metadata is not an object")
    return pending


def _read_marker(client: Any, config: ShadowConfig, processing_date: str, run_id: str) -> dict[str, object] | None:
    blob = client.bucket(GCS_BUCKET).blob(_marker_name(config, processing_date, run_id))
    payload = _read_bounded_blob(blob, config, "commit marker", missing_ok=True)
    if payload is None:
        return None
    marker = json.loads(payload)
    if not isinstance(marker, dict):
        raise TypeError("Matcher shadow commit marker is not an object")
    return marker


def _read_bounded_blob(blob: Any, config: ShadowConfig, label: str, *, missing_ok: bool = False) -> bytes | None:
    """Read small JSON metadata only after checking its current object metadata."""
    try:
        if not blob.exists():
            if missing_ok:
                return None
            raise RuntimeError(f"Matcher shadow {label} does not exist")
        blob.reload()
    except NotFound:
        if missing_ok:
            return None
        raise RuntimeError(f"Matcher shadow {label} does not exist") from None
    size = getattr(blob, "size", None)
    if not isinstance(size, int) or size < 0:
        raise RuntimeError(f"Matcher shadow {label} has no valid byte size")
    if size > config.max_marker_bytes:
        raise RuntimeError(f"Matcher shadow {label} exceeds configured size: {size} > {config.max_marker_bytes}")
    payload = blob.download_as_bytes()
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(
            f"Matcher shadow {label} exceeds configured size after metadata check: "
            f"{len(payload)} > {config.max_marker_bytes}"
        )
    return payload


def _marker_is_identical(existing: bytes, payload: bytes) -> bool:
    if existing == payload:
        return True
    try:
        return json.loads(existing) == json.loads(payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        return False


def _write_marker(
    client: Any, config: ShadowConfig, processing_date: str, run_id: str, marker: dict[str, object]
) -> str:
    name = _marker_name(config, processing_date, run_id)
    blob = client.bucket(GCS_BUCKET).blob(name)
    payload = _json_bytes(marker)
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(f"Matcher shadow marker exceeds configured size: {len(payload)} > {config.max_marker_bytes}")
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
    except PreconditionFailed as exc:
        existing = _read_bounded_blob(blob, config, "existing commit marker", missing_ok=True)
        if existing is None or not _marker_is_identical(existing, payload):
            raise RuntimeError(
                f"Matcher shadow commit marker already exists with different content: gs://{GCS_BUCKET}/{name}"
            ) from exc
    return f"gs://{GCS_BUCKET}/{name}"


def _gate_thresholds() -> dict[str, Any]:
    """Return all committed gate settings, including advisory thresholds."""
    return {
        "current_row_retention_min": _env_nonnegative_float(
            "MATCHER_SHADOW_GATE_CURRENT_ROW_RETENTION_MIN", DEFAULT_GATE_CURRENT_RETENTION_MIN
        ),
        "prior_row_retention_min": _env_nonnegative_float(
            "MATCHER_SHADOW_GATE_PRIOR_ROW_RETENTION_MIN", DEFAULT_GATE_PRIOR_RETENTION_MIN
        ),
        "complete_rate_drop_max": _env_nonnegative_float(
            "MATCHER_SHADOW_GATE_COMPLETE_RATE_DROP_MAX", DEFAULT_GATE_COMPLETE_RATE_DROP_MAX
        ),
        "expected_rate_delta_max": _env_nonnegative_float(
            "MATCHER_SHADOW_GATE_EXPECTED_RATE_DELTA_MAX", DEFAULT_GATE_EXPECTED_RATE_DELTA_MAX
        ),
        "delay_percentile_ratio_max": _env_nonnegative_float(
            "MATCHER_SHADOW_GATE_DELAY_PERCENTILE_RATIO_MAX", DEFAULT_GATE_DELAY_PERCENTILE_RATIO_MAX
        ),
        "delay_tail_delta_max": _env_nonnegative_float(
            "MATCHER_SHADOW_GATE_DELAY_TAIL_DELTA_MAX", DEFAULT_GATE_DELAY_TAIL_DELTA_MAX
        ),
        "material_line_rows_min": _env_positive_int(
            "MATCHER_SHADOW_GATE_MATERIAL_LINE_ROWS_MIN", DEFAULT_GATE_MATERIAL_LINE_ROWS
        ),
        "peak_rss_bytes_max": _env_positive_int("MATCHER_SHADOW_GATE_PEAK_RSS_BYTES_MAX", DEFAULT_GATE_PEAK_RSS_BYTES),
        "swapping_observed_must_be": False,
    }


def _gate_issue(level: str, category: str, message: str, **details: object) -> dict[str, object]:
    return {"level": level, "category": category, "message": message, **details}


def _ratio(numerator: float, denominator: float) -> float | None:
    return None if denominator == 0 else round(float(numerator) / float(denominator), 6)


def _retention_issue_level(artifact: str, service_date: str, current_date: str) -> str:
    """Only current-date trip retention can reject a strict shadow run."""
    return "fail" if artifact == "trip" and service_date == current_date else "warn"


def evaluate_shadow_gate(comparison: dict[str, object], metrics: dict[str, object]) -> dict[str, object]:
    """Evaluate deterministic shadow evidence; it never grants canonical ownership."""
    thresholds = _gate_thresholds()
    raw_aggregates, raw_dates = comparison.get("aggregates", []), comparison.get("service_dates", [])
    if (
        not isinstance(raw_aggregates, list)
        or not isinstance(raw_dates, list)
        or len(raw_dates) != COMPARISON_DATE_COUNT
    ):
        raise ValueError("Matcher shadow comparison requires aggregate rows for prior and current dates")
    aggregates = cast("list[dict[str, Any]]", raw_aggregates)
    dates = cast("list[Any]", raw_dates)
    _, current_date = (str(value) for value in dates)
    issues: list[dict[str, object]] = []
    paired: dict[tuple[str, ...], dict[str, dict[str, Any]]] = {}
    mode_counts: dict[tuple[str, str, str, str], int] = {}
    line_counts: dict[tuple[str, str, str, str, str], int] = {}
    trip_counts: dict[tuple[str, str, str], dict[str, int]] = {}
    expected_counts: dict[tuple[str, str, str], list[int]] = {}
    for row in aggregates:
        if not isinstance(row, dict) or row.get("source") not in {"shadow", "canonical"}:
            raise ValueError("Matcher shadow comparison contains an invalid aggregate row")
        source, count, distinct = str(row["source"]), int(row.get("row_count", 0)), int(row.get("distinct_grains", 0))
        if count != distinct:
            issues.append(
                _gate_issue(
                    "fail",
                    "structural",
                    "row count differs from distinct grain count",
                    source=source,
                    artifact=row.get("artifact"),
                    service_date=row.get("service_date"),
                    mode=row.get("mode"),
                    row_count=count,
                    distinct_grains=distinct,
                )
            )
        key = tuple(
            str(row.get(field) or "")
            for field in (
                "artifact",
                "service_date",
                "mode",
                "line",
                "gtfs_snapshot_id",
                "trip_quality",
                "observation_status",
            )
        )
        paired.setdefault(key, {})[source] = row
        artifact, service_date, mode, line = (
            str(row.get("artifact") or ""),
            str(row.get("service_date") or ""),
            str(row.get("mode") or ""),
            str(row.get("line") or ""),
        )
        mode_key = (artifact, service_date, mode, source)
        mode_counts[mode_key] = mode_counts.get(mode_key, 0) + count
        line_key = (artifact, service_date, mode, line, source)
        line_counts[line_key] = line_counts.get(line_key, 0) + count
        rate_key = (str(row.get("service_date") or ""), str(row.get("mode") or ""), source)
        if row.get("artifact") == "trip":
            quality = str(row.get("trip_quality") or "unknown")
            trip_counts.setdefault(rate_key, {})[quality] = trip_counts.setdefault(rate_key, {}).get(quality, 0) + count
        if row.get("artifact") == "expected_stop_event":
            values = expected_counts.setdefault(rate_key, [0, 0, 0])
            values[0] += count
            values[1] += int(row.get("uncertain_count", 0))
            values[2] += int(row.get("missed_count", 0))

    retention = []
    material_line_retention = []
    delay_changes = []
    canonical_mode_keys = {key[:3] for key in mode_counts if key[3] == "canonical"}
    for artifact, service_date, mode in sorted(canonical_mode_keys):
        shadow_rows = mode_counts.get((artifact, service_date, mode, "shadow"), 0)
        canonical_rows = mode_counts[(artifact, service_date, mode, "canonical")]
        threshold = float(
            thresholds["current_row_retention_min"]
            if service_date == current_date
            else thresholds["prior_row_retention_min"]
        )
        retention_ratio = _ratio(shadow_rows, canonical_rows)
        evidence = {
            "artifact": artifact,
            "service_date": service_date,
            "mode": mode,
            "shadow_rows": shadow_rows,
            "canonical_rows": canonical_rows,
            "ratio": retention_ratio,
            "minimum": threshold,
        }
        retention.append(evidence)
        level = _retention_issue_level(artifact, service_date, current_date)
        if shadow_rows == 0:
            issues.append(_gate_issue(level, "retention", "shadow mode is completely missing", **evidence))
        elif retention_ratio is not None and retention_ratio < threshold:
            issues.append(_gate_issue(level, "retention", "shadow mode row retention below threshold", **evidence))
    canonical_line_keys = {key[:4] for key in line_counts if key[4] == "canonical"}
    for artifact, service_date, mode, line in sorted(canonical_line_keys):
        canonical_rows = line_counts[(artifact, service_date, mode, line, "canonical")]
        if canonical_rows < int(thresholds["material_line_rows_min"]):
            continue
        shadow_rows = line_counts.get((artifact, service_date, mode, line, "shadow"), 0)
        threshold = float(
            thresholds["current_row_retention_min"]
            if service_date == current_date
            else thresholds["prior_row_retention_min"]
        )
        retention_ratio = _ratio(shadow_rows, canonical_rows)
        evidence = {
            "artifact": artifact,
            "service_date": service_date,
            "mode": mode,
            "line": line,
            "shadow_rows": shadow_rows,
            "canonical_rows": canonical_rows,
            "ratio": retention_ratio,
            "minimum": threshold,
        }
        material_line_retention.append(evidence)
        # Material lines are mandatory manual-review evidence; only mode-wide
        # current-trip retention is a hard automated cutover gate.
        level = "warn"
        mode_shadow_rows = mode_counts.get((artifact, service_date, mode, "shadow"), 0)
        if shadow_rows == 0 and mode_shadow_rows:
            issues.append(_gate_issue(level, "retention", "material line is completely missing", **evidence))
        elif shadow_rows and retention_ratio is not None and retention_ratio < threshold:
            issues.append(_gate_issue(level, "retention", "material line retention below threshold", **evidence))
    for key, sources in sorted(paired.items()):
        shadow, canonical = sources.get("shadow"), sources.get("canonical")
        if shadow is None or canonical is None:
            continue
        for percentile in ("p50", "p90", "p95"):
            shadow_value, canonical_value = (
                shadow.get(f"delay_{percentile}_seconds"),
                canonical.get(f"delay_{percentile}_seconds"),
            )
            difference = (
                None
                if shadow_value is None or canonical_value is None
                else round(float(shadow_value) - float(canonical_value), 6)
            )
            ratio = (
                None
                if canonical_value in (None, 0) or shadow_value is None
                else round(abs(float(shadow_value)) / abs(float(canonical_value)), 6)
            )
            evidence = {
                "group": list(key),
                "percentile": percentile,
                "shadow_seconds": shadow_value,
                "canonical_seconds": canonical_value,
                "difference_seconds": difference,
                "absolute_difference_seconds": None if difference is None else abs(difference),
                "ratio": ratio,
            }
            delay_changes.append(evidence)
            if canonical_value == 0 and shadow_value not in (None, 0):
                issues.append(_gate_issue("warn", "delay", "delay percentile changed from zero baseline", **evidence))
            elif ratio is not None and ratio > float(thresholds["delay_percentile_ratio_max"]):
                issues.append(
                    _gate_issue("warn", "delay", "delay percentile ratio exceeds advisory threshold", **evidence)
                )
        shadow_tail_rate = _ratio(int(shadow.get("abs_delay_over_3600_count", 0)), int(shadow.get("row_count", 0)))
        canonical_tail_rate = _ratio(
            int(canonical.get("abs_delay_over_3600_count", 0)), int(canonical.get("row_count", 0))
        )
        tail_delta = (
            None
            if shadow_tail_rate is None or canonical_tail_rate is None
            else round((shadow_tail_rate - canonical_tail_rate) * 100, 6)
        )
        evidence = {
            "group": list(key),
            "shadow_tail_rate": shadow_tail_rate,
            "canonical_tail_rate": canonical_tail_rate,
            "tail_rate_delta_percentage_points": tail_delta,
        }
        delay_changes.append(evidence)
        if tail_delta is not None and abs(tail_delta) > float(thresholds["delay_tail_delta_max"]) * 100:
            issues.append(_gate_issue("warn", "delay", "delay tail changed beyond advisory threshold", **evidence))

    trip_quality_rates = []
    expected_status_rates = []
    for service_date, mode in sorted({key[:2] for key in trip_counts if key[2] == "canonical"}):
        shadow, canonical = (
            trip_counts.get((service_date, mode, "shadow"), {}),
            trip_counts.get((service_date, mode, "canonical"), {}),
        )
        shadow_total, canonical_total = sum(shadow.values()), sum(canonical.values())
        rates: dict[str, dict[str, float | None]] = {}
        for quality in ("complete", "partial", "broken"):
            shadow_rate = 0.0 if shadow_total == 0 else _ratio(shadow.get(quality, 0), shadow_total)
            canonical_rate = 0.0 if canonical_total == 0 else _ratio(canonical.get(quality, 0), canonical_total)
            rates[quality] = {
                "shadow": shadow_rate,
                "canonical": canonical_rate,
                "delta_percentage_points": None
                if shadow_rate is None or canonical_rate is None
                else round(shadow_rate - canonical_rate, 6),
            }
        evidence = {"service_date": service_date, "mode": mode, "rates": rates}
        trip_quality_rates.append(evidence)
        complete_delta = rates["complete"]["delta_percentage_points"]
        if complete_delta is not None and complete_delta < -float(thresholds["complete_rate_drop_max"]):
            issues.append(_gate_issue("fail", "quality", "complete trip rate dropped beyond threshold", **evidence))
    for service_date, mode in sorted({key[:2] for key in expected_counts}):
        shadow, canonical = (
            expected_counts.get((service_date, mode, "shadow"), [0, 0, 0]),
            expected_counts.get((service_date, mode, "canonical"), [0, 0, 0]),
        )
        evidence: dict[str, object] = {"service_date": service_date, "mode": mode, "rates": {}}
        for label, index in (("uncertain", 1), ("missed", 2)):
            shadow_rate, canonical_rate = _ratio(shadow[index], shadow[0]), _ratio(canonical[index], canonical[0])
            delta = None if shadow_rate is None or canonical_rate is None else round(shadow_rate - canonical_rate, 6)
            evidence["rates"][label] = {
                "shadow": shadow_rate,
                "canonical": canonical_rate,
                "delta_percentage_points": delta,
            }
            if delta is not None and abs(delta) > float(thresholds["expected_rate_delta_max"]):
                issues.append(
                    _gate_issue(
                        "warn", "expected_status", f"{label} rate changed beyond advisory threshold", **evidence
                    )
                )
        expected_status_rates.append(evidence)

    resource_bounds = {
        "peak_rss_bytes": metrics.get("peak_rss_bytes"),
        "swapping_observed": metrics.get("swapping_observed"),
        "peak_rss_bytes_max": thresholds["peak_rss_bytes_max"],
        "swapping_observed_must_be": False,
    }
    peak_rss = cast("Any", resource_bounds["peak_rss_bytes"])
    if peak_rss is not None and int(peak_rss) > int(resource_bounds["peak_rss_bytes_max"]):
        issues.append(_gate_issue("fail", "resource", "peak RSS exceeds configured bound", **resource_bounds))
    if resource_bounds["swapping_observed"] is True:
        issues.append(_gate_issue("fail", "resource", "swapping was observed", **resource_bounds))
    differences = comparison.get("differences", [])
    if not isinstance(differences, list):
        raise TypeError("Matcher shadow comparison differences must be a list")
    status = "fail" if any(issue["level"] == "fail" for issue in issues) else "warn" if issues else "pass"
    return {
        "status": status,
        "manual_review_required": bool(differences) or any(issue["level"] == "warn" for issue in issues),
        "comparison_contract_version": COMPARISON_CONTRACT_VERSION,
        "thresholds": thresholds,
        "structural_violations": [issue for issue in issues if issue["category"] == "structural"],
        "resource_bounds": resource_bounds,
        "retention": retention,
        "material_line_retention": material_line_retention,
        "trip_quality_rates": trip_quality_rates,
        "expected_status_rates": expected_status_rates,
        "delay_changes": delay_changes,
        "issues": issues,
    }


def _gate_has_hard_failure(gate: dict[str, object]) -> bool:
    issues = gate.get("issues", [])
    if not isinstance(issues, list):
        return False
    return any(
        isinstance(issue, dict)
        and issue.get("level") == "fail"
        and issue.get("category") in {"structural", "resource", "retention"}
        for issue in issues
    )


def _marker_diagnostics(metrics: dict[str, object]) -> dict[str, object]:
    return {
        name: metrics.get(name)
        for name in ("duty_execution_status_counts", "stop_alignment_missing_stops", "stop_alignment_ambiguous_trips")
    }


def _reject_conflicting_marker(
    client: Any, config: ShadowConfig, processing_date: str, run_id: str, pending: dict[str, object]
) -> None:
    """Fail before loading when an immutable marker belongs to different content."""
    marker = _read_marker(client, config, processing_date, run_id)
    if marker is None:
        return
    expected = json.loads(_json_bytes(pending))
    if any(marker.get(key) != value for key, value in expected.items()):
        name = _marker_name(config, processing_date, run_id)
        raise RuntimeError(
            f"Matcher shadow commit marker already exists with different immutable run content: gs://{GCS_BUCKET}/{name}"
        )


def _pending_tables(config: ShadowConfig, run_id: str, pending: dict[str, object]) -> dict[str, dict[str, str]]:
    artifacts = pending.get("artifacts")
    tables = pending.get("tables")
    if not isinstance(artifacts, dict) or not isinstance(tables, dict):
        raise TypeError("Matcher shadow pending metadata has no artifact/table inventory")
    validated = {}
    for spec in ARTIFACTS:
        artifact = artifacts.get(spec.key)
        table = tables.get(spec.key)
        if not isinstance(artifact, dict) or not isinstance(table, dict):
            raise TypeError(f"Matcher shadow pending metadata is missing {spec.key}")
        sha256 = artifact.get("sha256")
        if not isinstance(sha256, str):
            raise TypeError(f"Matcher shadow pending metadata has no hash for {spec.key}")
        expected_table = _table_id(config.dataset or "", run_id, spec, sha256)
        expected_job = _load_job_id(run_id, spec, sha256)
        if table.get("table_id") != expected_table or table.get("job_id") != expected_job:
            raise RuntimeError(f"Matcher shadow pending table/job identity mismatch for {spec.key}")
        validated[spec.key] = {"table_id": expected_table, "job_id": expected_job}
    return validated


def run_matcher_shadow_load(
    processing_date: str, snapshot_id: str, run_id: str, *, try_number: int = 1
) -> dict[str, object]:
    """Write validated, loaded shadow metadata without creating a commit marker."""
    config = ShadowConfig.from_env()
    config.validate()
    if not config.enabled:
        return {"enabled": False, "reason": "MATCHER_SHADOW_ENABLED is false"}

    run_workspace = config.workspace_root / _run_id(run_id)
    workspace = run_workspace / f"attempt-{try_number}"
    output = workspace / "output"
    _validate_run_workspace(config, workspace)
    try:
        # Cleanup is local only; loaded tables and pending/commit metadata retain durable retry evidence.
        shutil.rmtree(run_workspace, ignore_errors=True)
        workspace.mkdir(parents=True, exist_ok=False)
        bq_client = bigquery.Client(project=GCP_PROJECT)
        storage_client = storage.Client(project=GCP_PROJECT)
        snapshot_uri = _snapshot_gcs_path(bq_client, snapshot_id)
        gps_inventory, gtfs_inventory, gps_root, gtfs_zip = _download_inputs(
            storage_client, config, processing_date, snapshot_uri, workspace
        )
        _invoke_matcher(_matcher_argv(config, processing_date, snapshot_id, gps_root, gtfs_zip, output), config)
        artifacts, manifest = _validate_outputs(output, processing_date, snapshot_id)
        pending = {
            "run_id": run_id,
            "processing_date": processing_date,
            "snapshot_id": snapshot_id,
            "snapshot_gcs_path": snapshot_uri,
            "gps_inventory": gps_inventory,
            "gtfs_inventory": gtfs_inventory,
            "artifacts": {
                key: {
                    "rows": item.rows,
                    "bytes": item.bytes,
                    "sha256": item.sha256,
                    "service_dates": item.service_dates,
                }
                for key, item in artifacts.items()
            },
            "tables": {
                spec.key: _table_identity(config.dataset or "", run_id, spec, artifacts[spec.key].sha256)
                for spec in ARTIFACTS
            },
            "metrics": manifest.get("metrics", _read_metrics(output)),
        }
        _reject_conflicting_marker(storage_client, config, processing_date, run_id, pending)
        for spec in ARTIFACTS:
            _load_artifact(bq_client, config.dataset or "", run_id, spec, artifacts[spec.key])
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


def run_matcher_shadow_compare_commit(
    processing_date: str, run_id: str, pending_context: dict[str, object]
) -> dict[str, object]:
    """Compare same-run shadow tables after canonical fact tests, then commit once."""
    if not pending_context.get("enabled"):
        return {"enabled": False, "reason": "MATCHER_SHADOW_ENABLED is false"}
    if pending_context.get("status") != "loaded_pending":
        return {"enabled": True, "status": "skipped_shadow_load_failed"}
    if pending_context.get("processing_date") != processing_date or pending_context.get("run_id") != run_id:
        raise RuntimeError("Matcher shadow pending context does not match this DAG run")
    config = ShadowConfig.from_env()
    config.validate()
    storage_client = storage.Client(project=GCP_PROJECT)
    pending = _read_pending(storage_client, config, processing_date, run_id)
    if pending.get("processing_date") != processing_date or pending.get("run_id") != run_id:
        raise RuntimeError("Matcher shadow pending metadata does not match this DAG run")
    snapshot_id = pending.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise RuntimeError("Matcher shadow pending metadata has no snapshot ID")
    tables = _pending_tables(config, run_id, pending)
    comparison = _comparison_report(
        bigquery.Client(project=GCP_PROJECT), processing_date, snapshot_id, tables, config.max_comparison_bytes
    )
    raw_metrics = pending.get("metrics")
    if not isinstance(raw_metrics, dict):
        raise TypeError("Matcher shadow pending metadata has no metrics object")
    metrics = cast("dict[str, object]", raw_metrics)
    gate = evaluate_shadow_gate(comparison, metrics)
    if config.strict and _gate_has_hard_failure(gate):
        raise RuntimeError("Matcher shadow strict gate rejected a structural, resource, or retention failure")
    marker_uri = _write_marker(
        storage_client,
        config,
        processing_date,
        run_id,
        pending
        | {
            "comparison": comparison,
            "comparison_contract_version": COMPARISON_CONTRACT_VERSION,
            "quality_gate": gate,
            "diagnostics": _marker_diagnostics(metrics),
        },
    )
    return {"enabled": True, "status": "committed", "marker_uri": marker_uri, "quality_gate_status": gate["status"]}


def _validate_run_workspace(config: ShadowConfig, workspace: Path) -> None:
    """Do not let per-run cleanup delete the mounted matcher project."""
    if config.project_dir is not None and config.project_dir.resolve().is_relative_to(workspace.resolve()):
        raise ValueError("MATCHER_SHADOW_PROJECT_DIR must not be inside the run workspace or output")


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


def matcher_cutover_publication_dbt_args(processing_date: str, gtfs_snapshot_id: str) -> dict[str, dict[str, object]]:
    """Return publication args without forcing retained rows to one GTFS snapshot."""
    current = date.fromisoformat(processing_date)
    selector = (
        "int_gtfs_processing_snapshot int_gtfs_trip_schedule_history int_schedule_version "
        "dim_schedule_version fct_trip fct_stop_arrival fct_expected_stop_event"
    )
    base_vars = {
        "processing_date": current.isoformat(),
        "gtfs_snapshot_id": gtfs_snapshot_id,
        "use_python_reconstruction": True,
    }
    return {
        "current": {
            "selector": selector,
            "vars": base_vars | {"publish_service_date": current.isoformat()},
        },
        "prior": {
            "selector": selector,
            "vars": base_vars | {"publish_service_date": (current - timedelta(days=1)).isoformat()},
        },
    }


def _promotion_table_id(
    dataset: str, processing_date: str, run_id: str, spec: ArtifactSpec, artifact_sha256: str
) -> str:
    digest = _validated_sha256(artifact_sha256)
    return f"{GCP_PROJECT}.{dataset}.matcher_input_stage_{spec.table_suffix}_{processing_date.replace('-', '')}_{_run_id(run_id)}_{digest[:16]}"


def _promotion_job_id(action: str, processing_date: str, run_id: str, spec: ArtifactSpec, artifact_sha256: str) -> str:
    """Return a retry-safe job ID that cannot collide across normalized run IDs."""
    digest = _validated_sha256(artifact_sha256)[:24]
    run_digest = hashlib.sha256(_run_id(run_id).encode("utf-8")).hexdigest()[:16]
    return f"matcher_cutover_{action}_{spec.table_suffix}_{processing_date.replace('-', '')}_{run_digest}_{digest}"


def _promotion_transaction_job_id(processing_date: str, run_id: str, artifacts: dict[str, dict[str, object]]) -> str:
    digests = ":".join(_validated_sha256(str(artifacts[key]["sha256"])) for key in sorted(STABLE_INPUT_TABLES))
    content_digest = hashlib.sha256(digests.encode("ascii")).hexdigest()[:24]
    run_digest = hashlib.sha256(_run_id(run_id).encode("utf-8")).hexdigest()[:16]
    return f"matcher_cutover_replace_all_{processing_date.replace('-', '')}_{run_digest}_{content_digest}"


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
        job = client.query(query, job_config=config, job_id=job_id, location=BIGQUERY_LOCATION)
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
        raise RuntimeError(f"Matcher cutover query did not complete successfully: {job_id}")
    if getattr(job, "state", "DONE") != "DONE":
        raise RuntimeError(f"Matcher cutover query is not complete: {job_id}")
    if (actual_query := getattr(job, "query", None)) is not None and actual_query != query:
        raise RuntimeError(f"Matcher cutover query text does not match reused job: {job_id}")
    actual_destination = getattr(job, "destination", None)
    if destination is not None and actual_destination is not None and str(actual_destination) != destination:
        raise RuntimeError(f"Matcher cutover query destination does not match reused job: {job_id}")
    if (location := getattr(job, "location", None)) is not None and location != BIGQUERY_LOCATION:
        raise RuntimeError(f"Matcher cutover query location does not match reused job: {job_id}")
    if (actual_max_bytes := getattr(job, "maximum_bytes_billed", None)) is not None and actual_max_bytes != max_bytes:
        raise RuntimeError(f"Matcher cutover query byte cap does not match reused job: {job_id}")
    configuration = getattr(job, "configuration", None)
    if configuration is not None and getattr(configuration, "maximum_bytes_billed", max_bytes) != max_bytes:
        raise RuntimeError(f"Matcher cutover query byte cap does not match reused job: {job_id}")
    if configuration is not None and not _query_parameters_match(
        getattr(configuration, "query_parameters", None), parameters
    ):
        raise RuntimeError(f"Matcher cutover query parameters do not match reused job: {job_id}")


def _promotion_table_counts(
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
        raise RuntimeError("Matcher cutover validation did not return one count row")
    row = rows[0]
    values = {
        key: row.get(key) if isinstance(row, dict) else getattr(row, key, None)
        for key in ("total_rows", "partition_rows", "wrong_partition_rows", "wrong_processing_date_rows")
    }
    if not all(isinstance(value, int) for value in values.values()):
        raise TypeError("Matcher cutover validation returned invalid row counts")
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
        raise RuntimeError("Matcher cutover stable validation did not return one count row")
    row = rows[0]
    values = {
        key: row.get(key) if isinstance(row, dict) else getattr(row, key, None)
        for key in ("total_rows", "wrong_processing_date_rows")
    }
    if not all(isinstance(value, int) for value in values.values()):
        raise TypeError("Matcher cutover stable validation returned invalid row counts")
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
    counts = _promotion_table_counts(client, table_id, processing_date, spec, job_id, max_bytes)
    if counts["total_rows"] != expected_rows or counts["partition_rows"] != expected_rows:
        raise RuntimeError(
            f"Matcher cutover {table_id} row count does not equal its processing {spec.partition_field} partition"
        )
    if counts["wrong_partition_rows"] or counts["wrong_processing_date_rows"]:
        raise RuntimeError(f"Matcher cutover {table_id} has rows outside processing_date={processing_date}")
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
            f"Matcher cutover {table_id} stable {spec.partition_field} partition failed lineage validation"
        )
    return counts["total_rows"]


def _schema_signature(fields: list[Any]) -> tuple[tuple[str, str, str], ...]:
    return tuple((str(field.name), str(field.field_type).upper(), str(field.mode).upper()) for field in fields)


def _expected_schema(spec: ArtifactSpec) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (field.name, field.bigquery_type, "REPEATED" if field.repeated else "NULLABLE") for field in spec.fields
    )


def _stage_labels(spec: ArtifactSpec, artifact_sha256: str) -> dict[str, str]:
    return {
        "matcher_schema_version": ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem],
        "matcher_artifact_sha256": _validated_sha256(artifact_sha256),
    }


def _verify_table_contract(
    client: Any, table_id: str, spec: ArtifactSpec, labels: dict[str, str] | None = None
) -> None:
    table = client.get_table(table_id)
    if _schema_signature(list(getattr(table, "schema", []))) != _expected_schema(spec):
        raise RuntimeError(f"Matcher cutover table schema does not exactly match {spec.key}: {table_id}")
    partitioning = getattr(table, "time_partitioning", None)
    if getattr(partitioning, "field", None) != spec.partition_field:
        raise RuntimeError(f"Matcher cutover table must be partitioned by {spec.partition_field}: {table_id}")
    if labels is not None:
        actual_labels = getattr(table, "labels", None) or {}
        if {key: actual_labels.get(key) for key in labels} != labels:
            raise RuntimeError(
                f"Matcher cutover stage table labels do not match immutable artifact identity: {table_id}"
            )


def _column_list(spec: ArtifactSpec) -> str:
    return ", ".join(f"`{field.name}`" for field in spec.fields)


def _promotion_transaction_query(promoted: dict[str, dict[str, object]]) -> str:
    """Replace all stable partitions together; callers must preflight every stage first."""
    statements = ["begin transaction;"]
    for spec in ARTIFACTS:
        stable_table = str(promoted[spec.key]["stable_table"])
        staged_table = str(promoted[spec.key]["staged_table"])
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
) -> None:
    """Create a stage only through its deterministic query job, then validate it."""
    columns = _column_list(spec)
    labels = _stage_labels(spec, artifact_sha256)
    label_sql = ", ".join(f"{key}='{value}'" for key, value in labels.items())
    _query_job(
        client,
        f"""
        create table `{staged_table}`
        partition by {spec.partition_field}
        options (labels=[{label_sql}]) as
        select {columns}
        from `{source_table}`
        where {spec.partition_field} = @processing_date
        """,
        _promotion_job_id("stage", processing_date, run_id, spec, artifact_sha256),
        max_bytes,
        [bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))],
        staged_table,
    )
    _verify_table_contract(client, staged_table, spec, labels)


def _promotion_marker_name(config: ShadowConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/promotion.json"


def _require_partition_field(client: Any, table_id: str, spec: ArtifactSpec) -> None:
    table = client.get_table(table_id)
    if getattr(getattr(table, "time_partitioning", None), "field", None) != spec.partition_field:
        raise RuntimeError(
            f"Matcher cutover stable input table must be partitioned by {spec.partition_field}: {table_id}"
        )


def _write_promotion_marker(
    client: Any, config: ShadowConfig, processing_date: str, run_id: str, marker: dict[str, object]
) -> str:
    name = _promotion_marker_name(config, processing_date, run_id)
    blob = client.bucket(GCS_BUCKET).blob(name)
    payload = _json_bytes(marker)
    if len(payload) > config.max_marker_bytes:
        raise RuntimeError(
            f"Matcher cutover marker exceeds configured size: {len(payload)} > {config.max_marker_bytes}"
        )
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
    except PreconditionFailed as exc:
        existing = _read_bounded_blob(blob, config, "existing promotion marker", missing_ok=True)
        if existing is None or not _marker_is_identical(existing, payload):
            raise RuntimeError(
                f"Matcher cutover marker already exists with different content: gs://{GCS_BUCKET}/{name}"
            ) from exc
    return f"gs://{GCS_BUCKET}/{name}"


def _validate_manual_gate_exception(
    processing_date: str,
    run_id: str,
    marker: dict[str, object],
    exception: dict[str, object] | None,
) -> dict[str, object] | None:
    """Allow only an audited swap exception; correctness and retention failures remain blocking."""
    gate = marker.get("quality_gate")
    if isinstance(gate, dict) and gate.get("status") == "pass":
        if exception is not None:
            raise RuntimeError("Matcher cutover received an exception for an already passing gate")
        return None
    if exception is None:
        raise RuntimeError("Matcher cutover requires a passing shadow quality gate")
    if exception.get("processing_date") != processing_date or exception.get("run_id") != run_id:
        raise RuntimeError("Matcher cutover gate exception does not match this processing date and run")
    reason = exception.get("reason")
    approved_by = exception.get("approved_by")
    approved_at = exception.get("approved_at")
    max_swap = exception.get("max_current_swap_bytes")
    if not isinstance(reason, str) or not reason.strip():
        raise RuntimeError("Matcher cutover gate exception requires an operator reason")
    if not isinstance(approved_by, str) or not approved_by.strip():
        raise RuntimeError("Matcher cutover gate exception requires an approver")
    if not isinstance(approved_at, str):
        raise TypeError("Matcher cutover gate exception requires an approval timestamp")
    try:
        approval_time = datetime.fromisoformat(approved_at)
    except ValueError as exc:
        raise RuntimeError("Matcher cutover gate exception has an invalid approval timestamp") from exc
    if approval_time.tzinfo is None or approval_time.utcoffset() is None:
        raise RuntimeError("Matcher cutover gate exception approval timestamp must include a timezone")
    if not isinstance(max_swap, int) or not 0 < max_swap <= MAX_MANUAL_SWAP_EXCEPTION_BYTES:
        raise RuntimeError("Matcher cutover swap exception exceeds the manual exception bound")
    issues = gate.get("issues") if isinstance(gate, dict) else None
    failures = (
        [issue for issue in issues if isinstance(issue, dict) and issue.get("level") == "fail"]
        if isinstance(issues, list)
        else []
    )
    if (
        len(failures) != 1
        or failures[0].get("category") != "resource"
        or failures[0].get("message") != "swapping was observed"
    ):
        raise RuntimeError("Matcher cutover gate exception applies only to a sole swap failure")
    metrics = marker.get("metrics")
    observed_swap = metrics.get("current_swap_bytes") if isinstance(metrics, dict) else None
    if not isinstance(observed_swap, int) or observed_swap < 0 or observed_swap > max_swap:
        raise RuntimeError("Matcher cutover observed swap exceeds the accepted exception")
    return {
        "processing_date": processing_date,
        "run_id": run_id,
        "reason": reason.strip(),
        "approved_by": approved_by.strip(),
        "approved_at": approval_time.isoformat(),
        "max_current_swap_bytes": max_swap,
        "observed_current_swap_bytes": observed_swap,
        "accepted_failure": failures[0],
    }


def promote_validated_shadow_artifacts(
    processing_date: str,
    run_id: str,
    *,
    accepted_gate_exception: dict[str, object] | None = None,
) -> dict[str, object]:
    """Promote one validated shadow run; no DAG task calls this manual-only function.

    External copies of each previous stable input partition are a precondition.
    This function records counts but neither creates those copies nor performs rollback.
    """
    shadow = ShadowConfig.from_env()
    shadow.validate()
    cutover = CutoverConfig.from_env()
    cutover.validate(shadow)
    if not cutover.enabled:
        return {"enabled": False, "reason": "MATCHER_CUTOVER_ENABLED is false"}
    date.fromisoformat(processing_date)

    storage_client = storage.Client(project=GCP_PROJECT)
    marker = _read_marker(storage_client, shadow, processing_date, run_id)
    if marker is None:
        raise RuntimeError("Matcher cutover requires a committed shadow marker")
    if marker.get("processing_date") != processing_date or marker.get("run_id") != run_id:
        raise RuntimeError("Matcher cutover marker does not match this processing date and run")
    accepted_exception = _validate_manual_gate_exception(processing_date, run_id, marker, accepted_gate_exception)
    shadow_tables = _pending_tables(shadow, run_id, marker)
    artifacts = marker.get("artifacts")
    if not isinstance(artifacts, dict):
        raise TypeError("Matcher cutover marker has no artifact inventory")

    # Complete every source/stage check before inspecting or changing stable inputs.
    # A stage failure therefore cannot delete either retained stable partition.
    bq_client = bigquery.Client(project=GCP_PROJECT)
    promoted: dict[str, dict[str, object]] = {}
    for spec in ARTIFACTS:
        artifact = artifacts.get(spec.key)
        if (
            not isinstance(artifact, dict)
            or not isinstance(artifact.get("sha256"), str)
            or not isinstance(artifact.get("rows"), int)
        ):
            raise TypeError(f"Matcher cutover marker has invalid {spec.key} artifact metadata")
        sha256, expected_rows = str(artifact.get("sha256")), int(cast("int", artifact.get("rows")))
        source_table = shadow_tables[spec.key]["table_id"]
        staged_table = _promotion_table_id(cutover.input_dataset, processing_date, run_id, spec, sha256)
        stable_table = f"{GCP_PROJECT}.{cutover.input_dataset}.{STABLE_INPUT_TABLES[spec.key]}"
        source_rows = _require_exact_processing_partition(
            bq_client,
            source_table,
            processing_date,
            spec,
            expected_rows,
            _promotion_job_id("source_validate", processing_date, run_id, spec, sha256),
            cutover.max_promotion_bytes,
        )
        _stage_artifact(
            bq_client,
            source_table,
            staged_table,
            processing_date,
            run_id,
            spec,
            sha256,
            cutover.max_promotion_bytes,
        )
        staged_rows = _require_exact_processing_partition(
            bq_client,
            staged_table,
            processing_date,
            spec,
            expected_rows,
            _promotion_job_id("stage_validate", processing_date, run_id, spec, sha256),
            cutover.max_promotion_bytes,
        )
        promoted[spec.key] = {
            "source_table": source_table,
            "staged_table": staged_table,
            "stable_table": stable_table,
            "rows": staged_rows,
            "source_rows": source_rows,
            "sha256": sha256,
            "job_ids": {
                action: _promotion_job_id(action, processing_date, run_id, spec, sha256)
                for action in ("source_validate", "stage", "stage_validate")
            },
        }

    for spec in ARTIFACTS:
        _verify_table_contract(bq_client, str(promoted[spec.key]["stable_table"]), spec)

    pre_counts = {
        spec.key: _stable_partition_counts(
            bq_client,
            str(promoted[spec.key]["stable_table"]),
            processing_date,
            spec,
            _promotion_job_id("precount", processing_date, run_id, spec, str(promoted[spec.key]["sha256"])),
            cutover.max_promotion_bytes,
        )
        for spec in ARTIFACTS
    }
    transaction_job_id = _promotion_transaction_job_id(processing_date, run_id, promoted)
    _query_job(
        bq_client,
        _promotion_transaction_query(promoted),
        transaction_job_id,
        cutover.max_promotion_bytes,
        [bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date))],
    )
    try:
        for spec in ARTIFACTS:
            stable_rows = _require_stable_processing_partition(
                bq_client,
                str(promoted[spec.key]["stable_table"]),
                processing_date,
                spec,
                cast("int", promoted[spec.key]["rows"]),
                _promotion_job_id("postvalidate", processing_date, run_id, spec, str(promoted[spec.key]["sha256"])),
                cutover.max_promotion_bytes,
            )
            promoted[spec.key]["rows"] = stable_rows
            promoted[spec.key]["job_ids"] = cast("dict[str, str]", promoted[spec.key]["job_ids"]) | {
                "precount": _promotion_job_id(
                    "precount", processing_date, run_id, spec, str(promoted[spec.key]["sha256"])
                ),
                "postvalidate": _promotion_job_id(
                    "postvalidate", processing_date, run_id, spec, str(promoted[spec.key]["sha256"])
                ),
            }
    except Exception:
        LOGGER.exception(
            "Matcher cutover transaction committed but post-commit validation failed; marker remains absent. "
            "Restore only the captured pre-promotion partitions if rollback is required: %s",
            pre_counts,
        )
        raise
    marker_uri = _write_promotion_marker(
        storage_client,
        shadow,
        processing_date,
        run_id,
        {
            "processing_date": processing_date,
            "run_id": run_id,
            "stable_inputs": promoted,
            "transaction_job_id": transaction_job_id,
            "pre_promotion_partition_counts": pre_counts,
            "accepted_gate_exception": accepted_exception,
            "rollback_boundary": (
                "The four-table transaction is committed before post-validation. If post-validation fails, "
                "the marker is absent but the transaction is not rolled back; restore only the captured "
                "pre-promotion partitions."
            ),
        },
    )
    return {"enabled": True, "status": "promoted", "marker_uri": marker_uri, "stable_inputs": promoted}


def run_matcher_shadow_load_task(
    processing_date: str, snapshot_id: str, run_id: str, *, try_number: int = 1
) -> dict[str, object]:
    """Keep canonical DAG publication runnable unless strict mode is explicitly requested."""
    try:
        return run_matcher_shadow_load(processing_date, snapshot_id, run_id, try_number=try_number)
    except Exception as exc:
        if _env_bool("MATCHER_SHADOW_STRICT", False):
            raise
        LOGGER.exception("Matcher shadow load failed without affecting canonical publication")
        return {"enabled": True, "status": "failed_non_strict", "error": str(exc)}


def run_matcher_shadow_compare_commit_task(
    processing_date: str, run_id: str, pending_context: dict[str, object]
) -> dict[str, object]:
    """Keep canonical publication independent from non-strict comparison failures."""
    try:
        return run_matcher_shadow_compare_commit(processing_date, run_id, pending_context)
    except Exception as exc:
        if _env_bool("MATCHER_SHADOW_STRICT", False):
            raise
        LOGGER.exception("Matcher shadow comparison failed without affecting canonical publication")
        return {"enabled": True, "status": "failed_non_strict", "error": str(exc)}
