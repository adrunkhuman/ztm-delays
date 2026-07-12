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
from datetime import date, timedelta
from importlib import import_module
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from google.api_core.exceptions import Conflict, NotFound, PreconditionFailed
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_INT_DATASET,
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
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
    "trip_universe": "trip-universe-v1",
}
IDENTIFIER_PATTERN = re.compile(r"[^a-z0-9_]+")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
VALIDATION_MEMORY_LIMIT = "320MB"
VALIDATION_TEMP_LIMIT = "20GB"
VALIDATION_BATCH_SIZE = 8_192
DEFAULT_MAX_GPS_OBJECTS = 5_000
DEFAULT_MAX_GPS_BYTES = 20 * 1024**3
DEFAULT_MIN_FREE_DISK_BYTES = 5 * 1024**3


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
        forbidden = {BIGQUERY_RAW_DATASET, BIGQUERY_INT_DATASET, BIGQUERY_MARTS_DATASET}
        if self.dataset in forbidden:
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


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() == "true"


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
        "320MB",
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
        bad_lineage = _query_count(
            connection,
            """
            select count(*)
            from read_parquet(?)
            where cast(processing_date as varchar) != ? or gtfs_snapshot_id != ?
            """,
            [str(path), processing_date, snapshot_id],
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
        raise RuntimeError(f"Lineage mismatch in {path.name}")
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
        if not isinstance(expected_rows, int) or expected_rows != artifact.rows:
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
    _verify_load_job(job, job_id, table_id)
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


def _verify_load_job(job: Any, job_id: str, table_id: str) -> None:
    """Accept a retried load only when its immutable job/table identity matches."""
    job.result()
    if getattr(job, "job_id", None) != job_id:
        raise RuntimeError(f"Shadow load job ID does not match artifact-bound ID: {job_id}")
    if getattr(job, "state", None) != "DONE" or getattr(job, "error_result", None) is not None:
        raise RuntimeError(f"Shadow load job did not complete successfully: {job_id}")
    if _table_reference_id(getattr(job, "destination", None)) != table_id:
        raise RuntimeError(f"Shadow load job destination does not match expected table: {job_id}")


def _comparison_query(shadow_tables: dict[str, dict[str, str]]) -> str:
    sections = []
    for spec in ARTIFACTS:
        shadow = shadow_tables[spec.key]["table_id"]
        canonical = CANONICAL_FACT_TABLES[spec.key]
        source_date = "gps_date" if spec.key == "trip" else "source_gps_date"
        delay = "end_delay_seconds" if spec.key == "trip" else "delay_seconds"
        status = "cast(null as string)" if spec.key != "expected_stop_event" else "observation_status"
        grain = ", ".join(spec.grain)
        for source, table in (("shadow", shadow), ("canonical", canonical)):
            sections.append(
                f"""
                select '{spec.key}' as artifact, '{source}' as source, service_date, mode,
                    trip_quality, {status} as observation_status, count(*) as row_count,
                    count(distinct to_json_string(struct({grain}))) as distinct_grains,
                    avg({delay}) as avg_delay_seconds,
                    countif({source_date} != service_date) as overnight_rows,
                    count(distinct {source_date}) as source_date_count,
                    array_agg(distinct cast({source_date} as string) ignore nulls order by cast({source_date} as string)) as source_dates
                from `{table}`
                where service_date in unnest(@service_dates)
                  and {source_date} = @processing_date
                group by artifact, source, service_date, mode, trip_quality, observation_status
                """
            )
    return " union all ".join(sections)


def _comparison_report(
    client: Any, processing_date: str, shadow_tables: dict[str, dict[str, str]]
) -> dict[str, object]:
    dates = [date.fromisoformat(processing_date) - timedelta(days=1), date.fromisoformat(processing_date)]
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("service_dates", "DATE", dates),
            bigquery.ScalarQueryParameter("processing_date", "DATE", date.fromisoformat(processing_date)),
        ]
    )
    rows = [
        {key: _json_value(value) for key, value in (dict(row.items()) if hasattr(row, "items") else dict(row)).items()}
        for row in client.query(_comparison_query(shadow_tables), job_config=config).result()
    ]
    # BigQuery does not preserve result order without ORDER BY; markers must be rerun-stable.
    rows.sort(key=lambda row: _json_bytes(row).decode("utf-8"))
    grouped: dict[tuple[object, ...], dict[str, dict[str, object]]] = {}
    for row in rows:
        key = tuple(row[field] for field in ("artifact", "service_date", "mode", "trip_quality", "observation_status"))
        grouped.setdefault(key, {})[str(row["source"])] = row
    differences = [
        {"group": list(key), "shadow": values.get("shadow"), "canonical": values.get("canonical")}
        for key, values in grouped.items()
        if _comparison_payload(values.get("shadow")) != _comparison_payload(values.get("canonical"))
    ]
    differences.sort(key=lambda difference: _json_bytes(difference).decode("utf-8"))
    return {"service_dates": [item.isoformat() for item in dates], "aggregates": rows, "differences": differences}


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
    pending = json.loads(blob.download_as_bytes())
    if not isinstance(pending, dict):
        raise TypeError("Matcher shadow pending metadata is not an object")
    return pending


def _read_marker(client: Any, config: ShadowConfig, processing_date: str, run_id: str) -> dict[str, object] | None:
    blob = client.bucket(GCS_BUCKET).blob(_marker_name(config, processing_date, run_id))
    try:
        marker = json.loads(blob.download_as_bytes())
    except NotFound:
        return None
    if not isinstance(marker, dict):
        raise TypeError("Matcher shadow commit marker is not an object")
    return marker


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
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
    except PreconditionFailed as exc:
        if not _marker_is_identical(blob.download_as_bytes(), payload):
            raise RuntimeError(
                f"Matcher shadow commit marker already exists with different content: gs://{GCS_BUCKET}/{name}"
            ) from exc
    return f"gs://{GCS_BUCKET}/{name}"


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
        # Clear stale attempts before each retry; the marker retains all evidence we need afterwards.
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
    tables = _pending_tables(config, run_id, pending)
    comparison = _comparison_report(bigquery.Client(project=GCP_PROJECT), processing_date, tables)
    marker_uri = _write_marker(storage_client, config, processing_date, run_id, pending | {"comparison": comparison})
    return {"enabled": True, "status": "committed", "marker_uri": marker_uri}


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
