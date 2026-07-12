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
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_INT_DATASET,
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    RAW_GPS_PREFIX,
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
    "trip": "reconstruction-trip-facts-v2",
    "stop_arrival": "reconstruction-stop-arrivals-v2",
    "expected_stop_event": "reconstruction-expected-stop-events-v2",
    "trip_universe": "trip-universe-v1",
}
IDENTIFIER_PATTERN = re.compile(r"[^a-z0-9_]+")


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
    command: str
    project_dir: Path | None
    timeout_seconds: int
    marker_prefix: str
    keep_workspace: bool = False

    @classmethod
    def from_env(cls) -> ShadowConfig:
        """Read the shadow-only runtime configuration."""
        project_dir = os.getenv("MATCHER_SHADOW_PROJECT_DIR", "/opt/airflow/matcher").strip()
        return cls(
            enabled=_env_bool("MATCHER_SHADOW_ENABLED", False),
            strict=_env_bool("MATCHER_SHADOW_STRICT", False),
            dataset=os.getenv("BIGQUERY_MATCHER_SHADOW_DATASET", "").strip() or None,
            workspace_root=Path(os.getenv("MATCHER_SHADOW_WORKSPACE_ROOT", "/opt/airflow/matcher-shadow")),
            command=os.getenv("MATCHER_SHADOW_COMMAND", "ztm-matcher").strip() or "ztm-matcher",
            project_dir=Path(project_dir) if project_dir else None,
            timeout_seconds=_env_positive_int("MATCHER_SHADOW_TIMEOUT_SECONDS", 45 * 60),
            marker_prefix=os.getenv("MATCHER_SHADOW_GCS_PREFIX", "shadow/matcher").strip("/"),
            keep_workspace=_env_bool("MATCHER_SHADOW_KEEP_WORKSPACE", False),
        )

    def validate(self) -> None:
        """Reject enabled configurations that could reach canonical datasets."""
        if not self.enabled:
            return
        if not self.dataset:
            raise ValueError("BIGQUERY_MATCHER_SHADOW_DATASET is required when MATCHER_SHADOW_ENABLED=true")
        forbidden = {BIGQUERY_RAW_DATASET, BIGQUERY_INT_DATASET, BIGQUERY_MARTS_DATASET}
        if self.dataset in forbidden:
            raise ValueError("BIGQUERY_MATCHER_SHADOW_DATASET must not name a canonical raw, int, or marts dataset")
        if not self.marker_prefix:
            raise ValueError("MATCHER_SHADOW_GCS_PREFIX must not be empty")
        if not self.workspace_root.is_absolute():
            raise ValueError("MATCHER_SHADOW_WORKSPACE_ROOT must be absolute")
        if ".." in Path(self.command).parts:
            raise ValueError("MATCHER_SHADOW_COMMAND must not contain path traversal")


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


def _env_positive_int(name: str, default: int) -> int:
    value = os.getenv(name, str(default)).strip()
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _safe_id(value: str, *, prefix: str) -> str:
    normalized = IDENTIFIER_PATTERN.sub("_", value.lower()).strip("_")
    normalized = normalized[:80] or "run"
    return f"{prefix}_{normalized}"[:1024]


def _run_id(value: str) -> str:
    return _safe_id(value, prefix="run")


def _table_id(dataset: str, run_id: str, spec: ArtifactSpec) -> str:
    return f"{GCP_PROJECT}.{dataset}.{_safe_id(run_id, prefix=f'matcher_shadow_{spec.table_suffix}')}"


def _load_job_id(run_id: str, spec: ArtifactSpec) -> str:
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    return f"matcher_shadow_load_{spec.table_suffix}_{digest}"


def _gps_prefixes(processing_date: str) -> list[str]:
    return [f"{RAW_GPS_PREFIX}/vehicle_type={mode}/date={processing_date}/" for mode in VEHICLE_TYPES]


def _list_gps_objects(bucket: Any, processing_date: str) -> list[GcsObject]:
    objects = []
    for prefix in _gps_prefixes(processing_date):
        objects.extend(
            GcsObject(
                blob.name,
                str(getattr(blob, "generation", "")) or None,
                int(blob.size) if getattr(blob, "size", None) is not None else None,
                getattr(blob, "md5_hash", None),
                getattr(blob, "crc32c", None),
            )
            for blob in bucket.list_blobs(prefix=prefix)
            if blob.name.endswith(".parquet") and "/part-" in blob.name
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
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"GTFS snapshot path is not a GCS URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _download_inputs(
    client: Any, processing_date: str, snapshot_uri: str, workspace: Path
) -> tuple[list[dict[str, object]], dict[str, object], Path, Path]:
    gps_root = workspace / "gps"
    gps_bucket = client.bucket(GCS_BUCKET)
    objects = _list_gps_objects(gps_bucket, processing_date)
    inventory = []
    for item in objects:
        relative = Path(item.name).relative_to(RAW_GPS_PREFIX)
        destination = gps_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        gps_bucket.blob(item.name, generation=item.generation).download_to_filename(destination)
        inventory.append(asdict(item))

    gtfs_bucket_name, gtfs_name = _gcs_uri_parts(snapshot_uri)
    gtfs_bucket = client.bucket(gtfs_bucket_name)
    gtfs_blob = gtfs_bucket.get_blob(gtfs_name)
    if gtfs_blob is None:
        raise RuntimeError(f"Pinned GTFS ZIP no longer exists: {snapshot_uri}")
    gtfs_path = workspace / "gtfs" / "snapshot.zip"
    gtfs_path.parent.mkdir(parents=True, exist_ok=True)
    gtfs_blob.download_to_filename(gtfs_path)
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
    return inventory, gtfs_inventory, gps_root, gtfs_path


def _matcher_argv(
    config: ShadowConfig, processing_date: str, snapshot_id: str, gps_root: Path, gtfs_zip: Path, output: Path
) -> list[str]:
    return [
        config.command,
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
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_arrow_type(field: FieldSpec) -> str:
    base = {
        "STRING": "string",
        "DATE": "date32[day]",
        "TIMESTAMP": "timestamp[us, tz=UTC]",
        "INTEGER": "int64",
        "FLOAT": "double",
        "BOOLEAN": "bool",
    }[field.bigquery_type]
    return f"list<element: {base}>" if field.repeated else base


def _inspect_artifact(path: Path, spec: ArtifactSpec, processing_date: str, snapshot_id: str) -> ArtifactValidation:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required in the Airflow image for matcher shadow validation") from exc

    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    expected_names = [field.name for field in spec.fields]
    if schema.names != expected_names:
        raise RuntimeError(f"Unexpected schema columns in {path.name}")
    for field, expected in zip(schema, spec.fields, strict=True):
        if str(field.type) != _expected_arrow_type(expected):
            raise RuntimeError(f"Unexpected schema type for {path.name}.{field.name}: {field.type}")

    grains: set[tuple[object, ...]] = set()
    service_dates: set[str] = set()
    rows = 0
    for batch in parquet.iter_batches(columns=[*spec.grain, "processing_date", "service_date", "gtfs_snapshot_id"]):
        for row in batch.to_pylist():
            if str(row["processing_date"]) != processing_date or row["gtfs_snapshot_id"] != snapshot_id:
                raise RuntimeError(f"Lineage mismatch in {path.name}")
            service_dates.add(str(row["service_date"]))
            grain = tuple(row[column] for column in spec.grain)
            if grain in grains:
                raise RuntimeError(f"Duplicate {spec.key} grain in {path.name}: {grain}")
            grains.add(grain)
            rows += 1
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
    required = {spec.key for spec in ARTIFACTS} | {"trip_universe"}
    if required - schema_versions.keys() or required - outputs.keys():
        raise RuntimeError("Matcher manifest is missing required reconstruction artifacts or trip universe")
    for key in required:
        if schema_versions[key] != ARTIFACT_SCHEMA_VERSIONS[key]:
            raise RuntimeError(f"Matcher manifest schema version mismatch for {key}")

    expected_dates = {processing_date, (date.fromisoformat(processing_date) - timedelta(days=1)).isoformat()}
    metrics = _read_metrics(output)
    validated = {}
    for spec in ARTIFACTS:
        artifact = _inspect_artifact(output / spec.filename, spec, processing_date, snapshot_id)
        expected_identity = outputs[spec.key]
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
    table_id = _table_id(dataset, run_id, spec)
    job_id = _load_job_id(run_id, spec)
    with artifact.path.open("rb") as source:
        try:
            job = client.load_table_from_file(
                source, table_id, job_config=_load_config(spec), job_id=job_id, location=BIGQUERY_LOCATION
            )
        except Conflict:
            job = client.get_job(job_id, project=GCP_PROJECT, location=BIGQUERY_LOCATION)
    job.result()
    return {"table_id": table_id, "job_id": job_id}


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
                group by artifact, source, service_date, mode, trip_quality, observation_status
                """
            )
    return " union all ".join(sections)


def _comparison_report(
    client: Any, processing_date: str, shadow_tables: dict[str, dict[str, str]]
) -> dict[str, object]:
    dates = [date.fromisoformat(processing_date) - timedelta(days=1), date.fromisoformat(processing_date)]
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("service_dates", "DATE", dates)])
    rows = [
        {key: _json_value(value) for key, value in (dict(row.items()) if hasattr(row, "items") else dict(row)).items()}
        for row in client.query(_comparison_query(shadow_tables), job_config=config).result()
    ]
    grouped: dict[tuple[object, ...], dict[str, dict[str, object]]] = {}
    for row in rows:
        key = tuple(row[field] for field in ("artifact", "service_date", "mode", "trip_quality", "observation_status"))
        grouped.setdefault(key, {})[str(row["source"])] = row
    differences = [
        {"group": list(key), "shadow": values.get("shadow"), "canonical": values.get("canonical")}
        for key, values in grouped.items()
        if _comparison_payload(values.get("shadow")) != _comparison_payload(values.get("canonical"))
    ]
    return {"service_dates": [item.isoformat() for item in dates], "aggregates": rows, "differences": differences}


def _comparison_payload(row: dict[str, object] | None) -> dict[str, object] | None:
    """Exclude the query-only source discriminator from aggregate equality."""
    return None if row is None else {key: value for key, value in row.items() if key != "source"}


def _marker_name(config: ShadowConfig, processing_date: str, run_id: str) -> str:
    return f"{config.marker_prefix}/processing_date={processing_date}/run_id={_run_id(run_id)}/commit.json"


def _write_marker(
    client: Any, config: ShadowConfig, processing_date: str, run_id: str, marker: dict[str, object]
) -> str:
    name = _marker_name(config, processing_date, run_id)
    client.bucket(GCS_BUCKET).blob(name).upload_from_string(
        json.dumps(marker, sort_keys=True, default=_json_value), content_type="application/json"
    )
    return f"gs://{GCS_BUCKET}/{name}"


def run_matcher_shadow(
    processing_date: str, snapshot_id: str, run_id: str, *, try_number: int = 1
) -> dict[str, object]:
    """Run one complete shadow reconstruction and write its marker last."""
    config = ShadowConfig.from_env()
    config.validate()
    if not config.enabled:
        return {"enabled": False, "reason": "MATCHER_SHADOW_ENABLED is false"}

    scoped_run_id = _run_id(run_id)
    run_workspace = config.workspace_root / scoped_run_id
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
            storage_client, processing_date, snapshot_uri, workspace
        )
        _invoke_matcher(_matcher_argv(config, processing_date, snapshot_id, gps_root, gtfs_zip, output), config)
        artifacts, manifest = _validate_outputs(output, processing_date, snapshot_id)
        tables = {
            spec.key: _load_artifact(bq_client, config.dataset or "", scoped_run_id, spec, artifacts[spec.key])
            for spec in ARTIFACTS
        }
        comparison = _comparison_report(bq_client, processing_date, tables)
        marker = {
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
            "tables": tables,
            "metrics": manifest.get("metrics", _read_metrics(output)),
            "comparison": comparison,
        }
        marker_uri = _write_marker(storage_client, config, processing_date, run_id, marker)
    except Exception:
        if not config.keep_workspace:
            shutil.rmtree(run_workspace, ignore_errors=True)
        raise
    if not config.keep_workspace:
        shutil.rmtree(run_workspace, ignore_errors=True)
    return {"enabled": True, "marker_uri": marker_uri, "tables": tables}


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


def run_matcher_shadow_task(
    processing_date: str, snapshot_id: str, run_id: str, *, try_number: int = 1
) -> dict[str, object]:
    """Keep canonical DAG publication runnable unless strict mode is explicitly requested."""
    try:
        return run_matcher_shadow(processing_date, snapshot_id, run_id, try_number=try_number)
    except Exception as exc:
        if _env_bool("MATCHER_SHADOW_STRICT", False):
            raise
        LOGGER.exception("Matcher shadow failed without affecting canonical publication")
        return {"enabled": True, "status": "failed_non_strict", "error": str(exc)}
