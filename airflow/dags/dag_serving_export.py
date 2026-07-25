from __future__ import annotations

import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from airflow.sdk import DAG, PartitionedAssetTimetable, get_current_context, task
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    GPS_MODELS_DATE_ASSET,
    SERVING_EXPORT_DIR,
    SERVING_EXPORT_FILENAME,
    SERVING_EXPORT_GCS_PREFIX,
    SERVING_EXPORT_MAX_BYTES,
    airflow_failure_alert,
)

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import ModuleType
    from typing import Protocol

    class DuckdbResult(Protocol):
        """Minimal DuckDB result protocol used by build-time validation."""

        def fetchone(self) -> tuple[Any, ...] | None:
            """Return one query row."""
            ...

    class DuckdbConnection(Protocol):
        """Minimal DuckDB connection protocol used by build-time settings."""

        def execute(self, query: str, parameters: Sequence[Any] | None = None) -> DuckdbResult:
            """DuckDB-compatible execute."""
            ...

        def executemany(self, query: str, parameters: Sequence[Sequence[Any]]) -> DuckdbResult:
            """DuckDB-compatible repeated parameter execution."""
            ...


EXPORT_VERSION = "alpha-1"
EXPORT_SOURCE_MODE = "current_pipeline_provisional"
EXPORT_ID_PATTERN = re.compile(r"^[0-9A-Za-z_.=-]+$")
EXPORT_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
POLLER_HEARTBEAT_GCS_PATH = "health/poller/latest.json"
POLLER_HEARTBEAT_STALE_SECONDS = 180
DUCKDB_MEMORY_LIMIT = "1GB"
DUCKDB_TEMP_DIRECTORY_LIMIT = "2GB"
DUCKDB_THREADS = 2
SEMANTIC_VALIDATION_TIMEOUT_SECONDS = 600
SEMANTIC_VALIDATION_MEMORY_LIMIT_MB = 4096
SEMANTIC_VALIDATION_TEMP_LIMIT_MB = 6144
SEMANTIC_VALIDATION_THREADS = 1
SEMANTIC_VALIDATION_MAX_REPORT_BYTES = 64 * 1024
SEMANTIC_VALIDATION_MAX_WARNINGS = 100
STAGING_RETENTION_DAYS = 3
PARTITIONED_STORE_MIN_FREE_BYTES = 5 * 1024 * 1024 * 1024
SEMANTIC_WARNING_FIELDS = {
    "serving_date_absent": {"code", "service_date"},
    "pipeline_status_mode_absent": {"code", "service_date", "mode"},
    "incomplete_gps_day": {"code", "service_date", "mode", "completeness_ratio"},
    "zero_service_coverage": {"code", "service_date", "mode", "expected_trips"},
}
MART_TABLES = (
    "dim_serving_date",
    "dim_serving_window_date",
    "dim_schedule_version",
    "dim_stop_group_current",
    "dim_stop_post_current",
    "fct_expected_stop_event",
    "mart_entity_daily_summary",
    "mart_entity_window_daily_summary",
    "mart_entity_rankings",
    "mart_entity_timeline_daily",
    "mart_hour_window_summary",
    "mart_line_course_stop_window",
    "mart_line_course_window",
    "mart_line_reliability_daily",
    "mart_line_trip_group_daily",
    "mart_line_window_summary",
    "mart_mode_window_summary",
    "mart_pipeline_status",
    "mart_pipeline_status_recent_summary",
    "mart_stop_group_line_group_window",
    "mart_stop_group_window_summary",
    "mart_stop_line_window_summary",
    "mart_stop_post_line_group_window",
    "mart_stop_post_window_summary",
    "mart_trip_daily",
    "mart_trip_line_daily",
    "mart_trip_mode_daily_summary",
    "mart_worst_delay_event",
)
GLOBAL_EXPORT_TABLES = (
    "dim_schedule_version",
    "dim_serving_date",
    "dim_stop_group_current",
    "dim_stop_post_current",
    "mart_pipeline_status_recent_summary",
)
SHARDED_EXPORT_TABLES = {
    "dim_serving_window_date": "source_end_date",
    "fct_expected_stop_event": "service_date",
    "mart_entity_daily_summary": "service_date",
    "mart_entity_rankings": "source_end_date",
    "mart_entity_timeline_daily": "service_date",
    "mart_entity_window_daily_summary": "source_end_date",
    "mart_hour_window_summary": "source_end_date",
    "mart_line_course_stop_window": "source_end_date",
    "mart_line_course_window": "source_end_date",
    "mart_line_reliability_daily": "service_date",
    "mart_line_trip_group_daily": "service_date",
    "mart_line_window_summary": "source_end_date",
    "mart_mode_window_summary": "source_end_date",
    "mart_pipeline_status": "service_date",
    "mart_stop_group_line_group_window": "source_end_date",
    "mart_stop_group_window_summary": "source_end_date",
    "mart_stop_line_window_summary": "source_end_date",
    "mart_stop_post_line_group_window": "source_end_date",
    "mart_stop_post_window_summary": "source_end_date",
    "mart_trip_daily": "service_date",
    "mart_trip_line_daily": "service_date",
    "mart_trip_mode_daily_summary": "service_date",
    "mart_worst_delay_event": "service_date",
}
if set(GLOBAL_EXPORT_TABLES) | set(SHARDED_EXPORT_TABLES) != set(MART_TABLES):
    raise RuntimeError("Global and sharded serving table groups must partition MART_TABLES")
if set(GLOBAL_EXPORT_TABLES) & set(SHARDED_EXPORT_TABLES):
    raise RuntimeError("Serving tables cannot be both global and sharded")
PARTITIONED_EXPORT_TABLES = {
    "dim_serving_window_date": "source_end_date",
    "fct_expected_stop_event": "service_date",
    "mart_entity_timeline_daily": "service_date",
    "mart_entity_window_daily_summary": "source_end_date",
    "mart_hour_window_summary": "source_end_date",
}
PARTITION_CACHE_MANIFEST = "_MANIFEST.json"
LOCAL_PARTITION_DIRECTORY = "parquet"
LOCAL_GENERATION_INACTIVE_MARKER = ".inactive"
LOCAL_GENERATION_PENDING_MARKER = ".pending"
DATE_RANGE_SQL_BY_TABLE = {
    "dim_serving_date": "service_date",
    "dim_serving_window_date": "source_end_date",
    "fct_expected_stop_event": "service_date",
    "mart_entity_daily_summary": "service_date",
    "mart_entity_window_daily_summary": "source_end_date",
    "mart_entity_rankings": "source_end_date",
    "mart_entity_timeline_daily": "service_date",
    "mart_hour_window_summary": "source_end_date",
    "mart_line_course_stop_window": "source_end_date",
    "mart_line_course_window": "source_end_date",
    "mart_line_reliability_daily": "service_date",
    "mart_line_trip_group_daily": "service_date",
    "mart_line_window_summary": "source_end_date",
    "mart_mode_window_summary": "source_end_date",
    "mart_pipeline_status": "service_date",
    "mart_stop_group_line_group_window": "source_end_date",
    "mart_stop_group_window_summary": "source_end_date",
    "mart_stop_line_window_summary": "source_end_date",
    "mart_stop_post_line_group_window": "source_end_date",
    "mart_stop_post_window_summary": "source_end_date",
    "mart_trip_daily": "service_date",
    "mart_trip_line_daily": "service_date",
    "mart_trip_mode_daily_summary": "service_date",
    "mart_worst_delay_event": "service_date",
}


@dataclass(frozen=True)
class ExportConfig:
    """Airflow conf/env settings that control where the serving artifact is published."""

    export_id: str
    output_dir: Path
    output_filename: str
    gcs_bucket: str
    gcs_prefix: str
    max_source_bytes: int
    max_duckdb_bytes: int
    cleanup_gcs_staging: bool
    partitioned_store: bool = False
    partitioned_store_min_free_bytes: int = PARTITIONED_STORE_MIN_FREE_BYTES
    partitioned_store_view_root: Path | None = None
    changed_partition_dates: tuple[str, ...] = ()
    validation_timeout_seconds: int = SEMANTIC_VALIDATION_TIMEOUT_SECONDS
    validation_memory_limit_mb: int = SEMANTIC_VALIDATION_MEMORY_LIMIT_MB
    validation_temp_limit_mb: int = SEMANTIC_VALIDATION_TEMP_LIMIT_MB
    validation_threads: int = SEMANTIC_VALIDATION_THREADS
    staging_retention_days: int = STAGING_RETENTION_DAYS


@dataclass(frozen=True)
class TableStats:
    """BigQuery source-table metadata recorded into the serving artifact."""

    table_name: str
    row_count: int
    size_bytes: int
    min_date: str | None = None
    max_date: str | None = None
    date_count: int | None = None


@dataclass(frozen=True)
class ExportResult:
    """Published serving artifact metadata returned to Airflow."""

    export_id: str
    duckdb_path: str
    metadata_path: str
    duckdb_size_bytes: int
    source_size_bytes: int
    source_row_count: int
    exported_table_count: int


@dataclass(frozen=True)
class StagingCleanupResult:
    """Summary of stale temporary GCS objects deleted after publication."""

    deleted_object_count: int
    deleted_bytes: int


@dataclass(frozen=True)
class DuckdbBuildInput:
    """Inputs needed to materialize the DuckDB file and embedded metadata."""

    parquet_paths_by_table: dict[str, list[Path]]
    source_stats: Sequence[TableStats]
    config: ExportConfig
    exported_at: datetime
    temp_directory: Path


def _export_config(context: dict[str, object], now: datetime | None = None) -> ExportConfig:
    conf = getattr(context.get("dag_run"), "conf", None)
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        raise TypeError("dag_serving_export config must be a dictionary")

    export_id = _string_config(conf, "export_id", _default_export_id(now or datetime.now(UTC)))
    if not EXPORT_ID_PATTERN.fullmatch(export_id):
        raise ValueError("export_id may contain only letters, numbers, dot, underscore, dash, and equals")

    output_dir = Path(_string_config(conf, "output_dir", os.getenv("SERVING_EXPORT_DIR", SERVING_EXPORT_DIR)))
    output_filename = _string_config(
        conf,
        "output_filename",
        os.getenv("SERVING_EXPORT_FILENAME", SERVING_EXPORT_FILENAME),
    )
    if Path(output_filename).name != output_filename:
        raise ValueError("output_filename must be a bare filename")
    gcs_prefix = _string_config(
        conf,
        "gcs_prefix",
        os.getenv("SERVING_EXPORT_GCS_PREFIX", SERVING_EXPORT_GCS_PREFIX),
    ).strip("/")
    if not gcs_prefix:
        raise ValueError("gcs_prefix must contain a non-slash path component")

    return ExportConfig(
        export_id=export_id,
        output_dir=output_dir,
        output_filename=output_filename,
        gcs_bucket=_string_config(conf, "gcs_bucket", os.getenv("GCS_BUCKET", GCS_BUCKET)),
        gcs_prefix=gcs_prefix,
        max_source_bytes=_int_config(
            conf,
            "max_source_bytes",
            os.getenv("SERVING_EXPORT_MAX_SOURCE_BYTES", str(SERVING_EXPORT_MAX_BYTES)),
        ),
        max_duckdb_bytes=_int_config(
            conf,
            "max_duckdb_bytes",
            os.getenv("SERVING_EXPORT_MAX_DUCKDB_BYTES", str(SERVING_EXPORT_MAX_BYTES)),
        ),
        cleanup_gcs_staging=_bool_config(conf, "cleanup_gcs_staging", True),
        partitioned_store=_bool_config(
            conf,
            "partitioned_store",
            os.getenv("SERVING_EXPORT_PARTITIONED_STORE", "false").lower() == "true",
        ),
        partitioned_store_min_free_bytes=_int_config(
            conf,
            "partitioned_store_min_free_bytes",
            os.getenv("SERVING_EXPORT_PARTITIONED_STORE_MIN_FREE_BYTES", str(PARTITIONED_STORE_MIN_FREE_BYTES)),
        ),
        partitioned_store_view_root=Path(
            _string_config(
                conf,
                "partitioned_store_view_root",
                os.getenv("SERVING_EXPORT_PARTITIONED_STORE_VIEW_ROOT", "/serving"),
            )
        ),
        changed_partition_dates=_changed_partition_dates(context, conf),
        validation_timeout_seconds=_int_config(
            conf,
            "validation_timeout_seconds",
            os.getenv("SERVING_EXPORT_VALIDATION_TIMEOUT_SECONDS", str(SEMANTIC_VALIDATION_TIMEOUT_SECONDS)),
        ),
        validation_memory_limit_mb=_int_config(
            conf,
            "validation_memory_limit_mb",
            os.getenv("SERVING_EXPORT_VALIDATION_MEMORY_LIMIT_MB", str(SEMANTIC_VALIDATION_MEMORY_LIMIT_MB)),
        ),
        validation_temp_limit_mb=_int_config(
            conf,
            "validation_temp_limit_mb",
            os.getenv("SERVING_EXPORT_VALIDATION_TEMP_LIMIT_MB", str(SEMANTIC_VALIDATION_TEMP_LIMIT_MB)),
        ),
        validation_threads=_int_config(
            conf,
            "validation_threads",
            os.getenv("SERVING_EXPORT_VALIDATION_THREADS", str(SEMANTIC_VALIDATION_THREADS)),
        ),
        staging_retention_days=_int_config(
            conf,
            "staging_retention_days",
            os.getenv("SERVING_EXPORT_STAGING_RETENTION_DAYS", str(STAGING_RETENTION_DAYS)),
        ),
    )


def _changed_partition_dates(context: dict[str, object], conf: dict[str, object]) -> tuple[str, ...]:
    configured_dates = _date_list_config(conf, "changed_partition_dates")
    configured_date = _date_config(
        conf,
        "changed_partition_date",
        os.getenv("SERVING_EXPORT_CHANGED_PARTITION_DATE", ""),
    )
    if configured_dates and configured_date is not None:
        raise ValueError("changed_partition_dates and changed_partition_date cannot both be configured")
    if configured_dates:
        return configured_dates
    if configured_date is not None:
        return (configured_date,)
    return _asset_changed_partition_dates(context)


def _asset_changed_partition_dates(context: dict[str, object]) -> tuple[str, ...]:
    triggering_asset_events = context.get("triggering_asset_events")
    if not isinstance(triggering_asset_events, Mapping) or GPS_MODELS_DATE_ASSET not in triggering_asset_events:
        return ()

    asset_events = triggering_asset_events[GPS_MODELS_DATE_ASSET]
    if not isinstance(asset_events, Sequence) or isinstance(asset_events, (str, bytes)) or not asset_events:
        raise RuntimeError("dag_serving_export requires a GPS models asset event with processing_date")

    changed_partition_dates = []
    for asset_event in asset_events:
        extra = getattr(asset_event, "extra", None)
        if not isinstance(extra, dict):
            raise TypeError("dag_serving_export requires GPS models asset events with processing_date")
        event_dates = _date_list_config(extra, "changed_partition_dates")
        if not event_dates:
            processing_date = extra.get("processing_date")
            if not isinstance(processing_date, str) or not EXPORT_DATE_PATTERN.fullmatch(processing_date):
                raise RuntimeError("dag_serving_export requires GPS models asset events with processing_date")
            date.fromisoformat(processing_date)
            event_dates = (processing_date,)
        for partition_date in event_dates:
            if partition_date not in changed_partition_dates:
                changed_partition_dates.append(partition_date)
    return tuple(changed_partition_dates)


def _default_export_id(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _string_config(conf: dict[str, object], key: str, default: str) -> str:
    value = conf.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{key} must be a non-empty string")
    return value.strip()


def _int_config(conf: dict[str, object], key: str, default: str) -> int:
    value = conf.get(key, default)
    if isinstance(value, str):
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def _bool_config(conf: dict[str, object], key: str, default: bool) -> bool:
    value = conf.get(key, default)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a boolean")
    return value


def _date_config(conf: dict[str, object], key: str, default: str) -> str | None:
    value = conf.get(key, default)
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not EXPORT_DATE_PATTERN.fullmatch(value):
        raise ValueError(f"{key} must be an ISO date string")
    date.fromisoformat(value)
    return value


def _date_list_config(conf: dict[str, object], key: str) -> tuple[str, ...]:
    value = conf.get(key)
    if value in (None, []):
        return ()
    if not isinstance(value, list):
        raise TypeError(f"{key} must be a list of ISO date strings")

    dates = []
    for item in value:
        if not isinstance(item, str) or not EXPORT_DATE_PATTERN.fullmatch(item):
            raise ValueError(f"{key} must contain only ISO date strings")
        date.fromisoformat(item)
        if item not in dates:
            dates.append(item)
    return tuple(dates)


def _run_serving_export(config: ExportConfig) -> ExportResult:
    if config.partitioned_store:
        _validate_partitioned_store_view_root(config)
    bigquery_client = bigquery.Client(project=GCP_PROJECT)
    storage_client = storage.Client(project=GCP_PROJECT)
    exported_at = datetime.now(UTC)
    poller_status = _poller_status(storage_client, exported_at, config.gcs_bucket)
    source_stats = _source_table_stats(bigquery_client)
    _validate_source_stats(source_stats, None if config.partitioned_store else config.max_source_bytes)

    with tempfile.TemporaryDirectory(prefix="ztm-serving-export-") as temp_dir:
        local_export_dir = Path(temp_dir)
        if config.partitioned_store:
            parquet_paths_by_table = _extract_partitioned_store_inputs(
                bigquery_client,
                storage_client,
                config,
                local_export_dir,
            )
        else:
            parquet_paths_by_table = _extract_and_download_marts(
                bigquery_client,
                storage_client,
                config,
                local_export_dir,
            )
        result = _publish_duckdb(
            config,
            parquet_paths_by_table,
            source_stats,
            exported_at,
            poller_status,
        )

    if config.cleanup_gcs_staging:
        _cleanup_gcs_staging_best_effort(storage_client, config)
    _cleanup_stale_gcs_staging_best_effort(storage_client, config, exported_at)
    if config.partitioned_store:
        _cleanup_stale_local_generations_best_effort(config, exported_at)

    return result


def _source_table_stats(client: bigquery.Client) -> list[TableStats]:
    stats_by_table = {}
    partitioned_table_names = set()
    for table_name in MART_TABLES:
        table_ref = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.{table_name}"
        try:
            table = client.get_table(table_ref)
        except NotFound:
            continue
        if (
            getattr(table, "time_partitioning", None) is not None
            or getattr(table, "range_partitioning", None) is not None
        ):
            partitioned_table_names.add(table_name)
        stats_by_table[table_name] = TableStats(
            table_name=table_name,
            row_count=int(table.num_rows or 0),
            size_bytes=int(table.num_bytes or 0),
        )

    dated_stats = {stat.table_name: stat for stat in _source_date_ranges(client, partitioned_table_names)}
    return [
        TableStats(
            table_name=table_name,
            row_count=stats_by_table[table_name].row_count,
            size_bytes=stats_by_table[table_name].size_bytes,
            min_date=dated_stats.get(table_name, stats_by_table[table_name]).min_date,
            max_date=dated_stats.get(table_name, stats_by_table[table_name]).max_date,
            date_count=dated_stats.get(table_name, stats_by_table[table_name]).date_count,
        )
        for table_name in MART_TABLES
        if table_name in stats_by_table
    ]


def _source_date_ranges(client: bigquery.Client, partitioned_table_names: set[str]) -> list[TableStats]:
    stats = []
    for table_name in DATE_RANGE_SQL_BY_TABLE:
        if table_name not in MART_TABLES or table_name not in partitioned_table_names:
            continue
        partition_dates = _table_partition_dates(client, table_name)
        if not partition_dates:
            continue
        stats.append(
            TableStats(
                table_name=table_name,
                row_count=0,
                size_bytes=0,
                min_date=partition_dates[0].isoformat(),
                max_date=partition_dates[-1].isoformat(),
                date_count=len(partition_dates),
            )
        )
    return stats


def _table_partition_dates(client: bigquery.Client, table_name: str) -> list[date]:
    table_ref = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.{table_name}"
    partitions = []
    for partition_id in client.list_partitions(table_ref):
        if not isinstance(partition_id, str) or not re.fullmatch(r"\d{8}", partition_id):
            continue
        partitions.append(date.fromisoformat(f"{partition_id[:4]}-{partition_id[4:6]}-{partition_id[6:]}"))
    return sorted(partitions)


def _validate_source_stats(stats: Sequence[TableStats], max_source_bytes: int | None) -> None:
    found_tables = {stat.table_name for stat in stats}
    missing_tables = sorted(set(MART_TABLES) - found_tables)
    if missing_tables:
        raise RuntimeError(f"Missing mart tables for serving export: {', '.join(missing_tables)}")

    empty_required_tables = sorted(
        stat.table_name
        for stat in stats
        if stat.table_name
        in {
            "dim_serving_date",
            "fct_expected_stop_event",
            "mart_mode_window_summary",
            "mart_line_window_summary",
            "mart_stop_group_window_summary",
            "mart_stop_post_window_summary",
            "mart_entity_rankings",
            "mart_trip_daily",
            "mart_pipeline_status",
        }
        and stat.row_count == 0
    )
    if empty_required_tables:
        raise RuntimeError(f"Required serving tables are empty: {', '.join(empty_required_tables)}")

    source_size_bytes = sum(stat.size_bytes for stat in stats)
    if max_source_bytes is not None and source_size_bytes > max_source_bytes:
        raise RuntimeError(
            f"Serving export source size {source_size_bytes} exceeds configured limit {max_source_bytes}"
        )


def _extract_and_download_marts(
    bigquery_client: bigquery.Client,
    storage_client: storage.Client,
    config: ExportConfig,
    local_export_dir: Path,
) -> dict[str, list[Path]]:
    parquet_paths_by_table = {}
    for table_name in MART_TABLES:
        if table_name in PARTITIONED_EXPORT_TABLES and config.changed_partition_dates:
            _sync_partition_cache(bigquery_client, storage_client, config, table_name)
            parquet_paths_by_table[table_name] = _download_partitioned_mart_parquet(
                storage_client, config, table_name, local_export_dir
            )
            continue

        _extract_mart_to_gcs(bigquery_client, config, table_name)
        parquet_paths_by_table[table_name] = _download_mart_parquet(
            storage_client, config, table_name, local_export_dir
        )
    return parquet_paths_by_table


def _extract_partitioned_store_inputs(
    bigquery_client: bigquery.Client,
    storage_client: storage.Client,
    config: ExportConfig,
    local_export_dir: Path,
) -> dict[str, list[Path]]:
    parquet_paths_by_table = {}
    for table_name in GLOBAL_EXPORT_TABLES:
        _extract_mart_to_gcs(bigquery_client, config, table_name)
        parquet_paths_by_table[table_name] = _download_mart_parquet(
            storage_client,
            config,
            table_name,
            local_export_dir,
        )

    for table_name in SHARDED_EXPORT_TABLES:
        _sync_partition_cache(bigquery_client, storage_client, config, table_name)
        partition_dates = [
            partition_date.isoformat() for partition_date in _table_partition_dates(bigquery_client, table_name)
        ]
        refresh_dates = set(config.changed_partition_dates or partition_dates)
        _deactivate_removed_local_partitions(config, table_name, set(partition_dates))
        paths = []
        for partition_date in partition_dates:
            active_paths = _active_local_partition_paths(config, table_name, partition_date)
            if partition_date in refresh_dates or not active_paths:
                active_paths = _replace_local_partition_cache(
                    storage_client,
                    config,
                    table_name,
                    partition_date,
                )
            paths.extend(active_paths)
        if not paths:
            raise RuntimeError(f"No local Parquet partitions found for {table_name}")
        parquet_paths_by_table[table_name] = paths
    return parquet_paths_by_table


def _extract_mart_to_gcs(client: bigquery.Client, config: ExportConfig, table_name: str) -> None:
    source_table = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.{table_name}"
    destination_uri = _table_extract_uri(config, table_name)
    job_config = bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET)
    job_id = _bigquery_job_id("serving_export", config.export_id, table_name)
    try:
        job = client.extract_table(
            source_table, destination_uri, job_config=job_config, job_id=job_id, location=BIGQUERY_LOCATION
        )
    except Conflict:
        raise RuntimeError(
            f"Serving export job already exists for export_id={config.export_id} table={table_name}"
        ) from None
    job.result()


def _sync_partition_cache(
    bigquery_client: bigquery.Client,
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
) -> None:
    partition_dates = tuple(
        partition_date.isoformat() for partition_date in _table_partition_dates(bigquery_client, table_name)
    )
    actual_partition_dates = set(partition_dates)
    date_column = SHARDED_EXPORT_TABLES[table_name]
    changed_partition_dates = config.changed_partition_dates
    if config.partitioned_store and not changed_partition_dates:
        changed_partition_dates = partition_dates
    changed_partition_date_set = set(changed_partition_dates)
    cached_dates = _cached_partition_dates(storage_client, config, table_name, date_column)
    for partition_date in cached_dates - actual_partition_dates:
        _delete_partition_cache(storage_client, config, table_name, date_column, partition_date)

    for partition_date in changed_partition_dates:
        if partition_date in actual_partition_dates:
            _extract_mart_partition_to_cache(bigquery_client, storage_client, config, table_name, partition_date)

    for partition_date in partition_dates:
        if partition_date in cached_dates or partition_date in changed_partition_date_set:
            continue
        _extract_mart_partition_to_cache(
            bigquery_client,
            storage_client,
            config,
            table_name,
            partition_date,
        )


def _delete_partition_cache(
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    date_column: str,
    partition_date: str,
) -> None:
    bucket = storage_client.bucket(config.gcs_bucket)
    cache_prefix = _partition_cache_prefix(config, table_name, date_column, partition_date)
    for blob in list(bucket.list_blobs(prefix=f"{cache_prefix}/")):
        bucket.blob(blob.name).delete()


def _extract_mart_partition_to_cache(
    bigquery_client: bigquery.Client,
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    partition_date: str,
) -> None:
    date_column = SHARDED_EXPORT_TABLES[table_name]
    source_table = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.{table_name}${partition_date.replace('-', '')}"
    destination_uri = _partition_staging_extract_uri(config, table_name, date_column, partition_date)
    job_config = bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET)
    job_id = _bigquery_job_id("serving_export_partition", config.export_id, table_name, partition_date)
    try:
        job = bigquery_client.extract_table(
            source_table,
            destination_uri,
            job_config=job_config,
            job_id=job_id,
            location=BIGQUERY_LOCATION,
        )
    except Conflict:
        raise RuntimeError(
            f"Serving export partition job already exists for export_id={config.export_id} table={table_name}"
        ) from None
    job.result()
    _replace_partition_cache(storage_client, config, table_name, date_column, partition_date)


def _cached_partition_dates(
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    date_column: str,
) -> set[str]:
    bucket = storage_client.bucket(config.gcs_bucket)
    table_prefix = _partition_table_cache_prefix(config, table_name)
    blobs_by_date: dict[str, list[storage.Blob]] = {}
    partition_prefix = f"{date_column}="
    for blob in bucket.list_blobs(prefix=table_prefix):
        relative_name = blob.name.removeprefix(table_prefix)
        partition_dir = relative_name.split("/", maxsplit=1)[0]
        if not partition_dir.startswith(partition_prefix):
            continue
        partition_date = partition_dir.removeprefix(partition_prefix)
        blobs_by_date.setdefault(partition_date, []).append(blob)
    return {
        partition_date
        for partition_date, blobs in blobs_by_date.items()
        if _active_partition_blob_names(blobs, _partition_cache_prefix(config, table_name, date_column, partition_date))
    }


def _replace_partition_cache(
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    date_column: str,
    partition_date: str,
) -> None:
    bucket = storage_client.bucket(config.gcs_bucket)
    cache_prefix = _partition_cache_prefix(config, table_name, date_column, partition_date)
    staging_prefix = _partition_staging_prefix(config, table_name, date_column, partition_date)
    staging_blobs = [blob for blob in bucket.list_blobs(prefix=f"{staging_prefix}/") if blob.name.endswith(".parquet")]
    if not staging_blobs:
        raise RuntimeError(f"Partitioned export produced no parquet files for {table_name} {partition_date}")

    old_blobs = list(bucket.list_blobs(prefix=f"{cache_prefix}/"))
    generation_prefix = f"{cache_prefix}/generation={config.export_id}"
    new_blob_names = []
    for blob in staging_blobs:
        destination_name = f"{generation_prefix}/{Path(blob.name).name}"
        bucket.copy_blob(blob, bucket, destination_name)
        new_blob_names.append(destination_name)

    manifest_name = f"{cache_prefix}/{PARTITION_CACHE_MANIFEST}"
    bucket.blob(manifest_name).upload_from_string(
        json.dumps({"parquet_blobs": new_blob_names}, sort_keys=True),
        content_type="application/json",
    )
    for blob in old_blobs:
        if blob.name != manifest_name and blob.name not in new_blob_names:
            bucket.blob(blob.name).delete()


def _active_partition_blob_names(blobs: Sequence[storage.Blob], cache_prefix: str) -> list[str]:
    manifest_name = f"{cache_prefix}/{PARTITION_CACHE_MANIFEST}"
    manifest = next((blob for blob in blobs if blob.name == manifest_name), None)
    available_names = {blob.name for blob in blobs}
    if manifest is not None:
        payload = json.loads(manifest.download_as_bytes())
        blob_names = payload.get("parquet_blobs") if isinstance(payload, dict) else None
        if not isinstance(blob_names, list) or not blob_names or not all(isinstance(name, str) for name in blob_names):
            raise RuntimeError(f"Invalid partition cache manifest: {manifest_name}")
        if not set(blob_names) <= available_names:
            raise RuntimeError(f"Incomplete partition cache generation: {manifest_name}")
        return sorted(blob_names)

    legacy_prefix = f"{cache_prefix}/"
    return sorted(
        blob.name
        for blob in blobs
        if blob.name.startswith(legacy_prefix)
        and "/" not in blob.name.removeprefix(legacy_prefix)
        and blob.name.endswith(".parquet")
    )


def _table_extract_uri(config: ExportConfig, table_name: str) -> str:
    return f"gs://{config.gcs_bucket}/{_table_staging_prefix(config, table_name)}/part-*.parquet"


def _partition_staging_extract_uri(
    config: ExportConfig,
    table_name: str,
    date_column: str,
    partition_date: str,
) -> str:
    return f"gs://{config.gcs_bucket}/{_partition_staging_prefix(config, table_name, date_column, partition_date)}/part-*.parquet"


def _table_staging_prefix(config: ExportConfig, table_name: str) -> str:
    return f"{config.gcs_prefix}/export_id={config.export_id}/{table_name}"


def _partition_staging_prefix(config: ExportConfig, table_name: str, date_column: str, partition_date: str) -> str:
    return f"{config.gcs_prefix}/partition_staging/export_id={config.export_id}/{table_name}/{date_column}={partition_date}"


def _partition_cache_prefix(config: ExportConfig, table_name: str, date_column: str, partition_date: str) -> str:
    return f"{config.gcs_prefix}/partition_cache/{table_name}/{date_column}={partition_date}"


def _partition_table_cache_prefix(config: ExportConfig, table_name: str) -> str:
    return f"{config.gcs_prefix}/partition_cache/{table_name}/"


def _download_mart_parquet(
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    local_export_dir: Path,
) -> list[Path]:
    bucket = storage_client.bucket(config.gcs_bucket)
    table_dir = local_export_dir / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
    blob_names = []
    for blob in bucket.list_blobs(prefix=f"{_table_staging_prefix(config, table_name)}/"):
        if not blob.name.endswith(".parquet"):
            continue
        blob_names.append(blob.name)

    paths = []
    for blob_name in sorted(blob_names):
        path = table_dir / Path(blob_name).name
        # Avoid stale listed generations; download the current object by name.
        bucket.blob(blob_name).download_to_filename(str(path))
        paths.append(path)

    if not paths:
        raise RuntimeError(f"BigQuery extract produced no parquet files for {table_name}")
    return paths


def _download_partitioned_mart_parquet(
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    local_export_dir: Path,
) -> list[Path]:
    bucket = storage_client.bucket(config.gcs_bucket)
    table_dir = local_export_dir / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
    table_prefix = _partition_table_cache_prefix(config, table_name)
    blobs_by_partition: dict[str, list[storage.Blob]] = {}
    for blob in bucket.list_blobs(prefix=table_prefix):
        relative_name = blob.name.removeprefix(table_prefix)
        partition_dir = relative_name.split("/", maxsplit=1)[0]
        if "=" not in partition_dir:
            continue
        blobs_by_partition.setdefault(partition_dir, []).append(blob)

    paths = []
    for partition_dir, blobs in sorted(blobs_by_partition.items()):
        cache_prefix = f"{table_prefix}{partition_dir}"
        for blob_name in _active_partition_blob_names(blobs, cache_prefix):
            local_partition_dir = table_dir / partition_dir
            local_partition_dir.mkdir(parents=True, exist_ok=True)
            path = local_partition_dir / Path(blob_name).name
            bucket.blob(blob_name).download_to_filename(str(path))
            paths.append(path)

    if not paths:
        raise RuntimeError(f"No cached partition parquet files found for {table_name}")
    return paths


def _replace_local_partition_cache(
    storage_client: storage.Client,
    config: ExportConfig,
    table_name: str,
    partition_date: str,
) -> list[Path]:
    date_column = SHARDED_EXPORT_TABLES[table_name]
    cache_prefix = _partition_cache_prefix(config, table_name, date_column, partition_date)
    bucket = storage_client.bucket(config.gcs_bucket)
    blobs = list(bucket.list_blobs(prefix=f"{cache_prefix}/"))
    blob_names = _active_partition_blob_names(blobs, cache_prefix)
    if not blob_names:
        raise RuntimeError(f"No active GCS partition cache found for {table_name} {partition_date}")

    config.output_dir.mkdir(parents=True, exist_ok=True)
    _remove_incomplete_local_generations(config, table_name, partition_date)
    blobs_by_name = {blob.name: blob for blob in blobs}
    required_bytes = sum(int(blobs_by_name[name].size or 0) for name in blob_names)
    free_bytes = shutil.disk_usage(config.output_dir).free
    if free_bytes - required_bytes < config.partitioned_store_min_free_bytes:
        raise RuntimeError(
            f"Insufficient serving disk headroom for {table_name} {partition_date}: "
            f"free={free_bytes}, download={required_bytes}, required_free={config.partitioned_store_min_free_bytes}"
        )

    partition_dir = _local_partition_cache_dir(config, table_name, date_column, partition_date)
    generation_dir = partition_dir / f"generation={config.export_id}"
    if generation_dir.exists():
        raise RuntimeError(f"Local partition generation already exists: {generation_dir}")
    generation_dir.mkdir(parents=True)
    pending_marker = generation_dir / LOCAL_GENERATION_PENDING_MARKER
    pending_marker.touch()

    paths = []
    try:
        for blob_name in blob_names:
            path = generation_dir / Path(blob_name).name
            bucket.blob(blob_name).download_to_filename(str(path))
            paths.append(path)
        _write_metadata_file(
            partition_dir / PARTITION_CACHE_MANIFEST,
            {
                "export_id": config.export_id,
                "parquet_files": [path.relative_to(config.output_dir).as_posix() for path in paths],
            },
        )
        pending_marker.unlink()
    except Exception:
        shutil.rmtree(generation_dir, ignore_errors=True)
        raise
    return paths


def _active_local_partition_paths(
    config: ExportConfig,
    table_name: str,
    partition_date: str,
) -> list[Path]:
    date_column = SHARDED_EXPORT_TABLES[table_name]
    partition_dir = _local_partition_cache_dir(config, table_name, date_column, partition_date)
    manifest_path = partition_dir / PARTITION_CACHE_MANIFEST
    if not manifest_path.exists():
        return []

    payload = json.loads(manifest_path.read_bytes())
    relative_paths = payload.get("parquet_files") if isinstance(payload, dict) else None
    if (
        not isinstance(relative_paths, list)
        or not relative_paths
        or not all(isinstance(path, str) for path in relative_paths)
    ):
        raise RuntimeError(f"Invalid local partition cache manifest: {manifest_path}")

    output_dir = config.output_dir.resolve()
    expected_dir = partition_dir.resolve()
    paths = []
    for relative_path in relative_paths:
        path = (config.output_dir / relative_path).resolve()
        if not path.is_relative_to(expected_dir) or not path.is_relative_to(output_dir) or path.suffix != ".parquet":
            raise RuntimeError(f"Unsafe local partition cache path in {manifest_path}: {relative_path}")
        if not path.is_file():
            raise RuntimeError(f"Missing local partition cache file: {path}")
        paths.append(path)
    return sorted(paths)


def _remove_incomplete_local_generations(
    config: ExportConfig,
    table_name: str,
    partition_date: str,
) -> int:
    date_column = SHARDED_EXPORT_TABLES[table_name]
    partition_dir = _local_partition_cache_dir(config, table_name, date_column, partition_date)
    active_generation_dirs = {path.parent for path in _active_local_partition_paths(config, table_name, partition_date)}
    deleted_count = 0
    for generation_dir in partition_dir.glob("generation=*"):
        pending_marker = generation_dir / LOCAL_GENERATION_PENDING_MARKER
        if not pending_marker.exists():
            continue
        if generation_dir.resolve() in active_generation_dirs:
            pending_marker.unlink()
            continue
        shutil.rmtree(generation_dir)
        deleted_count += 1
    return deleted_count


def _local_partition_cache_dir(
    config: ExportConfig,
    table_name: str,
    date_column: str,
    partition_date: str,
) -> Path:
    return config.output_dir / LOCAL_PARTITION_DIRECTORY / table_name / f"{date_column}={partition_date}"


def _deactivate_removed_local_partitions(
    config: ExportConfig,
    table_name: str,
    actual_partition_dates: set[str],
) -> None:
    date_column = SHARDED_EXPORT_TABLES[table_name]
    table_dir = config.output_dir / LOCAL_PARTITION_DIRECTORY / table_name
    if not table_dir.exists():
        return
    for partition_dir in table_dir.glob(f"{date_column}=*"):
        partition_date = partition_dir.name.removeprefix(f"{date_column}=")
        if partition_date not in actual_partition_dates:
            (partition_dir / PARTITION_CACHE_MANIFEST).unlink(missing_ok=True)


def _cleanup_stale_local_generations(config: ExportConfig, now: datetime) -> int:
    cutoff = now - timedelta(days=config.staging_retention_days)
    deleted_count = 0
    root = config.output_dir / LOCAL_PARTITION_DIRECTORY
    for table_name, date_column in SHARDED_EXPORT_TABLES.items():
        table_dir = root / table_name
        if not table_dir.exists():
            continue
        for partition_dir in table_dir.glob(f"{date_column}=*"):
            manifest_path = partition_dir / PARTITION_CACHE_MANIFEST
            active_generation_dirs = set()
            if manifest_path.is_file():
                partition_date = partition_dir.name.removeprefix(f"{date_column}=")
                active_generation_dirs = {
                    path.parent for path in _active_local_partition_paths(config, table_name, partition_date)
                }
            for generation_dir in partition_dir.glob("generation=*"):
                if generation_dir.resolve() in active_generation_dirs:
                    (generation_dir / LOCAL_GENERATION_INACTIVE_MARKER).unlink(missing_ok=True)
                    continue
                inactive_marker = generation_dir / LOCAL_GENERATION_INACTIVE_MARKER
                if not inactive_marker.exists():
                    inactive_marker.touch()
                    continue
                inactive_at = datetime.fromtimestamp(inactive_marker.stat().st_mtime, UTC)
                if inactive_at >= cutoff:
                    continue
                shutil.rmtree(generation_dir)
                deleted_count += 1
    return deleted_count


def _cleanup_stale_local_generations_best_effort(config: ExportConfig, now: datetime) -> None:
    try:
        deleted_count = _cleanup_stale_local_generations(config, now)
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to clean stale local serving generations", exc_info=True)
        return
    if deleted_count:
        LOGGER.info("Deleted %s stale local serving generations", deleted_count)


def _publish_duckdb(
    config: ExportConfig,
    parquet_paths_by_table: dict[str, list[Path]],
    source_stats: Sequence[TableStats],
    exported_at: datetime,
    poller_status: dict[str, object] | None = None,
) -> ExportResult:
    duckdb_module = _duckdb_module()
    config.output_dir.mkdir(parents=True, exist_ok=True)
    final_path = config.output_dir / config.output_filename
    temp_path = config.output_dir / f".{config.output_filename}.{config.export_id}.tmp"
    duckdb_temp_dir = config.output_dir / f".duckdb-tmp-{config.export_id}"
    validation_temp_dir = config.output_dir / f".validation-tmp-{config.export_id}"
    metadata_path = config.output_dir / f"{config.output_filename}.meta.json"
    if temp_path.exists():
        temp_path.unlink()
    _remove_duckdb_sidecar_files(temp_path)
    if duckdb_temp_dir.exists():
        shutil.rmtree(duckdb_temp_dir)
    if validation_temp_dir.exists():
        shutil.rmtree(validation_temp_dir)
    duckdb_temp_dir.mkdir(parents=True)
    validation_temp_dir.mkdir(parents=True)

    try:
        build_input = DuckdbBuildInput(
            parquet_paths_by_table=parquet_paths_by_table,
            source_stats=source_stats,
            config=config,
            exported_at=exported_at,
            temp_directory=duckdb_temp_dir,
        )
        if config.partitioned_store:
            _build_partitioned_catalog_file(duckdb_module, temp_path, build_input)
        else:
            _build_duckdb_file(duckdb_module, temp_path, build_input)
        duckdb_size_bytes = temp_path.stat().st_size
        _enforce_duckdb_size(duckdb_size_bytes, config.max_duckdb_bytes)

        _validate_duckdb_export(duckdb_module, temp_path, source_stats)
        semantic_validation = _run_semantic_validation(config, temp_path, validation_temp_dir)
        _record_semantic_validation(duckdb_module, temp_path, semantic_validation)
        duckdb_size_bytes = temp_path.stat().st_size
        _enforce_duckdb_size(duckdb_size_bytes, config.max_duckdb_bytes)
        _update_duckdb_file_size(duckdb_module, temp_path, duckdb_size_bytes)
        duckdb_size_bytes = temp_path.stat().st_size
        metadata = _export_metadata(
            config,
            source_stats,
            exported_at,
            duckdb_size_bytes,
            poller_status,
            semantic_validation,
        )
    except Exception:
        temp_path.unlink(missing_ok=True)
        _remove_duckdb_sidecar_files(temp_path)
        shutil.rmtree(duckdb_temp_dir, ignore_errors=True)
        shutil.rmtree(validation_temp_dir, ignore_errors=True)
        raise
    shutil.rmtree(duckdb_temp_dir, ignore_errors=True)
    shutil.rmtree(validation_temp_dir, ignore_errors=True)

    previous_metadata = metadata_path.read_bytes() if metadata_path.exists() else None
    try:
        _write_metadata_file(metadata_path, metadata)
        temp_path.replace(final_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        _remove_duckdb_sidecar_files(temp_path)
        _restore_metadata_file(metadata_path, previous_metadata)
        raise
    return ExportResult(
        export_id=config.export_id,
        duckdb_path=str(final_path),
        metadata_path=str(metadata_path),
        duckdb_size_bytes=duckdb_size_bytes,
        source_size_bytes=sum(stat.size_bytes for stat in source_stats),
        source_row_count=sum(stat.row_count for stat in source_stats),
        exported_table_count=len(MART_TABLES),
    )


def _enforce_duckdb_size(duckdb_size_bytes: int, max_duckdb_bytes: int) -> None:
    if duckdb_size_bytes > max_duckdb_bytes:
        raise RuntimeError(f"DuckDB export size {duckdb_size_bytes} exceeds configured limit {max_duckdb_bytes}")


def _duckdb_module() -> ModuleType:
    try:
        duckdb = import_module("duckdb")
    except ModuleNotFoundError as exc:
        raise RuntimeError("dag_serving_export requires the duckdb Python package in the Airflow image") from exc
    return duckdb


def _build_duckdb_file(
    duckdb_module: ModuleType,
    path: Path,
    build_input: DuckdbBuildInput,
) -> None:
    with duckdb_module.connect(str(path)) as connection:
        _configure_duckdb_build_connection(connection, build_input.temp_directory)
        for table_name in MART_TABLES:
            connection.execute(
                f"create table {_identifier(table_name)} as select * from read_parquet({_duckdb_path_list(build_input.parquet_paths_by_table[table_name])})"
            )
        _create_export_metadata_tables(connection, build_input)


def _build_partitioned_catalog_file(
    duckdb_module: ModuleType,
    path: Path,
    build_input: DuckdbBuildInput,
) -> None:
    """Build a small DuckDB catalog over stable, locally cached Parquet shards."""
    with duckdb_module.connect(str(path)) as connection:
        _configure_duckdb_build_connection(connection, build_input.temp_directory)
        for table_name in GLOBAL_EXPORT_TABLES:
            connection.execute(
                f"create table {_identifier(table_name)} as select * from read_parquet({_duckdb_path_list(build_input.parquet_paths_by_table[table_name])})"
            )
        for table_name in SHARDED_EXPORT_TABLES:
            catalog_paths = _partitioned_catalog_paths(
                build_input.config,
                build_input.parquet_paths_by_table[table_name],
            )
            connection.execute(
                f"create view {_identifier(table_name)} as select * from read_parquet({_duckdb_path_list(catalog_paths)}, hive_partitioning = false)"
            )
        _create_export_metadata_tables(connection, build_input)


def _validate_partitioned_store_view_root(config: ExportConfig) -> None:
    view_root = config.partitioned_store_view_root or config.output_dir
    config.output_dir.mkdir(parents=True, exist_ok=True)
    if not view_root.is_absolute() or not view_root.is_dir() or not config.output_dir.samefile(view_root):
        raise RuntimeError(
            f"Partitioned serving view root {view_root} must resolve to the same directory as {config.output_dir}"
        )


def _partitioned_catalog_paths(config: ExportConfig, paths: Sequence[Path]) -> list[Path]:
    output_dir = config.output_dir.resolve()
    view_root = config.partitioned_store_view_root or output_dir
    catalog_paths = []
    for path in paths:
        resolved_path = path.resolve()
        if not resolved_path.is_relative_to(output_dir):
            raise RuntimeError(f"Partitioned serving path is outside the output directory: {path}")
        catalog_paths.append(view_root / resolved_path.relative_to(output_dir))
    return catalog_paths


def _create_export_metadata_tables(connection: DuckdbConnection, build_input: DuckdbBuildInput) -> None:
    connection.execute(
        """
        create table export_table_stats (
            table_name varchar,
            row_count ubigint,
            source_size_bytes ubigint,
            min_date varchar,
            max_date varchar,
            date_count ubigint
        )
        """
    )
    connection.executemany(
        "insert into export_table_stats values (?, ?, ?, ?, ?, ?)",
        [
            (stat.table_name, stat.row_count, stat.size_bytes, stat.min_date, stat.max_date, stat.date_count)
            for stat in build_input.source_stats
        ],
    )
    connection.execute(
        """
        create table export_metadata (
            export_id varchar,
            export_version varchar,
            source_mode varchar,
            exported_at timestamptz,
            source_project varchar,
            source_dataset varchar,
            source_size_bytes ubigint,
            source_row_count ubigint,
            exported_table_count ubigint,
            duckdb_file_size_bytes ubigint
        )
        """
    )
    connection.execute(
        "insert into export_metadata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            build_input.config.export_id,
            EXPORT_VERSION,
            EXPORT_SOURCE_MODE,
            build_input.exported_at,
            GCP_PROJECT,
            BIGQUERY_MARTS_DATASET,
            sum(stat.size_bytes for stat in build_input.source_stats),
            sum(stat.row_count for stat in build_input.source_stats),
            len(MART_TABLES),
            0,
        ],
    )


def _configure_duckdb_build_connection(connection: DuckdbConnection, temp_directory: Path) -> None:
    temp_directory_sql = temp_directory.as_posix().replace("'", "''")
    # Build runs on a constrained Airflow worker; cap memory/threads and spill under the serving mount.
    connection.execute(f"set temp_directory = '{temp_directory_sql}'")
    connection.execute(f"set max_temp_directory_size = '{DUCKDB_TEMP_DIRECTORY_LIMIT}'")
    connection.execute(f"set memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    connection.execute(f"set threads = {DUCKDB_THREADS}")
    # Do not rely on Parquet import order; serving queries must specify their own ordering.
    connection.execute("set preserve_insertion_order = false")


def _update_duckdb_file_size(duckdb_module: ModuleType, path: Path, duckdb_size_bytes: int) -> None:
    with duckdb_module.connect(str(path)) as connection:
        connection.execute("update export_metadata set duckdb_file_size_bytes = ?", [duckdb_size_bytes])


def _run_semantic_validation(
    config: ExportConfig,
    path: Path,
    temp_directory: Path,
) -> dict[str, object]:
    validator_path = Path(__file__).with_name("serving_export_validator.py")
    command = [
        sys.executable,
        str(validator_path),
        str(path),
        "--memory-limit-mb",
        str(config.validation_memory_limit_mb),
        "--temp-limit-mb",
        str(config.validation_temp_limit_mb),
        "--threads",
        str(config.validation_threads),
        "--temp-directory",
        str(temp_directory),
    ]
    for partition_date in config.changed_partition_dates:
        command.extend(["--date", partition_date])
    process = subprocess.Popen(  # noqa: S603 - command uses fixed local code and validated scalar arguments.
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=config.validation_timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        _kill_validation_process(process)
        process.communicate()
        raise RuntimeError(
            f"Serving semantic validation timed out after {config.validation_timeout_seconds} seconds"
        ) from exc
    if process.returncode != 0:
        error = stderr.strip()[-1000:] or f"exit code {process.returncode}"
        raise RuntimeError(f"Serving semantic validation failed: {error}")
    if len(stdout.encode("utf-8")) > SEMANTIC_VALIDATION_MAX_REPORT_BYTES:
        raise RuntimeError("Serving semantic validation report exceeds configured bound")
    try:
        report = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Serving semantic validation returned invalid JSON") from exc
    return _validate_semantic_report(report)


def _kill_validation_process(process: subprocess.Popen[str]) -> None:
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        return
    process.kill()


def _validate_semantic_report(report: object) -> dict[str, object]:
    if not isinstance(report, dict) or report.get("status") not in {"pass", "warning"}:
        raise TypeError("Serving semantic validation returned an invalid report")
    report_dict = cast("dict[str, object]", report)
    warnings = report_dict.get("warnings")
    checked_dates = report_dict.get("checked_dates")
    warning_count = report_dict.get("warning_count")
    warnings_truncated = report_dict.get("warnings_truncated")
    if (
        not isinstance(warnings, list)
        or not isinstance(checked_dates, list)
        or not all(isinstance(value, str) and EXPORT_DATE_PATTERN.fullmatch(value) for value in checked_dates)
        or not isinstance(warning_count, int)
        or warning_count < len(warnings)
        or not isinstance(warnings_truncated, bool)
        or warnings_truncated != (warning_count > len(warnings))
        or (not warnings_truncated and warning_count != len(warnings))
        or len(warnings) > SEMANTIC_VALIDATION_MAX_WARNINGS
        or report_dict["status"] != ("warning" if warning_count else "pass")
    ):
        raise TypeError("Serving semantic validation returned an invalid report")
    for warning in warnings:
        if not isinstance(warning, dict):
            raise TypeError("Serving semantic validation returned an invalid report")
        code = warning.get("code")
        if not isinstance(code, str) or set(warning) != SEMANTIC_WARNING_FIELDS.get(code):
            raise TypeError("Serving semantic validation returned an invalid report")
        service_date = warning.get("service_date")
        if not isinstance(service_date, str) or not EXPORT_DATE_PATTERN.fullmatch(service_date):
            raise TypeError("Serving semantic validation returned an invalid report")
    return report_dict


def _record_semantic_validation(
    duckdb_module: ModuleType,
    path: Path,
    report: dict[str, object],
) -> None:
    with duckdb_module.connect(str(path)) as connection:
        connection.execute("alter table export_metadata add column semantic_validation_status varchar")
        connection.execute("alter table export_metadata add column semantic_validation_warnings_json varchar")
        connection.execute(
            "update export_metadata set semantic_validation_status = ?, semantic_validation_warnings_json = ?",
            [report["status"], json.dumps(report["warnings"], sort_keys=True)],
        )


def _validate_duckdb_export(duckdb_module: ModuleType, path: Path, source_stats: Sequence[TableStats]) -> None:
    with duckdb_module.connect(str(path), read_only=True) as connection:
        table_rows = connection.execute(
            "select table_name from information_schema.tables where table_schema = 'main'"
        ).fetchall()
        actual_tables = {row[0] for row in table_rows}
        required_tables = set(MART_TABLES) | {"export_metadata", "export_table_stats"}
        missing_tables = sorted(required_tables - actual_tables)
        if missing_tables:
            raise RuntimeError(f"DuckDB export missing tables: {', '.join(missing_tables)}")

        for table_name in [
            "dim_serving_date",
            "dim_stop_group_current",
            "dim_stop_post_current",
            "mart_mode_window_summary",
            "mart_line_window_summary",
            "mart_stop_group_window_summary",
            "mart_stop_post_window_summary",
            "mart_entity_rankings",
            "mart_hour_window_summary",
            "mart_entity_timeline_daily",
            "mart_trip_daily",
            "mart_pipeline_status",
        ]:
            row_count = connection.execute(f"select count(*) from {_identifier(table_name)}").fetchone()[0]
            if row_count == 0:
                raise RuntimeError(f"DuckDB export required table is empty: {table_name}")

        for stat in source_stats:
            row_count = connection.execute(f"select count(*) from {_identifier(stat.table_name)}").fetchone()[0]
            if row_count != stat.row_count:
                raise RuntimeError(
                    f"DuckDB export row count mismatch for {stat.table_name}: expected {stat.row_count}, got {row_count}"
                )


def _export_metadata(  # noqa: PLR0913
    config: ExportConfig,
    source_stats: Sequence[TableStats],
    exported_at: datetime,
    duckdb_size_bytes: int,
    poller_status: dict[str, object] | None = None,
    semantic_validation: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "export_id": config.export_id,
        "export_version": EXPORT_VERSION,
        "source_mode": EXPORT_SOURCE_MODE,
        "exported_at": exported_at.isoformat(),
        "last_export_at": exported_at.isoformat(),
        "source_project": GCP_PROJECT,
        "source_dataset": BIGQUERY_MARTS_DATASET,
        "duckdb_path": str(config.output_dir / config.output_filename),
        "duckdb_file_size_bytes": duckdb_size_bytes,
        "source_size_bytes": sum(stat.size_bytes for stat in source_stats),
        "source_row_count": sum(stat.row_count for stat in source_stats),
        "exported_table_count": len(MART_TABLES),
        "poller_status": poller_status or _unknown_poller_status("not_collected"),
        "semantic_validation": semantic_validation
        or {
            "status": "unknown",
            "checked_dates": [],
            "warning_count": 0,
            "warnings_truncated": False,
            "warnings": [],
        },
        "tables": [asdict(stat) for stat in source_stats],
    }


def _poller_status(storage_client: storage.Client, now: datetime, bucket_name: str) -> dict[str, object]:
    path = os.getenv("POLLER_HEARTBEAT_GCS_PATH", POLLER_HEARTBEAT_GCS_PATH).strip("/")
    try:
        payload = json.loads(storage_client.bucket(bucket_name).blob(path).download_as_bytes().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return _unknown_poller_status(type(exc).__name__)

    if not isinstance(payload, dict):
        return _unknown_poller_status("invalid_payload")

    updated_at = _string_or_none(payload.get("updated_at"))
    heartbeat_time = _parse_utc_datetime(updated_at)
    if heartbeat_time is None:
        return _unknown_poller_status("invalid_updated_at")

    status = _safe_status(payload.get("status"))
    if now.astimezone(UTC) - heartbeat_time > timedelta(seconds=POLLER_HEARTBEAT_STALE_SECONDS):
        status = "stale"

    vehicle_types = _poller_vehicle_statuses(payload.get("vehicle_types"))
    return {
        "status": status,
        "updated_at": updated_at,
        "last_success_at": _latest_vehicle_success_at(vehicle_types),
        "stale_after_seconds": POLLER_HEARTBEAT_STALE_SECONDS,
        "vehicle_types": vehicle_types,
    }


def _unknown_poller_status(error_type: str) -> dict[str, object]:
    return {
        "status": "unknown",
        "updated_at": None,
        "last_success_at": None,
        "stale_after_seconds": POLLER_HEARTBEAT_STALE_SECONDS,
        "vehicle_types": {},
        "error_type": error_type,
    }


def _poller_vehicle_statuses(raw_vehicle_types: object) -> dict[str, dict[str, object]]:
    if not isinstance(raw_vehicle_types, dict):
        return {}

    statuses: dict[str, dict[str, object]] = {}
    for mode in ("bus", "tram"):
        raw_status = raw_vehicle_types.get(mode)
        if not isinstance(raw_status, dict):
            continue
        statuses[mode] = {
            "last_success_at": _string_or_none(raw_status.get("last_success_at")),
            "last_accepted_rows": _int_or_zero(raw_status.get("last_accepted_rows")),
            "consecutive_failures": _int_or_zero(raw_status.get("consecutive_failures")),
            "last_error_type": _string_or_none(raw_status.get("last_error_type")),
        }
    return statuses


def _latest_vehicle_success_at(vehicle_types: dict[str, dict[str, object]]) -> str | None:
    success_times = [
        success_at for status in vehicle_types.values() if isinstance(success_at := status.get("last_success_at"), str)
    ]
    return max(success_times) if success_times else None


def _safe_status(value: object) -> str:
    if value in {"ok", "starting", "degraded", "down"}:
        return str(value)
    return "unknown"


def _parse_utc_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _int_or_zero(value: object) -> int:
    return value if isinstance(value, int) else 0


def _write_metadata_file(path: Path, metadata: dict[str, object]) -> None:
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def _restore_metadata_file(path: Path, previous_metadata: bytes | None) -> None:
    if previous_metadata is None:
        path.unlink(missing_ok=True)
        return
    temp_path = path.with_suffix(f"{path.suffix}.restore.tmp")
    temp_path.write_bytes(previous_metadata)
    temp_path.replace(path)


def _remove_duckdb_sidecar_files(path: Path) -> None:
    path.with_name(f"{path.name}.wal").unlink(missing_ok=True)


def _cleanup_gcs_staging(storage_client: storage.Client, config: ExportConfig) -> None:
    bucket = storage_client.bucket(config.gcs_bucket)
    prefixes = [
        f"{config.gcs_prefix}/export_id={config.export_id}/",
        f"{config.gcs_prefix}/partition_staging/export_id={config.export_id}/",
    ]
    for prefix in prefixes:
        for blob in bucket.list_blobs(prefix=prefix):
            bucket.blob(blob.name).delete(if_generation_match=blob.generation)


def _cleanup_gcs_staging_best_effort(storage_client: storage.Client, config: ExportConfig) -> None:
    try:
        _cleanup_gcs_staging(storage_client, config)
    except Exception:
        LOGGER.exception("Failed to clean current serving export GCS staging")


def _cleanup_stale_gcs_staging(
    storage_client: storage.Client,
    config: ExportConfig,
    now: datetime,
) -> StagingCleanupResult:
    bucket = storage_client.bucket(config.gcs_bucket)
    cutoff = now - timedelta(days=config.staging_retention_days)
    temporary_roots = (
        f"{config.gcs_prefix}/export_id=",
        f"{config.gcs_prefix}/partition_staging/export_id=",
    )
    blobs_by_export: dict[str, list[storage.Blob]] = {}
    for root in temporary_roots:
        for blob in bucket.list_blobs(prefix=root):
            relative_name = blob.name.removeprefix(root)
            export_id, separator, _remainder = relative_name.partition("/")
            if not separator or not export_id:
                continue
            blobs_by_export.setdefault(export_id, []).append(blob)

    deleted_object_count = 0
    deleted_bytes = 0
    for export_id, blobs in blobs_by_export.items():
        if export_id == config.export_id or any(
            blob.updated is None or blob.generation is None or blob.updated >= cutoff for blob in blobs
        ):
            continue
        for blob in blobs:
            deleted_bytes += int(blob.size or 0)
            bucket.blob(blob.name).delete(if_generation_match=blob.generation)
            deleted_object_count += 1
    return StagingCleanupResult(
        deleted_object_count=deleted_object_count,
        deleted_bytes=deleted_bytes,
    )


def _cleanup_stale_gcs_staging_best_effort(
    storage_client: storage.Client,
    config: ExportConfig,
    now: datetime,
) -> None:
    try:
        result = _cleanup_stale_gcs_staging(storage_client, config, now)
    except Exception:
        LOGGER.exception("Failed to clean stale serving export GCS staging")
        return
    LOGGER.info(
        "Cleaned stale serving export GCS staging: deleted_objects=%d deleted_bytes=%d retention_days=%d",
        result.deleted_object_count,
        result.deleted_bytes,
        config.staging_retention_days,
    )


def _bigquery_job_id(*parts: str) -> str:
    raw_job_id = "_".join(parts)
    return re.sub(r"[^A-Za-z0-9_]", "_", raw_job_id).strip("_")[:1024]


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _duckdb_path_list(paths: Iterable[Path]) -> str:
    quoted_paths = ", ".join("'" + path.as_posix().replace("'", "''") + "'" for path in paths)
    return f"[{quoted_paths}]"


with DAG(
    dag_id="dag_serving_export",
    dag_display_name="Serving DuckDB export",
    description="Export mart tables to an atomically swapped DuckDB serving file after GPS warehouse completion.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=PartitionedAssetTimetable(assets=GPS_MODELS_DATE_ASSET),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "serving"],
) as dag:

    @task(retries=0, on_failure_callback=airflow_failure_alert)
    def export_serving_duckdb() -> dict[str, object]:
        """Publish the validated serving artifact."""
        result = _run_serving_export(_export_config(get_current_context()))
        return asdict(result)

    export_serving_duckdb_task = export_serving_duckdb()


if __name__ == "__main__":
    dag.test()
