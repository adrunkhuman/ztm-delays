from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.sdk import DAG, get_current_context, task
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    SERVING_EXPORT_DIR,
    SERVING_EXPORT_FILENAME,
    SERVING_EXPORT_GCS_PREFIX,
    SERVING_EXPORT_MAX_BYTES,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from types import ModuleType
    from typing import Protocol

    class DuckdbConnection(Protocol):
        """Minimal DuckDB connection protocol used by build-time settings."""

        def execute(self, query: str) -> object:
            """DuckDB-compatible execute; return value is ignored."""
            ...


EXPORT_VERSION = "alpha-1"
EXPORT_SOURCE_MODE = "current_pipeline_provisional"
EXPORT_ID_PATTERN = re.compile(r"^[0-9A-Za-z_.=-]+$")
DUCKDB_MEMORY_LIMIT = "1GB"
DUCKDB_TEMP_DIRECTORY_LIMIT = "2GB"
DUCKDB_THREADS = 2
MART_TABLES = (
    "agg_line_daily",
    "agg_line_stop_period",
    "agg_service_coverage",
    "agg_stop_period",
    "agg_time_period",
    "dim_date",
    "dim_line",
    "dim_line_current",
    "dim_schedule_date",
    "dim_schedule_date_current",
    "dim_schedule_version",
    "dim_stop_group",
    "dim_stop_group_current",
    "dim_stop_post",
    "dim_stop_post_current",
    "fct_stop_arrival",
    "fct_trip",
    "mart_day_completeness",
    "mart_pipeline_status",
)
DATE_RANGE_SQL_BY_TABLE = {
    "agg_line_daily": "service_date",
    "agg_service_coverage": "scheduled_start_date",
    "fct_stop_arrival": "service_date",
    "fct_trip": "service_date",
    "mart_day_completeness": "gps_date",
    "mart_pipeline_status": "service_date",
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

    return ExportConfig(
        export_id=export_id,
        output_dir=output_dir,
        output_filename=output_filename,
        gcs_bucket=_string_config(conf, "gcs_bucket", os.getenv("GCS_BUCKET", GCS_BUCKET)),
        gcs_prefix=_string_config(
            conf,
            "gcs_prefix",
            os.getenv("SERVING_EXPORT_GCS_PREFIX", SERVING_EXPORT_GCS_PREFIX),
        ).strip("/"),
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
        cleanup_gcs_staging=_bool_config(conf, "cleanup_gcs_staging", False),
    )


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
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return value


def _bool_config(conf: dict[str, object], key: str, default: bool) -> bool:
    value = conf.get(key, default)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a boolean")
    return value


def _run_serving_export(config: ExportConfig) -> ExportResult:
    bigquery_client = bigquery.Client(project=GCP_PROJECT)
    storage_client = storage.Client(project=GCP_PROJECT)
    exported_at = datetime.now(UTC)
    source_stats = _source_table_stats(bigquery_client)
    _validate_source_stats(source_stats, config.max_source_bytes)

    with tempfile.TemporaryDirectory(prefix="ztm-serving-export-") as temp_dir:
        local_export_dir = Path(temp_dir)
        parquet_paths_by_table = _extract_and_download_marts(bigquery_client, storage_client, config, local_export_dir)
        result = _publish_duckdb(config, parquet_paths_by_table, source_stats, exported_at)

    if config.cleanup_gcs_staging:
        _cleanup_gcs_staging(storage_client, config)

    return result


def _source_table_stats(client: bigquery.Client) -> list[TableStats]:
    rows = list(client.query(_table_stats_sql()).result())
    stats_by_table = {
        row.table_id: TableStats(
            table_name=row.table_id,
            row_count=int(row.row_count),
            size_bytes=int(row.size_bytes),
        )
        for row in rows
    }

    dated_stats = {stat.table_name: stat for stat in _source_date_ranges(client)}
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


def _table_stats_sql() -> str:
    quoted_tables = ", ".join(f"'{table_name}'" for table_name in MART_TABLES)
    return f"""
        select table_id, row_count, size_bytes
        from `{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.__TABLES__`
        where table_id in ({quoted_tables})
    """


def _source_date_ranges(client: bigquery.Client) -> list[TableStats]:
    selects = [
        _date_range_select(table_name, date_column)
        for table_name, date_column in DATE_RANGE_SQL_BY_TABLE.items()
        if table_name in MART_TABLES
    ]
    if not selects:
        return []

    rows = list(client.query(" union all ".join(selects)).result())
    return [
        TableStats(
            table_name=row.table_name,
            row_count=0,
            size_bytes=0,
            min_date=str(row.min_date) if row.min_date is not None else None,
            max_date=str(row.max_date) if row.max_date is not None else None,
            date_count=int(row.date_count) if row.date_count is not None else None,
        )
        for row in rows
    ]


def _date_range_select(table_name: str, date_column: str) -> str:
    return f"""
        select
            '{table_name}' as table_name,
            min({date_column}) as min_date,
            max({date_column}) as max_date,
            count(distinct {date_column}) as date_count
        from `{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.{table_name}`
        where {date_column} >= date '1900-01-01'
    """


def _validate_source_stats(stats: Sequence[TableStats], max_source_bytes: int) -> None:
    found_tables = {stat.table_name for stat in stats}
    missing_tables = sorted(set(MART_TABLES) - found_tables)
    if missing_tables:
        raise RuntimeError(f"Missing mart tables for serving export: {', '.join(missing_tables)}")

    empty_required_tables = sorted(
        stat.table_name
        for stat in stats
        if stat.table_name
        in {"agg_line_stop_period", "agg_stop_period", "fct_stop_arrival", "fct_trip", "mart_pipeline_status"}
        and stat.row_count == 0
    )
    if empty_required_tables:
        raise RuntimeError(f"Required serving tables are empty: {', '.join(empty_required_tables)}")

    source_size_bytes = sum(stat.size_bytes for stat in stats)
    if source_size_bytes > max_source_bytes:
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
        _extract_mart_to_gcs(bigquery_client, config, table_name)
        parquet_paths_by_table[table_name] = _download_mart_parquet(
            storage_client, config, table_name, local_export_dir
        )
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


def _table_extract_uri(config: ExportConfig, table_name: str) -> str:
    return f"gs://{config.gcs_bucket}/{_table_staging_prefix(config, table_name)}/part-*.parquet"


def _table_staging_prefix(config: ExportConfig, table_name: str) -> str:
    return f"{config.gcs_prefix}/export_id={config.export_id}/{table_name}"


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


def _publish_duckdb(
    config: ExportConfig,
    parquet_paths_by_table: dict[str, list[Path]],
    source_stats: Sequence[TableStats],
    exported_at: datetime,
) -> ExportResult:
    duckdb_module = _duckdb_module()
    config.output_dir.mkdir(parents=True, exist_ok=True)
    final_path = config.output_dir / config.output_filename
    temp_path = config.output_dir / f".{config.output_filename}.{config.export_id}.tmp"
    duckdb_temp_dir = config.output_dir / f".duckdb-tmp-{config.export_id}"
    metadata_path = config.output_dir / f"{config.output_filename}.meta.json"
    if temp_path.exists():
        temp_path.unlink()
    _remove_duckdb_sidecar_files(temp_path)
    if duckdb_temp_dir.exists():
        shutil.rmtree(duckdb_temp_dir)
    duckdb_temp_dir.mkdir(parents=True)

    try:
        build_input = DuckdbBuildInput(
            parquet_paths_by_table=parquet_paths_by_table,
            source_stats=source_stats,
            config=config,
            exported_at=exported_at,
            temp_directory=duckdb_temp_dir,
        )
        _build_duckdb_file(duckdb_module, temp_path, build_input)
        duckdb_size_bytes = temp_path.stat().st_size
        _enforce_duckdb_size(duckdb_size_bytes, config.max_duckdb_bytes)

        _update_duckdb_file_size(duckdb_module, temp_path, duckdb_size_bytes)
        duckdb_size_bytes = temp_path.stat().st_size
        metadata = _export_metadata(config, source_stats, exported_at, duckdb_size_bytes)
        _validate_duckdb_export(duckdb_module, temp_path, source_stats)
    except Exception:
        temp_path.unlink(missing_ok=True)
        _remove_duckdb_sidecar_files(temp_path)
        shutil.rmtree(duckdb_temp_dir, ignore_errors=True)
        raise
    shutil.rmtree(duckdb_temp_dir, ignore_errors=True)

    try:
        temp_path.replace(final_path)
        _write_metadata_file(metadata_path, metadata)
    except Exception:
        temp_path.unlink(missing_ok=True)
        _remove_duckdb_sidecar_files(temp_path)
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
    # Serving SQL must order explicitly; preserving import order is wasted memory here.
    connection.execute("set preserve_insertion_order = false")


def _update_duckdb_file_size(duckdb_module: ModuleType, path: Path, duckdb_size_bytes: int) -> None:
    with duckdb_module.connect(str(path)) as connection:
        connection.execute("update export_metadata set duckdb_file_size_bytes = ?", [duckdb_size_bytes])


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
            "agg_line_stop_period",
            "agg_stop_period",
            "fct_stop_arrival",
            "fct_trip",
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


def _export_metadata(
    config: ExportConfig,
    source_stats: Sequence[TableStats],
    exported_at: datetime,
    duckdb_size_bytes: int,
) -> dict[str, object]:
    return {
        "export_id": config.export_id,
        "export_version": EXPORT_VERSION,
        "source_mode": EXPORT_SOURCE_MODE,
        "exported_at": exported_at.isoformat(),
        "source_project": GCP_PROJECT,
        "source_dataset": BIGQUERY_MARTS_DATASET,
        "duckdb_path": str(config.output_dir / config.output_filename),
        "duckdb_file_size_bytes": duckdb_size_bytes,
        "source_size_bytes": sum(stat.size_bytes for stat in source_stats),
        "source_row_count": sum(stat.row_count for stat in source_stats),
        "exported_table_count": len(MART_TABLES),
        "tables": [asdict(stat) for stat in source_stats],
    }


def _write_metadata_file(path: Path, metadata: dict[str, object]) -> None:
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def _remove_duckdb_sidecar_files(path: Path) -> None:
    path.with_name(f"{path.name}.wal").unlink(missing_ok=True)


def _cleanup_gcs_staging(storage_client: storage.Client, config: ExportConfig) -> None:
    bucket = storage_client.bucket(config.gcs_bucket)
    prefix = f"{config.gcs_prefix}/export_id={config.export_id}/"
    for blob_name in [blob.name for blob in bucket.list_blobs(prefix=prefix)]:
        bucket.blob(blob_name).delete()


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
    description="Manually export all mart tables to an atomically swapped DuckDB serving file.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ztm", "serving", "manual"],
) as dag:

    @task
    def export_serving_duckdb() -> dict[str, object]:
        """Airflow task entrypoint for the manual alpha export."""
        result = _run_serving_export(_export_config(get_current_context()))
        return asdict(result)

    export_serving_duckdb_task = export_serving_duckdb()


if __name__ == "__main__":
    dag.test()
