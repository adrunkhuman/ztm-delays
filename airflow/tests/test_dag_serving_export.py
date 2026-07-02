from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest


def test_mart_table_list_exports_all_current_marts() -> None:
    dag = _load_dag_module()

    assert set(dag.MART_TABLES) == {
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
    }


def test_export_config_uses_safe_defaults() -> None:
    dag = _load_dag_module()

    config = dag._export_config({"dag_run": FakeDagRun({})}, datetime(2026, 7, 2, 12, 30, tzinfo=UTC))

    assert config.export_id == "20260702T123000Z"
    assert config.output_dir == Path("/opt/airflow/serving")
    assert config.output_filename == "ztm.duckdb"
    assert config.gcs_bucket == "ztm-analytics-bucket"
    assert config.gcs_prefix == "serving/duckdb/staging"
    assert config.max_source_bytes == 20 * 1024 * 1024 * 1024
    assert config.max_duckdb_bytes == 20 * 1024 * 1024 * 1024
    assert config.cleanup_gcs_staging is False


def test_export_config_accepts_manual_overrides(tmp_path: Path) -> None:
    dag = _load_dag_module()

    config = dag._export_config(
        {
            "dag_run": FakeDagRun(
                {
                    "export_id": "manual-1",
                    "output_dir": str(tmp_path),
                    "output_filename": "alpha.duckdb",
                    "gcs_bucket": "bucket",
                    "gcs_prefix": "/custom/prefix/",
                    "max_source_bytes": 123,
                    "max_duckdb_bytes": "456",
                    "cleanup_gcs_staging": True,
                }
            )
        }
    )

    assert config.export_id == "manual-1"
    assert config.output_dir == tmp_path
    assert config.output_filename == "alpha.duckdb"
    assert config.gcs_bucket == "bucket"
    assert config.gcs_prefix == "custom/prefix"
    assert config.max_source_bytes == 123
    assert config.max_duckdb_bytes == 456
    assert config.cleanup_gcs_staging is True


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("export_id", "bad;id", ValueError),
        ("output_filename", "nested/ztm.duckdb", ValueError),
        ("max_source_bytes", 0, ValueError),
        ("cleanup_gcs_staging", "yes", TypeError),
    ],
)
def test_export_config_rejects_unsafe_manual_overrides(key: str, value: object, error: type[Exception]) -> None:
    dag = _load_dag_module()

    with pytest.raises(error):
        dag._export_config({"dag_run": FakeDagRun({key: value})})


def test_validate_source_stats_requires_every_mart() -> None:
    dag = _load_dag_module()
    stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=1) for table_name in dag.MART_TABLES[:-1]]

    with pytest.raises(RuntimeError, match="Missing mart tables"):
        dag._validate_source_stats(stats, 1000)


def test_validate_source_stats_enforces_size_guardrail() -> None:
    dag = _load_dag_module()
    stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=10) for table_name in dag.MART_TABLES]

    with pytest.raises(RuntimeError, match="exceeds configured limit"):
        dag._validate_source_stats(stats, 100)


def test_source_table_stats_merges_date_ranges() -> None:
    dag = _load_dag_module()
    client = FakeStatsBigQueryClient(dag)

    stats = dag._source_table_stats(client)

    by_table = {stat.table_name: stat for stat in stats}
    assert len(stats) == len(dag.MART_TABLES)
    assert by_table["fct_trip"].row_count == 10
    assert by_table["fct_trip"].size_bytes == 100
    assert by_table["fct_trip"].min_date == "2026-06-27"
    assert by_table["fct_trip"].max_date == "2026-07-02"
    assert by_table["fct_trip"].date_count == 6
    assert "where service_date >= date '1900-01-01'" in client.queries[1].lower()


@pytest.mark.parametrize(
    ("table_name", "date_column"), [("fct_stop_arrival", "service_date"), ("mart_day_completeness", "gps_date")]
)
def test_date_range_select_keeps_required_partition_filter(table_name: str, date_column: str) -> None:
    dag = _load_dag_module()

    query = dag._date_range_select(table_name, date_column).lower()

    assert f"from `ztm-data.ztm_marts.{table_name}`" in query
    assert f"where {date_column} >= date '1900-01-01'" in query


def test_extract_mart_to_gcs_uses_parquet_extract_contract() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    dag._extract_mart_to_gcs(client, config, "fct_trip")

    assert client.extract_call is not None
    assert client.extract_call.source_table == "ztm-data.ztm_marts.fct_trip"
    assert client.extract_call.destination_uri == "gs://bucket/prefix/export_id=export-1/fct_trip/part-*.parquet"
    assert client.extract_call.job_id == "serving_export_export_1_fct_trip"
    assert client.extract_call.location == dag.BIGQUERY_LOCATION
    assert client.extract_call.job_config.destination_format == dag.bigquery.DestinationFormat.PARQUET
    assert client.extract_call.job.result_called is True


def test_extract_mart_to_gcs_rejects_existing_job_after_conflict() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(raise_conflict=True)
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    with pytest.raises(RuntimeError, match="job already exists"):
        dag._extract_mart_to_gcs(client, config, "fct_trip")

    assert client.existing_job.result_called is False


def test_download_mart_parquet_downloads_only_parquet_files(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/export_id=export-1/fct_trip/part-000.parquet"),
            FakeBlob("prefix/export_id=export-1/fct_trip_extra/part-999.parquet"),
            FakeBlob("prefix/export_id=export-1/fct_trip/_SUCCESS"),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    paths = dag._download_mart_parquet(storage_client, config, "fct_trip", tmp_path)

    assert [path.name for path in paths] == ["part-000.parquet"]
    assert paths[0].read_text(encoding="utf-8") == "downloaded"
    assert storage_client.list_prefixes == ["prefix/export_id=export-1/fct_trip/"]
    assert storage_client.blob_names == ["prefix/export_id=export-1/fct_trip/part-000.parquet"]


def test_download_mart_parquet_resolves_listed_blobs_by_name(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [FakeBlob("prefix/export_id=export-1/fct_trip/part-000.parquet", fail_download=True)]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    paths = dag._download_mart_parquet(storage_client, config, "fct_trip", tmp_path)

    assert [path.name for path in paths] == ["part-000.parquet"]
    assert paths[0].read_text(encoding="utf-8") == "downloaded"
    assert storage_client.blob_names == ["prefix/export_id=export-1/fct_trip/part-000.parquet"]


def test_cleanup_gcs_staging_resolves_listed_blobs_by_name(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/export_id=export-1/fct_trip/part-000.parquet", fail_delete=True),
            FakeBlob("prefix/export_id=export-1/fct_trip/_SUCCESS", fail_delete=True),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    dag._cleanup_gcs_staging(storage_client, config)

    assert storage_client.list_prefixes == ["prefix/export_id=export-1/"]
    assert storage_client.blob_names == [
        "prefix/export_id=export-1/fct_trip/part-000.parquet",
        "prefix/export_id=export-1/fct_trip/_SUCCESS",
    ]
    assert storage_client.deleted_blob_names == [
        "prefix/export_id=export-1/fct_trip/part-000.parquet",
        "prefix/export_id=export-1/fct_trip/_SUCCESS",
    ]


def test_publish_duckdb_removes_temp_file_after_build_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )
    source_stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=1) for table_name in dag.MART_TABLES]
    parquet_paths_by_table = {table_name: [tmp_path / f"{table_name}.parquet"] for table_name in dag.MART_TABLES}
    monkeypatch.setattr(dag, "_duckdb_module", FailingDuckdbModule)

    with pytest.raises(RuntimeError, match="build failed"):
        dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []


def test_publish_duckdb_builds_queryable_file_with_metadata(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=1,
            size_bytes=10,
            min_date="2026-06-27" if table_name == "fct_trip" else None,
            max_date="2026-07-02" if table_name == "fct_trip" else None,
            date_count=6 if table_name == "fct_trip" else None,
        )
        for table_name in dag.MART_TABLES
    ]
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )

    result = dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert Path(result.duckdb_path).exists()
    assert Path(result.metadata_path).exists()
    with duckdb.connect(result.duckdb_path, read_only=True) as connection:
        assert connection.execute("select count(*) from fct_trip").fetchone()[0] == 1
        assert connection.execute("select export_id from export_metadata").fetchone()[0] == "export-1"
        assert connection.execute("select count(*) from export_table_stats").fetchone()[0] == len(dag.MART_TABLES)


def test_publish_duckdb_keeps_previous_file_when_validation_fails(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    final_path = tmp_path / "ztm.duckdb"
    metadata_path = tmp_path / "ztm.duckdb.meta.json"
    final_path.write_text("old duckdb", encoding="utf-8")
    metadata_path.write_text("old metadata", encoding="utf-8")
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [dag.TableStats(table_name=table_name, row_count=2, size_bytes=10) for table_name in dag.MART_TABLES]
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )

    with pytest.raises(RuntimeError, match="row count mismatch"):
        dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert final_path.read_text(encoding="utf-8") == "old duckdb"
    assert metadata_path.read_text(encoding="utf-8") == "old metadata"
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []


def test_dag_is_manual_and_exposes_single_export_task() -> None:
    dag = _load_dag_module()

    assert dag.dag.kwargs["schedule"] is None
    assert dag.dag.kwargs["max_active_runs"] == 1


def _load_dag_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()

    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    module_path = dag_dir / "dag_serving_export.py"
    module_name = "dag_serving_export_under_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load DAG module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _install_airflow_stubs() -> None:
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.task = FakeTaskDecorator()
    airflow_sdk_module.get_current_context = lambda: {"dag_run": FakeDagRun({})}
    airflow_sdk_module.Asset = FakeAsset

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_api_core_module = types.ModuleType("google.api_core")
    google_api_core_exceptions_module = types.ModuleType("google.api_core.exceptions")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    storage_module = types.ModuleType("google.cloud.storage")

    google_api_core_exceptions_module.Conflict = Conflict
    bigquery_module.Client = lambda project: FakeBigQueryClient()
    bigquery_module.ExtractJobConfig = FakeExtractJobConfig
    bigquery_module.DestinationFormat = types.SimpleNamespace(PARQUET="PARQUET")
    storage_module.Client = lambda project: FakeStorageClient([])
    google_cloud_module.bigquery = bigquery_module
    google_cloud_module.storage = storage_module

    sys.modules["google"] = google_module
    sys.modules["google.api_core"] = google_api_core_module
    sys.modules["google.api_core.exceptions"] = google_api_core_exceptions_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module
    sys.modules["google.cloud.storage"] = storage_module


class FakeTaskDecorator:
    def __call__(self, function: Any | None = None, **kwargs: Any) -> Any:
        if function is None:
            return lambda decorated: FakeTask(decorated, kwargs)
        return FakeTask(function, kwargs)


class FakeTask:
    def __init__(self, function: Any, kwargs: dict[str, Any] | None = None) -> None:
        self.function = function
        self.kwargs = kwargs or {}

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self


class FakeAsset:
    def __init__(self, uri: str, *, name: str | None = None) -> None:
        self.uri = uri
        self.name = name


class FakeDAG:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


@dataclass(frozen=True)
class FakeDagRun:
    conf: dict[str, object]


@dataclass(frozen=True)
class ExtractCall:
    source_table: str
    destination_uri: str
    job_config: Any
    job_id: str
    location: str
    job: FakeJob


class FakeBigQueryClient:
    def __init__(self, *, raise_conflict: bool = False) -> None:
        self.raise_conflict = raise_conflict
        self.extract_call: ExtractCall | None = None
        self.existing_job = FakeJob()
        self.get_job_call: tuple[str, str, str] | None = None

    def extract_table(
        self,
        source_table: str,
        destination_uri: str,
        *,
        job_config: Any,
        job_id: str,
        location: str,
    ) -> FakeJob:
        if self.raise_conflict:
            raise Conflict("job exists")
        job = FakeJob()
        self.extract_call = ExtractCall(source_table, destination_uri, job_config, job_id, location, job)
        return job

    def get_job(self, job_id: str, *, project: str, location: str) -> FakeJob:
        self.get_job_call = (job_id, project, location)
        return self.existing_job


class FakeStatsBigQueryClient:
    def __init__(self, dag: types.ModuleType) -> None:
        self.dag = dag
        self.queries: list[str] = []

    def query(self, query: str) -> FakeQueryJob:
        self.queries.append(query)
        if "__TABLES__" in query:
            return FakeQueryJob([FakeTableStatsRow(table_name, 10, 100) for table_name in self.dag.MART_TABLES])
        return FakeQueryJob(
            [
                FakeDateRangeRow(table_name, "2026-06-27", "2026-07-02", 6)
                for table_name in self.dag.DATE_RANGE_SQL_BY_TABLE
            ]
        )


class FakeQueryJob:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def result(self) -> list[object]:
        return self.rows


class FakeTableStatsRow:
    def __init__(self, table_id: str, row_count: int, size_bytes: int) -> None:
        self.table_id = table_id
        self.row_count = row_count
        self.size_bytes = size_bytes


class FakeDateRangeRow:
    def __init__(self, table_name: str, min_date: str, max_date: str, date_count: int) -> None:
        self.table_name = table_name
        self.min_date = min_date
        self.max_date = max_date
        self.date_count = date_count


class FakeExtractJobConfig:
    def __init__(self, *, destination_format: str) -> None:
        self.destination_format = destination_format


class FakeJob:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeStorageClient:
    def __init__(self, blobs: list[FakeBlob]) -> None:
        self.blobs = blobs
        self.list_prefixes: list[str] = []
        self.blob_names: list[str] = []
        self.deleted_blob_names: list[str] = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(bucket_name, self.blobs, self.list_prefixes, self.blob_names, self.deleted_blob_names)


class FakeBucket:
    def __init__(
        self,
        bucket_name: str,
        blobs: list[FakeBlob],
        list_prefixes: list[str],
        blob_names: list[str],
        deleted_blob_names: list[str],
    ) -> None:
        self.bucket_name = bucket_name
        self.blobs = blobs
        self.list_prefixes = list_prefixes
        self.blob_names = blob_names
        self.deleted_blob_names = deleted_blob_names

    def list_blobs(self, *, prefix: str) -> list[FakeBlob]:
        self.list_prefixes.append(prefix)
        return [blob for blob in self.blobs if blob.name.startswith(prefix)]

    def blob(self, blob_name: str) -> FakeBlob:
        if blob_name not in {blob.name for blob in self.blobs}:
            raise RuntimeError(f"unexpected blob lookup: {blob_name}")
        self.blob_names.append(blob_name)
        return FakeBlob(blob_name, deleted_blob_names=self.deleted_blob_names)


@dataclass
class FakeBlob:
    name: str
    fail_download: bool = False
    fail_delete: bool = False
    deleted_blob_names: list[str] | None = None

    def download_to_filename(self, filename: str) -> None:
        if self.fail_download:
            raise RuntimeError("stale listed blob was downloaded")
        Path(filename).write_text("downloaded", encoding="utf-8")

    def delete(self) -> None:
        if self.fail_delete:
            raise RuntimeError("stale listed blob was deleted")
        if self.deleted_blob_names is not None:
            self.deleted_blob_names.append(self.name)


class Conflict(Exception):
    pass


def _write_minimal_parquet_files(tmp_path: Path, table_names: tuple[str, ...], duckdb: Any) -> dict[str, list[Path]]:
    paths_by_table = {}
    with duckdb.connect() as connection:
        for table_name in table_names:
            table_dir = tmp_path / "parquet" / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            path = table_dir / "part-000.parquet"
            escaped_path = path.as_posix().replace("'", "''")
            connection.execute(
                f"copy (select 1 as id, ? as table_name) to '{escaped_path}' (format parquet)", [table_name]
            )
            paths_by_table[table_name] = [path]
    return paths_by_table


class FailingDuckdbModule:
    def connect(self, path: str, *, read_only: bool = False) -> FailingDuckdbConnection:
        return FailingDuckdbConnection(path, read_only)


class FailingDuckdbConnection:
    def __init__(self, path: str, read_only: bool) -> None:
        self.path = Path(path)
        self.read_only = read_only

    def __enter__(self) -> FailingDuckdbConnection:
        self.path.write_bytes(b"partial duckdb")
        raise RuntimeError("build failed")

    def __exit__(self, *_args: object) -> None:
        return None
