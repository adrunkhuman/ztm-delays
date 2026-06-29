from __future__ import annotations

import csv
import importlib.util
import sys
import types
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest


def test_parse_gcs_uri_splits_bucket_and_blob() -> None:
    dag = _load_dag_module()

    assert dag._parse_gcs_uri("gs://bucket/raw/gtfs/test.zip") == ("bucket", "raw/gtfs/test.zip")


def test_parse_gcs_uri_rejects_non_gcs_uri() -> None:
    dag = _load_dag_module()

    with pytest.raises(ValueError, match="Expected GCS URI"):
        dag._parse_gcs_uri("https://example.com/test.zip")


def test_extract_gtfs_table_adds_snapshot_id_and_ignores_extra_columns(tmp_path: Path) -> None:
    dag = _load_dag_module()
    zip_path = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr(
            "trips.txt",
            "trip_id,route_id,service_id,trip_headsign,direction_id,block_id,block_short_name,shape_id,extra\n"
            "trip-1,187,svc,Head,1,block,12,shape,ignored\n",
        )

    with zipfile.ZipFile(zip_path) as zip_file:
        output_path = dag._extract_gtfs_table(zip_file, dag.GTFS_TABLES[0], "snapshot-1", tmp_path)

    with output_path.open(newline="", encoding="utf-8") as output_file:
        rows = list(csv.DictReader(output_file))

    assert rows == [
        {
            "trip_id": "trip-1",
            "route_id": "187",
            "service_id": "svc",
            "trip_headsign": "Head",
            "direction_id": "1",
            "block_id": "block",
            "block_short_name": "12",
            "shape_id": "shape",
            "gtfs_snapshot_id": "snapshot-1",
        }
    ]


def test_extract_gtfs_table_normalizes_gtfs_dates_for_bigquery(tmp_path: Path) -> None:
    dag = _load_dag_module()
    zip_path = tmp_path / "gtfs.zip"
    calendar_dates_spec = dag.GTFS_TABLES[-1]
    with zipfile.ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr("calendar_dates.txt", "service_id,date,exception_type\nsvc,20260625,1\n")

    with zipfile.ZipFile(zip_path) as zip_file:
        output_path = dag._extract_gtfs_table(zip_file, calendar_dates_spec, "snapshot-1", tmp_path)

    with output_path.open(newline="", encoding="utf-8") as output_file:
        rows = list(csv.DictReader(output_file))

    assert rows == [
        {
            "service_id": "svc",
            "date": "2026-06-25",
            "exception_type": "1",
            "gtfs_snapshot_id": "snapshot-1",
        }
    ]


def test_extract_gtfs_table_rejects_missing_required_file(tmp_path: Path) -> None:
    dag = _load_dag_module()
    zip_path = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr("stops.txt", "stop_id,stop_name\n")

    with zipfile.ZipFile(zip_path) as zip_file, pytest.raises(RuntimeError, match=r"trips\.txt"):
        dag._extract_gtfs_table(zip_file, dag.GTFS_TABLES[0], "snapshot-1", tmp_path)


def test_load_csv_to_bigquery_uses_expected_load_contract(tmp_path: Path) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    csv_path = tmp_path / "trips.csv"
    csv_path.write_text("trip_id,gtfs_snapshot_id\ntrip-1,snapshot-1\n", encoding="utf-8")

    dag._load_csv_to_bigquery(client, csv_path, dag.GTFS_TABLES[0], "snapshot-1")

    assert client.load_call is not None
    assert client.load_call.destination == "ztm-data.ztm_raw.raw_gtfs_trips"
    assert client.load_call.job_id == "load_raw_gtfs_trips_snapshot_1"
    assert client.load_call.location == dag.BIGQUERY_LOCATION
    assert client.load_call.job_config.source_format == dag.bigquery.SourceFormat.CSV
    assert client.load_call.job_config.skip_leading_rows == 1
    assert client.load_call.job_config.create_disposition == dag.bigquery.CreateDisposition.CREATE_IF_NEEDED
    assert client.load_call.job_config.write_disposition == dag.bigquery.WriteDisposition.WRITE_APPEND
    assert client.load_call.job_config.clustering_fields == ["gtfs_snapshot_id"]
    assert [field.name for field in client.load_call.job_config.schema] == [
        field.name for field in dag.GTFS_TABLES[0].schema
    ]
    assert client.load_call.job.result_called is True


def test_load_csv_to_bigquery_waits_on_existing_job_after_conflict(tmp_path: Path) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(raise_conflict=True)
    csv_path = tmp_path / "trips.csv"
    csv_path.write_text("trip_id,gtfs_snapshot_id\ntrip-1,snapshot-1\n", encoding="utf-8")

    dag._load_csv_to_bigquery(client, csv_path, dag.GTFS_TABLES[0], "snapshot-1")

    assert client.get_job_call == ("load_raw_gtfs_trips_snapshot_1", "ztm-data", dag.BIGQUERY_LOCATION)
    assert client.existing_job.result_called is True


def test_latest_gtfs_snapshot_returns_latest_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    dag_run = FakeDagRun(
        {
            "snapshot_id": "snapshot-1",
            "gcs_path": "gs://bucket/raw/gtfs/test.zip",
            "processing_date": "2026-06-26",
        }
    )

    assert dag._selected_gtfs_snapshot(dag_run) == {
        "snapshot_id": "snapshot-1",
        "gcs_path": "gs://bucket/raw/gtfs/test.zip",
    }


def test_latest_gtfs_snapshot_rejects_missing_metadata_table(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()

    with pytest.raises(TypeError, match=r"dag_run\.conf"):
        dag._selected_gtfs_snapshot(object())


def test_latest_gtfs_snapshot_rejects_empty_metadata_table(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()

    with pytest.raises(RuntimeError, match="snapshot_id, gcs_path, and processing_date"):
        dag._selected_gtfs_snapshot(FakeDagRun({"snapshot_id": "snapshot-1"}))


def test_load_gtfs_snapshot_extracts_all_required_files_and_loads_all_raw_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _load_dag_module()
    zip_bytes = _complete_gtfs_zip()
    client = FakeBigQueryClient()
    storage_client = FakeStorageClient(zip_bytes)
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)
    monkeypatch.setattr(dag.storage, "Client", lambda project: storage_client)

    dag._load_gtfs_snapshot({"snapshot_id": "snapshot-1", "gcs_path": "gs://ztm-analytics-bucket/raw/gtfs/test.zip"})

    assert storage_client.bucket_name == "ztm-analytics-bucket"
    assert storage_client.blob_name == "raw/gtfs/test.zip"
    assert [load.destination for load in client.load_calls] == [spec.table for spec in dag.GTFS_TABLES]
    assert len(client.load_calls) == len(dag.GTFS_TABLES)
    assert all(load.job.result_called for load in client.load_calls)
    assert any("snapshot-1" in load.loaded_text for load in client.load_calls)


def test_dag_runs_tests_gtfs_staging_and_dimensions_after_raw_load() -> None:
    dag = _load_dag_module()

    assert dag.dbt_run_gtfs_staging.task_id == "dbt_run_gtfs_staging"
    assert dag.dbt_test_gtfs_staging.task_id == "dbt_test_gtfs_staging"
    assert dag.dbt_run_gtfs_dimensions.task_id == "dbt_run_gtfs_dimensions"
    assert dag.dbt_test_gtfs_dimensions.task_id == "dbt_test_gtfs_dimensions"
    expected_dimension_models = {
        "dim_line",
        "dim_stop_post",
        "dim_stop_group",
        "dim_date",
        "dim_schedule_date",
        "int_gtfs_trip_schedule",
        "int_schedule_version",
        "dim_schedule_version",
        "dim_line_current",
        "dim_stop_post_current",
        "dim_stop_group_current",
        "dim_schedule_date_current",
    }
    assert set(dag.GTFS_DIMENSION_MODELS.split()) == expected_dimension_models
    _assert_dbt_command(dag.dbt_run_gtfs_staging.bash_command, "run", dag.GTFS_STAGING_MODELS)
    _assert_dbt_command(dag.dbt_test_gtfs_staging.bash_command, "test", dag.GTFS_STAGING_MODELS)
    assert dag.GTFS_RAW_SOURCES in dag.dbt_test_gtfs_staging.bash_command
    _assert_dbt_command(dag.dbt_run_gtfs_dimensions.bash_command, "run", dag.GTFS_DIMENSION_MODELS)
    _assert_dbt_command(dag.dbt_test_gtfs_dimensions.bash_command, "test", dag.GTFS_DIMENSION_MODELS)
    for model_name in expected_dimension_models:
        assert model_name in dag.dbt_run_gtfs_dimensions.bash_command
        assert model_name in dag.dbt_test_gtfs_dimensions.bash_command
    assert dag.dbt_run_gtfs_staging in dag.loaded_gtfs_snapshot.downstream
    assert dag.dbt_test_gtfs_staging in dag.dbt_run_gtfs_staging.downstream
    assert dag.dbt_run_gtfs_dimensions in dag.dbt_test_gtfs_staging.downstream
    assert dag.dbt_test_gtfs_dimensions in dag.dbt_run_gtfs_dimensions.downstream


def _assert_dbt_command(command: str, dbt_subcommand: str, selector: str) -> None:
    assert f"dbt {dbt_subcommand}" in command
    assert "--select" in command
    assert selector in command
    assert "--vars" in command
    assert "processing_date" in command
    assert "gtfs_snapshot_id" in command


def _load_dag_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()

    module_path = Path(__file__).parents[1] / "dags" / "dag_gtfs_load.py"
    module_name = "dag_gtfs_load_under_test"
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
    airflow_decorators_module = types.ModuleType("airflow.decorators")
    airflow_operators_python_module = types.ModuleType("airflow.operators.python")
    airflow_providers_module = types.ModuleType("airflow.providers")
    airflow_providers_standard_module = types.ModuleType("airflow.providers.standard")
    airflow_providers_standard_operators_module = types.ModuleType("airflow.providers.standard.operators")
    airflow_providers_standard_bash_module = types.ModuleType("airflow.providers.standard.operators.bash")
    airflow_operators_module = types.ModuleType("airflow.operators")
    airflow_operators_bash_module = types.ModuleType("airflow.operators.bash")

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.task = FakeTaskDecorator()
    airflow_decorators_module.task = FakeTaskDecorator()
    airflow_providers_standard_bash_module.BashOperator = FakeBashOperator
    airflow_operators_bash_module.BashOperator = FakeBashOperator
    airflow_sdk_module.get_current_context = lambda: {"dag_run": FakeDagRun({})}
    airflow_operators_python_module.get_current_context = lambda: {"dag_run": FakeDagRun({})}

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.decorators"] = airflow_decorators_module
    sys.modules["airflow.providers"] = airflow_providers_module
    sys.modules["airflow.providers.standard"] = airflow_providers_standard_module
    sys.modules["airflow.providers.standard.operators"] = airflow_providers_standard_operators_module
    sys.modules["airflow.providers.standard.operators.bash"] = airflow_providers_standard_bash_module
    sys.modules["airflow.operators"] = airflow_operators_module
    sys.modules["airflow.operators.bash"] = airflow_operators_bash_module
    sys.modules["airflow.operators.python"] = airflow_operators_python_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_api_core_module = types.ModuleType("google.api_core")
    google_api_core_exceptions_module = types.ModuleType("google.api_core.exceptions")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    storage_module = types.ModuleType("google.cloud.storage")

    google_api_core_exceptions_module.Conflict = Conflict
    google_api_core_exceptions_module.NotFound = NotFound
    bigquery_module.Client = lambda project: FakeBigQueryClient()
    bigquery_module.SchemaField = FakeSchemaField
    bigquery_module.SourceFormat = types.SimpleNamespace(CSV="CSV")
    bigquery_module.CreateDisposition = types.SimpleNamespace(CREATE_IF_NEEDED="CREATE_IF_NEEDED")
    bigquery_module.WriteDisposition = types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND")
    bigquery_module.LoadJobConfig = FakeLoadJobConfig
    storage_module.Client = lambda project: FakeStorageClient()
    google_cloud_module.bigquery = bigquery_module
    google_cloud_module.storage = storage_module

    sys.modules["google"] = google_module
    sys.modules["google.api_core"] = google_api_core_module
    sys.modules["google.api_core.exceptions"] = google_api_core_exceptions_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module
    sys.modules["google.cloud.storage"] = storage_module


class FakeTaskDecorator:
    def __call__(self, function: Any) -> FakeTask:
        return FakeTask(function)


class FakeTask:
    def __init__(self, function: Any) -> None:
        self.function = function
        self.downstream: list[object] = []

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self

    def __rshift__(self, downstream: object) -> object:
        self.downstream.append(downstream)
        return downstream


class FakeBashOperator:
    def __init__(self, *, task_id: str, bash_command: str) -> None:
        self.task_id = task_id
        self.bash_command = bash_command
        self.downstream: list[object] = []

    def __rshift__(self, downstream: object) -> object:
        self.downstream.append(downstream)
        return downstream


class FakeDAG:
    def __init__(self, **_kwargs: object) -> None:
        return None

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeDagRun:
    def __init__(self, conf: dict[str, str]) -> None:
        self.conf = conf


class FakeSchemaField:
    def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode


class FakeLoadJobConfig:
    def __init__(self, **kwargs: Any) -> None:
        self.source_format = kwargs["source_format"]
        self.skip_leading_rows = kwargs["skip_leading_rows"]
        self.schema = kwargs["schema"]
        self.create_disposition = kwargs["create_disposition"]
        self.write_disposition = kwargs["write_disposition"]
        self.clustering_fields = kwargs.get("clustering_fields")


class FakeJob:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeBigQueryClient:
    def __init__(
        self,
        *,
        raise_conflict: bool = False,
        latest_snapshot: dict[str, str] | None = None,
        query_raises_not_found: bool = False,
    ) -> None:
        self.raise_conflict = raise_conflict
        self.latest_snapshot = latest_snapshot
        self.query_raises_not_found = query_raises_not_found
        self.load_call: LoadCall | None = None
        self.load_calls: list[LoadCall] = []
        self.existing_job = FakeJob()
        self.get_job_call: tuple[str, str, str] | None = None

    def load_table_from_file(
        self,
        file_obj: Any,
        destination: str,
        *,
        job_config: Any,
        job_id: str,
        location: str,
    ) -> FakeJob:
        if self.raise_conflict:
            raise Conflict("job already exists")
        job = FakeJob()
        loaded_text = file_obj.read().decode("utf-8")
        self.load_call = LoadCall(destination, job_config, job_id, location, job, loaded_text)
        self.load_calls.append(self.load_call)
        return job

    def get_job(self, job_id: str, *, project: str, location: str) -> FakeJob:
        self.get_job_call = (job_id, project, location)
        return self.existing_job

    def query(self, _query: str) -> FakeQueryJob:
        if self.query_raises_not_found:
            raise NotFound("missing table")
        return FakeQueryJob(self.latest_snapshot)


@dataclass
class LoadCall:
    destination: str
    job_config: Any
    job_id: str
    location: str
    job: FakeJob
    loaded_text: str


class FakeQueryJob:
    def __init__(self, latest_snapshot: dict[str, str] | None) -> None:
        self.latest_snapshot = latest_snapshot

    def result(self) -> list[FakeSnapshotRow]:
        if self.latest_snapshot is None:
            return []
        return [FakeSnapshotRow(self.latest_snapshot)]


class FakeSnapshotRow:
    def __init__(self, latest_snapshot: dict[str, str]) -> None:
        self.snapshot_id = latest_snapshot["snapshot_id"]
        self.gcs_path = latest_snapshot["gcs_path"]


class FakeStorageClient:
    def __init__(self, data: bytes = b"") -> None:
        self.data = data
        self.bucket_name = ""
        self.blob_name = ""

    def bucket(self, _bucket_name: str) -> FakeBucket:
        self.bucket_name = _bucket_name
        return FakeBucket(self)


class FakeBucket:
    def __init__(self, client: FakeStorageClient) -> None:
        self.client = client

    def blob(self, _blob_name: str) -> FakeBlob:
        self.client.blob_name = _blob_name
        return FakeBlob(self.client.data)


class FakeBlob:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def download_as_bytes(self) -> bytes:
        return self.data


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass


def _complete_gtfs_zip() -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr(
            "trips.txt",
            "trip_id,route_id,service_id,trip_headsign,direction_id,block_id,block_short_name,shape_id\n"
            "trip-1,187,svc,Head,1,block,12,shape\n",
        )
        zip_file.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\ntrip-1,12:00:00,12:00:30,stop-1,1\n",
        )
        zip_file.writestr("stops.txt", "stop_id,stop_name,stop_lat,stop_lon\nstop-1,Stop,52.1,21.1\n")
        zip_file.writestr("shapes.txt", "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\nshape,52.1,21.1,1\n")
        zip_file.writestr("routes.txt", "route_id,route_short_name,route_type\n187,187,3\n")
        zip_file.writestr("calendar_dates.txt", "service_id,date,exception_type\nsvc,20260625,1\n")
    return buffer.getvalue()
