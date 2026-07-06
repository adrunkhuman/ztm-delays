from __future__ import annotations

import importlib.util
import sys
import types
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest


def test_gtfs_hash_and_paths_are_stable() -> None:
    dag = _load_dag_module()

    assert dag._sha256(b"abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert dag.gtfs_snapshot_id("2026-06-25T14:00:00Z", "abcdef1234567890") == "2026-06-25T14:00:00Z_abcdef123456"
    assert dag.gtfs_gcs_path("2026-06-25T14:00:00Z_abcdef123456") == "raw/gtfs/2026-06-25T14:00:00Z_abcdef123456.zip"
    assert (
        dag.gtfs_gcs_uri("2026-06-25T14:00:00Z_abcdef123456")
        == "gs://ztm-analytics-bucket/raw/gtfs/2026-06-25T14:00:00Z_abcdef123456.zip"
    )


def test_poll_gtfs_snapshot_skips_unchanged_snapshot(monkeypatch: Any) -> None:
    dag = _load_dag_module()
    zip_bytes = _zip_bytes()
    file_hash = dag._sha256(zip_bytes)
    client = FakeBigQueryClient(latest_hash=file_hash)

    monkeypatch.setattr(dag, "_download_gtfs_zip", lambda: zip_bytes)
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)
    monkeypatch.setattr(dag, "_upload_gtfs_zip", _fail_if_called)
    monkeypatch.setattr(dag, "_insert_gtfs_snapshot", _fail_if_called)

    assert dag._poll_gtfs_snapshot("2026-06-25T14:00:00Z") == {"status": "unchanged"}
    assert client.created_table is not None


def test_poll_gtfs_snapshot_uploads_changed_snapshot(monkeypatch: Any) -> None:
    dag = _load_dag_module()
    zip_bytes = _zip_bytes()
    client = FakeBigQueryClient(latest_hash="old-hash")
    storage_client = FakeStorageClient()

    monkeypatch.setattr(dag, "_download_gtfs_zip", lambda: zip_bytes)
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)
    monkeypatch.setattr(dag.storage, "Client", lambda project: storage_client)

    poll_result = dag._poll_gtfs_snapshot("2026-06-25T14:00:00Z")

    assert poll_result["status"] == "uploaded"
    assert client.created_table is not None
    assert poll_result["snapshot_id"] == "2026-06-25T14:00:00Z_" + dag._sha256(zip_bytes)[:12]
    expected_path = "raw/gtfs/2026-06-25T14:00:00Z_" + dag._sha256(zip_bytes)[:12] + ".zip"
    assert poll_result["gcs_path"] == "gs://ztm-analytics-bucket/" + expected_path
    assert poll_result["file_hash"] == dag._sha256(zip_bytes)
    assert "processing_date" in poll_result
    assert storage_client.bucket_obj.uploads == [(expected_path, zip_bytes, "application/zip")]
    assert any(call[2].startswith("merge_raw_gtfs_snapshots_") for call in client.query_calls)


def test_gtfs_load_branch_routes_only_changed_snapshots() -> None:
    dag = _load_dag_module()

    assert dag._gtfs_load_branch({"status": "uploaded"}) == "emit_gtfs_snapshot_asset"
    assert dag._gtfs_load_branch({"status": "unchanged"}) == "skip_gtfs_load"

    with pytest.raises(ValueError, match="Unexpected GTFS poll result"):
        dag._gtfs_load_branch({"status": "bad-result"})


def test_gtfs_staging_processing_date_uses_next_warsaw_local_date() -> None:
    dag = _load_dag_module()

    assert dag._gtfs_staging_processing_date("2026-06-25T20:00:00Z") == "2026-06-26"
    assert dag._gtfs_staging_processing_date("2026-06-25T23:30:00Z") == "2026-06-27"


def test_dag_emits_gtfs_snapshot_asset_on_changed_snapshot() -> None:
    dag = _load_dag_module()

    assert dag.dag.kwargs["default_args"] == dag.AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS
    assert dag.dag.kwargs["on_failure_callback"] is dag.airflow_failure_alert
    assert dag.emit_gtfs_snapshot_asset.kwargs == {"outlets": [dag.GTFS_SNAPSHOT_ASSET]}
    assert dag.skip_gtfs_load.task_id == "skip_gtfs_load"


def test_gtfs_snapshot_asset_extra_contains_loader_context() -> None:
    dag = _load_dag_module()
    poll_result = {
        "status": "uploaded",
        "snapshot_id": "2026-06-25T14:00:00Z_abcdef123456",
        "gcs_path": "gs://ztm-analytics-bucket/raw/gtfs/2026-06-25T14:00:00Z_abcdef123456.zip",
        "file_hash": "abcdef1234567890",
        "processing_date": "2026-06-26",
    }

    assert dag._gtfs_snapshot_asset_extra(poll_result) == {
        "snapshot_id": "2026-06-25T14:00:00Z_abcdef123456",
        "gcs_path": "gs://ztm-analytics-bucket/raw/gtfs/2026-06-25T14:00:00Z_abcdef123456.zip",
        "file_hash": "abcdef1234567890",
        "processing_date": "2026-06-26",
    }


def test_insert_gtfs_snapshot_uses_expected_metadata_row() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()

    dag._insert_gtfs_snapshot(client, "2026-06-25T14:00:00Z", "abcdef1234567890", "gs://bucket/raw/gtfs/test.zip")

    assert len(client.query_calls) == 1
    query, job_config, job_id, location = client.query_calls[0]
    assert "merge `ztm-data.ztm_raw.raw_gtfs_snapshots`" in query
    assert job_id == "merge_raw_gtfs_snapshots_2026_06_25T14_00_00Z_abcdef123456"
    assert location == dag.BIGQUERY_LOCATION
    assert [(param.name, param.type_, param.value) for param in job_config.query_parameters] == [
        ("snapshot_id", "STRING", "2026-06-25T14:00:00Z_abcdef123456"),
        ("snapshot_timestamp", "STRING", "2026-06-25T14:00:00Z"),
        ("file_hash", "STRING", "abcdef1234567890"),
        ("gcs_path", "STRING", "gs://bucket/raw/gtfs/test.zip"),
    ]


def test_insert_gtfs_snapshot_waits_on_existing_merge_job_after_conflict() -> None:
    dag = _load_dag_module()
    job_id = "merge_raw_gtfs_snapshots_2026_06_25T14_00_00Z_abcdef123456"
    client = FakeBigQueryClient(query_conflict_job_ids={job_id})

    dag._insert_gtfs_snapshot(client, "2026-06-25T14:00:00Z", "abcdef1234567890", "gs://bucket/raw/gtfs/test.zip")

    assert client.get_job_call == (job_id, "ztm-data", dag.BIGQUERY_LOCATION)
    assert client.existing_job.result_called is True


def test_download_gtfs_zip_rejects_non_zip_response(monkeypatch: Any) -> None:
    dag = _load_dag_module()
    response = FakeResponse(b"not a zip")
    monkeypatch.setattr(dag.requests, "get", lambda url, timeout: response)

    with pytest.raises(RuntimeError, match="valid ZIP"):
        dag._download_gtfs_zip()

    assert response.raise_for_status_called is True


def _load_dag_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()
    _install_requests_stub()

    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    module_path = dag_dir / "dag_gtfs_poll.py"
    module_name = "dag_gtfs_poll_under_test"
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
    airflow_providers_module = types.ModuleType("airflow.providers")
    airflow_providers_standard_module = types.ModuleType("airflow.providers.standard")
    airflow_providers_standard_operators_module = types.ModuleType("airflow.providers.standard.operators")
    airflow_providers_standard_empty_module = types.ModuleType("airflow.providers.standard.operators.empty")
    airflow_providers_standard_trigger_module = types.ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    airflow_operators_module = types.ModuleType("airflow.operators")
    airflow_operators_empty_module = types.ModuleType("airflow.operators.empty")
    airflow_operators_trigger_module = types.ModuleType("airflow.operators.trigger_dagrun")

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.task = FakeTaskDecorator()
    airflow_sdk_module.Asset = FakeAsset
    airflow_sdk_module.Metadata = FakeMetadata
    airflow_decorators_module.task = FakeTaskDecorator()
    airflow_providers_standard_empty_module.EmptyOperator = FakeEmptyOperator
    airflow_providers_standard_trigger_module.TriggerDagRunOperator = FakeTriggerDagRunOperator
    airflow_operators_empty_module.EmptyOperator = FakeEmptyOperator
    airflow_operators_trigger_module.TriggerDagRunOperator = FakeTriggerDagRunOperator

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.decorators"] = airflow_decorators_module
    sys.modules["airflow.providers"] = airflow_providers_module
    sys.modules["airflow.providers.standard"] = airflow_providers_standard_module
    sys.modules["airflow.providers.standard.operators"] = airflow_providers_standard_operators_module
    sys.modules["airflow.providers.standard.operators.empty"] = airflow_providers_standard_empty_module
    sys.modules["airflow.providers.standard.operators.trigger_dagrun"] = airflow_providers_standard_trigger_module
    sys.modules["airflow.operators"] = airflow_operators_module
    sys.modules["airflow.operators.empty"] = airflow_operators_empty_module
    sys.modules["airflow.operators.trigger_dagrun"] = airflow_operators_trigger_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_api_core_module = types.ModuleType("google.api_core")
    google_api_core_exceptions_module = types.ModuleType("google.api_core.exceptions")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    storage_module = types.ModuleType("google.cloud.storage")

    google_api_core_exceptions_module.Conflict = Conflict
    google_api_core_exceptions_module.NotFound = NotFound
    google_api_core_exceptions_module.PreconditionFailed = PreconditionFailed
    bigquery_module.Client = lambda project: FakeBigQueryClient()
    bigquery_module.Table = FakeTable
    bigquery_module.SchemaField = FakeSchemaField
    bigquery_module.QueryJobConfig = FakeQueryJobConfig
    bigquery_module.ScalarQueryParameter = FakeScalarQueryParameter
    storage_module.Client = lambda project: FakeStorageClient()
    google_cloud_module.bigquery = bigquery_module
    google_cloud_module.storage = storage_module

    sys.modules["google"] = google_module
    sys.modules["google.api_core"] = google_api_core_module
    sys.modules["google.api_core.exceptions"] = google_api_core_exceptions_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module
    sys.modules["google.cloud.storage"] = storage_module


def _install_requests_stub() -> None:
    requests_module = types.ModuleType("requests")
    requests_module.get = _fail_if_called
    sys.modules["requests"] = requests_module


class FakeTaskDecorator:
    def __init__(self) -> None:
        self.branch = self

    def __call__(self, function: Any | None = None, **kwargs: Any) -> Any:
        if function is None:
            return lambda decorated: FakeTask(decorated, kwargs)
        return FakeTask(function, kwargs)


class FakeAsset:
    def __init__(self, uri: str, *, name: str | None = None) -> None:
        self.uri = uri
        self.name = name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeAsset) and self.uri == other.uri

    def __hash__(self) -> int:
        return hash(self.uri)


class FakeMetadata:
    def __init__(self, asset: FakeAsset, extra: dict[str, str]) -> None:
        self.asset = asset
        self.extra = extra


class FakeTask:
    def __init__(self, function: Any, kwargs: dict[str, Any] | None = None) -> None:
        self.function = function
        self.kwargs = kwargs or {}
        self.downstream: list[object] = []

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self

    def __rshift__(self, downstream: object) -> object:
        if isinstance(downstream, list):
            self.downstream.extend(downstream)
        else:
            self.downstream.append(downstream)
        return downstream

    def __rrshift__(self, upstream: list[object]) -> FakeTask:
        for task in upstream:
            if hasattr(task, "downstream"):
                task.downstream.append(self)
        return self


class FakeEmptyOperator:
    def __init__(self, *, task_id: str) -> None:
        self.task_id = task_id


class FakeTriggerDagRunOperator:
    def __init__(self, *, task_id: str, trigger_dag_id: str, conf: dict[str, str], wait_for_completion: bool) -> None:
        self.task_id = task_id
        self.trigger_dag_id = trigger_dag_id
        self.conf = conf
        self.wait_for_completion = wait_for_completion


class FakeDAG:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeSchemaField:
    def __init__(self, name: str, field_type: str, *, mode: str) -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode


class FakeTable:
    def __init__(self, table_id: str, *, schema: list[FakeSchemaField]) -> None:
        self.table_id = table_id
        self.schema = schema


class FakeBigQueryClient:
    def __init__(self, *, latest_hash: str | None = None, query_conflict_job_ids: set[str] | None = None) -> None:
        self.latest_hash = latest_hash
        self.query_conflict_job_ids = query_conflict_job_ids or set()
        self.created_table: FakeTable | None = None
        self.insert_table: str | None = None
        self.inserted_rows: list[dict[str, str]] = []
        self.query_calls: list[tuple[str, Any, str, str]] = []
        self.existing_job = FakeQueryJob(self.latest_hash)
        self.get_job_call: tuple[str, str, str] | None = None

    def create_table(self, table: FakeTable, *, exists_ok: bool) -> None:
        assert exists_ok is True
        self.created_table = table

    def query(
        self, query: str, *, job_config: Any | None = None, job_id: str | None = None, location: str | None = None
    ) -> FakeQueryJob:
        if job_id in self.query_conflict_job_ids:
            raise Conflict("job already exists")
        self.query_calls.append((query, job_config, job_id or "", location or ""))
        return FakeQueryJob(self.latest_hash)

    def insert_rows_json(self, table: str, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        self.insert_table = table
        self.inserted_rows = rows
        return []

    def get_job(self, job_id: str, *, project: str, location: str) -> FakeQueryJob:
        self.get_job_call = (job_id, project, location)
        return self.existing_job


class FakeQueryJob:
    def __init__(self, latest_hash: str | None) -> None:
        self.latest_hash = latest_hash
        self.result_called = False

    def result(self) -> list[FakeRow]:
        self.result_called = True
        if self.latest_hash is None:
            return []
        return [FakeRow(self.latest_hash)]


class FakeQueryJobConfig:
    def __init__(self, *, query_parameters: list[FakeScalarQueryParameter]) -> None:
        self.query_parameters = query_parameters


class FakeScalarQueryParameter:
    def __init__(self, name: str, type_: str, value: str) -> None:
        self.name = name
        self.type_ = type_
        self.value = value


class FakeRow:
    def __init__(self, file_hash: str) -> None:
        self.file_hash = file_hash


class FakeStorageClient:
    def __init__(self) -> None:
        self.bucket_obj = FakeBucket()

    def bucket(self, bucket_name: str) -> FakeBucket:
        assert bucket_name == "ztm-analytics-bucket"
        return self.bucket_obj


class FakeBucket:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes, str]] = []

    def blob(self, path: str) -> FakeBlob:
        return FakeBlob(path, self.uploads)


class FakeBlob:
    def __init__(self, path: str, uploads: list[tuple[str, bytes, str]]) -> None:
        self.path = path
        self.uploads = uploads

    def upload_from_string(self, data: bytes, *, content_type: str, if_generation_match: int | None = None) -> None:
        self.uploads.append((self.path, data, content_type))


class NotFound(Exception):
    pass


class PreconditionFailed(Exception):
    pass


class Conflict(Exception):
    pass


def _fail_if_called(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("function should not be called")


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.raise_for_status_called = False

    def raise_for_status(self) -> None:
        self.raise_for_status_called = True


def _zip_bytes() -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr("trips.txt", "trip_id,route_id\n")
    return buffer.getvalue()
