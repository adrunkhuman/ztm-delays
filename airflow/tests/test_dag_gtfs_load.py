from __future__ import annotations

import csv
import importlib.util
import sys
import types
import zipfile
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
    assert client.load_call.destination == "ztm-data.ztm_bq.raw_gtfs_trips"
    assert client.load_call.job_id == "load_raw_gtfs_trips_snapshot_1"
    assert client.load_call.job_config.source_format == dag.bigquery.SourceFormat.CSV
    assert client.load_call.job_config.skip_leading_rows == 1
    assert client.load_call.job_config.write_disposition == dag.bigquery.WriteDisposition.WRITE_APPEND
    assert client.load_call.job.result_called is True


def test_load_csv_to_bigquery_waits_on_existing_job_after_conflict(tmp_path: Path) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(raise_conflict=True)
    csv_path = tmp_path / "trips.csv"
    csv_path.write_text("trip_id,gtfs_snapshot_id\ntrip-1,snapshot-1\n", encoding="utf-8")

    dag._load_csv_to_bigquery(client, csv_path, dag.GTFS_TABLES[0], "snapshot-1")

    assert client.get_job_call == ("load_raw_gtfs_trips_snapshot_1", "ztm-data")
    assert client.existing_job.result_called is True


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

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.task = FakeTaskDecorator()
    airflow_decorators_module.task = FakeTaskDecorator()

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.decorators"] = airflow_decorators_module


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

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self


class FakeDAG:
    def __init__(self, **_kwargs: object) -> None:
        return None

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeSchemaField:
    def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode


class FakeLoadJobConfig:
    def __init__(
        self,
        *,
        source_format: str,
        skip_leading_rows: int,
        schema: list[FakeSchemaField],
        create_disposition: str,
        write_disposition: str,
    ) -> None:
        self.source_format = source_format
        self.skip_leading_rows = skip_leading_rows
        self.schema = schema
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition


class FakeJob:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeBigQueryClient:
    def __init__(self, *, raise_conflict: bool = False) -> None:
        self.raise_conflict = raise_conflict
        self.load_call: LoadCall | None = None
        self.existing_job = FakeJob()
        self.get_job_call: tuple[str, str] | None = None

    def load_table_from_file(self, file_obj: Any, destination: str, *, job_config: Any, job_id: str) -> FakeJob:
        if self.raise_conflict:
            raise Conflict("job already exists")
        job = FakeJob()
        self.load_call = LoadCall(destination, job_config, job_id, job)
        file_obj.read()
        return job

    def get_job(self, job_id: str, *, project: str) -> FakeJob:
        self.get_job_call = (job_id, project)
        return self.existing_job


class LoadCall:
    def __init__(self, destination: str, job_config: Any, job_id: str, job: FakeJob) -> None:
        self.destination = destination
        self.job_config = job_config
        self.job_id = job_id
        self.job = job


class FakeStorageClient:
    def bucket(self, _bucket_name: str) -> FakeBucket:
        return FakeBucket()


class FakeBucket:
    def blob(self, _blob_name: str) -> FakeBlob:
        return FakeBlob()


class FakeBlob:
    def download_as_bytes(self) -> bytes:
        return b""


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass
