from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


def test_available_gps_part_uris_lists_existing_bus_and_tram_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    blobs = [
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/not-a-part.txt",
        "raw/gps/vehicle_type=tram/date=2026-06-25/hour=08/part-b.parquet",
    ]
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))

    uris = dag._available_gps_part_uris("2026-06-25")

    assert uris == [
        "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=2026-06-25/hour=08/part-b.parquet",
    ]


def test_load_raw_gps_pings_returns_when_no_parts_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient([]))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert client.load_calls == []


def test_load_raw_gps_pings_uses_expected_bigquery_load_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    blobs = [
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "raw/gps/vehicle_type=tram/date=2026-06-25/hour=07/part-b.parquet",
    ]
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert len(client.load_calls) == 2
    assert (
        client.load_calls[0].uri
        == "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"
    )
    assert client.load_calls[0].destination == "ztm-data.ztm_raw.raw_gps_pings"
    assert client.load_calls[0].job_id == dag._load_job_id(client.load_calls[0].uri)
    assert client.load_calls[0].location == dag.BIGQUERY_LOCATION
    assert client.load_calls[0].job_config.source_format == dag.bigquery.SourceFormat.PARQUET
    assert client.load_calls[0].job_config.create_disposition == dag.bigquery.CreateDisposition.CREATE_IF_NEEDED
    assert client.load_calls[0].job_config.write_disposition == dag.bigquery.WriteDisposition.WRITE_APPEND
    assert client.load_calls[0].job_config.time_partitioning.field == "Time"
    assert client.load_calls[0].job_config.time_partitioning.require_partition_filter is True
    assert client.load_calls[0].job_config.clustering_fields == ["Lines"]
    assert all(load_call.job.result_called for load_call in client.load_calls)


def test_load_raw_gps_pings_waits_on_existing_job_after_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    blobs = ["raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"]
    uri = "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"
    client = FakeBigQueryClient(conflict_job_ids={dag._load_job_id(uri)})
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert client.get_job_call == (dag._load_job_id(uri), "ztm-data", dag.BIGQUERY_LOCATION)
    assert client.existing_job.result_called is True


def test_load_raw_gps_pings_continues_after_one_existing_job(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    existing_uri = "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"
    new_uri = "gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=2026-06-25/hour=07/part-b.parquet"
    client = FakeBigQueryClient(conflict_job_ids={dag._load_job_id(existing_uri)})
    blobs = [
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "raw/gps/vehicle_type=tram/date=2026-06-25/hour=07/part-b.parquet",
    ]
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert client.get_job_calls == [(dag._load_job_id(existing_uri), "ztm-data", dag.BIGQUERY_LOCATION)]
    assert [load_call.uri for load_call in client.load_calls] == [new_uri]
    assert client.load_calls[0].job.result_called is True
    assert client.existing_job.result_called is True


def test_selected_gtfs_snapshot_id_returns_latest_snapshot_before_processing_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(snapshot_rows=[FakeRow(snapshot_id="snapshot-1")])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    assert dag._selected_gtfs_snapshot_id("2026-06-27") == "snapshot-1"

    assert client.query_call is not None
    assert "raw_gtfs_snapshots" in client.query_call.query
    assert "date(snapshot_timestamp, 'Europe/Warsaw') < date(@processing_date)" in client.query_call.query
    assert client.query_call.job_config.query_parameters == [
        dag.bigquery.ScalarQueryParameter("processing_date", "DATE", "2026-06-27")
    ]


def test_selected_gtfs_snapshot_id_rejects_missing_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(snapshot_rows=[])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    with pytest.raises(dag.AirflowException, match="No GTFS snapshot"):
        dag._selected_gtfs_snapshot_id("2026-06-27")


def test_dag_runs_stop_arrivals_after_trip_matching() -> None:
    dag = _load_dag_module()

    assert dag.selected_gtfs_snapshot_id.kwargs == {
        "task_id": "selected_gtfs_snapshot_id",
        "python_callable": dag._selected_gtfs_snapshot_id,
        "op_kwargs": {"processing_date": dag.PROCESSING_DATE},
    }
    assert dag.dbt_run_int_ping_trip.kwargs == {
        "task_id": "dbt_run_int_ping_trip",
        "bash_command": (
            f"cd {dag.DBT_PROJECT_DIR} && "
            f"dbt run --select {dag.GTFS_TRIP_MATCHING_STAGING_MODELS} int_ping_trip "
            f"--vars '{dag.GPS_TRIP_DBT_VARS}'"
        ),
    }
    assert dag.dbt_test_int_ping_trip.kwargs == {
        "task_id": "dbt_test_int_ping_trip",
        "bash_command": (
            f"cd {dag.DBT_PROJECT_DIR} && dbt test --select int_ping_trip --vars '{dag.GPS_TRIP_DBT_VARS}'"
        ),
    }
    assert dag.dbt_run_int_gps_hourly_completeness.kwargs == {
        "task_id": "dbt_run_int_gps_hourly_completeness",
        "bash_command": (
            f"cd {dag.DBT_PROJECT_DIR} && dbt run --select {dag.GPS_COMPLETENESS_MODEL} --vars '{dag.GPS_DBT_VARS}'"
        ),
    }
    assert dag.dbt_test_int_gps_hourly_completeness.kwargs == {
        "task_id": "dbt_test_int_gps_hourly_completeness",
        "bash_command": (
            f"cd {dag.DBT_PROJECT_DIR} && dbt test --select {dag.GPS_COMPLETENESS_MODEL} --vars '{dag.GPS_DBT_VARS}'"
        ),
    }
    assert dag.dbt_run_int_stop_arrivals.kwargs == {
        "task_id": "dbt_run_int_stop_arrivals",
        "bash_command": (
            f"cd {dag.DBT_PROJECT_DIR} && "
            f"dbt run --select {dag.GTFS_STOP_ARRIVAL_STAGING_MODELS} int_stop_arrivals "
            f"--vars '{dag.GPS_TRIP_DBT_VARS}'"
        ),
    }
    assert dag.dbt_test_int_stop_arrivals.kwargs == {
        "task_id": "dbt_test_int_stop_arrivals",
        "bash_command": (
            f"cd {dag.DBT_PROJECT_DIR} && dbt test --select int_stop_arrivals --vars '{dag.GPS_TRIP_DBT_VARS}'"
        ),
    }
    assert dag.load_raw_gps_pings.downstream == [dag.dbt_run_stg_gps_pings]
    assert dag.dbt_run_stg_gps_pings.downstream == [
        dag.dbt_run_int_ping_trip,
        dag.dbt_run_int_gps_hourly_completeness,
        dag.dbt_test_stg_gps_pings,
    ]
    assert dag.selected_gtfs_snapshot_id.downstream == [dag.dbt_run_int_ping_trip]
    assert dag.dbt_run_int_ping_trip.downstream == [dag.dbt_test_int_ping_trip]
    assert dag.dbt_test_int_ping_trip.downstream == [dag.dbt_run_int_stop_arrivals]
    assert dag.dbt_run_int_gps_hourly_completeness.downstream == [dag.dbt_test_int_gps_hourly_completeness]
    assert dag.dbt_run_int_stop_arrivals.downstream == [dag.dbt_test_int_stop_arrivals]


@dataclass
class FakeBlob:
    name: str


class FakeBucket:
    def __init__(self, blob_names: list[str]) -> None:
        self.blob_names = blob_names

    def list_blobs(self, *, prefix: str) -> list[FakeBlob]:
        return [FakeBlob(blob_name) for blob_name in self.blob_names if blob_name.startswith(prefix)]


class FakeStorageClient:
    def __init__(self, blob_names: list[str]) -> None:
        self.blob_names = blob_names

    def bucket(self, bucket_name: str) -> FakeBucket:
        assert bucket_name == "ztm-analytics-bucket"
        return FakeBucket(self.blob_names)


@dataclass
class LoadCall:
    uri: str
    destination: str
    job_config: Any
    job_id: str
    location: str
    job: FakeJob


@dataclass(frozen=True)
class QueryCall:
    query: str
    job_config: Any


@dataclass(frozen=True)
class FakeRow:
    snapshot_id: str


class FakeJob:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeQueryJob:
    def __init__(self, rows: list[FakeRow]) -> None:
        self.rows = rows

    def result(self) -> list[FakeRow]:
        return self.rows


class FakeBigQueryClient:
    def __init__(
        self,
        *,
        conflict_job_ids: set[str] | None = None,
        snapshot_rows: list[FakeRow] | None = None,
    ) -> None:
        self.conflict_job_ids = conflict_job_ids or set()
        self.snapshot_rows = snapshot_rows or []
        self.load_calls: list[LoadCall] = []
        self.existing_job = FakeJob()
        self.get_job_call: tuple[str, str, str] | None = None
        self.get_job_calls: list[tuple[str, str, str]] = []
        self.query_call: QueryCall | None = None

    def load_table_from_uri(
        self,
        uri: str,
        destination: str,
        *,
        job_config: Any,
        job_id: str,
        location: str,
    ) -> FakeJob:
        if job_id in self.conflict_job_ids:
            raise Conflict("job already exists")
        job = FakeJob()
        self.load_calls.append(LoadCall(uri, destination, job_config, job_id, location, job))
        return job

    def get_job(self, job_id: str, *, project: str, location: str) -> FakeJob:
        self.get_job_call = (job_id, project, location)
        self.get_job_calls.append((job_id, project, location))
        return self.existing_job

    def query(self, query: str, *, job_config: Any) -> FakeQueryJob:
        self.query_call = QueryCall(query, job_config)
        return FakeQueryJob(self.snapshot_rows)


def _load_dag_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()

    module_path = Path(__file__).parents[1] / "dags" / "dag_daily_gps.py"
    module_name = "dag_daily_gps_under_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load DAG module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _install_airflow_stubs() -> None:
    airflow_module = types.ModuleType("airflow")
    airflow_exceptions_module = types.ModuleType("airflow.exceptions")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    bash_module = types.ModuleType("airflow.providers.standard.operators.bash")
    python_module = types.ModuleType("airflow.providers.standard.operators.python")

    airflow_exceptions_module.AirflowException = type("AirflowException", (Exception,), {})
    airflow_sdk_module.DAG = FakeDAG
    bash_module.BashOperator = FakeOperator
    python_module.PythonOperator = FakeOperator

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.exceptions"] = airflow_exceptions_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.providers"] = types.ModuleType("airflow.providers")
    sys.modules["airflow.providers.standard"] = types.ModuleType("airflow.providers.standard")
    sys.modules["airflow.providers.standard.operators"] = types.ModuleType("airflow.providers.standard.operators")
    sys.modules["airflow.providers.standard.operators.bash"] = bash_module
    sys.modules["airflow.providers.standard.operators.python"] = python_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_api_core_module = types.ModuleType("google.api_core")
    google_api_core_exceptions_module = types.ModuleType("google.api_core.exceptions")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    storage_module = types.ModuleType("google.cloud.storage")

    google_api_core_exceptions_module.Conflict = Conflict
    bigquery_module.Client = lambda project: FakeBigQueryClient()
    bigquery_module.SourceFormat = types.SimpleNamespace(PARQUET="PARQUET")
    bigquery_module.CreateDisposition = types.SimpleNamespace(CREATE_IF_NEEDED="CREATE_IF_NEEDED")
    bigquery_module.WriteDisposition = types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND")
    bigquery_module.TimePartitioningType = types.SimpleNamespace(DAY="DAY")
    bigquery_module.TimePartitioning = FakeTimePartitioning
    bigquery_module.LoadJobConfig = FakeLoadJobConfig
    bigquery_module.QueryJobConfig = FakeQueryJobConfig
    bigquery_module.ScalarQueryParameter = FakeScalarQueryParameter
    storage_module.Client = lambda project: FakeStorageClient([])
    google_cloud_module.bigquery = bigquery_module
    google_cloud_module.storage = storage_module

    sys.modules["google"] = google_module
    sys.modules["google.api_core"] = google_api_core_module
    sys.modules["google.api_core.exceptions"] = google_api_core_exceptions_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module
    sys.modules["google.cloud.storage"] = storage_module


class Conflict(Exception):
    pass


class FakeDAG:
    def __init__(self, **_kwargs: Any) -> None:
        return None

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeOperator:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.downstream: list[FakeOperator] = []

    def __rshift__(self, _other: FakeOperator) -> FakeOperator:
        self.downstream.append(_other)
        return _other


class FakeTimePartitioning:
    def __init__(self, *, type_: str, field: str, require_partition_filter: bool = False) -> None:
        self.type_ = type_
        self.field = field
        self.require_partition_filter = require_partition_filter


class FakeLoadJobConfig:
    def __init__(
        self,
        *,
        source_format: str,
        create_disposition: str,
        write_disposition: str,
        time_partitioning: FakeTimePartitioning,
        clustering_fields: list[str],
    ) -> None:
        self.source_format = source_format
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.clustering_fields = clustering_fields


@dataclass(frozen=True)
class FakeScalarQueryParameter:
    name: str
    type_: str
    value: str


class FakeQueryJobConfig:
    def __init__(self, *, query_parameters: list[FakeScalarQueryParameter]) -> None:
        self.query_parameters = query_parameters
