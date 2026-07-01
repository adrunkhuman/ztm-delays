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
    client = FakeBigQueryClient(snapshot_rows=[FakeRow(gtfs_snapshot_id="snapshot-1")])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    assert dag._selected_gtfs_snapshot_id("2026-06-27") == "snapshot-1"

    assert client.query_call is not None
    assert "dim_schedule_date" in client.query_call.query
    assert "service_date = date(@processing_date)" in client.query_call.query
    assert client.query_call.job_config.query_parameters == [
        dag.bigquery.ScalarQueryParameter("processing_date", "DATE", "2026-06-27")
    ]


def test_selected_gtfs_snapshot_id_rejects_missing_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(snapshot_rows=[])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    with pytest.raises(dag.AirflowException, match="No built GTFS schedule dimension"):
        dag._selected_gtfs_snapshot_id("2026-06-27")


def test_dag_runs_trip_fact_after_stop_arrivals() -> None:
    dag = _load_dag_module()

    assert isinstance(dag.raw_gps_dag.kwargs["schedule"], FakeCronPartitionTimetable)
    assert dag.raw_gps_dag.kwargs["schedule"].cron == "20 * * * *"
    assert dag.raw_gps_dag.kwargs["schedule"].timezone == "Europe/Warsaw"
    assert "dag_run.partition_key" in dag.RAW_GPS_PROCESSING_DATE
    assert "data_interval_start" not in dag.RAW_GPS_PROCESSING_DATE
    assert dag.load_raw_gps_pings.kwargs == {"outlets": [dag.RAW_GPS_DATE_ASSET]}
    assert isinstance(dag.dag.kwargs["schedule"], FakePartitionedAssetTimetable)
    assert dag.selected_gtfs_snapshot_id.kwargs == {}
    assert dag.dbt_run_fct_trip_current.kwargs["bash_command"].startswith("cd /opt/airflow/dbt && dbt run")
    assert dag.TRIP_MATCHING_SCHEDULE_MODELS in dag.dbt_run_int_ping_trip.kwargs["bash_command"]
    assert dag.TRIP_MATCHING_SCHEDULE_MODELS in dag.dbt_run_int_trip_summary.kwargs["bash_command"]
    assert "--exclude test_type:unit" in dag.dbt_test_fct_stop_arrival_current.kwargs["bash_command"]
    assert '"publish_service_date": "' + dag.PROCESSING_DATE in dag.dbt_run_fct_trip_current.kwargs["bash_command"]
    assert '"publish_service_date": "' + dag.PRIOR_SERVICE_DATE in dag.dbt_run_fct_trip_prior.kwargs["bash_command"]
    assert dag.AGGREGATE_MODELS in dag.dbt_run_aggregate_marts.kwargs["bash_command"]
    assert dag.PIPELINE_STATUS_MODEL in dag.dbt_run_pipeline_status.kwargs["bash_command"]
    assert dag.emit_gps_models_date_asset.kwargs == {"outlets": [dag.GPS_MODELS_DATE_ASSET]}

    assert dag.dbt_test_stg_gps_pings in dag.dbt_run_stg_gps_pings.downstream
    assert dag.dbt_run_int_ping_trip in dag.selected_gtfs_snapshot.downstream
    assert dag.dbt_run_int_ping_trip in dag.dbt_test_stg_gps_pings.downstream
    assert dag.dbt_run_int_gps_hourly_completeness in dag.dbt_test_stg_gps_pings.downstream
    assert dag.dbt_test_int_ping_trip in dag.dbt_run_int_ping_trip.downstream
    assert dag.dbt_run_int_stop_arrivals in dag.dbt_test_int_ping_trip.downstream
    assert dag.dbt_test_int_gps_hourly_completeness in dag.dbt_run_int_gps_hourly_completeness.downstream
    assert dag.dbt_test_int_stop_arrivals in dag.dbt_run_int_stop_arrivals.downstream
    assert dag.dbt_run_int_trip_summary in dag.dbt_test_int_stop_arrivals.downstream
    assert dag.dbt_test_int_trip_summary in dag.dbt_run_int_trip_summary.downstream
    assert dag.dbt_run_fct_trip_current in dag.dbt_test_int_trip_summary.downstream
    assert dag.dbt_run_fct_trip_prior in dag.dbt_test_int_trip_summary.downstream
    assert dag.dbt_run_fct_stop_arrival_current in dag.dbt_test_fct_trip_current.downstream
    assert dag.dbt_run_fct_stop_arrival_prior in dag.dbt_test_fct_trip_prior.downstream
    assert dag.dbt_run_completeness_and_coverage in dag.dbt_test_fct_stop_arrival_current.downstream
    assert dag.dbt_run_completeness_and_coverage in dag.dbt_test_fct_stop_arrival_prior.downstream
    assert dag.dbt_run_completeness_and_coverage in dag.dbt_test_int_gps_hourly_completeness.downstream
    assert dag.dbt_run_pipeline_status in dag.dbt_test_aggregate_marts.downstream
    assert dag.watcher in dag.dbt_test_pipeline_status.downstream


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
    gtfs_snapshot_id: str


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

    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    module_path = dag_dir / "dag_daily_gps.py"
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

    airflow_exceptions_module.AirflowException = type("AirflowException", (Exception,), {})
    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.Asset = FakeAsset
    airflow_sdk_module.CronPartitionTimetable = FakeCronPartitionTimetable
    airflow_sdk_module.Metadata = FakeMetadata
    airflow_sdk_module.PartitionedAssetTimetable = FakePartitionedAssetTimetable
    airflow_sdk_module.StartOfDayMapper = FakeStartOfDayMapper
    airflow_sdk_module.TriggerRule = types.SimpleNamespace(ONE_FAILED="one_failed")
    airflow_sdk_module.task = FakeTaskDecorator()
    bash_module.BashOperator = FakeOperator

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.exceptions"] = airflow_exceptions_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.providers"] = types.ModuleType("airflow.providers")
    sys.modules["airflow.providers.standard"] = types.ModuleType("airflow.providers.standard")
    sys.modules["airflow.providers.standard.operators"] = types.ModuleType("airflow.providers.standard.operators")
    sys.modules["airflow.providers.standard.operators.bash"] = bash_module


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
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

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

    def __rrshift__(self, upstream: list[object]) -> FakeOperator:
        for task in upstream:
            if hasattr(task, "downstream"):
                task.downstream.append(self)
        return self


class FakeTaskDecorator:
    def __call__(self, function: Any | None = None, **kwargs: Any) -> Any:
        if function is None:
            return lambda decorated: FakeTask(decorated, kwargs)
        return FakeTask(function, kwargs)


class FakeTask:
    def __init__(self, function: Any, kwargs: dict[str, Any] | None = None) -> None:
        self.function = function
        self.kwargs = kwargs or {}
        self.downstream: list[object] = []

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self

    def __rshift__(self, downstream: object) -> object:
        self.downstream.append(downstream)
        return downstream

    def __rrshift__(self, upstream: list[object]) -> FakeTask:
        for task in upstream:
            if hasattr(task, "downstream"):
                task.downstream.append(self)
        return self


class FakeAsset:
    def __init__(self, uri: str, *, name: str | None = None) -> None:
        self.uri = uri
        self.name = name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeAsset) and self.uri == other.uri

    def __hash__(self) -> int:
        return hash(self.uri)


class FakeCronPartitionTimetable:
    def __init__(self, cron: str, *, timezone: str) -> None:
        self.cron = cron
        self.timezone = timezone


class FakeStartOfDayMapper:
    pass


class FakePartitionedAssetTimetable:
    def __init__(self, *, assets: FakeAsset, default_partition_mapper: FakeStartOfDayMapper) -> None:
        self.assets = assets
        self.default_partition_mapper = default_partition_mapper


class FakeMetadata:
    def __init__(self, asset: FakeAsset, extra: dict[str, Any]) -> None:
        self.asset = asset
        self.extra = extra


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
