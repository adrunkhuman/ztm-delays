from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


def test_weekly_audit_dag_runs_audit_tag() -> None:
    dag = _load_dag_module()

    assert dag.dag.kwargs["schedule"] == "0 7 * * 0"
    assert dag.dbt_test_weekly_audits.kwargs["task_id"] == "dbt_test_weekly_audits"
    assert "dbt test --select tag:audit" in dag.dbt_test_weekly_audits.kwargs["bash_command"]
    assert "--indirect-selection eager" in dag.dbt_test_weekly_audits.kwargs["bash_command"]
    assert "--exclude test_type:unit" in dag.dbt_test_weekly_audits.kwargs["bash_command"]
    assert "run_" not in dag.dbt_test_weekly_audits.kwargs["bash_command"]
    assert dag.dbt_test_weekly_audits in dag.selected_gtfs_snapshot.downstream


def test_selected_gtfs_snapshot_id_uses_processing_date_filter(monkeypatch: Any) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient([FakeRow(gtfs_snapshot_id="snapshot-1")])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    assert dag._selected_gtfs_snapshot_id("2026-07-08") == "snapshot-1"
    assert "where processing_date = @processing_date" in client.query_call
    assert client.job_config is not None
    assert client.job_config.query_parameters == [FakeScalarQueryParameter("processing_date", "DATE", "2026-07-08")]


def _load_dag_module() -> types.ModuleType:
    for module_name in list(sys.modules):
        if module_name.startswith(("airflow", "google")) or module_name == "ztm_airflow_common":
            sys.modules.pop(module_name, None)

    _install_airflow_stubs()
    _install_google_stubs()
    _load_common_module()

    module_path = Path(__file__).resolve().parents[1] / "dags" / "dag_weekly_audit.py"
    spec = importlib.util.spec_from_file_location("dag_weekly_audit", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["dag_weekly_audit"] = module
    spec.loader.exec_module(module)
    return module


def _load_common_module() -> None:
    module_path = Path(__file__).resolve().parents[1] / "dags" / "ztm_airflow_common.py"
    spec = importlib.util.spec_from_file_location("ztm_airflow_common", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["ztm_airflow_common"] = module
    spec.loader.exec_module(module)


def _install_airflow_stubs() -> None:
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    airflow_providers_module = types.ModuleType("airflow.providers")
    airflow_providers_standard_module = types.ModuleType("airflow.providers.standard")
    airflow_providers_standard_operators_module = types.ModuleType("airflow.providers.standard.operators")
    bash_module = types.ModuleType("airflow.providers.standard.operators.bash")

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.Asset = FakeAsset
    airflow_sdk_module.task = FakeTaskDecorator()
    bash_module.BashOperator = FakeOperator

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.providers"] = airflow_providers_module
    sys.modules["airflow.providers.standard"] = airflow_providers_standard_module
    sys.modules["airflow.providers.standard.operators"] = airflow_providers_standard_operators_module
    sys.modules["airflow.providers.standard.operators.bash"] = bash_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")

    bigquery_module.Client = lambda project: FakeBigQueryClient([])
    bigquery_module.QueryJobConfig = FakeQueryJobConfig
    bigquery_module.ScalarQueryParameter = FakeScalarQueryParameter
    google_cloud_module.bigquery = bigquery_module

    sys.modules["google"] = google_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module


class FakeDAG:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def test(self) -> None:
        return None


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


class FakeOperator:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.downstream: list[object] = []

    def __rrshift__(self, upstream: object) -> FakeOperator:
        if hasattr(upstream, "downstream"):
            cast("FakeTask", upstream).downstream.append(self)
        return self


class FakeAsset:
    def __init__(self, uri: str, *, name: str | None = None) -> None:
        self.uri = uri
        self.name = name


@dataclass(frozen=True)
class FakeRow:
    gtfs_snapshot_id: str


class FakeBigQueryClient:
    def __init__(self, rows: list[FakeRow]) -> None:
        self.rows = rows
        self.query_call = ""
        self.job_config: FakeQueryJobConfig | None = None

    def query(self, query: str, job_config: FakeQueryJobConfig | None = None) -> FakeQueryJob:
        self.query_call = query
        self.job_config = job_config
        return FakeQueryJob(self.rows)


class FakeQueryJob:
    def __init__(self, rows: list[FakeRow]) -> None:
        self.rows = rows

    def result(self) -> list[FakeRow]:
        return self.rows


@dataclass(frozen=True)
class FakeScalarQueryParameter:
    name: str
    type_: str
    value: str


@dataclass(frozen=True)
class FakeQueryJobConfig:
    query_parameters: list[FakeScalarQueryParameter]
