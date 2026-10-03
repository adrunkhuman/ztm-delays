from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"


def test_dag_runs_on_the_second_after_the_month_is_final() -> None:
    dag = _load_dag_module()

    assert dag.dag.kwargs["schedule"].cron == "0 12 2 * *"
    assert dag.dag.kwargs["catchup"] is False
    assert dag.dag.kwargs["max_active_runs"] == 1
    # A late nightly run delays the readiness check for up to 12 hours instead of skipping the month.
    assert TASK_KWARGS["check_month_final"]["retries"] * TASK_KWARGS["check_month_final"]["retry_delay"].seconds == 12 * 3600


def test_selected_month_prefers_conf_then_previous_warsaw_month(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()

    def context(conf: dict[str, str], logical_date: datetime | None) -> None:
        run = types.SimpleNamespace(conf=conf)
        monkeypatch.setattr(dag, "get_current_context", lambda: {"dag_run": run, "logical_date": logical_date})

    context({"month": "2026-07"}, datetime(2026, 10, 2, 10, tzinfo=UTC))
    assert dag._selected_month() == "2026-07"
    # 23:30 UTC on 31 October is already 1 November in Warsaw.
    context({}, datetime(2026, 10, 31, 23, 30, tzinfo=UTC))
    assert dag._selected_month() == "2026-10"
    context({"month": "July"}, None)
    with pytest.raises(ValueError, match="YYYY-MM"):
        dag._selected_month()


def test_month_check_requires_expected_facts_and_next_days_completed_nightly_run() -> None:
    dag = _load_dag_module()
    facts = [
        FakeRow(table_name="fct_expected_stop_event", partition_id=f"202607{day:02d}")
        for day in range(1, 32)
        if day not in {5, 6, 7}
    ]
    # The 1st's facts alone are not enough: the prior-day republish runs in parallel and may not be done.
    next_day_facts = FakeRow(table_name="fct_expected_stop_event", partition_id="20260801")
    client = FakeClient([[*facts, next_day_facts]])

    with pytest.raises(RuntimeError, match=r"\['mart_pipeline_status 2026-08-01'\]"):
        dag._check_month_published(client, "2026-07")
    partition_ids = client.calls[0].job_config.query_parameters[0].values
    assert "20260705" not in partition_ids
    assert partition_ids[-1] == "20260801"

    client = FakeClient([[*facts, FakeRow(table_name="mart_pipeline_status", partition_id="20260801")]])
    dag._check_month_published(client, "2026-07")


def test_extract_rows_prune_shapes_to_the_month_snapshots() -> None:
    dag = _load_dag_module()
    client = FakeClient(
        [
            [],
            [FakeRow(gtfs_snapshot_id="s2"), FakeRow(gtfs_snapshot_id="s1")],
            [FakeRow(kind="segment", payload="{}"), FakeRow(kind="coverage", payload="{}")],
        ]
    )

    assert list(dag._extract_rows(client, "2026-02")) == [("segment", "{}"), ("coverage", "{}")]
    statistics, snapshots, pooled = client.calls
    assert "`ztm-data.ztm_marts.fct_expected_stop_event`" in statistics.query
    assert [parameter.value for parameter in statistics.job_config.query_parameters] == ["2026-02-01", "2026-02-28"]
    assert "from `p.d.anon`" in snapshots.query
    assert "`p.d.anon`" in pooled.query
    assert "`ztm-data.ztm_raw.raw_gtfs_shapes`" in pooled.query
    assert pooled.job_config.query_parameters[0].values == ["s1", "s2"]
    assert all(call.job_config.maximum_bytes_billed for call in client.calls)


def _load_dag_module() -> types.ModuleType:
    for module_name in list(sys.modules):
        if module_name.startswith(("airflow", "google")) or module_name in {"ztm_airflow_common", "dag_monthly_route_map"}:
            sys.modules.pop(module_name, None)
    _install_stubs()
    if str(DAGS) not in sys.path:
        sys.path.insert(0, str(DAGS))
    spec = importlib.util.spec_from_file_location("dag_monthly_route_map", DAGS / "dag_monthly_route_map.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["dag_monthly_route_map"] = module
    spec.loader.exec_module(module)
    return module


def _install_stubs() -> None:
    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG = FakeDAG
    sdk.Asset = lambda uri, **_kwargs: uri
    sdk.CronTriggerTimetable = FakeCronTriggerTimetable
    sdk.get_current_context = dict
    sdk.task = fake_task
    bigquery = types.ModuleType("google.cloud.bigquery")
    bigquery.Client = lambda project: FakeClient([])
    bigquery.QueryJobConfig = FakeQueryJobConfig
    bigquery.ScalarQueryParameter = FakeScalarQueryParameter
    bigquery.ArrayQueryParameter = FakeArrayQueryParameter
    cloud = types.ModuleType("google.cloud")
    cloud.bigquery = bigquery
    sys.modules.update(
        {
            "airflow": types.ModuleType("airflow"),
            "airflow.sdk": sdk,
            "google": types.ModuleType("google"),
            "google.cloud": cloud,
            "google.cloud.bigquery": bigquery,
        }
    )


def fake_task(function: Any = None, **kwargs: Any) -> Any:
    """Record retry settings; declaring the DAG calls the tasks, so they must not run."""
    if function is None:
        return lambda decorated: fake_task(decorated, **kwargs)
    TASK_KWARGS[function.__name__] = kwargs
    return lambda *_args: None


TASK_KWARGS: dict[str, dict[str, Any]] = {}


class FakeDAG:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


@dataclass(frozen=True)
class FakeCronTriggerTimetable:
    cron: str
    timezone: object


@dataclass(frozen=True)
class FakeScalarQueryParameter:
    name: str
    type_: str
    value: object

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", str(self.value))


@dataclass(frozen=True)
class FakeArrayQueryParameter:
    name: str
    array_type: str
    values: list[str]


@dataclass(frozen=True)
class FakeQueryJobConfig:
    query_parameters: list[Any] = field(default_factory=list)
    maximum_bytes_billed: int | None = None


class FakeRow(types.SimpleNamespace):
    pass


@dataclass
class FakeResult:
    rows: list[FakeRow]

    @property
    def total_rows(self) -> int:
        return len(self.rows)

    def __iter__(self) -> Any:
        return iter(self.rows)


@dataclass
class FakeJob:
    query: str
    job_config: FakeQueryJobConfig
    rows: list[FakeRow]
    destination: Any = field(default_factory=lambda: types.SimpleNamespace(project="p", dataset_id="d", table_id="anon"))
    total_bytes_billed: int = 1

    def result(self) -> FakeResult:
        return FakeResult(self.rows)


@dataclass
class FakeClient:
    results: list[list[FakeRow]]
    calls: list[FakeJob] = field(default_factory=list)

    def query(self, query: str, job_config: FakeQueryJobConfig) -> FakeJob:
        job = FakeJob(query, job_config, self.results[len(self.calls)])
        self.calls.append(job)
        return job
