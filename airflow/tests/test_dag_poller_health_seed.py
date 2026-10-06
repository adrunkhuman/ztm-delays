from __future__ import annotations

import importlib.util
import re
import sys
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from . import test_dag_poller_health as helpers
from .test_dag_poller_health import FakeBucket
from .test_poller_health import HOUR, START, health, summary

dag_module = helpers.dag_module
COLLECTION = HOUR + timedelta(minutes=34, seconds=56)


class FakeSeedClient:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query(self, query: str, **kwargs: Any) -> Any:
        self.queries.append((query, kwargs))
        return types.SimpleNamespace(result=lambda: self.rows)


@pytest.fixture
def seed_module(dag_module: Any, monkeypatch: Any) -> types.ModuleType:
    monkeypatch.setitem(sys.modules, "dag_poller_health", dag_module)
    spec = importlib.util.spec_from_file_location(
        "dag_poller_health_seed_under_test", Path(__file__).parents[1] / "dags/dag_poller_health_seed.py"
    )
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def gps_rows() -> list[dict[str, Any]]:
    before = HOUR - timedelta(hours=1)
    return [
        {"mode": "bus", "minute": before + timedelta(minutes=minute), "fleet": 100 + minute} for minute in range(60)
    ] + [
        {"mode": "tram", "minute": before + timedelta(minutes=59), "fleet": 40},
        {"mode": "bus", "minute": before - timedelta(hours=1), "fleet": 90},
    ]


def test_seed_window_is_whole_hours_strictly_before_collection(seed_module: Any) -> None:
    start, end = seed_module.seed_window(COLLECTION, 28)
    assert end == HOUR
    assert start == HOUR - timedelta(days=28)
    assert seed_module.seed_window(COLLECTION.astimezone(health.WARSAW), 1) == (HOUR - timedelta(days=1), HOUR)
    assert seed_module.seed_window(HOUR, 1)[1] == HOUR
    for days in (0, 29, -1):
        with pytest.raises(ValueError, match=r"days must be 1\.\.28"):
            seed_module.seed_window(COLLECTION, days)


def test_run_seed_queries_the_window_and_writes_hourly_seeds(seed_module: Any, dag_module: Any) -> None:
    bucket = FakeBucket()
    bucket.put(sys.modules["poller_health_gcs"].HEARTBEAT_PATH, {"collection_started_at": health.iso(COLLECTION)})
    client = FakeSeedClient(gps_rows())
    assert seed_module.run_seed(bucket, client, 7) == 2
    query, kwargs = client.queries[0]
    assert "`ztm-data.ztm_raw.raw_gps_pings`" in query
    assert kwargs["location"] == "europe-north1"
    params = {param.name: (param.type_, param.value) for param in kwargs["job_config"].query_parameters}
    assert params == {"start": ("TIMESTAMP", HOUR - timedelta(days=7)), "end": ("TIMESTAMP", HOUR)}
    before = HOUR - timedelta(hours=1)
    paths = {health.hour_path("baseline-seed", before), health.hour_path("baseline-seed", before - timedelta(hours=1))}
    assert set(bucket.uploads) == paths
    written = bucket.get(health.hour_path("baseline-seed", before))
    health.validate_seed(written, before)
    assert written["vehicle_types"]["bus"]["minutes"][59] == {"minute": 59, "fresh_vehicles": 159}
    assert written["vehicle_types"]["tram"]["minutes"] == [{"minute": 59, "fresh_vehicles": 40}]
    # Seeds are what the monitor reads where no summary exists.
    _, sample = dag_module.read_sample(bucket, before)
    assert sample == written
    # Rerunning overwrites the same objects with the same content.
    snapshot = dict(bucket.objects)
    seed_module.run_seed(bucket, FakeSeedClient(gps_rows()), 7)
    assert {path: data for path, (data, _) in bucket.objects.items()} == {
        path: data for path, (data, _) in snapshot.items()
    }


def test_run_seed_falls_back_to_first_summary_and_requires_collection(seed_module: Any) -> None:
    bucket = FakeBucket()
    client = FakeSeedClient([])
    with pytest.raises(ValueError, match="collection start unknown"):
        seed_module.run_seed(bucket, client, 28)
    assert client.queries == []
    bucket.put(health.hour_path("hourly", HOUR), summary())
    assert seed_module.run_seed(bucket, client, 28) == 0
    params = {param.name: param.value for param in client.queries[0][1]["job_config"].query_parameters}
    assert params["end"] == health.timestamp(START)
    assert bucket.uploads == []


def test_write_seeds_rejects_oversized_seed(seed_module: Any, monkeypatch: Any) -> None:
    monkeypatch.setattr(seed_module, "SEED_MAX_BYTES", 10)
    bucket = FakeBucket()
    with pytest.raises(ValueError, match="seed exceeds bound"):
        seed_module.write_seeds(bucket, health.build_seeds([("bus", HOUR, 1)]))
    assert bucket.uploads == []


def test_seed_dag_is_manual_and_reads_days_from_conf(seed_module: Any, monkeypatch: Any) -> None:
    dag = seed_module.dag
    assert dag.kwargs["dag_id"] == "poller_health_seed"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["start_date"].utcoffset() == timedelta(0)
    assert seed_module.seed_baselines.calls == [()]
    monkeypatch.setattr(
        seed_module.storage,
        "Client",
        lambda **kwargs: types.SimpleNamespace(bucket=lambda name: FakeBucket()),
        raising=False,
    )
    monkeypatch.setattr(seed_module.bigquery, "Client", lambda **kwargs: FakeSeedClient([]), raising=False)
    requested = []
    monkeypatch.setattr(seed_module, "run_seed", lambda bucket, client, days: requested.append(days) or 0)
    for conf, days in (({"days": 7}, 7), ({}, 28), (None, 28)):
        context = {"dag_run": types.SimpleNamespace(conf=conf)}
        monkeypatch.setattr(seed_module, "get_current_context", lambda context=context: context)
        assert seed_module.seed_baselines.function() == 0
        assert requested[-1] == days
    monkeypatch.setattr(seed_module, "get_current_context", dict)
    seed_module.seed_baselines.function()
    assert requested[-1] == 28
    for value in ("7", 7.0, True, None):
        context = {"dag_run": types.SimpleNamespace(conf={"days": value})}
        monkeypatch.setattr(seed_module, "get_current_context", lambda context=context: context)
        with pytest.raises(ValueError, match="conf days must be an integer"):
            seed_module.seed_baselines.function()


def test_seed_rows_from_bigquery_may_be_non_utc(seed_module: Any) -> None:
    local = datetime(2026, 10, 5, 13, 59, tzinfo=health.WARSAW)
    seeds = health.build_seeds([("tram", local, 3)])
    assert list(seeds) == [datetime(2026, 10, 5, 11, tzinfo=UTC)]
    assert seeds[datetime(2026, 10, 5, 11, tzinfo=UTC)]["vehicle_types"]["tram"]["minutes"] == [
        {"minute": 59, "fresh_vehicles": 3}
    ]


def test_dag_files_never_import_other_dag_files() -> None:
    # Airflow registers an imported file's DAG again under the importing file.
    dags = Path(__file__).parents[1] / "dags"
    offenders = [
        path.name
        for path in dags.glob("dag_*.py")
        if re.search(r"^\s*(from|import)\s+dag_\w+", path.read_text(), flags=re.MULTILINE)
    ]
    assert offenders == []
