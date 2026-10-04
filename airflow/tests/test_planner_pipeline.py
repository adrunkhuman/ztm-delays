from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import types
from datetime import date
from pathlib import Path
from typing import Any

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"
PLANNER_SETTINGS = Path(__file__).resolve().parents[2] / "planner" / "src" / "ztm_planner" / "settings.py"


def _load(name: str) -> types.ModuleType:
    for module_name in list(sys.modules):
        if module_name.startswith(("airflow", "google")) or module_name in {
            "ztm_airflow_common", "planner_pipeline", "planner_queries", "dag_planner",
        }:  # fmt: skip
            sys.modules.pop(module_name, None)
    _install_stubs()
    if str(DAGS) not in sys.path:
        sys.path.insert(0, str(DAGS))
    spec = importlib.util.spec_from_file_location(name, DAGS / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _install_stubs() -> None:
    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG = FakeDAG
    sdk.Asset = lambda uri, **_kwargs: uri
    sdk.CronTriggerTimetable = lambda cron, timezone: {"cron": cron, "timezone": timezone}
    sdk.task = fake_task
    bigquery = types.ModuleType("google.cloud.bigquery")
    bigquery.Client = lambda project: None
    bigquery.QueryJobConfig = lambda **kwargs: kwargs
    bigquery.ScalarQueryParameter = lambda name, kind, value: (name, kind, value)
    bigquery.ExtractJobConfig = lambda **kwargs: kwargs
    storage = types.ModuleType("google.cloud.storage")
    storage.Client = lambda project: None
    cloud = types.ModuleType("google.cloud")
    cloud.bigquery, cloud.storage = bigquery, storage
    sys.modules.update({
        "airflow": types.ModuleType("airflow"), "airflow.sdk": sdk, "google": types.ModuleType("google"),
        "google.cloud": cloud, "google.cloud.bigquery": bigquery, "google.cloud.storage": storage,
    })  # fmt: skip


def fake_task(function: Any = None, **kwargs: Any) -> Any:
    """Declaring a DAG calls its tasks; they must not run."""
    if function is None:
        return lambda decorated: fake_task(decorated, **kwargs)
    return lambda *_args, **_kwargs: None


class FakeDAG:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeBlob:
    def __init__(self, bucket: FakeBucket, name: str) -> None:
        self.bucket, self.name = bucket, name

    def delete(self) -> None:
        self.bucket.objects.pop(self.name)

    def exists(self) -> bool:
        return self.name in self.bucket.objects

    def download_to_filename(self, path: str) -> None:
        Path(path).write_bytes(self.bucket.objects[self.name])

    def download_as_text(self) -> str:
        return self.bucket.objects[self.name].decode()

    def upload_from_filename(self, path: str) -> None:
        self.bucket.objects[self.name] = Path(path).read_bytes()
        self.bucket.uploads.append(self.name)

    def upload_from_string(self, text: str, content_type: str = "") -> None:
        self.bucket.objects[self.name] = text.encode()
        self.bucket.uploads.append(self.name)


class FakeBucket:
    name = "bucket"

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: list[str] = []

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)

    def list_blobs(self, prefix: str) -> list[FakeBlob]:
        return [FakeBlob(self, n) for n in sorted(self.objects) if n.startswith(prefix)]


class FakeJob:
    destination = "project.dataset.anon"
    total_bytes_billed = 1

    def __init__(self, on_result: Any = None) -> None:
        self.on_result = on_result

    def result(self) -> None:
        if self.on_result:
            self.on_result()


class FakeClient:
    def __init__(self, bucket: FakeBucket, shards: int) -> None:
        self.bucket, self.shards, self.queries = bucket, shards, []

    def query(self, sql: str, job_config: dict[str, Any]) -> FakeJob:
        self.queries.append((sql, job_config))
        return FakeJob()

    def extract_table(self, table: str, uri: str, job_config: dict[str, Any]) -> FakeJob:
        prefix = uri.removeprefix(f"gs://{self.bucket.name}/").rsplit("/", 1)[0]

        def write() -> None:
            for i in range(self.shards):
                self.bucket.objects[f"{prefix}/part-{i:012d}.parquet"] = b"PAR1"

        return FakeJob(write)


def test_promotion_requires_beating_timetable_and_lookup_on_held_out_days() -> None:
    pipeline = _load("planner_pipeline")
    good = {"metrics": {"segment_mae_s": {"timetable": 27.3, "lookup": 18.2, "model": 17.6}}}
    assert pipeline.promotable(good)[0]
    assert not pipeline.promotable({"metrics": {"segment_mae_s": {"timetable": 27.3, "lookup": 18.2, "model": 18.5}}})[
        0
    ]
    assert not pipeline.promotable({"metrics": {}})[0]


def test_queries_use_the_planner_settings() -> None:
    queries = _load("planner_queries")
    spec = importlib.util.spec_from_file_location("planner_settings", PLANNER_SETTINGS)
    assert spec is not None
    assert spec.loader is not None
    settings = importlib.util.module_from_spec(spec)
    sys.modules["planner_settings"] = settings  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(settings)
    assert tuple(pm / 1000 for pm in queries.STOP_EPS_PER_MILLE) == settings.STOP_EPS_GRID
    assert queries.STOP_TOLERANCE_S == settings.STOP_TOLERANCE_S
    assert queries.STOP_MISS_TARGET == settings.STOP_MISS_TARGET
    assert queries.STOP_MIN_ARRIVALS == settings.STOP_MIN_ARRIVALS
    slots = queries.stop_slots("p.d")
    for level in ("line_stop_hour", "line_stop_band", "line_stop", "generic"):
        assert f"'{level}' AS level" in slots
    assert slots.count("e7") == 1
    assert "@start" in slots
    assert "@end" in slots
    assert "@cal_start" in queries.stop_eps("p.d")
    assert "actual_s" in queries.training_segments("p.d")
    assert "GROUP BY 1, 2, 3" in queries.recent_daily("p.d")


def test_dags_run_weekly_training_and_nightly_scoring_before_the_warehouse() -> None:
    dag = _load("dag_planner")
    assert dag.train_dag.kwargs["schedule"] == {"cron": "0 13 * * 0", "timezone": "Europe/Warsaw"}
    assert dag.score_dag.kwargs["schedule"] == {"cron": "30 2 * * *", "timezone": "Europe/Warsaw"}
    for d in (dag.train_dag, dag.score_dag):
        assert d.kwargs["catchup"] is False
        assert d.kwargs["max_active_runs"] == 1


def test_extract_replaces_old_shards_and_downloads_all(tmp_path: Path) -> None:
    pipeline = _load("planner_pipeline")
    bucket = FakeBucket()
    bucket.objects["planner/extracts/x/part-stale.parquet"] = b"old"
    client = FakeClient(bucket, shards=2)
    paths = pipeline.export_parquet(
        client, bucket, "select 1", {"start": date(2026, 9, 1)}, "planner/extracts/x", tmp_path
    )
    assert [p.name for p in paths] == ["part-000000000000.parquet", "part-000000000001.parquet"]
    assert not [n for n in bucket.objects if n.startswith("planner/extracts/x/")]  # staged shards are removed
    assert client.queries[0][1]["maximum_bytes_billed"] == pipeline.MAX_BYTES_BILLED
    with pytest.raises(RuntimeError, match="no files"):
        pipeline.export_parquet(FakeClient(bucket, shards=0), bucket, "select 1", {}, "planner/extracts/y", tmp_path)


def test_bundle_pointer_moves_last_and_downloads_are_cached(tmp_path: Path) -> None:
    pipeline = _load("planner_pipeline")
    bucket = FakeBucket()
    with pytest.raises(RuntimeError, match="No planner model"):
        pipeline.download_current_bundle(bucket, tmp_path / "cache")
    local = tmp_path / "bundle"
    local.mkdir()
    for name in ("gbm.txt", "meta.json"):
        (local / name).write_text(name)
    pipeline.upload_bundle(bucket, local, "v1")
    assert bucket.uploads[-1] == pipeline.CURRENT_POINTER
    first = pipeline.download_current_bundle(bucket, tmp_path / "cache")
    assert (first / "gbm.txt").read_text() == "gbm.txt"
    bucket.objects.clear()  # a cached version is not downloaded again
    bucket.objects[pipeline.CURRENT_POINTER] = json.dumps({"version": "v1"}).encode()
    assert pipeline.download_current_bundle(bucket, tmp_path / "cache") == first


def test_planner_command_summary_is_the_last_stdout_line(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pipeline = _load("planner_pipeline")
    calls = []

    def fake_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, stdout='noise\n{"trips": 3}\n', stderr="log")

    monkeypatch.setattr(pipeline.subprocess, "run", fake_run)
    config = pipeline.PlannerConfig(command=["ztm-planner"], workdir=tmp_path, timeout_seconds=60)
    assert pipeline.run_planner(config, ["score"]) == {"trips": 3}
    assert calls[0][0] == ["ztm-planner", "score"]
    assert calls[0][1]["shell"] is False
    assert calls[0][1]["check"] is True
