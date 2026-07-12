from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from .test_dag_daily_gps import Conflict, FakeJob, _install_airflow_stubs, _install_google_stubs


def test_shadow_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MATCHER_SHADOW_ENABLED", raising=False)
    shadow = _load_shadow_module()

    assert shadow.run_matcher_shadow("2026-07-09", "snapshot", "run") == {
        "enabled": False,
        "reason": "MATCHER_SHADOW_ENABLED is false",
    }


@pytest.mark.parametrize("dataset", ["", "ztm_raw", "ztm_int", "ztm_marts"])
def test_enabled_shadow_fails_closed_for_missing_or_canonical_dataset(
    monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", dataset)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match=r"DATASET|dataset"):
        shadow.ShadowConfig.from_env().validate()


def test_scoped_prefixes_and_deterministic_ids() -> None:
    shadow = _load_shadow_module()
    trip = shadow.ARTIFACTS[0]

    assert shadow._gps_prefixes("2026-07-09") == [
        "raw/gps/vehicle_type=bus/date=2026-07-09/",
        "raw/gps/vehicle_type=tram/date=2026-07-09/",
    ]
    assert shadow._table_id("matcher_shadow", "scheduled__2026-07-09T04:00:00+00:00", trip).startswith(
        "ztm-data.matcher_shadow.matcher_shadow_trip_scheduled__2026_07_09t04_00_00_00_00"
    )
    assert shadow._load_job_id("scheduled__2026-07-09T04:00:00+00:00", trip) == shadow._load_job_id(
        "scheduled__2026-07-09T04:00:00+00:00", trip
    )


def test_snapshot_lookup_is_parameterized(monkeypatch: pytest.MonkeyPatch) -> None:
    shadow = _load_shadow_module()
    client = FakeQueryClient([types.SimpleNamespace(gcs_path="gs://bucket/exact.zip")])

    assert shadow._snapshot_gcs_path(client, "snapshot'not-sql") == "gs://bucket/exact.zip"
    assert "snapshot'not-sql" not in client.query_text
    assert client.job_config.query_parameters == [
        shadow.bigquery.ScalarQueryParameter("snapshot_id", "STRING", "snapshot'not-sql")
    ]


def test_matcher_command_uses_argv_and_bounded_resources(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", tmp_path, "matcher-bin", None, 2700, "shadow/matcher")

    argv = shadow._matcher_argv(
        config, "2026-07-09", "snapshot", tmp_path / "gps", tmp_path / "gtfs.zip", tmp_path / "out"
    )

    assert argv[:2] == ["matcher-bin", "prepare"]
    options = dict(zip(argv[2::2], argv[3::2], strict=True))
    assert options["--threads"] == "2"
    assert options["--alignment-workers"] == "1"
    assert options["--memory-limit"] == "320MB"
    assert options["--temp-limit"] == "20GB"


@pytest.mark.parametrize(("canonical_rows", "difference_count"), [(10, 0), (11, 1)])
def test_comparison_ignores_source_but_retains_source_rows(
    monkeypatch: pytest.MonkeyPatch, canonical_rows: int, difference_count: int
) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setattr(shadow.bigquery, "ArrayQueryParameter", lambda *args: args, raising=False)
    rows = [
        {
            "artifact": "trip",
            "source": "shadow",
            "service_date": "2026-07-09",
            "mode": "bus",
            "trip_quality": "complete",
            "observation_status": None,
            "row_count": 10,
            "distinct_grains": 10,
            "avg_delay_seconds": 5.0,
            "overnight_rows": 0,
            "source_date_count": 1,
            "source_dates": ["2026-07-09"],
        },
        {
            "artifact": "trip",
            "source": "canonical",
            "service_date": "2026-07-09",
            "mode": "bus",
            "trip_quality": "complete",
            "observation_status": None,
            "row_count": canonical_rows,
            "distinct_grains": 10,
            "avg_delay_seconds": 5.0,
            "overnight_rows": 0,
            "source_date_count": 1,
            "source_dates": ["2026-07-09"],
        },
    ]

    report = shadow._comparison_report(FakeQueryClient(rows), "2026-07-09", _shadow_tables())

    assert len(report["differences"]) == difference_count
    assert {row["source"] for row in report["aggregates"]} == {"shadow", "canonical"}


def test_validate_outputs_rejects_bad_manifest_and_retains_artifact_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shadow = _load_shadow_module()
    output = tmp_path / "output"
    output.mkdir()
    files = {spec.key: output / spec.filename for spec in shadow.ARTIFACTS}
    files["trip_universe"] = output / "trip_universe.parquet"
    for path in files.values():
        path.write_bytes(path.name.encode())
    manifest = {
        "processing_date": "2026-07-09",
        "snapshot_id": "snapshot",
        "schema_versions": dict(shadow.ARTIFACT_SCHEMA_VERSIONS),
        "outputs": {key: {"sha256": shadow._sha256(path), "bytes": path.stat().st_size} for key, path in files.items()},
    }
    (output / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (output / "metrics.json").write_text(
        json.dumps({Path(spec.filename).stem: 4 for spec in shadow.ARTIFACTS}), encoding="utf-8"
    )

    def inspect(path: Path, _spec: Any, _date: str, _snapshot: str) -> Any:
        return shadow.ArtifactValidation(
            path, 4, shadow._sha256(path), path.stat().st_size, ("2026-07-08", "2026-07-09")
        )

    monkeypatch.setattr(shadow, "_inspect_artifact", inspect)
    artifacts, _ = shadow._validate_outputs(output, "2026-07-09", "snapshot")

    assert artifacts["trip"].rows == 4
    assert artifacts["trip_universe"].service_dates == ("2026-07-08", "2026-07-09")
    manifest["schema_versions"]["trip"] = "wrong"
    (output / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="schema version mismatch"):
        shadow._validate_outputs(output, "2026-07-09", "snapshot")


def test_load_uses_explicit_schema_write_truncate_and_recovers_conflict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setattr(shadow.bigquery, "SchemaField", lambda *args, **kwargs: (args, kwargs), raising=False)

    def load_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(shadow.bigquery, "LoadJobConfig", load_job_config)
    shadow.bigquery.WriteDisposition = types.SimpleNamespace(WRITE_TRUNCATE="WRITE_TRUNCATE")
    artifact_path = tmp_path / "trip.parquet"
    artifact_path.write_bytes(b"parquet")
    artifact = shadow.ArtifactValidation(artifact_path, 1, "hash", 7, ("2026-07-08", "2026-07-09"))
    client = FakeLoadClient(conflict=True)

    result = shadow._load_artifact(client, "shadow", "run-id", shadow.ARTIFACTS[0], artifact)

    assert result["table_id"].startswith("ztm-data.shadow.matcher_shadow_trip_")
    assert client.existing_job.result_called is True
    assert client.load_config.write_disposition == "WRITE_TRUNCATE"
    assert len(client.load_config.schema) == len(shadow.ARTIFACTS[0].fields)


def test_marker_is_written_only_after_loads_and_comparison(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MATCHER_SHADOW_PROJECT_DIR", "")
    shadow = _load_shadow_module()
    events: list[str] = []
    artifact = shadow.ArtifactValidation(tmp_path / "artifact.parquet", 1, "hash", 7, ("2026-07-08", "2026-07-09"))
    monkeypatch.setattr(shadow, "_snapshot_gcs_path", lambda *_args: "gs://bucket/snapshot.zip")
    monkeypatch.setattr(shadow, "_download_inputs", lambda *_args: ([], {}, tmp_path / "gps", tmp_path / "gtfs.zip"))
    monkeypatch.setattr(shadow, "_invoke_matcher", lambda *_args: events.append("matcher"))
    monkeypatch.setattr(
        shadow,
        "_validate_outputs",
        lambda *_args: (
            {spec.key: artifact for spec in shadow.ARTIFACTS} | {"trip_universe": artifact},
            {"metrics": {"rows": 1}},
        ),
    )
    monkeypatch.setattr(
        shadow, "_load_artifact", lambda *_args: events.append("load") or {"table_id": "shadow.table", "job_id": "job"}
    )
    monkeypatch.setattr(shadow, "_comparison_report", lambda *_args: events.append("comparison") or {"aggregates": []})
    monkeypatch.setattr(shadow, "_write_marker", lambda *_args: events.append("marker") or "gs://marker")
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())

    assert shadow.run_matcher_shadow("2026-07-09", "snapshot", "run")["marker_uri"] == "gs://marker"
    assert events == ["matcher", "load", "load", "load", "comparison", "marker"]
    monkeypatch.setattr(
        shadow,
        "_invoke_matcher",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("matcher failed")),
    )
    with pytest.raises(RuntimeError, match="matcher failed"):
        shadow.run_matcher_shadow("2026-07-09", "snapshot", "failed-run")
    assert events.count("marker") == 1


def test_workspace_retry_cleanup_and_marker_last(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MATCHER_SHADOW_PROJECT_DIR", "")
    shadow = _load_shadow_module()
    run_workspace = tmp_path / shadow._run_id("run")
    stale = run_workspace / "attempt-1" / "stale.txt"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale", encoding="utf-8")
    artifact = shadow.ArtifactValidation(tmp_path / "artifact.parquet", 1, "hash", 7, ("2026-07-08", "2026-07-09"))
    marker_workspaces: list[bool] = []

    def invoke(argv: list[str], _config: Any) -> None:
        assert not stale.exists()
        assert Path(argv[argv.index("--output-dir") + 1]).parent.is_dir()

    _stub_successful_run(monkeypatch, shadow, artifact, invoke=invoke)
    monkeypatch.setattr(
        shadow,
        "_write_marker",
        lambda *_args: marker_workspaces.append(run_workspace.is_dir()) or "gs://marker",
    )

    assert shadow.run_matcher_shadow("2026-07-09", "snapshot", "run", try_number=2)["marker_uri"] == "gs://marker"
    assert marker_workspaces == [True]
    assert not run_workspace.exists()


def test_failed_shadow_cleans_workspace_unless_keep_requested(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MATCHER_SHADOW_PROJECT_DIR", "")
    shadow = _load_shadow_module()
    run_workspace = tmp_path / shadow._run_id("run")
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow, "_snapshot_gcs_path", lambda *_args: "gs://bucket/snapshot.zip")
    monkeypatch.setattr(shadow, "_download_inputs", lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")))

    result = shadow.run_matcher_shadow_task("2026-07-09", "snapshot", "run")
    assert result["status"] == "failed_non_strict"
    assert not run_workspace.exists()

    monkeypatch.setenv("MATCHER_SHADOW_KEEP_WORKSPACE", "true")
    monkeypatch.setenv("MATCHER_SHADOW_STRICT", "true")
    with pytest.raises(RuntimeError, match="boom"):
        shadow.run_matcher_shadow_task("2026-07-09", "snapshot", "run")
    assert run_workspace.is_dir()


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("MATCHER_SHADOW_WORKSPACE_ROOT", "relative/workspace", "must be absolute"),
        ("MATCHER_SHADOW_COMMAND", "../matcher", "path traversal"),
    ],
)
def test_shadow_config_rejects_unsafe_workspace_or_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str, value: str, message: str
) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv(name, value)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match=message):
        shadow.ShadowConfig.from_env().validate()


def test_shadow_config_rejects_project_inside_run_workspace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    workspace = tmp_path / shadow._run_id("run") / "attempt-1"
    config = shadow.ShadowConfig.from_env()
    config = shadow.ShadowConfig(
        config.enabled,
        config.strict,
        config.dataset,
        config.workspace_root,
        config.command,
        workspace / "matcher",
        config.timeout_seconds,
        config.marker_prefix,
        config.keep_workspace,
    )

    with pytest.raises(ValueError, match="must not be inside"):
        shadow._validate_run_workspace(config, workspace)


def test_failure_never_writes_marker_and_strict_mode_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setattr(
        shadow, "run_matcher_shadow", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    assert shadow.run_matcher_shadow_task("2026-07-09", "snapshot", "run")["status"] == "failed_non_strict"
    monkeypatch.setenv("MATCHER_SHADOW_STRICT", "true")
    with pytest.raises(RuntimeError, match="boom"):
        shadow.run_matcher_shadow_task("2026-07-09", "snapshot", "run")


class FakeQueryClient:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows
        self.query_text = ""
        self.job_config: Any = None

    def query(self, query: str, *, job_config: Any) -> Any:
        self.query_text, self.job_config = query, job_config
        return types.SimpleNamespace(result=lambda: self.rows)


class FakeLoadClient:
    def __init__(self, *, conflict: bool) -> None:
        self.conflict = conflict
        self.load_config: Any = None
        self.existing_job = FakeJob()

    def load_table_from_file(self, _source: Any, _table: str, *, job_config: Any, job_id: str, location: str) -> Any:
        self.load_config = job_config
        if self.conflict:
            raise Conflict(job_id)
        return FakeJob()

    def get_job(self, _job_id: str, *, project: str, location: str) -> FakeJob:
        return self.existing_job


def _shadow_tables() -> dict[str, dict[str, str]]:
    return {
        "trip": {"table_id": "shadow.trip", "job_id": "trip"},
        "stop_arrival": {"table_id": "shadow.stop_arrival", "job_id": "stop_arrival"},
        "expected_stop_event": {"table_id": "shadow.expected_stop_event", "job_id": "expected_stop_event"},
    }


def _stub_successful_run(
    monkeypatch: pytest.MonkeyPatch, shadow: types.ModuleType, artifact: Any, *, invoke: Any
) -> None:
    monkeypatch.setattr(shadow, "_snapshot_gcs_path", lambda *_args: "gs://bucket/snapshot.zip")
    monkeypatch.setattr(shadow, "_download_inputs", lambda *_args: ([], {}, Path("gps"), Path("gtfs.zip")))
    monkeypatch.setattr(shadow, "_invoke_matcher", invoke)
    monkeypatch.setattr(
        shadow,
        "_validate_outputs",
        lambda *_args: ({spec.key: artifact for spec in shadow.ARTIFACTS}, {"metrics": {"rows": 1}}),
    )
    monkeypatch.setattr(shadow, "_load_artifact", lambda *_args: {"table_id": "shadow.table", "job_id": "job"})
    monkeypatch.setattr(shadow, "_comparison_report", lambda *_args: {"aggregates": []})
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())


def _load_shadow_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()
    sys.modules.pop("ztm_airflow_common", None)
    sys.modules.pop("ztm_matcher_shadow", None)
    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    spec = importlib.util.spec_from_file_location("ztm_matcher_shadow", dag_dir / "ztm_matcher_shadow.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load matcher shadow module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["ztm_matcher_shadow"] = module
    spec.loader.exec_module(module)
    return module
