from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from .test_dag_daily_gps import Conflict, PreconditionFailed, _install_airflow_stubs, _install_google_stubs


def test_shadow_load_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MATCHER_SHADOW_ENABLED", raising=False)
    shadow = _load_shadow_module()

    assert shadow.run_matcher_shadow_load("2026-07-09", "snapshot", "run") == {
        "enabled": False,
        "reason": "MATCHER_SHADOW_ENABLED is false",
    }


@pytest.mark.parametrize("dataset", ["", "ztm_raw", "ztm_int", "ztm_marts"])
def test_enabled_shadow_rejects_missing_or_canonical_dataset(monkeypatch: pytest.MonkeyPatch, dataset: str) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", dataset)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match=r"DATASET|dataset"):
        shadow.ShadowConfig.from_env().validate()


def test_arrow_schema_uses_data_type_objects_for_repeated_fields() -> None:
    pa = pytest.importorskip("pyarrow")
    shadow = _load_shadow_module()

    assert shadow._expected_arrow_type(shadow.FieldSpec("tags", "STRING", True), pa) == pa.list_(pa.string())
    assert shadow._expected_arrow_type(shadow.FieldSpec("event_at", "TIMESTAMP"), pa) == pa.timestamp("us", tz="UTC")


def test_inspect_artifact_uses_bounded_duckdb_validation(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    shadow = _load_shadow_module()
    path = tmp_path / "artifact.parquet"
    spec = shadow.ArtifactSpec(
        "test",
        path.name,
        "test",
        (
            shadow.FieldSpec("processing_date", "DATE"),
            shadow.FieldSpec("service_date", "DATE"),
            shadow.FieldSpec("gtfs_snapshot_id", "STRING"),
            shadow.FieldSpec("trip_id", "STRING"),
            shadow.FieldSpec("tags", "STRING", True),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id"),
        "processing_date",
    )
    pq.write_table(
        pa.table(
            {
                "processing_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "service_date": [date(2026, 7, 8), date(2026, 7, 9)],
                "gtfs_snapshot_id": ["snapshot", "snapshot"],
                "trip_id": ["prior", "current"],
                "tags": [["one"], ["two"]],
            }
        ),
        path,
    )

    result = shadow._inspect_artifact(path, spec, "2026-07-09", "snapshot")

    assert result.rows == 2
    assert result.service_dates == ("2026-07-08", "2026-07-09")
    assert list(tmp_path.glob(".validation-*")) == []
    source = Path(str(shadow.__file__)).read_text(encoding="utf-8")
    assert "to_pylist" not in source
    assert 'group by {", ".join(spec.grain)}' in source
    assert shadow.VALIDATION_MEMORY_LIMIT == "320MB"
    assert shadow.VALIDATION_TEMP_LIMIT == "20GB"
    assert duckdb is not None


def test_load_job_id_binds_artifact_hash_and_conflict_checks_existing_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setattr(shadow.bigquery, "SchemaField", lambda *args, **kwargs: (args, kwargs), raising=False)

    def load_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(shadow.bigquery, "LoadJobConfig", load_job_config, raising=False)
    shadow.bigquery.WriteDisposition = types.SimpleNamespace(WRITE_TRUNCATE="WRITE_TRUNCATE")
    artifact = shadow.ArtifactValidation(tmp_path / "trip.parquet", 1, "a" * 64, 7, ("2026-07-09",))
    artifact.path.write_bytes(b"parquet")
    spec = shadow.ARTIFACTS[0]
    expected_table = shadow._table_id("shadow", "run-id", spec)
    expected_job = shadow._load_job_id("run-id", spec, artifact.sha256)
    assert expected_job != shadow._load_job_id("run-id", spec, "b" * 64)
    client = FakeLoadClient(FakeLoadJob(expected_job, expected_table), conflict=True)

    result = shadow._load_artifact(client, "shadow", "run-id", spec, artifact)

    assert result == {"table_id": expected_table, "job_id": expected_job}
    assert client.existing_job.result_called is True
    bad_client = FakeLoadClient(FakeLoadJob(expected_job, "ztm-data.shadow.other"), conflict=True)
    with pytest.raises(RuntimeError, match="destination"):
        shadow._load_artifact(bad_client, "shadow", "run-id", spec, artifact)
    failed_job = FakeLoadJob(expected_job, expected_table)
    failed_job.error_result = {"reason": "invalid"}
    with pytest.raises(RuntimeError, match="complete successfully"):
        shadow._load_artifact(FakeLoadClient(failed_job, conflict=True), "shadow", "run-id", spec, artifact)


def test_marker_uses_create_only_precondition_and_rejects_conflicts(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", tmp_path, "matcher", None, 1, "shadow/matcher")
    bucket = FakeMarkerBucket(existing=None)
    marker = {"run_id": "run", "state": "committed"}

    uri = shadow._write_marker(FakeStorageClient(bucket), config, "2026-07-09", "run", marker)

    assert uri.endswith("commit.json")
    assert bucket.blob_instance.if_generation_match == 0
    same = FakeMarkerBucket(existing=bucket.blob_instance.payload)
    assert shadow._write_marker(FakeStorageClient(same), config, "2026-07-09", "run", marker) == uri
    logical_same = FakeMarkerBucket(existing=b'{ "run_id": "run", "state": "committed" }')
    assert shadow._write_marker(FakeStorageClient(logical_same), config, "2026-07-09", "run", marker) == uri
    conflict = FakeMarkerBucket(existing=b'{"state":"other"}')
    with pytest.raises(RuntimeError, match="different content"):
        shadow._write_marker(FakeStorageClient(conflict), config, "2026-07-09", "run", marker)


@pytest.mark.parametrize("name", ["/absolute/part.parquet", "raw\\gps\\part.parquet", "raw/gps/../part.parquet"])
def test_gcs_paths_reject_traversal(name: str) -> None:
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match="Unsafe"):
        shadow._strict_posix_name(name)
    with pytest.raises(ValueError, match=r"Unsafe|outside"):
        shadow._gcs_uri_parts(f"gs://bucket/{name}")


def test_gcs_paths_require_expected_prefix_and_contained_destination(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    relative = shadow._gcs_relative_name(
        "raw/gps/vehicle_type=bus/date=2026-07-09/hour=01/part-a.parquet",
        "raw/gps/vehicle_type=bus/date=2026-07-09/",
    )

    assert shadow._contained_destination(tmp_path, relative).is_relative_to(tmp_path)
    with pytest.raises(ValueError, match="outside expected"):
        shadow._gcs_relative_name("raw/other/part-a.parquet", "raw/gps")


def test_input_bounds_apply_before_download(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(
        True,
        False,
        "shadow",
        tmp_path,
        "matcher",
        None,
        1,
        "shadow/matcher",
        max_gps_objects=1,
        max_gps_bytes=10,
        min_free_disk_bytes=5,
    )
    objects = [shadow.GcsObject("one", "1", 6, "hash", None), shadow.GcsObject("two", "2", 6, "hash", None)]
    with pytest.raises(RuntimeError, match="object count"):
        shadow._enforce_input_bounds(config, objects, 1, tmp_path)
    byte_config = shadow.ShadowConfig(
        True,
        False,
        "shadow",
        tmp_path,
        "matcher",
        None,
        1,
        "shadow/matcher",
        max_gps_objects=5,
        max_gps_bytes=10,
        min_free_disk_bytes=5,
    )
    with pytest.raises(RuntimeError, match="input bytes"):
        shadow._enforce_input_bounds(byte_config, objects, 1, tmp_path)
    disk_config = shadow.ShadowConfig(
        True,
        False,
        "shadow",
        tmp_path,
        "matcher",
        None,
        1,
        "shadow/matcher",
        max_gps_objects=5,
        max_gps_bytes=20,
        min_free_disk_bytes=10,
    )
    monkeypatch.setattr(shadow.shutil, "disk_usage", lambda _path: types.SimpleNamespace(free=10))
    with pytest.raises(RuntimeError, match="free disk"):
        shadow._enforce_input_bounds(disk_config, objects, 1, tmp_path)


@pytest.mark.parametrize(
    "name", ["MATCHER_SHADOW_MAX_GPS_OBJECTS", "MATCHER_SHADOW_MAX_GPS_BYTES", "MATCHER_SHADOW_MIN_FREE_DISK_BYTES"]
)
def test_config_rejects_nonpositive_bounds(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv(name, "0")
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match="positive"):
        shadow.ShadowConfig.from_env()


def test_load_writes_pending_not_marker_and_compare_commits_afterwards(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MATCHER_SHADOW_PROJECT_DIR", "")
    shadow = _load_shadow_module()
    artifact = shadow.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_load(monkeypatch, shadow, artifact, events)

    context = shadow.run_matcher_shadow_load("2026-07-09", "snapshot", "run")

    assert context == {
        "enabled": True,
        "status": "loaded_pending",
        "processing_date": "2026-07-09",
        "run_id": "run",
        "pending_uri": "gs://pending",
    }
    assert events == ["matcher", "load", "load", "load", "pending"]
    pending = {
        "run_id": "run",
        "processing_date": "2026-07-09",
        "artifacts": {spec.key: {"sha256": artifact.sha256} for spec in shadow.ARTIFACTS},
        "tables": {
            spec.key: {
                "table_id": shadow._table_id("matcher_shadow", shadow._run_id("run"), spec),
                "job_id": shadow._load_job_id(shadow._run_id("run"), spec, artifact.sha256),
            }
            for spec in shadow.ARTIFACTS
        },
    }
    monkeypatch.setattr(shadow, "_read_pending", lambda *_args: pending)
    monkeypatch.setattr(shadow, "_comparison_report", lambda *_args: events.append("compare") or {"aggregates": []})
    monkeypatch.setattr(shadow, "_write_marker", lambda *_args: events.append("marker") or "gs://marker")

    assert shadow.run_matcher_shadow_compare_commit("2026-07-09", "run", context)["marker_uri"] == "gs://marker"
    assert events[-2:] == ["compare", "marker"]


def test_non_strict_load_and_compare_failures_return_success_context(monkeypatch: pytest.MonkeyPatch) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setattr(
        shadow, "run_matcher_shadow_load", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("load"))
    )
    monkeypatch.setattr(
        shadow,
        "run_matcher_shadow_compare_commit",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("compare")),
    )

    assert shadow.run_matcher_shadow_load_task("2026-07-09", "snapshot", "run")["status"] == "failed_non_strict"
    assert (
        shadow.run_matcher_shadow_compare_commit_task("2026-07-09", "run", {"enabled": True})["status"]
        == "failed_non_strict"
    )
    monkeypatch.setenv("MATCHER_SHADOW_STRICT", "true")
    with pytest.raises(RuntimeError, match="load"):
        shadow.run_matcher_shadow_load_task("2026-07-09", "snapshot", "run")
    with pytest.raises(RuntimeError, match="compare"):
        shadow.run_matcher_shadow_compare_commit_task("2026-07-09", "run", {"enabled": True})


class FakeLoadJob:
    def __init__(self, job_id: str, destination: str) -> None:
        self.job_id = job_id
        self.destination = destination
        self.state = "DONE"
        self.error_result = None
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeLoadClient:
    def __init__(self, existing_job: FakeLoadJob, *, conflict: bool) -> None:
        self.existing_job = existing_job
        self.conflict = conflict

    def load_table_from_file(self, _source: Any, _table: str, **_kwargs: Any) -> FakeLoadJob:
        if self.conflict:
            raise Conflict("already exists")
        return self.existing_job

    def get_job(self, _job_id: str, **_kwargs: Any) -> FakeLoadJob:
        return self.existing_job


class FakeMarkerBlob:
    def __init__(self, existing: bytes | None) -> None:
        self.existing = existing
        self.payload = b""
        self.if_generation_match: int | None = None

    def upload_from_string(self, payload: bytes, **kwargs: Any) -> None:
        self.if_generation_match = kwargs.get("if_generation_match")
        if self.existing is not None:
            raise PreconditionFailed("exists")
        self.payload = payload

    def download_as_bytes(self) -> bytes:
        return self.existing if self.existing is not None else self.payload


class FakeMarkerBucket:
    def __init__(self, *, existing: bytes | None) -> None:
        self.blob_instance = FakeMarkerBlob(existing)

    def blob(self, _name: str) -> FakeMarkerBlob:
        return self.blob_instance


class FakeStorageClient:
    def __init__(self, bucket: FakeMarkerBucket) -> None:
        self.bucket_instance = bucket

    def bucket(self, _name: str) -> FakeMarkerBucket:
        return self.bucket_instance


def _stub_load(monkeypatch: pytest.MonkeyPatch, shadow: types.ModuleType, artifact: Any, events: list[str]) -> None:
    monkeypatch.setattr(shadow, "_snapshot_gcs_path", lambda *_args: "gs://bucket/raw/gtfs/snapshot.zip")
    monkeypatch.setattr(shadow, "_download_inputs", lambda *_args: ([], {}, Path("gps"), Path("gtfs.zip")))
    monkeypatch.setattr(shadow, "_invoke_matcher", lambda *_args: events.append("matcher"))
    monkeypatch.setattr(
        shadow,
        "_validate_outputs",
        lambda *_args: (
            {spec.key: artifact for spec in shadow.ARTIFACTS} | {"trip_universe": artifact},
            {"metrics": {}},
        ),
    )
    monkeypatch.setattr(
        shadow, "_load_artifact", lambda *_args: events.append("load") or {"table_id": "table", "job_id": "job"}
    )
    monkeypatch.setattr(shadow, "_write_pending", lambda *_args: events.append("pending") or "gs://pending")
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
