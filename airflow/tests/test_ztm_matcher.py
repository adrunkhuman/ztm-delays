from __future__ import annotations

import importlib.util
import sys
import types
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from .test_dag_daily_gps import Conflict, PreconditionFailed, _install_airflow_stubs, _install_google_stubs


def test_config_uses_only_matcher_environment_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_STAGING_DATASET", "matcher_stage")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", "matcher_input")
    monkeypatch.setenv("MATCHER_WORKSPACE_ROOT", str(tmp_path))
    matcher = _load_matcher()

    config = matcher.MatcherConfig.from_env()

    config.validate()
    assert config.marker_prefix == "matcher/runs"
    assert config.max_publication_bytes == matcher.DEFAULT_MAX_PUBLICATION_BYTES
    assert config.max_rss_bytes == matcher.DEFAULT_MAX_RSS_BYTES
    assert config.max_rss_bytes == 3 * 1024**3
    assert config.max_current_swap_bytes == matcher.DEFAULT_MAX_CURRENT_SWAP_BYTES
    assert config.max_current_swap_bytes == 2 * 1024**3
    assert config.staging_retention_days == 3
    assert config.intermediate_marker_retention_days == 3
    assert config.published_marker_retention_days == 30


def test_config_accepts_strict_zero_current_swap_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_STAGING_DATASET", "matcher_stage")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", "matcher_input")
    monkeypatch.setenv("MATCHER_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("MATCHER_MAX_SWAP_BYTES", "0")
    matcher = _load_matcher()

    config = matcher.MatcherConfig.from_env()

    config.validate()
    assert config.max_current_swap_bytes == 0


@pytest.mark.parametrize("dataset", ["", "project.dataset", "bad-dataset"])
def test_enabled_config_rejects_invalid_staging_dataset(monkeypatch: pytest.MonkeyPatch, dataset: str) -> None:
    monkeypatch.setenv("MATCHER_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_STAGING_DATASET", dataset)
    matcher = _load_matcher()

    with pytest.raises(ValueError, match=r"DATASET|dataset ID"):
        matcher.MatcherConfig.from_env().validate()


def test_config_requires_separate_staging_and_input_datasets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MATCHER_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_STAGING_DATASET", "matcher")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", "matcher")
    matcher = _load_matcher()

    with pytest.raises(ValueError, match="must differ"):
        matcher.MatcherConfig.from_env().validate()


def test_run_and_load_identities_are_deterministic_and_content_bound() -> None:
    matcher = _load_matcher()
    spec = matcher.ARTIFACTS[0]

    first = matcher._table_identity("matcher_stage", "manual/run", spec, "a" * 64)
    assert first == matcher._table_identity("matcher_stage", "manual/run", spec, "a" * 64)
    assert first["table_id"].startswith(f"{matcher.GCP_PROJECT}.matcher_stage.matcher_run_")
    assert first["job_id"].startswith("matcher_load_")
    assert first != matcher._table_identity("matcher_stage", "manual/run", spec, "b" * 64)
    assert matcher._run_id("manual/run") != matcher._run_id("manual_run")


def test_input_bounds_reject_excess_objects(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, max_gps_objects=1)
    objects = [matcher.GcsObject("one", "1", 1, "hash", None), matcher.GcsObject("two", "2", 1, "hash", None)]

    with pytest.raises(RuntimeError, match="object count"):
        matcher._enforce_input_bounds(config, objects, 1, tmp_path)


def test_artifact_schema_and_stage_labels_are_exact() -> None:
    matcher = _load_matcher()
    spec = matcher.ARTIFACTS[0]
    labels = matcher._stage_labels(spec, "a" * 64)
    fields = [
        types.SimpleNamespace(name=name, field_type=field_type, mode=mode)
        for name, field_type, mode in matcher._expected_schema(spec)
    ]
    table = types.SimpleNamespace(
        schema=fields, labels=labels, time_partitioning=types.SimpleNamespace(field=spec.partition_field)
    )

    matcher._verify_table_contract(
        types.SimpleNamespace(get_table=lambda _table: table), "project.input.stage", spec, labels
    )
    table.labels = {}
    with pytest.raises(RuntimeError, match="labels"):
        matcher._verify_table_contract(
            types.SimpleNamespace(get_table=lambda _table: table), "project.input.stage", spec, labels
        )


def test_artifact_inspection_enforces_lineage_grain_and_hash(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    matcher = _load_matcher()
    spec = matcher.ArtifactSpec(
        "test",
        "artifact.parquet",
        "test",
        (
            matcher.FieldSpec("processing_date", "DATE"),
            matcher.FieldSpec("gps_date", "DATE"),
            matcher.FieldSpec("service_date", "DATE"),
            matcher.FieldSpec("gtfs_snapshot_id", "STRING"),
            matcher.FieldSpec("trip_id", "STRING"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id"),
        "gps_date",
    )
    artifact = tmp_path / spec.filename
    pq.write_table(
        pa.table(
            {
                "processing_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "gps_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "service_date": [date(2026, 7, 8), date(2026, 7, 9)],
                "gtfs_snapshot_id": ["snapshot", "snapshot"],
                "trip_id": ["prior", "current"],
            }
        ),
        artifact,
    )

    validated = matcher._inspect_artifact(artifact, spec, "2026-07-09", "snapshot")

    assert validated.rows == 2
    assert validated.sha256 == matcher._sha256(artifact)
    assert validated.service_dates == ("2026-07-08", "2026-07-09")


def test_artifact_relationships_require_complete_trip_semantics_and_events(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    matcher = _load_matcher()
    identity = {
        "gtfs_snapshot_id": ["snapshot"],
        "processing_date": [date(2026, 7, 9)],
        "service_date": [date(2026, 7, 9)],
        "trip_id": ["trip"],
    }
    trip = pa.table(identity | {"vehicle_number": ["100"]})
    semantics = pa.table(identity | {"are_passenger_boundaries_settled": [True], "is_passenger_stop": [True]})
    event = pa.table(identity | {"vehicle_number": ["100"], "stop_sequence": [1]})
    arrival = pa.table(identity | {"vehicle_number": ["100"], "stop_sequence": [1]})
    tables = {"trip": trip, "stop_semantics": semantics, "expected_stop_event": event, "stop_arrival": arrival}
    artifacts = {}
    for name, table in tables.items():
        path = tmp_path / f"{name}.parquet"
        pq.write_table(table, path)
        artifacts[name] = types.SimpleNamespace(path=path)

    matcher._validate_artifact_relationships(artifacts)
    pq.write_table(event.slice(0, 0), artifacts["expected_stop_event"].path)

    with pytest.raises(RuntimeError, match="trips_with_incomplete_events"):
        matcher._validate_artifact_relationships(artifacts)


def test_publication_invariants_reject_missing_mode_and_resource_evidence(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    pending = _pending(matcher)

    assert matcher._validate_publication_invariants(pending, config)["status"] == "pass"
    pending["artifacts"]["trip"]["modes"] = ["bus"]
    pending["metrics"]["peak_rss_bytes"] = config.max_rss_bytes + 1
    pending["metrics"]["current_swap_bytes"] = config.max_current_swap_bytes + 1
    result = matcher._validate_publication_invariants(pending, config)
    assert result["status"] == "fail"
    assert len(result["issues"]) >= 3


def test_publication_invariants_bound_current_swap_inclusively(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, max_current_swap_bytes=10)
    pending = _pending(matcher)
    pending["metrics"]["current_swap_bytes"] = 10

    assert matcher._validate_publication_invariants(pending, config)["status"] == "pass"

    del pending["metrics"]["current_swap_bytes"]
    result = matcher._validate_publication_invariants(pending, config)
    assert result["status"] == "fail"
    assert any(issue["category"] == "resource" for issue in result["issues"])

    for invalid in (-1, False):
        pending["metrics"]["current_swap_bytes"] = invalid
        assert matcher._validate_publication_invariants(pending, config)["status"] == "fail"


def test_existing_validated_marker_does_not_bypass_current_resource_bounds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, max_current_swap_bytes=0)
    pending = _pending(matcher)
    pending["metrics"]["current_swap_bytes"] = 1
    marker = pending | {
        "immutable_run_identity": matcher._immutable_matcher_run_identity(pending),
        "validation": {"status": "pass"},
    }
    monkeypatch.setattr(matcher.MatcherConfig, "from_env", lambda: config)
    monkeypatch.setattr(matcher.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher, "_read_pending", lambda *_args: pending)
    monkeypatch.setattr(matcher, "_pending_tables", lambda *_args: {})
    monkeypatch.setattr(matcher, "_read_validated_marker", lambda *_args: marker)

    with pytest.raises(RuntimeError, match="publication invariants failed"):
        matcher.publish_matcher_artifacts("2026-07-09", "run", {"status": "loaded_pending", **pending})


def test_marker_writes_are_create_only_and_idempotent(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _pending(matcher)
    marker["immutable_run_identity"] = matcher._immutable_matcher_run_identity(marker)
    bucket = _MarkerBucket(None)

    uri = matcher._write_validated_marker(_Storage(bucket), config, "2026-07-09", "run", marker)

    assert uri.endswith("validated.json")
    assert bucket.item.if_generation_match == 0
    same = _MarkerBucket(bucket.item.payload)
    assert matcher._write_validated_marker(_Storage(same), config, "2026-07-09", "run", marker) == uri
    conflicting = _pending(matcher)
    conflicting["snapshot_id"] = "other-snapshot"
    conflicting["immutable_run_identity"] = matcher._immutable_matcher_run_identity(conflicting)
    with pytest.raises(RuntimeError, match="different immutable"):
        matcher._write_validated_marker(
            _Storage(_MarkerBucket(matcher._json_bytes(conflicting))), config, "2026-07-09", "run", marker
        )


def test_validated_marker_reuses_identity_but_rejects_changed_artifact_or_input(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _pending(matcher)
    marker["metrics"] = {"wall_seconds": 5, "cpu_seconds": 4, "peak_rss_bytes": 10}
    marker["diagnostics"] = {"stop_alignment_missing_stops": 2}
    marker["immutable_run_identity"] = matcher._immutable_matcher_run_identity(marker)
    bucket = _MarkerBucket(matcher._json_bytes(marker))

    rerun = _pending(matcher)
    rerun["metrics"] = {"wall_seconds": 50, "cpu_seconds": 40, "peak_rss_bytes": 20}
    rerun["diagnostics"] = {"stop_alignment_missing_stops": 99}
    rerun["immutable_run_identity"] = matcher._immutable_matcher_run_identity(rerun)
    matcher._write_validated_marker(_Storage(bucket), config, "2026-07-09", "run", rerun)
    assert __import__("json").loads(bucket.item.download_as_bytes())["diagnostics"] == marker["diagnostics"]

    changed_artifact = _pending(matcher)
    changed_artifact["artifacts"]["trip"]["rows"] = 2
    changed_artifact["immutable_run_identity"] = matcher._immutable_matcher_run_identity(changed_artifact)
    with pytest.raises(RuntimeError, match="different immutable"):
        matcher._write_validated_marker(_Storage(bucket), config, "2026-07-09", "run", changed_artifact)

    changed_input = _pending(matcher)
    changed_input["gps_inventory"][0]["generation"] = "2"
    changed_input["immutable_run_identity"] = matcher._immutable_matcher_run_identity(changed_input)
    with pytest.raises(RuntimeError, match="different immutable"):
        matcher._write_validated_marker(_Storage(bucket), config, "2026-07-09", "run", changed_input)


def test_marker_reads_are_bounded(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, max_marker_bytes=1)
    bucket = _MarkerBucket(b"{}")

    with pytest.raises(RuntimeError, match="exceeds"):
        matcher._read_pending(_Storage(bucket), config, "2026-07-09", "run")
    assert bucket.item.download_called is False


def test_publication_transaction_replaces_all_four_partitions_with_explicit_columns() -> None:
    matcher = _load_matcher()
    published = {
        spec.key: {
            "stable_table": f"project.input.{matcher.STABLE_INPUT_TABLES[spec.key]}",
            "staged_table": f"project.input.stage_{spec.key}",
        }
        for spec in matcher.ARTIFACTS
    }

    query = matcher._publication_transaction_query(published)

    assert query.count("delete from") == 4
    assert query.count("insert into") == 4
    assert query.count("@processing_date") == 8
    assert "select *" not in query


def test_partition_validation_rejects_rows_outside_requested_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    matcher = _load_matcher()
    monkeypatch.setattr(
        matcher,
        "_publication_table_counts",
        lambda *_args: {
            "total_rows": 3,
            "partition_rows": 2,
            "wrong_partition_rows": 1,
            "wrong_processing_date_rows": 1,
        },
    )

    with pytest.raises(RuntimeError, match="row count does not equal"):
        matcher._require_exact_processing_partition(
            object(), "project.stage.trip", "2026-07-09", matcher.ARTIFACTS[0], 3, "job", 1
        )


def test_post_validation_rejects_wrong_processing_lineage(monkeypatch: pytest.MonkeyPatch) -> None:
    matcher = _load_matcher()
    monkeypatch.setattr(
        matcher,
        "_stable_partition_counts",
        lambda *_args: {"total_rows": 1, "wrong_processing_date_rows": 1},
    )

    with pytest.raises(RuntimeError, match="lineage"):
        matcher._require_stable_processing_partition(
            object(), "project.input.trip", "2026-07-09", matcher.ARTIFACTS[0], 1, "job", 1
        )


def test_stable_partition_content_comparison_uses_fresh_bounded_query(monkeypatch: pytest.MonkeyPatch) -> None:
    matcher = _load_matcher()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(matcher.bigquery, "QueryJobConfig", lambda **kwargs: kwargs)

    class Client:
        def query(self, query: str, **kwargs: Any) -> Any:
            calls.append({"query": query, **kwargs})
            return types.SimpleNamespace(result=lambda: [{"staged_only_rows": 0, "stable_only_rows": 0}])

    matcher._require_stable_partition_equals_stage(
        Client(), "project.input.stable", "project.input.stage", "2026-07-09", matcher.ARTIFACTS[0], 100
    )

    assert "to_json_string(struct(" in calls[0]["query"]
    assert "except distinct" in calls[0]["query"]
    assert "job_id" not in calls[0]
    assert calls[0]["job_config"]["use_query_cache"] is False


def test_stable_partition_content_comparison_rejects_one_sided_difference(monkeypatch: pytest.MonkeyPatch) -> None:
    matcher = _load_matcher()
    monkeypatch.setattr(matcher.bigquery, "QueryJobConfig", lambda **kwargs: kwargs)
    client = types.SimpleNamespace(
        query=lambda *_args, **_kwargs: types.SimpleNamespace(
            result=lambda: [{"staged_only_rows": 0, "stable_only_rows": 1}]
        )
    )

    with pytest.raises(RuntimeError, match="not content-identical"):
        matcher._require_stable_partition_equals_stage(
            client, "project.input.stable", "project.input.stage", "2026-07-09", matcher.ARTIFACTS[0], 100
        )


def test_stable_bootstrap_is_idempotent_and_partitioned() -> None:
    matcher = _load_matcher()

    class Client:
        def __init__(self) -> None:
            self.dataset: Any = None
            self.tables: dict[str, Any] = {}

        def create_dataset(self, dataset: Any, *, exists_ok: bool) -> None:
            assert exists_ok
            self.dataset = dataset

        def get_dataset(self, _dataset_id: str) -> Any:
            return self.dataset

        def create_table(self, table: Any, *, exists_ok: bool) -> None:
            assert exists_ok
            self.tables.setdefault(table.table_id, table)

        def get_table(self, table_id: str) -> Any:
            return self.tables[table_id]

    client = Client()
    matcher._ensure_stable_input_tables(client, "matcher_input")
    matcher._ensure_stable_input_tables(client, "matcher_input")
    assert len(client.tables) == 4
    assert all(table.time_partitioning.require_partition_filter for table in client.tables.values())


def test_stage_failure_prevents_transaction(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _pending(matcher)
    marker["immutable_run_identity"] = matcher._immutable_matcher_run_identity(marker)
    calls: list[str] = []
    monkeypatch.setattr(matcher.MatcherConfig, "from_env", lambda: config)
    monkeypatch.setattr(matcher.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher, "_read_validated_marker", lambda *_args: marker)
    monkeypatch.setattr(matcher, "_ensure_stable_input_tables", lambda *_args: None)
    monkeypatch.setattr(
        matcher,
        "_pending_tables",
        lambda *_args: {key: {"table_id": value["table_id"]} for key, value in marker["tables"].items()},
    )
    monkeypatch.setattr(matcher, "_require_exact_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(matcher, "_stage_artifact", lambda *_args: (_ for _ in ()).throw(RuntimeError("stage failed")))
    monkeypatch.setattr(matcher, "_query_job", lambda *_args: calls.append("transaction"))

    with pytest.raises(RuntimeError, match="stage failed"):
        matcher.publish_staged_artifacts("2026-07-09", "run")
    assert calls == []


def test_matcher_command_is_parsed_and_builds_prepare_argv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_COMMAND", "uv run --locked --project /opt/airflow/matcher ztm-matcher")
    matcher = _load_matcher()
    config = matcher.MatcherConfig.from_env()

    assert config.command == ("uv", "run", "--locked", "--project", "/opt/airflow/matcher", "ztm-matcher")
    assert matcher._matcher_argv(
        config, "2026-07-09", "snapshot", True, tmp_path / "gps", tmp_path / "gtfs.zip", tmp_path / "out"
    )[-8:] == ["--threads", "2", "--alignment-workers", "1", "--memory-limit", "384MB", "--temp-limit", "20GB"]


@pytest.mark.parametrize(
    ("command", "error"),
    [("", "must not be empty"), ("uv run --locked --project /other ztm-matcher", "must match")],
)
def test_matcher_command_rejects_invalid_project(monkeypatch: pytest.MonkeyPatch, command: str, error: str) -> None:
    monkeypatch.setenv("MATCHER_COMMAND", command)
    matcher = _load_matcher()

    with pytest.raises(ValueError, match=error):
        matcher.MatcherConfig.from_env().validate()


def test_matcher_command_rejects_malformed_quoting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MATCHER_COMMAND", "uv run --project '/opt/airflow/matcher ztm-matcher")
    matcher = _load_matcher()

    with pytest.raises(ValueError, match="invalid shell-style quoting"):
        matcher.MatcherConfig.from_env()


def test_invoke_rejects_missing_project_directory(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, project_dir=tmp_path / "missing")

    with pytest.raises(RuntimeError, match="not mounted"):
        matcher._invoke_matcher(["matcher"], config)


def test_invoke_uses_configured_command_and_timeout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, project_dir=tmp_path)
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(matcher.subprocess, "run", lambda _argv, **kwargs: calls.append(kwargs))

    matcher._invoke_matcher(["matcher", "prepare"], config)

    assert calls == [{"cwd": tmp_path, "check": True, "timeout": 1, "shell": False}]


def test_gcs_paths_and_destinations_reject_traversal(tmp_path: Path) -> None:
    matcher = _load_matcher()
    for name in ("/absolute/part.parquet", "raw\\gps\\part.parquet", "raw/gps/../part.parquet"):
        with pytest.raises(ValueError, match="Unsafe"):
            matcher._strict_posix_name(name)
    relative = matcher._gcs_relative_name("raw/gps/vehicle_type=bus/date=2026-07-09/hour=01/part-a.parquet", "raw/gps")
    assert matcher._contained_destination(tmp_path, relative).is_relative_to(tmp_path)
    with pytest.raises(ValueError, match="outside expected"):
        matcher._gcs_relative_name("raw/other/part-a.parquet", "raw/gps")


def test_gps_inventory_requires_verified_object_metadata() -> None:
    matcher = _load_matcher()

    class Blob:
        name = "raw/gps/vehicle_type=bus/date=2026-07-09/hour=01/part-a.parquet"
        generation = "1"
        size = 4
        md5_hash = None
        crc32c = None

    class Bucket:
        def list_blobs(self, *, prefix: str) -> list[Blob]:
            assert prefix
            return [Blob()] if "vehicle_type=bus/date=2026-07-09" in prefix else []

    with pytest.raises(RuntimeError, match="hash metadata"):
        matcher._list_gps_objects(Bucket(), "2026-07-09", True)


def test_pending_table_identity_rejects_different_artifact(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    pending = _pending(matcher)
    pending["tables"]["trip"] = matcher._table_identity("matcher_stage", "run", matcher.ARTIFACTS[0], "b" * 64)

    with pytest.raises(RuntimeError, match="identity mismatch"):
        matcher._pending_tables(config, "run", pending)


def test_validated_marker_and_pending_metadata_are_bounded(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, max_marker_bytes=1)
    bucket = _MarkerBucket(b"{}")

    with pytest.raises(RuntimeError, match="exceeds"):
        matcher._read_validated_marker(_Storage(bucket), config, "2026-07-09", "run")
    assert bucket.item.download_called is False
    with pytest.raises(RuntimeError, match="exceeds"):
        matcher._write_pending(_Storage(_MarkerBucket(None)), config, "2026-07-09", "run", {"x": "y"})


def test_published_marker_is_create_only_and_idempotent(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _published_marker(matcher)
    bucket = _MarkerBucket(None)

    uri = matcher._write_published_marker(_Storage(bucket), config, "2026-07-09", "run", marker)

    assert uri.endswith("published.json")
    assert bucket.item.if_generation_match == 0
    assert (
        matcher._write_published_marker(
            _Storage(_MarkerBucket(bucket.item.payload)), config, "2026-07-09", "run", marker
        )
        == uri
    )


def test_published_marker_retry_preserves_create_once_payload(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _published_marker(matcher) | {"pre_publication_partition_counts": {"trip": {"total_rows": 3}}}
    bucket = _MarkerBucket(None)
    matcher._write_published_marker(_Storage(bucket), config, "2026-07-09", "run", marker)

    retry = _published_marker(matcher) | {"pre_publication_partition_counts": {"trip": {"total_rows": 99}}}
    matcher._write_published_marker(_Storage(_MarkerBucket(bucket.item.payload)), config, "2026-07-09", "run", retry)
    changed = _published_marker(matcher)
    changed["stable_inputs"]["trip"]["rows"] = 2
    changed["publication_identity"] = matcher._published_marker_identity_payload(changed)
    with pytest.raises(RuntimeError, match="different content"):
        matcher._write_published_marker(
            _Storage(_MarkerBucket(bucket.item.payload)), config, "2026-07-09", "run", changed
        )


def test_manifest_requires_complete_artifacts_and_binds_hashes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 2, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    manifest = {
        "processing_date": "2026-07-09",
        "snapshot_id": "snapshot",
        "config": {"include_prior_gps": True},
        "schema_versions": {
            **{
                Path(spec.filename).stem: matcher.ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem]
                for spec in matcher.ARTIFACTS
            },
            "trip_universe": matcher.ARTIFACT_SCHEMA_VERSIONS["trip_universe"],
        },
        "outputs": {
            **{
                Path(spec.filename).stem: {"sha256": artifact.sha256, "bytes": artifact.bytes}
                for spec in matcher.ARTIFACTS
            },
            "trip_universe": {"sha256": artifact.sha256, "bytes": artifact.bytes},
        },
    }
    (tmp_path / "manifest.json").write_text(__import__("json").dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(matcher, "_inspect_artifact", lambda *_args: artifact)
    monkeypatch.setattr(matcher, "_validate_artifact_relationships", lambda *_args: None)
    monkeypatch.setattr(
        matcher, "_read_metrics", lambda *_args: {Path(spec.filename).stem: artifact.rows for spec in matcher.ARTIFACTS}
    )

    validated, _ = matcher._validate_outputs(tmp_path, "2026-07-09", "snapshot", True)

    assert set(validated) == {spec.key for spec in matcher.ARTIFACTS} | {"trip_universe"}
    del manifest["outputs"]["stop_semantics"]
    (tmp_path / "manifest.json").write_text(__import__("json").dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="missing required"):
        matcher._validate_outputs(tmp_path, "2026-07-09", "snapshot", True)


def test_loaded_repeated_fields_must_match_artifact_evidence(tmp_path: Path) -> None:
    matcher = _load_matcher()
    artifact = matcher.ArtifactValidation(
        tmp_path / "trip.parquet", 2, "a" * 64, 7, ("2026-07-09",), (("quality_flags", 2),)
    )

    class Client:
        def __init__(self, count: int) -> None:
            self.count = count

        def query(self, _query: str, **_kwargs: Any) -> Any:
            return types.SimpleNamespace(result=lambda: [{"quality_flags": self.count}])

    matcher._verify_loaded_repeated_fields(Client(2), "project.stage.trip", artifact)
    with pytest.raises(RuntimeError, match="lost repeated-field"):
        matcher._verify_loaded_repeated_fields(Client(0), "project.stage.trip", artifact)


def test_load_job_retry_validates_content_bound_identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()

    def load_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(matcher.bigquery, "SchemaField", lambda *args, **kwargs: (args, kwargs), raising=False)
    monkeypatch.setattr(matcher.bigquery, "LoadJobConfig", load_job_config, raising=False)
    monkeypatch.setattr(
        matcher.bigquery, "ParquetOptions", type("ParquetOptions", (), {"enable_list_inference": False}), raising=False
    )
    matcher.bigquery.WriteDisposition = types.SimpleNamespace(WRITE_TRUNCATE="WRITE_TRUNCATE")
    artifact = matcher.ArtifactValidation(tmp_path / "trip.parquet", 1, "a" * 64, 7, ("2026-07-09",))
    artifact.path.write_bytes(b"parquet")
    spec = matcher.ARTIFACTS[0]
    identity = matcher._table_identity("matcher_stage", "run", spec, artifact.sha256)
    client = _LoadClient(_LoadJob(identity["job_id"], identity["table_id"]), conflict=True)

    assert matcher._load_artifact(client, "matcher_stage", "run", spec, artifact) == identity
    assert client.job.result_called is True
    assert client.updated_fields == ["expires"]
    assert datetime.now(UTC) + timedelta(days=2) < client.table.expires < datetime.now(UTC) + timedelta(days=4)
    with pytest.raises(RuntimeError, match="destination"):
        matcher._load_artifact(
            _LoadClient(_LoadJob(identity["job_id"], "project.stage.other"), conflict=True),
            "matcher_stage",
            "run",
            spec,
            artifact,
        )


def test_query_retry_rejects_different_query_or_destination() -> None:
    matcher = _load_matcher()
    job = types.SimpleNamespace(
        job_id="job", query="select 1", state="DONE", error_result=None, destination="project.input.other"
    )

    with pytest.raises(RuntimeError, match="destination"):
        matcher._verify_query_job_identity(job, "select 1", "job", 1, [], "project.input.expected")
    with pytest.raises(RuntimeError, match="text"):
        matcher._verify_query_job_identity(job, "select 2", "job", 1, [])


def test_stage_retry_rejects_tampered_stage_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    matcher = _load_matcher()
    spec = matcher.ARTIFACTS[0]
    labels = matcher._stage_labels(spec, "a" * 64)
    fields = [
        types.SimpleNamespace(name=name, field_type=field_type, mode=mode)
        for name, field_type, mode in matcher._expected_schema(spec)
    ]

    def query_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(matcher.bigquery, "QueryJobConfig", query_job_config)

    class Client:
        def query(self, query: str, **_kwargs: Any) -> Any:
            self.query_text = query
            raise Conflict("retry")

        def get_job(self, job_id: str, **_kwargs: Any) -> Any:
            return types.SimpleNamespace(
                job_id=job_id,
                query=self.query_text,
                state="DONE",
                error_result=None,
                destination="project.input.stage",
                result=list,
            )

        def get_table(self, _table_id: str) -> Any:
            return types.SimpleNamespace(
                schema=fields,
                labels=labels | {"matcher_artifact_sha256": "b" * 64},
                time_partitioning=types.SimpleNamespace(field="gps_date"),
            )

    client = Client()
    with pytest.raises(RuntimeError, match="labels"):
        matcher._stage_artifact(
            client, "project.stage.run", "project.input.stage", "2026-07-09", "run", spec, "a" * 64, 100, 3
        )
    assert "expiration_timestamp=timestamp_add(current_timestamp(), interval 3 day)" in client.query_text


def test_run_load_writes_pending_after_all_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_run_load(monkeypatch, matcher, config, artifact, events)

    context = matcher.run_matcher_load("2026-07-09", "snapshot", "run", include_prior_gps=True)

    assert context["status"] == "loaded_pending"
    assert events == ["matcher", "load", "load", "load", "load", "pending"]


def test_stale_marker_cleanup_uses_type_specific_retention_and_excludes_current_run(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    now = datetime(2026, 7, 15, tzinfo=UTC)

    class Blob:
        def __init__(
            self,
            name: str,
            updated: datetime | None,
            *,
            generation: int | None = 1,
            size: int = 10,
        ) -> None:
            self.name = name
            self.updated = updated
            self.generation = generation
            self.size = size

    blobs = [
        Blob("matcher/runs/processing_date=2026-07-01/run_id=old/pending.json", now - timedelta(days=4)),
        Blob("matcher/runs/processing_date=2026-07-01/run_id=old/validated.json", now - timedelta(days=4)),
        Blob("matcher/runs/processing_date=2026-07-01/run_id=old/published.json", now - timedelta(days=14)),
        Blob("matcher/runs/processing_date=2026-06-01/run_id=expired/published.json", now - timedelta(days=31)),
        Blob(
            f"matcher/runs/processing_date=2026-07-09/run_id={matcher._run_id('run')}/pending.json",
            now - timedelta(days=4),
        ),
        Blob("matcher/runs/processing_date=2026-07-14/run_id=recent/pending.json", now - timedelta(days=1)),
        Blob("matcher/runs/processing_date=2026-07-01/run_id=unknown/pending.json", None),
        Blob("matcher/runs/processing_date=2026-07-01/run_id=unknown/other.json", now - timedelta(days=31)),
    ]
    deleted: list[tuple[str, int | None]] = []

    class Bucket:
        def list_blobs(self, *, prefix: str) -> list[Blob]:
            return [blob for blob in blobs if blob.name.startswith(prefix)]

        def blob(self, name: str) -> Any:
            return types.SimpleNamespace(
                delete=lambda *, if_generation_match: deleted.append((name, if_generation_match))
            )

    deleted_count, deleted_bytes = matcher._cleanup_stale_run_markers(
        _Storage(Bucket()), config, "2026-07-09", "run", now
    )

    assert deleted_count == 3
    assert deleted_bytes == 30
    assert deleted == [
        ("matcher/runs/processing_date=2026-07-01/run_id=old/pending.json", 1),
        ("matcher/runs/processing_date=2026-07-01/run_id=old/validated.json", 1),
        ("matcher/runs/processing_date=2026-06-01/run_id=expired/published.json", 1),
    ]


def test_staging_retention_maintains_only_exact_transient_table_prefixes(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    now = datetime(2026, 7, 15, tzinfo=UTC)
    tables = {
        "ztm-data.matcher_stage.matcher_run_trip_old": types.SimpleNamespace(
            created=now - timedelta(days=4), expires=None
        ),
        "ztm-data.matcher_stage.matcher_run_trip_recent": types.SimpleNamespace(
            created=now - timedelta(days=1), expires=None
        ),
        "ztm-data.matcher_stage.unrelated": types.SimpleNamespace(created=now - timedelta(days=30), expires=None),
        "ztm-data.matcher_input.matcher_run_stage_trip_old": types.SimpleNamespace(
            created=now - timedelta(days=4), expires=None
        ),
        "ztm-data.matcher_input.reconstruction_trip_facts": types.SimpleNamespace(
            created=now - timedelta(days=30), expires=None
        ),
    }
    deleted: list[str] = []
    updated: list[str] = []

    class Client:
        def list_tables(self, dataset_id: str) -> list[Any]:
            prefix = f"{dataset_id}."
            return [
                types.SimpleNamespace(table_id=name.removeprefix(prefix)) for name in tables if name.startswith(prefix)
            ]

        def get_table(self, table_id: str) -> Any:
            return tables[table_id]

        def delete_table(self, table_id: str, *, not_found_ok: bool) -> None:
            assert not_found_ok
            deleted.append(table_id)

        def update_table(self, table: Any, fields: list[str]) -> Any:
            assert fields == ["expires"]
            updated.append(next(name for name, candidate in tables.items() if candidate is table))
            return table

    result = matcher._maintain_staging_table_retention(Client(), config, now)

    assert result == (2, 1)
    assert deleted == [
        "ztm-data.matcher_stage.matcher_run_trip_old",
        "ztm-data.matcher_input.matcher_run_stage_trip_old",
    ]
    assert updated == ["ztm-data.matcher_stage.matcher_run_trip_recent"]
    assert tables[updated[0]].expires == datetime(2026, 7, 17, tzinfo=UTC)


def test_run_load_does_not_write_pending_after_artifact_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_run_load(monkeypatch, matcher, config, artifact, events)
    monkeypatch.setattr(
        matcher, "_validate_outputs", lambda *_args: (_ for _ in ()).throw(RuntimeError("artifact failure"))
    )

    with pytest.raises(RuntimeError, match="artifact failure"):
        matcher.run_matcher_load("2026-07-09", "snapshot", "run", include_prior_gps=True)
    assert events == ["matcher"]


def test_run_load_rejects_conflicting_validated_marker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_run_load(monkeypatch, matcher, config, artifact, events)
    conflicting = _pending(matcher)
    conflicting["snapshot_id"] = "other-snapshot"
    conflicting["immutable_run_identity"] = matcher._immutable_matcher_run_identity(conflicting)
    monkeypatch.setattr(matcher, "_read_validated_marker", lambda *_args: conflicting)

    with pytest.raises(RuntimeError, match="different immutable"):
        matcher.run_matcher_load("2026-07-09", "snapshot", "run", include_prior_gps=True)
    assert events == ["matcher"]


@pytest.mark.parametrize("mutation", ["addition", "replacement"])
def test_approved_inventory_mismatch_blocks_before_matcher_invocation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutation: str
) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_run_load(monkeypatch, matcher, config, artifact, events)
    planned = _pending(matcher)
    actual = _pending(matcher)
    if mutation == "addition":
        actual["gps_inventory"].append({**actual["gps_inventory"][0], "name": "raw/gps/extra.parquet"})
    else:
        actual["gps_inventory"][0]["generation"] = "2"
    monkeypatch.setattr(
        matcher,
        "_download_inputs",
        lambda *_args: (actual["gps_inventory"], actual["gtfs_inventory"], Path("gps"), Path("gtfs.zip")),
    )
    expected = matcher.matcher_input_inventory_digest(
        processing_date="2026-07-09",
        snapshot_id="snapshot",
        snapshot_gcs_path="gs://bucket/raw/gtfs/snapshot.zip",
        include_prior_gps=True,
        input_dates=["2026-07-08", "2026-07-09"],
        gtfs_object=planned["gtfs_inventory"],
        gps_objects=planned["gps_inventory"],
    )

    with pytest.raises(RuntimeError, match="differs from approved"):
        matcher.run_matcher_load(
            "2026-07-09", "snapshot", "run", include_prior_gps=True, expected_input_inventory_digest=expected
        )
    assert events == []


def test_legacy_validated_marker_requires_new_run_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_run_load(monkeypatch, matcher, config, artifact, events)
    monkeypatch.setattr(
        matcher, "_read_validated_marker", lambda *_args: {"run_id": "run", "processing_date": "2026-07-09"}
    )

    with pytest.raises(RuntimeError, match="new Airflow run ID"):
        matcher.run_matcher_load("2026-07-09", "snapshot", "run", include_prior_gps=True)
    assert events == []


def test_historical_run_without_digest_cannot_execute(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    monkeypatch.setattr(matcher.MatcherConfig, "from_env", lambda: config)
    monkeypatch.setattr(
        matcher.bigquery, "Client", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not run"))
    )

    with pytest.raises(RuntimeError, match="require a valid expected_input_inventory_digest"):
        matcher.run_matcher_load(
            "2026-07-09", "snapshot", "matcher-historical-correction__v4-plan__2026-07-09", include_prior_gps=True
        )


def test_download_rejects_digest_mismatch_before_any_file_transfer(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    downloads: list[Path] = []

    class Blob:
        generation = "1"
        size = 1
        md5_hash = "hash"
        crc32c = None

        def __init__(self, name: str) -> None:
            self.name = name

        def download_to_filename(self, destination: Path) -> None:
            downloads.append(destination)

    class GpsBucket:
        def list_blobs(self, *, prefix: str) -> list[Blob]:
            return [Blob(f"{prefix}hour=01/part-a.parquet")]

        def blob(self, name: str, *, generation: str) -> Blob:
            return Blob(name)

    class GtfsBucket:
        def get_blob(self, name: str) -> Blob:
            return Blob(name)

        def blob(self, name: str, *, generation: str) -> Blob:
            return Blob(name)

    class StorageClient:
        def bucket(self, name: str) -> Any:
            return GpsBucket() if name == matcher.GCS_BUCKET else GtfsBucket()

    with pytest.raises(RuntimeError, match="differs from approved"):
        matcher._download_inputs(
            StorageClient(),
            config,
            "2026-07-09",
            True,
            "snapshot",
            "gs://gtfs/raw/gtfs/snapshot.zip",
            tmp_path,
            "0" * 64,
        )
    assert downloads == []


def test_download_checks_inventory_bounds_before_writing_files(tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path, max_gps_objects=1)
    downloads: list[Path] = []

    class Blob:
        generation = "1"
        size = 1
        md5_hash = "hash"
        crc32c = None

        def __init__(self, name: str) -> None:
            self.name = name

        def download_to_filename(self, destination: Path) -> None:
            downloads.append(destination)

    class GpsBucket:
        def list_blobs(self, *, prefix: str) -> list[Blob]:
            return [Blob(f"{prefix}hour=01/part-a.parquet"), Blob(f"{prefix}hour=02/part-b.parquet")]

    class GtfsBucket:
        def get_blob(self, _name: str) -> Blob:
            return Blob("raw/gtfs/snapshot.zip")

    class StorageClient:
        def bucket(self, name: str) -> Any:
            return GpsBucket() if name == matcher.GCS_BUCKET else GtfsBucket()

    with pytest.raises(RuntimeError, match="object count"):
        matcher._download_inputs(
            StorageClient(), config, "2026-07-09", True, "snapshot", "gs://gtfs/raw/gtfs/snapshot.zip", tmp_path
        )
    assert downloads == []


def test_run_workspace_cannot_contain_project_directory(tmp_path: Path) -> None:
    matcher = _load_matcher()
    workspace = tmp_path / "run"
    config = _config(matcher, tmp_path, project_dir=workspace / "matcher")

    with pytest.raises(ValueError, match="must not be inside"):
        matcher._validate_run_workspace(config, workspace)


def test_run_load_cleans_workspace_after_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    artifact = matcher.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    _stub_run_load(monkeypatch, matcher, config, artifact, [])
    monkeypatch.setattr(matcher, "_invoke_matcher", lambda *_args: (_ for _ in ()).throw(RuntimeError("invoke failed")))

    with pytest.raises(RuntimeError, match="invoke failed"):
        matcher.run_matcher_load("2026-07-09", "snapshot", "run", include_prior_gps=True)
    assert not (tmp_path / matcher._run_id("run")).exists()


def test_publication_writes_published_marker_after_post_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _pending(matcher)
    marker["validation"] = {"status": "pass"}
    marker["immutable_run_identity"] = matcher._immutable_matcher_run_identity(marker)
    queries: list[str] = []
    written: list[dict[str, object]] = []
    post_publish_events: list[str] = []
    monkeypatch.setattr(matcher.MatcherConfig, "from_env", lambda: config)
    monkeypatch.setattr(matcher.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher, "_read_validated_marker", lambda *_args: marker)
    monkeypatch.setattr(matcher, "_ensure_stable_input_tables", lambda *_args: None)
    monkeypatch.setattr(
        matcher,
        "_pending_tables",
        lambda *_args: {key: {"table_id": value["table_id"]} for key, value in marker["tables"].items()},
    )
    monkeypatch.setattr(matcher, "_require_exact_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(matcher, "_stage_artifact", lambda *_args: None)
    monkeypatch.setattr(matcher, "_verify_table_contract", lambda *_args: None)
    monkeypatch.setattr(
        matcher, "_stable_partition_counts", lambda *_args: {"total_rows": 1, "wrong_processing_date_rows": 0}
    )
    monkeypatch.setattr(matcher, "_require_stable_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(matcher, "_require_stable_partition_equals_stage", lambda *_args: None)
    monkeypatch.setattr(matcher, "_query_job", lambda _client, query, *_args: queries.append(query))
    monkeypatch.setattr(
        matcher,
        "_write_published_marker",
        lambda _client, _config, _date, _run, payload: (
            written.append(payload) or post_publish_events.append("marker") or "gs://published"
        ),
    )
    monkeypatch.setattr(
        matcher,
        "_delete_publication_stages_best_effort",
        lambda *_args: post_publish_events.append("delete_stages"),
    )
    monkeypatch.setattr(
        matcher,
        "_cleanup_stale_run_markers_best_effort",
        lambda *_args: post_publish_events.append("cleanup_markers"),
    )

    result = matcher.publish_staged_artifacts("2026-07-09", "run")

    assert result["status"] == "published"
    assert len(queries) == 1
    assert written[0]["transaction_job_id"].startswith("matcher_publish_v2_replace_all")
    assert post_publish_events == ["marker", "delete_stages", "cleanup_markers"]


def test_publication_stage_cleanup_deletes_only_run_scoped_stage_tables(tmp_path: Path) -> None:
    matcher = _load_matcher()
    published = {
        spec.key: {
            "staged_table": f"project.matcher_input.matcher_run_stage_{spec.key}",
            "stable_table": f"project.matcher_input.{matcher.STABLE_INPUT_TABLES[spec.key]}",
        }
        for spec in matcher.ARTIFACTS
    }
    deleted: list[tuple[str, bool]] = []
    client = types.SimpleNamespace(
        delete_table=lambda table_id, *, not_found_ok: deleted.append((table_id, not_found_ok))
    )

    matcher._delete_publication_stages_best_effort(client, published)

    assert deleted == [(f"project.matcher_input.matcher_run_stage_{spec.key}", True) for spec in matcher.ARTIFACTS]
    assert not any(
        table_id.endswith(matcher.STABLE_INPUT_TABLES[spec.key])
        for table_id, _ in deleted
        for spec in matcher.ARTIFACTS
    )


def test_publication_stage_cleanup_continues_after_one_delete_failure(tmp_path: Path) -> None:
    matcher = _load_matcher()
    published = {
        spec.key: {"staged_table": f"project.matcher_input.matcher_run_stage_{spec.key}"} for spec in matcher.ARTIFACTS
    }
    attempted: list[str] = []

    def delete_table(table_id: str, *, not_found_ok: bool) -> None:
        assert not_found_ok
        attempted.append(table_id)
        if len(attempted) == 2:
            raise RuntimeError("delete unavailable")

    matcher._delete_publication_stages_best_effort(
        types.SimpleNamespace(delete_table=delete_table),
        published,
    )

    assert attempted == [f"project.matcher_input.matcher_run_stage_{spec.key}" for spec in matcher.ARTIFACTS]


def test_reused_transaction_cannot_certify_newer_partition_replacement(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    matcher = _load_matcher()
    config = _config(matcher, tmp_path)
    marker = _pending(matcher)
    marker["validation"] = {"status": "pass"}
    marker["immutable_run_identity"] = matcher._immutable_matcher_run_identity(marker)
    writes: list[dict[str, object]] = []
    transaction_jobs: list[str] = []
    monkeypatch.setattr(matcher.MatcherConfig, "from_env", lambda: config)
    monkeypatch.setattr(matcher.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher, "_read_validated_marker", lambda *_args: marker)
    monkeypatch.setattr(matcher, "_ensure_stable_input_tables", lambda *_args: None)
    monkeypatch.setattr(
        matcher,
        "_pending_tables",
        lambda *_args: {key: {"table_id": value["table_id"]} for key, value in marker["tables"].items()},
    )
    monkeypatch.setattr(matcher, "_require_exact_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(matcher, "_stage_artifact", lambda *_args: None)
    monkeypatch.setattr(matcher, "_verify_table_contract", lambda *_args: None)
    monkeypatch.setattr(
        matcher, "_stable_partition_counts", lambda *_args: {"total_rows": 1, "wrong_processing_date_rows": 0}
    )
    monkeypatch.setattr(matcher, "_require_stable_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(
        matcher,
        "_query_job",
        lambda _client, _query, job_id, *_args: transaction_jobs.append(job_id) or types.SimpleNamespace(reused=True),
    )
    monkeypatch.setattr(
        matcher,
        "_require_stable_partition_equals_stage",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("newer partition replacement")),
    )
    monkeypatch.setattr(
        matcher,
        "_write_published_marker",
        lambda *_args: writes.append({}) or "gs://published",
    )

    with pytest.raises(RuntimeError, match="newer partition replacement"):
        matcher.publish_staged_artifacts("2026-07-09", "run")
    assert transaction_jobs[0].startswith("matcher_publish_v2_replace_all")
    assert writes == []


def test_inspection_rejects_artifact_outside_requested_lineage(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    matcher = _load_matcher()
    spec = matcher.ArtifactSpec(
        "test",
        "lineage.parquet",
        "test",
        (
            matcher.FieldSpec("processing_date", "DATE"),
            matcher.FieldSpec("gps_date", "DATE"),
            matcher.FieldSpec("service_date", "DATE"),
            matcher.FieldSpec("gtfs_snapshot_id", "STRING"),
            matcher.FieldSpec("trip_id", "STRING"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id"),
        "gps_date",
    )
    path = tmp_path / spec.filename
    pq.write_table(
        pa.table(
            {
                "processing_date": [date(2026, 7, 9)],
                "gps_date": [date(2026, 7, 8)],
                "service_date": [date(2026, 7, 9)],
                "gtfs_snapshot_id": ["snapshot"],
                "trip_id": ["trip"],
            }
        ),
        path,
    )

    with pytest.raises(RuntimeError, match="Lineage mismatch"):
        matcher._inspect_artifact(path, spec, "2026-07-09", "snapshot")


def test_inspection_rejects_duplicate_grain(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    matcher = _load_matcher()
    spec = matcher.ArtifactSpec(
        "test",
        "duplicate.parquet",
        "test",
        (
            matcher.FieldSpec("processing_date", "DATE"),
            matcher.FieldSpec("gps_date", "DATE"),
            matcher.FieldSpec("service_date", "DATE"),
            matcher.FieldSpec("gtfs_snapshot_id", "STRING"),
            matcher.FieldSpec("trip_id", "STRING"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id"),
        "gps_date",
    )
    path = tmp_path / spec.filename
    pq.write_table(
        pa.table(
            {
                "processing_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "gps_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "service_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "gtfs_snapshot_id": ["snapshot", "snapshot"],
                "trip_id": ["trip", "trip"],
            }
        ),
        path,
    )

    with pytest.raises(RuntimeError, match="Duplicate"):
        matcher._inspect_artifact(path, spec, "2026-07-09", "snapshot")


class _MarkerBlob:
    def __init__(self, existing: bytes | None) -> None:
        self.existing = existing
        self.payload = b""
        self.if_generation_match: int | None = None
        self.download_called = False

    @property
    def size(self) -> int:
        return len(self.existing if self.existing is not None else self.payload)

    def exists(self) -> bool:
        return self.existing is not None or bool(self.payload)

    def reload(self) -> None:
        return None

    def upload_from_string(self, payload: bytes, **kwargs: Any) -> None:
        self.if_generation_match = kwargs.get("if_generation_match")
        if self.existing is not None:
            raise PreconditionFailed("exists")
        self.payload = payload

    def download_as_bytes(self) -> bytes:
        self.download_called = True
        return self.existing if self.existing is not None else self.payload


class _MarkerBucket:
    def __init__(self, existing: bytes | None) -> None:
        self.item = _MarkerBlob(existing)

    def blob(self, _name: str) -> _MarkerBlob:
        return self.item


class _Storage:
    def __init__(self, bucket: _MarkerBucket) -> None:
        self.bucket_instance = bucket

    def bucket(self, _name: str) -> _MarkerBucket:
        return self.bucket_instance


class _LoadJob:
    def __init__(self, job_id: str, destination: str) -> None:
        self.job_id = job_id
        self.destination = destination
        self.state = "DONE"
        self.error_result = None
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class _LoadClient:
    def __init__(self, job: _LoadJob, *, conflict: bool) -> None:
        self.job = job
        self.conflict = conflict
        self.table = types.SimpleNamespace(expires=None)
        self.updated_fields: list[str] = []

    def load_table_from_file(self, _source: Any, _table: str, **_kwargs: Any) -> _LoadJob:
        if self.conflict:
            raise Conflict("already exists")
        return self.job

    def get_job(self, _job_id: str, **_kwargs: Any) -> _LoadJob:
        return self.job

    def get_table(self, _table_id: str) -> Any:
        return self.table

    def update_table(self, table: Any, fields: list[str]) -> Any:
        self.updated_fields.extend(fields)
        return table


def _stub_run_load(
    monkeypatch: pytest.MonkeyPatch,
    matcher: types.ModuleType,
    config: Any,
    artifact: Any,
    events: list[str],
) -> None:
    monkeypatch.setattr(matcher.MatcherConfig, "from_env", lambda: config)
    monkeypatch.setattr(matcher, "_snapshot_gcs_path", lambda *_args: "gs://bucket/raw/gtfs/snapshot.zip")
    pending = _pending(matcher)
    monkeypatch.setattr(
        matcher,
        "_download_inputs",
        lambda *_args: (pending["gps_inventory"], pending["gtfs_inventory"], Path("gps"), Path("gtfs.zip")),
    )
    monkeypatch.setattr(matcher, "_invoke_matcher", lambda *_args: events.append("matcher"))
    monkeypatch.setattr(
        matcher,
        "_validate_outputs",
        lambda *_args: (
            {spec.key: artifact for spec in matcher.ARTIFACTS} | {"trip_universe": artifact},
            {"metrics": {}},
        ),
    )
    monkeypatch.setattr(
        matcher,
        "_load_artifact",
        lambda _client, dataset, run_id, spec, loaded, *_args: (
            events.append("load") or matcher._table_identity(dataset, run_id, spec, loaded.sha256)
        ),
    )
    monkeypatch.setattr(matcher, "_write_pending", lambda *_args: events.append("pending") or "gs://pending")
    monkeypatch.setattr(matcher, "_read_validated_marker", lambda *_args: None)
    monkeypatch.setattr(matcher.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(matcher, "_cleanup_stale_run_markers_best_effort", lambda *_args: None)
    monkeypatch.setattr(matcher, "_maintain_staging_table_retention_best_effort", lambda *_args: None)


def _config(matcher: types.ModuleType, workspace: Path, **overrides: Any) -> Any:
    values = {
        "enabled": True,
        "staging_dataset": "matcher_stage",
        "input_dataset": "matcher_input",
        "workspace_root": workspace,
        "command": ("uv", "run", "--project", "/opt/airflow/matcher", "ztm-matcher"),
        "project_dir": Path("/opt/airflow/matcher"),
        "timeout_seconds": 1,
        "marker_prefix": "matcher/runs",
    } | overrides
    return matcher.MatcherConfig(**values)


def _pending(matcher: types.ModuleType) -> dict[str, Any]:
    artifact_schema_versions = {
        spec.key: matcher.ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem] for spec in matcher.ARTIFACTS
    } | {"trip_universe": matcher.ARTIFACT_SCHEMA_VERSIONS["trip_universe"]}
    artifacts = {
        key: {
            "rows": 1,
            "sha256": "a" * 64,
            "schema_version": schema_version,
            "modes": (
                ["bus", "tram"]
                if (spec := next((item for item in matcher.ARTIFACTS if item.key == key), None))
                and any(field.name == "mode" for field in spec.fields)
                else []
            ),
        }
        for key, schema_version in artifact_schema_versions.items()
    }
    return {
        "processing_date": "2026-07-09",
        "run_id": "run",
        "snapshot_id": "snapshot",
        "snapshot_gcs_path": "gs://bucket/raw/gtfs/snapshot.zip",
        "include_prior_gps": True,
        "gps_input_dates": ["2026-07-08", "2026-07-09"],
        "gps_inventory": [
            {
                "name": "raw/gps/vehicle_type=bus/date=2026-07-09/hour=01/part-a.parquet",
                "generation": "1",
                "size": 1,
                "md5_hash": "hash",
                "crc32c": None,
            }
        ],
        "gtfs_inventory": {
            "name": "raw/gtfs/snapshot.zip",
            "generation": "1",
            "size": 1,
            "md5_hash": "hash",
            "crc32c": None,
        },
        "artifacts": artifacts,
        "tables": {
            spec.key: matcher._table_identity("matcher_stage", "run", spec, "a" * 64) for spec in matcher.ARTIFACTS
        },
        "metrics": {
            "peak_rss_bytes": 1,
            "current_swap_bytes": 0,
            "swapping_observed": False,
            "accepted_fact_executions": 1,
        },
    }


def _published_marker(matcher: types.ModuleType) -> dict[str, Any]:
    marker: dict[str, Any] = {
        "processing_date": "2026-07-09",
        "run_id": "run",
        "transaction_job_id": "transaction",
        "stable_inputs": {
            spec.key: {
                "stable_table": f"project.matcher_input.{matcher.STABLE_INPUT_TABLES[spec.key]}",
                "staged_table": f"project.matcher_input.stage_{spec.key}",
                "sha256": "a" * 64,
                "rows": 1,
            }
            for spec in matcher.ARTIFACTS
        },
    }
    marker["publication_identity"] = matcher._published_marker_identity_payload(marker)
    return marker


def _load_matcher() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()
    sys.modules.pop("ztm_airflow_common", None)
    sys.modules.pop("ztm_matcher", None)
    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    spec = importlib.util.spec_from_file_location("ztm_matcher", dag_dir / "ztm_matcher.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load matcher module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["ztm_matcher"] = module
    spec.loader.exec_module(module)
    return module
