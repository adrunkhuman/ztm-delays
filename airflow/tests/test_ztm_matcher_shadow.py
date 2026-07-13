from __future__ import annotations

import importlib.util
import json
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


def test_cutover_is_disabled_by_default_and_cannot_mutate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MATCHER_CUTOVER_ENABLED", raising=False)
    shadow = _load_shadow_module()

    assert shadow.promote_validated_shadow_artifacts("2026-07-09", "run") == {
        "enabled": False,
        "reason": "MATCHER_CUTOVER_ENABLED is false",
    }


@pytest.mark.parametrize("dataset", ["ztm_raw", "ztm_int", "ztm_marts", "matcher_shadow"])
def test_enabled_cutover_requires_an_isolated_shadow_and_input_dataset(
    monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    monkeypatch.setenv("MATCHER_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", dataset)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match="must not name"):
        shadow.CutoverConfig.from_env().validate(shadow.ShadowConfig.from_env())


def test_cutover_publication_args_are_exact_current_and_prior_without_execution() -> None:
    shadow = _load_shadow_module()

    publication = shadow.matcher_cutover_publication_dbt_args("2026-07-09", "snapshot")

    assert publication == {
        "current": {
            "selector": (
                "int_gtfs_processing_snapshot int_gtfs_trip_schedule_history int_schedule_version "
                "dim_schedule_version fct_trip fct_stop_arrival fct_expected_stop_event"
            ),
            "vars": {
                "processing_date": "2026-07-09",
                "gtfs_snapshot_id": "snapshot",
                "use_python_reconstruction": True,
                "publish_service_date": "2026-07-09",
            },
        },
        "prior": {
            "selector": (
                "int_gtfs_processing_snapshot int_gtfs_trip_schedule_history int_schedule_version "
                "dim_schedule_version fct_trip fct_stop_arrival fct_expected_stop_event"
            ),
            "vars": {
                "processing_date": "2026-07-09",
                "gtfs_snapshot_id": "snapshot",
                "use_python_reconstruction": True,
                "publish_service_date": "2026-07-08",
            },
        },
    }


def test_cutover_refuses_non_passing_marker_before_bigquery_mutation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MATCHER_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", "matcher_input")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(
        shadow,
        "_read_marker",
        lambda *_args: {"processing_date": "2026-07-09", "run_id": "run", "quality_gate": {"status": "warn"}},
    )
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: pytest.fail("unsafe cutover reached BigQuery"))

    with pytest.raises(RuntimeError, match="passing shadow quality gate"):
        shadow.promote_validated_shadow_artifacts("2026-07-09", "run")


def test_cutover_accepts_only_exact_bounded_swap_exception() -> None:
    shadow = _load_shadow_module()
    marker = {
        "quality_gate": {
            "status": "fail",
            "issues": [{"level": "fail", "category": "resource", "message": "swapping was observed"}],
        },
        "metrics": {"current_swap_bytes": 27_627_520},
    }
    exception = {
        "processing_date": "2026-07-10",
        "run_id": "run",
        "reason": "Reviewed cold-page swap with RSS below the production bound",
        "approved_by": "operator@example.com",
        "approved_at": "2026-07-13T01:45:00Z",
        "max_current_swap_bytes": 32 * 1024**2,
    }

    accepted = shadow._validate_manual_gate_exception("2026-07-10", "run", marker, exception)

    assert accepted == {
        **exception,
        "approved_at": "2026-07-13T01:45:00+00:00",
        "observed_current_swap_bytes": 27_627_520,
        "accepted_failure": {"level": "fail", "category": "resource", "message": "swapping was observed"},
    }
    with pytest.raises(RuntimeError, match="does not match"):
        shadow._validate_manual_gate_exception("2026-07-10", "other-run", marker, exception)
    with pytest.raises(RuntimeError, match="manual exception bound"):
        shadow._validate_manual_gate_exception(
            "2026-07-10",
            "run",
            marker,
            {**exception, "max_current_swap_bytes": shadow.MAX_MANUAL_SWAP_EXCEPTION_BYTES + 1},
        )
    with pytest.raises(RuntimeError, match="observed swap exceeds"):
        shadow._validate_manual_gate_exception(
            "2026-07-10", "run", marker, {**exception, "max_current_swap_bytes": 16 * 1024**2}
        )
    with pytest.raises(RuntimeError, match="sole swap failure"):
        shadow._validate_manual_gate_exception(
            "2026-07-10",
            "run",
            {
                **marker,
                "quality_gate": {
                    "status": "fail",
                    "issues": [
                        *marker["quality_gate"]["issues"],
                        {"level": "fail", "category": "retention", "message": "retention failed"},
                    ],
                },
            },
            exception,
        )
    with pytest.raises(RuntimeError, match="already passing"):
        shadow._validate_manual_gate_exception("2026-07-10", "run", {"quality_gate": {"status": "pass"}}, exception)


def test_promotion_identities_are_deterministic_and_bound_to_content_hash() -> None:
    shadow = _load_shadow_module()
    spec = shadow.ARTIFACTS[0]

    assert shadow._promotion_table_id(
        "matcher_input", "2026-07-09", "run", spec, "a" * 64
    ) == shadow._promotion_table_id("matcher_input", "2026-07-09", "run", spec, "a" * 64)
    assert shadow._promotion_job_id("replace", "2026-07-09", "run-a", spec, "a" * 64) != shadow._promotion_job_id(
        "replace", "2026-07-09", "run-a", spec, "b" * 64
    )
    assert shadow._promotion_job_id("replace", "2026-07-09", "run-a", spec, "a" * 64) != shadow._promotion_job_id(
        "replace", "2026-07-09", "run-b", spec, "a" * 64
    )


def test_promotion_transaction_replaces_all_partitions_with_explicit_columns() -> None:
    shadow = _load_shadow_module()
    promoted = {
        spec.key: {
            "stable_table": f"project.input.{shadow.STABLE_INPUT_TABLES[spec.key]}",
            "staged_table": f"project.input.stage_{spec.key}",
        }
        for spec in shadow.ARTIFACTS
    }

    query = shadow._promotion_transaction_query(promoted)

    assert query.count("delete from") == 4
    assert query.count("insert into") == 4
    assert query.count("@processing_date") == 8
    assert "reconstruction_stop_semantics` where processing_date = @processing_date" in query
    assert "begin transaction;" in query
    assert "commit transaction;" in query
    assert "select *" not in query


def test_partition_validation_rejects_wrong_gps_date_before_stable_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setattr(
        shadow,
        "_promotion_table_counts",
        lambda *_args: {
            "total_rows": 3,
            "partition_rows": 2,
            "wrong_partition_rows": 1,
            "wrong_processing_date_rows": 1,
        },
    )

    with pytest.raises(RuntimeError, match="row count does not equal"):
        shadow._require_exact_processing_partition(
            object(), "project.shadow.trip", "2026-07-09", shadow.ARTIFACTS[0], 3, "job", 1
        )


def test_stage_failure_prevents_any_stable_mutation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", "matcher_input")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    marker = {
        "processing_date": "2026-07-09",
        "run_id": "run",
        "quality_gate": {"status": "pass"},
        "artifacts": {spec.key: {"sha256": "a" * 64, "rows": 1} for spec in shadow.ARTIFACTS},
    }
    tables = {spec.key: shadow._table_identity("matcher_shadow", "run", spec, "a" * 64) for spec in shadow.ARTIFACTS}
    mutations: list[str] = []
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow, "_read_marker", lambda *_args: marker)
    monkeypatch.setattr(shadow, "_pending_tables", lambda *_args: tables)
    monkeypatch.setattr(shadow, "_require_exact_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(shadow, "_stage_artifact", lambda *_args: (_ for _ in ()).throw(RuntimeError("stage failed")))
    monkeypatch.setattr(shadow, "_query_job", lambda *_args: mutations.append("query"))

    with pytest.raises(RuntimeError, match="stage failed"):
        shadow.promote_validated_shadow_artifacts("2026-07-09", "run")

    assert mutations == []


def test_promotion_records_accepted_swap_exception(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", "matcher_input")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    marker = {
        "processing_date": "2026-07-10",
        "run_id": "run",
        "quality_gate": {
            "status": "fail",
            "issues": [{"level": "fail", "category": "resource", "message": "swapping was observed"}],
        },
        "metrics": {"current_swap_bytes": 27_627_520},
        "artifacts": {spec.key: {"sha256": "a" * 64, "rows": 1} for spec in shadow.ARTIFACTS},
    }
    tables = {spec.key: shadow._table_identity("matcher_shadow", "run", spec, "a" * 64) for spec in shadow.ARTIFACTS}
    written: list[dict[str, object]] = []
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow, "_read_marker", lambda *_args: marker)
    monkeypatch.setattr(shadow, "_pending_tables", lambda *_args: tables)
    monkeypatch.setattr(shadow, "_require_exact_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(shadow, "_stage_artifact", lambda *_args: None)
    monkeypatch.setattr(shadow, "_verify_table_contract", lambda *_args: None)
    monkeypatch.setattr(shadow, "_stable_partition_counts", lambda *_args: {"partition_rows": 0})
    monkeypatch.setattr(shadow, "_query_job", lambda *_args: None)
    monkeypatch.setattr(shadow, "_require_stable_processing_partition", lambda *_args: 1)
    monkeypatch.setattr(
        shadow,
        "_write_promotion_marker",
        lambda _client, _config, _date, _run, payload: written.append(payload) or "gs://bucket/promotion.json",
    )
    exception = {
        "processing_date": "2026-07-10",
        "run_id": "run",
        "reason": "Reviewed cold-page swap with RSS below the production bound",
        "approved_by": "operator@example.com",
        "approved_at": "2026-07-13T01:45:00Z",
        "max_current_swap_bytes": 32 * 1024**2,
    }

    result = shadow.promote_validated_shadow_artifacts("2026-07-10", "run", accepted_gate_exception=exception)

    assert result["status"] == "promoted"
    assert written[0]["accepted_gate_exception"] == {
        **exception,
        "approved_at": "2026-07-13T01:45:00+00:00",
        "observed_current_swap_bytes": 27_627_520,
        "accepted_failure": {"level": "fail", "category": "resource", "message": "swapping was observed"},
    }


def test_promotion_marker_is_create_only_and_idempotent(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher")
    marker = {"run_id": "run", "stable_inputs": {"trip": {"sha256": "a" * 64}}}
    bucket = FakeMarkerBucket(existing=None)

    uri = shadow._write_promotion_marker(FakeStorageClient(bucket), config, "2026-07-09", "run", marker)

    assert uri.endswith("promotion.json")
    assert bucket.blob_instance.if_generation_match == 0
    same = FakeMarkerBucket(existing=bucket.blob_instance.payload)
    assert shadow._write_promotion_marker(FakeStorageClient(same), config, "2026-07-09", "run", marker) == uri


def test_stable_inputs_must_keep_the_configured_partition_field() -> None:
    shadow = _load_shadow_module()
    client = types.SimpleNamespace(
        get_table=lambda _table_id: types.SimpleNamespace(time_partitioning=types.SimpleNamespace(field="gps_date"))
    )

    shadow._require_partition_field(client, "project.matcher_input.reconstruction_trip_facts", shadow.ARTIFACTS[0])
    semantics = next(spec for spec in shadow.ARTIFACTS if spec.key == "stop_semantics")
    client.get_table = lambda _table_id: types.SimpleNamespace(
        time_partitioning=types.SimpleNamespace(field="processing_date")
    )
    shadow._require_partition_field(client, "project.matcher_input.reconstruction_stop_semantics", semantics)

    client.get_table = lambda _table_id: types.SimpleNamespace(
        time_partitioning=types.SimpleNamespace(field="service_date")
    )
    with pytest.raises(RuntimeError, match="partitioned by gps_date"):
        shadow._require_partition_field(client, "project.matcher_input.reconstruction_trip_facts", shadow.ARTIFACTS[0])


@pytest.mark.parametrize("dataset", ["", "ztm_raw", "ztm_int", "ztm_marts"])
def test_enabled_shadow_rejects_missing_or_canonical_dataset(monkeypatch: pytest.MonkeyPatch, dataset: str) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", dataset)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match=r"DATASET|dataset"):
        shadow.ShadowConfig.from_env().validate()


@pytest.mark.parametrize("dataset", ["project.matcher_input", "matcher`input", "matcher-input", "matcher input"])
def test_cutover_rejects_dataset_expression_injection(monkeypatch: pytest.MonkeyPatch, dataset: str) -> None:
    monkeypatch.setenv("MATCHER_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("BIGQUERY_MATCHER_INPUT_DATASET", dataset)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match="dataset ID"):
        shadow.CutoverConfig.from_env().validate(shadow.ShadowConfig.from_env())


def test_stage_contract_requires_exact_schema_and_content_labels() -> None:
    shadow = _load_shadow_module()
    spec = shadow.ARTIFACTS[0]
    expected = shadow._expected_schema(spec)
    fields = [types.SimpleNamespace(name=name, field_type=field_type, mode=mode) for name, field_type, mode in expected]
    labels = shadow._stage_labels(spec, "a" * 64)
    table = types.SimpleNamespace(
        schema=fields,
        labels=labels,
        time_partitioning=types.SimpleNamespace(field="gps_date"),
    )
    client = types.SimpleNamespace(get_table=lambda _table: table)

    shadow._verify_table_contract(client, "project.input.stage", spec, labels)
    table.labels = {"matcher_schema_version": labels["matcher_schema_version"]}
    with pytest.raises(RuntimeError, match="labels"):
        shadow._verify_table_contract(client, "project.input.stage", spec, labels)


def test_stable_postcommit_validation_scans_only_the_replaced_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shadow = _load_shadow_module()

    def query_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(shadow.bigquery, "QueryJobConfig", query_job_config)

    class Client:
        query_text = ""
        job_config: Any = None

        def query(self, query: str, **kwargs: Any) -> Any:
            self.query_text = query
            self.job_config = kwargs["job_config"]
            return types.SimpleNamespace(
                job_id="partition-check",
                query=query,
                state="DONE",
                error_result=None,
                result=lambda: [{"total_rows": 2, "wrong_processing_date_rows": 0}],
            )

    client = Client()
    assert (
        shadow._require_stable_processing_partition(
            client,
            "project.input.reconstruction_trip_facts",
            "2026-07-09",
            shadow.ARTIFACTS[0],
            2,
            "partition-check",
            100,
        )
        == 2
    )
    assert "where gps_date = @processing_date" in client.query_text.lower()
    assert "wrong_gps_date_rows" not in client.query_text
    assert client.job_config.maximum_bytes_billed == 100


def test_semantics_stage_and_postcommit_validation_use_processing_date(monkeypatch: pytest.MonkeyPatch) -> None:
    shadow = _load_shadow_module()
    semantics = next(spec for spec in shadow.ARTIFACTS if spec.key == "stop_semantics")

    def query_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(shadow.bigquery, "QueryJobConfig", query_job_config)

    class Client:
        query_text = ""

        def query(self, query: str, **_kwargs: Any) -> Any:
            self.query_text = query
            return types.SimpleNamespace(
                job_id="partition-check",
                query=query,
                state="DONE",
                error_result=None,
                result=lambda: [{"total_rows": 2, "wrong_processing_date_rows": 0}],
            )

    client = Client()
    assert (
        shadow._require_stable_processing_partition(
            client, "project.input.reconstruction_stop_semantics", "2026-07-09", semantics, 2, "partition-check", 100
        )
        == 2
    )
    assert "where processing_date = @processing_date" in client.query_text.lower()


def test_stage_preexisting_tampered_table_is_rejected_after_verified_job_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shadow = _load_shadow_module()

    def query_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    monkeypatch.setattr(shadow.bigquery, "QueryJobConfig", query_job_config)
    spec = shadow.ARTIFACTS[0]
    labels = shadow._stage_labels(spec, "a" * 64)
    fields = [
        types.SimpleNamespace(name=name, field_type=field_type, mode=mode)
        for name, field_type, mode in shadow._expected_schema(spec)
    ]

    class Client:
        query_text = ""
        recovered = False

        def query(self, query: str, **_kwargs: Any) -> Any:
            self.query_text = query
            raise Conflict("retry")

        def get_job(self, job_id: str, **_kwargs: Any) -> Any:
            self.recovered = True
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
        shadow._stage_artifact(
            client,
            "project.shadow.trip",
            "project.input.stage",
            "2026-07-09",
            "run",
            spec,
            "a" * 64,
            100,
        )
    assert client.recovered is True
    assert "create table if not exists" not in client.query_text.lower()


def test_query_recovery_rejects_different_sql_or_destination() -> None:
    shadow = _load_shadow_module()
    job = types.SimpleNamespace(
        job_id="stage-job",
        query="select 1",
        state="DONE",
        error_result=None,
        destination="project.input.other_stage",
    )

    with pytest.raises(RuntimeError, match="destination"):
        shadow._verify_query_job_identity(job, "select 1", "stage-job", 100, [], "project.input.expected_stage")
    with pytest.raises(RuntimeError, match="text"):
        shadow._verify_query_job_identity(job, "select 2", "stage-job", 100, [])


def test_matcher_command_is_parsed_and_prepended_to_prepare_args(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_COMMAND", "uv run --locked --project /opt/airflow/matcher ztm-matcher")
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig.from_env()

    assert config.command == ("uv", "run", "--locked", "--project", "/opt/airflow/matcher", "ztm-matcher")
    assert shadow._matcher_argv(
        config, "2026-07-09", "snapshot", tmp_path / "gps", tmp_path / "gtfs.zip", tmp_path / "out"
    ) == [
        "uv",
        "run",
        "--locked",
        "--project",
        "/opt/airflow/matcher",
        "ztm-matcher",
        "prepare",
        "--processing-date",
        "2026-07-09",
        "--snapshot-id",
        "snapshot",
        "--gps-root",
        str(tmp_path / "gps"),
        "--gtfs-zip",
        str(tmp_path / "gtfs.zip"),
        "--output-dir",
        str(tmp_path / "out"),
        "--threads",
        "2",
        "--alignment-workers",
        "1",
        "--memory-limit",
        "384MB",
        "--temp-limit",
        "20GB",
    ]


@pytest.mark.parametrize(
    ("command", "error"),
    [
        ("", "must not be empty"),
        ("uv run --locked --project /other ztm-matcher", "must match"),
    ],
)
def test_matcher_command_rejects_empty_or_inconsistent_project(
    monkeypatch: pytest.MonkeyPatch, command: str, error: str
) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_COMMAND", command)
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match=error):
        shadow.ShadowConfig.from_env().validate()


def test_matcher_command_rejects_malformed_quoting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_COMMAND", "uv run --project '/opt/airflow/matcher ztm-matcher")
    shadow = _load_shadow_module()

    with pytest.raises(ValueError, match="invalid shell-style quoting"):
        shadow.ShadowConfig.from_env()


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
            shadow.FieldSpec("gps_date", "DATE"),
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
                "gps_date": [date(2026, 7, 9), date(2026, 7, 9)],
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


def test_inspect_artifact_rejects_a_second_gps_date(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
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
            shadow.FieldSpec("gps_date", "DATE"),
            shadow.FieldSpec("service_date", "DATE"),
            shadow.FieldSpec("gtfs_snapshot_id", "STRING"),
            shadow.FieldSpec("trip_id", "STRING"),
        ),
        ("gtfs_snapshot_id", "service_date", "trip_id"),
        "gps_date",
    )
    pq.write_table(
        pa.table(
            {
                "processing_date": [date(2026, 7, 9), date(2026, 7, 9)],
                "gps_date": [date(2026, 7, 9), date(2026, 7, 8)],
                "service_date": [date(2026, 7, 8), date(2026, 7, 9)],
                "gtfs_snapshot_id": ["snapshot", "snapshot"],
                "trip_id": ["prior", "current"],
            }
        ),
        path,
    )

    with pytest.raises(RuntimeError, match="gps_date"):
        shadow._inspect_artifact(path, spec, "2026-07-09", "snapshot")


def test_manifest_requires_and_marker_binds_complete_stop_semantics(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shadow = _load_shadow_module()
    artifact = shadow.ArtifactValidation(tmp_path / "artifact.parquet", 2, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    semantics = next(spec for spec in shadow.ARTIFACTS if spec.key == "stop_semantics")
    manifest = {
        "processing_date": "2026-07-09",
        "snapshot_id": "snapshot",
        "schema_versions": {
            **{
                Path(spec.filename).stem: shadow.ARTIFACT_SCHEMA_VERSIONS[Path(spec.filename).stem]
                for spec in shadow.ARTIFACTS
            },
            "trip_universe": shadow.ARTIFACT_SCHEMA_VERSIONS["trip_universe"],
        },
        "outputs": {
            **{
                Path(spec.filename).stem: {"sha256": artifact.sha256, "bytes": artifact.bytes}
                for spec in shadow.ARTIFACTS
            },
            "trip_universe": {"sha256": artifact.sha256, "bytes": artifact.bytes},
        },
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(shadow, "_inspect_artifact", lambda *_args: artifact)
    monkeypatch.setattr(
        shadow, "_read_metrics", lambda *_args: {Path(spec.filename).stem: artifact.rows for spec in shadow.ARTIFACTS}
    )

    validated, _ = shadow._validate_outputs(tmp_path, "2026-07-09", "snapshot")

    assert validated[semantics.key] == artifact
    assert semantics.key in {spec.key for spec in shadow.ARTIFACTS}
    del manifest["outputs"][Path(semantics.filename).stem]
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="missing required reconstruction artifacts"):
        shadow._validate_outputs(tmp_path, "2026-07-09", "snapshot")


def test_load_job_id_binds_artifact_hash_and_conflict_checks_existing_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shadow = _load_shadow_module()
    monkeypatch.setattr(shadow.bigquery, "SchemaField", lambda *args, **kwargs: (args, kwargs), raising=False)

    def load_job_config(**kwargs: Any) -> types.SimpleNamespace:
        return types.SimpleNamespace(**kwargs)

    class ParquetOptions:
        enable_list_inference = False

    monkeypatch.setattr(shadow.bigquery, "LoadJobConfig", load_job_config, raising=False)
    monkeypatch.setattr(shadow.bigquery, "ParquetOptions", ParquetOptions, raising=False)
    shadow.bigquery.WriteDisposition = types.SimpleNamespace(WRITE_TRUNCATE="WRITE_TRUNCATE")
    artifact = shadow.ArtifactValidation(tmp_path / "trip.parquet", 1, "a" * 64, 7, ("2026-07-09",))
    artifact.path.write_bytes(b"parquet")
    spec = shadow.ARTIFACTS[0]
    expected_table = shadow._table_id("shadow", "run-id", spec, artifact.sha256)
    expected_job = shadow._load_job_id("run-id", spec, artifact.sha256)
    assert expected_job != shadow._load_job_id("run-id", spec, "b" * 64)
    client = FakeLoadClient(FakeLoadJob(expected_job, expected_table), conflict=True)

    config = shadow._load_config(spec)
    assert config.parquet_options.enable_list_inference is True

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


def test_content_addressed_table_identity_preserves_committed_tables() -> None:
    shadow = _load_shadow_module()
    spec = shadow.ARTIFACTS[0]
    original = shadow._table_identity("shadow", "run", spec, "a" * 64)
    changed = shadow._table_identity("shadow", "run", spec, "b" * 64)

    assert original["table_id"] != changed["table_id"]
    assert original["table_id"].endswith("a" * 64)
    assert changed["table_id"].endswith("b" * 64)
    assert original["job_id"] != changed["job_id"]


def test_run_id_hash_prevents_sanitized_and_truncated_collisions(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher")
    slash = "manual/run"
    underscore = "manual_run"
    long_a = "x" * 100 + "a"
    long_b = "x" * 100 + "b"

    assert shadow._run_id(slash) != shadow._run_id(underscore)
    assert shadow._run_id(long_a) != shadow._run_id(long_b)
    assert shadow._run_id(slash).endswith(shadow.hashlib.sha256(slash.encode()).hexdigest()[:16])
    assert shadow._marker_name(config, "2026-07-09", slash) != shadow._marker_name(config, "2026-07-09", underscore)
    assert shadow._pending_name(config, "2026-07-09", slash) != shadow._pending_name(config, "2026-07-09", underscore)
    assert (tmp_path / shadow._run_id(slash)) != (tmp_path / shadow._run_id(underscore))


def test_identical_run_and_artifact_resolve_identical_identities(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher")
    spec = shadow.ARTIFACTS[0]
    first = shadow._table_identity("shadow", "manual/run", spec, "a" * 64)
    second = shadow._table_identity("shadow", "manual/run", spec, "a" * 64)

    assert first == second
    assert shadow._marker_name(config, "2026-07-09", "manual/run") == shadow._marker_name(
        config, "2026-07-09", "manual/run"
    )
    assert shadow._pending_name(config, "2026-07-09", "manual/run") == shadow._pending_name(
        config, "2026-07-09", "manual/run"
    )


def test_marker_uses_create_only_precondition_and_rejects_conflicts(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher")
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


def test_pending_marker_and_conflict_reads_are_bounded_before_download(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(
        True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher", max_marker_bytes=1
    )
    bucket = FakeMarkerBucket(existing=b"{}")

    with pytest.raises(RuntimeError, match="commit marker exceeds"):
        shadow._read_marker(FakeStorageClient(bucket), config, "2026-07-09", "run")
    assert bucket.blob_instance.download_called is False
    assert bucket.blob_instance.reload_called is True
    with pytest.raises(RuntimeError, match="pending metadata exceeds"):
        shadow._read_pending(FakeStorageClient(bucket), config, "2026-07-09", "run")
    assert bucket.blob_instance.download_called is False
    conflict_config = shadow.ShadowConfig(
        True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher", max_marker_bytes=2
    )
    conflict_bucket = FakeMarkerBucket(existing=b'{"x":1}')
    with pytest.raises(RuntimeError, match="existing commit marker exceeds"):
        shadow._write_marker(FakeStorageClient(conflict_bucket), conflict_config, "2026-07-09", "run", {})
    assert conflict_bucket.blob_instance.download_called is False


def test_pending_verification_rejects_table_for_different_artifact() -> None:
    shadow = _load_shadow_module()
    config = shadow.ShadowConfig(True, False, "shadow", Path("workspace"), ("matcher",), None, 1, "shadow/matcher")
    pending = {
        "artifacts": {spec.key: {"sha256": "a" * 64} for spec in shadow.ARTIFACTS},
        "tables": {spec.key: shadow._table_identity("shadow", "run", spec, "b" * 64) for spec in shadow.ARTIFACTS},
    }

    with pytest.raises(RuntimeError, match="identity mismatch"):
        shadow._pending_tables(config, "run", pending)


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
        "raw/gps",
    )

    assert relative.as_posix() == "vehicle_type=bus/date=2026-07-09/hour=01/part-a.parquet"
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
        ("matcher",),
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
        ("matcher",),
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
        ("matcher",),
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
    assert events == ["matcher", "load", "load", "load", "load", "pending"]
    pending = {
        "run_id": "run",
        "processing_date": "2026-07-09",
        "snapshot_id": "snapshot",
        "metrics": {},
        "artifacts": {spec.key: {"sha256": artifact.sha256} for spec in shadow.ARTIFACTS},
        "tables": {
            spec.key: shadow._table_identity("matcher_shadow", "run", spec, artifact.sha256)
            for spec in shadow.ARTIFACTS
        },
    }
    monkeypatch.setattr(shadow, "_read_pending", lambda *_args: pending)
    comparison_calls: list[tuple[object, ...]] = []

    def comparison_report(*args: object) -> dict[str, object]:
        comparison_calls.append(args)
        events.append("compare")
        return {"service_dates": ["2026-07-08", "2026-07-09"], "aggregates": [], "differences": []}

    marker: dict[str, object] = {}
    monkeypatch.setattr(shadow, "_comparison_report", comparison_report)
    monkeypatch.setattr(
        shadow, "_write_marker", lambda *_args: marker.update(_args[-1]) or events.append("marker") or "gs://marker"
    )

    assert shadow.run_matcher_shadow_compare_commit("2026-07-09", "run", context)["marker_uri"] == "gs://marker"
    assert events[-2:] == ["compare", "marker"]
    assert comparison_calls[0][1:] == ("2026-07-09", "snapshot", pending["tables"], 5 * 1024**3)
    assert marker["snapshot_id"] == "snapshot"


def test_shadow_gate_reports_july9_like_quality_and_advisory_delay_difference() -> None:
    shadow = _load_shadow_module()
    aggregates = []
    for source, complete, partial, broken in (("canonical", 80, 15, 5), ("shadow", 70, 25, 5)):
        for quality, count in (("complete", complete), ("partial", partial), ("broken", broken)):
            aggregates.append(
                {
                    "artifact": "trip",
                    "source": source,
                    "service_date": "2026-07-09",
                    "mode": "bus",
                    "line": "20",
                    "gtfs_snapshot_id": "snapshot",
                    "trip_quality": quality,
                    "observation_status": None,
                    "row_count": count,
                    "distinct_grains": count,
                    "delay_p50_seconds": 60 if source == "canonical" else 180,
                    "delay_p90_seconds": 120,
                    "delay_p95_seconds": 180,
                    "abs_delay_over_3600_count": 1,
                }
            )
    comparison = {
        "service_dates": ["2026-07-08", "2026-07-09"],
        "aggregates": aggregates,
        "differences": [{"group": ["trip"]}],
    }

    gate = shadow.evaluate_shadow_gate(comparison, {"peak_rss_bytes": 1024, "swapping_observed": False})

    assert gate["status"] == "warn"
    assert gate["manual_review_required"] is True
    assert gate["trip_quality_rates"][0]["rates"]["complete"]["delta_percentage_points"] == -0.1
    assert any(issue["category"] == "delay" and issue["level"] == "warn" for issue in gate["issues"])


def test_shadow_gate_fails_structural_resource_and_quality_bounds() -> None:
    shadow = _load_shadow_module()
    aggregate = {
        "artifact": "trip",
        "source": "shadow",
        "service_date": "2026-07-09",
        "mode": "tram",
        "line": "145",
        "gtfs_snapshot_id": "snapshot",
        "trip_quality": "complete",
        "observation_status": None,
        "row_count": 10,
        "distinct_grains": 9,
        "delay_p50_seconds": 1,
        "delay_p90_seconds": 1,
        "delay_p95_seconds": 1,
        "abs_delay_over_3600_count": 0,
    }
    canonical = aggregate | {"source": "canonical", "row_count": 100, "distinct_grains": 100}
    gate = shadow.evaluate_shadow_gate(
        {"service_dates": ["2026-07-08", "2026-07-09"], "aggregates": [aggregate, canonical], "differences": []},
        {"peak_rss_bytes": shadow.DEFAULT_GATE_PEAK_RSS_BYTES + 1, "swapping_observed": True},
    )

    assert gate["status"] == "fail"
    assert gate["structural_violations"]
    assert shadow._gate_has_hard_failure(gate) is True


def test_shadow_gate_requires_canonical_mode_retention_and_material_lines() -> None:
    shadow = _load_shadow_module()

    def aggregate(source: str, mode: str, line: str, rows: int) -> dict[str, object]:
        return {
            "artifact": "trip",
            "source": source,
            "service_date": "2026-07-09",
            "mode": mode,
            "line": line,
            "gtfs_snapshot_id": "snapshot",
            "trip_quality": "complete",
            "observation_status": None,
            "row_count": rows,
            "distinct_grains": rows,
            "delay_p50_seconds": 0,
            "delay_p90_seconds": 0,
            "delay_p95_seconds": 0,
            "abs_delay_over_3600_count": 0,
        }

    gate = shadow.evaluate_shadow_gate(
        {
            "service_dates": ["2026-07-08", "2026-07-09"],
            "aggregates": [
                aggregate("canonical", "bus", "10", 80),
                aggregate("canonical", "bus", "20", 20),
                aggregate("shadow", "bus", "10", 95),
                aggregate("shadow", "bus", "20", 5),
                aggregate("canonical", "tram", "1", 50),
            ],
            "differences": [],
        },
        {"peak_rss_bytes": 1, "swapping_observed": False},
    )

    tram = next(item for item in gate["retention"] if item["mode"] == "tram")
    assert tram["shadow_rows"] == 0
    assert any(issue["message"] == "shadow mode is completely missing" for issue in gate["issues"])
    assert any(issue["category"] == "quality" and issue["level"] == "fail" for issue in gate["issues"])
    assert any(
        issue["message"] == "material line retention below threshold" and issue["level"] == "warn"
        for issue in gate["issues"]
    )
    assert shadow._gate_has_hard_failure(gate) is True


def test_shadow_gate_warns_for_passenger_adapter_and_prior_retention_differences() -> None:
    shadow = _load_shadow_module()

    def aggregate(artifact: str, source: str, service_date: str, rows: int) -> dict[str, object]:
        return {
            "artifact": artifact,
            "source": source,
            "service_date": service_date,
            "mode": "bus",
            "line": "10",
            "gtfs_snapshot_id": "snapshot",
            "trip_quality": "complete" if artifact == "trip" else None,
            "observation_status": None,
            "row_count": rows,
            "distinct_grains": rows,
            "delay_p50_seconds": 0,
            "delay_p90_seconds": 0,
            "delay_p95_seconds": 0,
            "abs_delay_over_3600_count": 0,
        }

    gate = shadow.evaluate_shadow_gate(
        {
            "service_dates": ["2026-07-08", "2026-07-09"],
            "aggregates": [
                aggregate("trip", "canonical", "2026-07-08", 20),
                aggregate("trip", "shadow", "2026-07-08", 5),
                aggregate("stop_arrival", "canonical", "2026-07-09", 20),
                aggregate("stop_arrival", "shadow", "2026-07-09", 5),
                aggregate("expected_stop_event", "canonical", "2026-07-09", 20),
                aggregate("expected_stop_event", "shadow", "2026-07-09", 5),
            ],
            "differences": [],
        },
        {"peak_rss_bytes": 1, "swapping_observed": False},
    )

    assert gate["status"] == "warn"
    assert gate["manual_review_required"] is True
    assert shadow._gate_has_hard_failure(gate) is False
    assert all(issue["level"] == "warn" for issue in gate["issues"])


def test_shadow_gate_delay_zero_baseline_is_advisory_and_reports_absolute_deltas() -> None:
    shadow = _load_shadow_module()
    base = {
        "artifact": "trip",
        "service_date": "2026-07-09",
        "mode": "bus",
        "line": "10",
        "gtfs_snapshot_id": "snapshot",
        "trip_quality": "complete",
        "observation_status": None,
        "row_count": 20,
        "distinct_grains": 20,
        "delay_p50_seconds": 0,
        "delay_p90_seconds": 0,
        "delay_p95_seconds": 0,
        "abs_delay_over_3600_count": 0,
    }
    comparison = {
        "service_dates": ["2026-07-08", "2026-07-09"],
        "aggregates": [base | {"source": "canonical"}, base | {"source": "shadow"}],
        "differences": [],
    }

    assert shadow.evaluate_shadow_gate(comparison, {})["status"] == "pass"
    comparison["aggregates"][1] = comparison["aggregates"][1] | {"delay_p50_seconds": 10}
    gate = shadow.evaluate_shadow_gate(comparison, {})

    change = next(item for item in gate["delay_changes"] if item.get("percentile") == "p50")
    assert change["difference_seconds"] == 10
    assert change["absolute_difference_seconds"] == 10
    assert any(issue["message"] == "delay percentile changed from zero baseline" for issue in gate["issues"])
    assert shadow._gate_has_hard_failure(gate) is False


def test_artifact_validation_failure_never_writes_pending(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    artifact = shadow.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_load(monkeypatch, shadow, artifact, events)
    monkeypatch.setattr(
        shadow,
        "_validate_outputs",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("artifact validation failed")),
    )

    with pytest.raises(RuntimeError, match="artifact validation failed"):
        shadow.run_matcher_shadow_load("2026-07-09", "snapshot", "run")

    assert events == ["matcher"]


def test_comparison_query_and_marker_bounds_are_configured(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    query = shadow._comparison_query(
        {spec.key: {"table_id": f"project.shadow.{spec.key}"} for spec in shadow.ARTIFACTS}
    )
    assert "approx_quantiles" in query
    assert "abs_delay_over_3600_count" in query
    assert "gtfs_snapshot_id" in query
    assert query.count("trip.gtfs_snapshot_id = @gtfs_snapshot_id") == len(shadow.COMPARISON_ARTIFACTS) * 2
    assert "line" in query
    assert "trip.scheduled_end_time >= timestamp(@processing_date, 'Europe/Warsaw')" in query
    assert query.count("from cohort as fact") == 2
    assert query.count("inner join cohort") == 4
    assert "fact.gps_date = cohort.gps_date" in query
    assert "fact.source_gps_date = cohort" not in query
    assert "fact.scheduled_arrival_time" not in query
    assert "struct(fact.gtfs_snapshot_id" in query
    config = shadow.ShadowConfig(
        True, False, "shadow", tmp_path, ("matcher",), None, 1, "shadow/matcher", max_marker_bytes=1
    )
    with pytest.raises(RuntimeError, match="marker exceeds"):
        shadow._write_marker(
            FakeStorageClient(FakeMarkerBucket(existing=None)), config, "2026-07-09", "run", {"x": "y"}
        )
    monkeypatch.setenv("MATCHER_SHADOW_MAX_COMPARISON_BYTES", "0")
    with pytest.raises(ValueError, match="positive"):
        shadow.ShadowConfig.from_env()


def test_comparison_report_binds_exact_snapshot_and_rejects_empty_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shadow = _load_shadow_module()
    query_calls: list[tuple[str, Any]] = []

    class QueryClient:
        def query(self, query: str, *, job_config: object) -> object:
            query_calls.append((query, job_config))
            return types.SimpleNamespace(result=list)

    monkeypatch.setattr(shadow.bigquery, "ArrayQueryParameter", lambda *args: args, raising=False)
    monkeypatch.setattr(shadow.bigquery, "QueryJobConfig", lambda **kwargs: kwargs)
    tables = {spec.key: {"table_id": f"project.shadow.{spec.key}"} for spec in shadow.ARTIFACTS}

    report = shadow._comparison_report(QueryClient(), "2026-07-09", "snapshot-a", tables, 123)

    assert report["comparison_contract_version"] == "matcher-shadow-comparison-v4"
    parameters = query_calls[0][1]["query_parameters"]
    assert any(
        getattr(parameter, "name", None) == "gtfs_snapshot_id" and getattr(parameter, "value", None) == "snapshot-a"
        for parameter in parameters
    )
    with pytest.raises(ValueError, match="nonempty snapshot"):
        shadow._comparison_report(QueryClient(), "2026-07-09", "   ", tables, 123)
    assert len(query_calls) == 1


def test_comparison_query_excludes_later_canonical_snapshot_from_same_date_cohort() -> None:
    shadow = _load_shadow_module()
    query = shadow._comparison_query(
        {spec.key: {"table_id": f"project.shadow.{spec.key}"} for spec in shadow.ARTIFACTS}
    )
    cohorts = query.split(" union all ")

    assert len(cohorts) == len(shadow.COMPARISON_ARTIFACTS) * 2
    assert all("trip.gtfs_snapshot_id = @gtfs_snapshot_id" in cohort for cohort in cohorts)


def test_strict_compare_rejects_hard_gate_failure_before_marker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MATCHER_SHADOW_STRICT", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    artifact = {spec.key: {"sha256": "a" * 64} for spec in shadow.ARTIFACTS}
    pending = {
        "run_id": "run",
        "processing_date": "2026-07-09",
        "snapshot_id": "snapshot",
        "metrics": {"peak_rss_bytes": 1, "swapping_observed": False},
        "artifacts": artifact,
        "tables": {
            spec.key: shadow._table_identity("matcher_shadow", "run", spec, "a" * 64) for spec in shadow.ARTIFACTS
        },
    }
    monkeypatch.setattr(shadow, "_read_pending", lambda *_args: pending)
    monkeypatch.setattr(
        shadow,
        "_comparison_report",
        lambda *_args: {
            "service_dates": ["2026-07-08", "2026-07-09"],
            "aggregates": [
                {
                    "artifact": "trip",
                    "source": "shadow",
                    "service_date": "2026-07-09",
                    "mode": "bus",
                    "line": "118",
                    "gtfs_snapshot_id": "snapshot",
                    "trip_quality": "complete",
                    "observation_status": None,
                    "row_count": 2,
                    "distinct_grains": 1,
                    "delay_p50_seconds": 1,
                    "delay_p90_seconds": 1,
                    "delay_p95_seconds": 1,
                    "abs_delay_over_3600_count": 0,
                }
            ],
            "differences": [],
        },
    )
    monkeypatch.setattr(shadow, "_write_marker", lambda *_args: pytest.fail("strict failure must not write marker"))
    monkeypatch.setattr(shadow.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(shadow.storage, "Client", lambda **_kwargs: object())

    with pytest.raises(RuntimeError, match="strict gate"):
        shadow.run_matcher_shadow_compare_commit(
            "2026-07-09",
            "run",
            {"enabled": True, "status": "loaded_pending", "processing_date": "2026-07-09", "run_id": "run"},
        )


def test_load_rejects_conflicting_marker_before_loading_tables(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MATCHER_SHADOW_ENABLED", "true")
    monkeypatch.setenv("BIGQUERY_MATCHER_SHADOW_DATASET", "matcher_shadow")
    monkeypatch.setenv("MATCHER_SHADOW_WORKSPACE_ROOT", str(tmp_path))
    shadow = _load_shadow_module()
    artifact = shadow.ArtifactValidation(tmp_path / "artifact.parquet", 1, "a" * 64, 7, ("2026-07-08", "2026-07-09"))
    events: list[str] = []
    _stub_load(monkeypatch, shadow, artifact, events)
    monkeypatch.setattr(shadow, "_read_marker", lambda *_args: {"run_id": "other"})

    with pytest.raises(RuntimeError, match="different immutable run content"):
        shadow.run_matcher_shadow_load("2026-07-09", "snapshot", "run")

    assert events == ["matcher"]


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


def test_loaded_repeated_fields_must_match_parquet_counts(tmp_path: Path) -> None:
    shadow = _load_shadow_module()
    artifact = shadow.ArtifactValidation(
        tmp_path / "trip.parquet",
        2,
        "a" * 64,
        7,
        ("2026-07-09",),
        (("quality_flags", 2), ("service_observation_flags", 1)),
    )

    class QueryJob:
        def __init__(self, row: dict[str, int]) -> None:
            self.row = row

        def result(self) -> list[dict[str, int]]:
            return [self.row]

    class Client:
        def __init__(self, row: dict[str, int]) -> None:
            self.row = row

        def query(self, _query: str, **_kwargs: Any) -> QueryJob:
            return QueryJob(self.row)

    shadow._verify_loaded_repeated_fields(
        Client({"quality_flags": 2, "service_observation_flags": 1}), "ztm-data.shadow.trip", artifact
    )
    with pytest.raises(RuntimeError, match="lost repeated-field evidence"):
        shadow._verify_loaded_repeated_fields(
            Client({"quality_flags": 0, "service_observation_flags": 0}), "ztm-data.shadow.trip", artifact
        )


class FakeMarkerBlob:
    def __init__(self, existing: bytes | None) -> None:
        self.existing = existing
        self.payload = b""
        self.if_generation_match: int | None = None
        self.download_called = False
        self.reload_called = False

    @property
    def size(self) -> int:
        return len(self.existing if self.existing is not None else self.payload)

    def exists(self) -> bool:
        return self.existing is not None or bool(self.payload)

    def reload(self) -> None:
        self.reload_called = True

    def upload_from_string(self, payload: bytes, **kwargs: Any) -> None:
        self.if_generation_match = kwargs.get("if_generation_match")
        if self.existing is not None:
            raise PreconditionFailed("exists")
        self.payload = payload

    def download_as_bytes(self) -> bytes:
        self.download_called = True
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
        shadow,
        "_load_artifact",
        lambda _client, dataset, run_id, spec, loaded: (
            events.append("load") or shadow._table_identity(dataset, run_id, spec, loaded.sha256)
        ),
    )
    monkeypatch.setattr(shadow, "_write_pending", lambda *_args: events.append("pending") or "gs://pending")
    monkeypatch.setattr(shadow, "_read_marker", lambda *_args: None)
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
