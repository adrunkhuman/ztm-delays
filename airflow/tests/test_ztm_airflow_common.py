from __future__ import annotations

import importlib.util
import sys
import types
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

RUNTIME_ENV_VARS = (
    "GCP_PROJECT",
    "BIGQUERY_RAW_DATASET",
    "BIGQUERY_STG_DATASET",
    "BIGQUERY_INT_DATASET",
    "BIGQUERY_MARTS_DATASET",
    "BIGQUERY_LOCATION",
    "GCS_BUCKET",
    "DBT_PROJECT_DIR",
    "RAW_GPS_PREFIX",
    "RAW_GTFS_PREFIX",
    "SERVING_EXPORT_DIR",
    "SERVING_EXPORT_GCS_PREFIX",
    "SERVING_EXPORT_FILENAME",
    "SERVING_EXPORT_MAX_BYTES",
)


def test_runtime_config_defaults(monkeypatch: Any) -> None:
    for name in RUNTIME_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    common = _load_common_module()
    expected_max_bytes = str(20 * 1024 * 1024 * 1024)

    assert common.GCP_PROJECT == "ztm-data"
    assert common.BIGQUERY_RAW_DATASET == "ztm_raw"
    assert common.BIGQUERY_STG_DATASET == "ztm_stg"
    assert common.BIGQUERY_INT_DATASET == "ztm_int"
    assert common.BIGQUERY_MARTS_DATASET == "ztm_marts"
    assert common.BIGQUERY_LOCATION == "europe-north1"
    assert common.GCS_BUCKET == "ztm-analytics-bucket"
    assert common.DBT_PROJECT_DIR == "/opt/airflow/dbt"
    assert common.RAW_GPS_PREFIX == "raw/gps"
    assert common.RAW_GTFS_PREFIX == "raw/gtfs"
    assert common.SERVING_EXPORT_DIR == "/opt/airflow/serving"
    assert common.SERVING_EXPORT_GCS_PREFIX == "serving/duckdb/staging"
    assert common.SERVING_EXPORT_FILENAME == "ztm.duckdb"
    assert expected_max_bytes == common.SERVING_EXPORT_MAX_BYTES


def test_runtime_config_env_overrides(monkeypatch: Any) -> None:
    monkeypatch.setenv("GCP_PROJECT", "other-project")
    monkeypatch.setenv("BIGQUERY_RAW_DATASET", "raw_dev")
    monkeypatch.setenv("BIGQUERY_STG_DATASET", "stg_dev")
    monkeypatch.setenv("BIGQUERY_INT_DATASET", "int_dev")
    monkeypatch.setenv("BIGQUERY_MARTS_DATASET", "marts_dev")
    monkeypatch.setenv("BIGQUERY_LOCATION", "europe-west1")
    monkeypatch.setenv("GCS_BUCKET", "other-bucket")
    monkeypatch.setenv("DBT_PROJECT_DIR", "/srv/dbt project")
    monkeypatch.setenv("RAW_GPS_PREFIX", "dev/raw/gps")
    monkeypatch.setenv("RAW_GTFS_PREFIX", "dev/raw/gtfs")
    monkeypatch.setenv("SERVING_EXPORT_DIR", "/srv/serving")
    monkeypatch.setenv("SERVING_EXPORT_GCS_PREFIX", "dev/serving")
    monkeypatch.setenv("SERVING_EXPORT_FILENAME", "dev.duckdb")
    monkeypatch.setenv("SERVING_EXPORT_MAX_BYTES", "12345")

    common = _load_common_module()

    assert common.GCP_PROJECT == "other-project"
    assert common.BIGQUERY_RAW_DATASET == "raw_dev"
    assert common.BIGQUERY_STG_DATASET == "stg_dev"
    assert common.BIGQUERY_INT_DATASET == "int_dev"
    assert common.BIGQUERY_MARTS_DATASET == "marts_dev"
    assert common.BIGQUERY_LOCATION == "europe-west1"
    assert common.GCS_BUCKET == "other-bucket"
    assert common.DBT_PROJECT_DIR == "/srv/dbt project"
    assert common.RAW_GPS_PREFIX == "dev/raw/gps"
    assert common.RAW_GTFS_PREFIX == "dev/raw/gtfs"
    assert common.SERVING_EXPORT_DIR == "/srv/serving"
    assert common.SERVING_EXPORT_GCS_PREFIX == "dev/serving"
    assert common.SERVING_EXPORT_FILENAME == "dev.duckdb"
    assert common.SERVING_EXPORT_MAX_BYTES == "12345"
    assert common.gtfs_gcs_uri("snapshot-id") == "gs://other-bucket/dev/raw/gtfs/snapshot-id.zip"
    assert (
        common.dbt_command("run", "model_name", "{}")
        == "cd '/srv/dbt project' && dbt run --select model_name --vars '{}'"
    )


def test_airflow_failure_payload_uses_public_context_fields() -> None:
    common = _load_common_module()
    task_instance = types.SimpleNamespace(
        dag_id="dag_daily_gps",
        task_id="dbt_run_pipeline_status",
        run_id="scheduled__2026-07-06",
        try_number=2,
        map_index=-1,
    )

    payload = common._airflow_failure_payload(
        {
            "task_instance": task_instance,
            "logical_date": datetime(2026, 7, 6, tzinfo=UTC),
            "exception": RuntimeError("boom"),
        }
    )

    assert payload == {
        "dag_id": "dag_daily_gps",
        "task_id": "dbt_run_pipeline_status",
        "run_id": "scheduled__2026-07-06",
        "try_number": 2,
        "map_index": -1,
        "logical_date": "2026-07-06T00:00:00+00:00",
        "exception_type": "RuntimeError",
    }


def test_airflow_failure_payload_handles_dag_level_context() -> None:
    common = _load_common_module()
    dag_run = types.SimpleNamespace(dag_id="dag_daily_gps", run_id="scheduled__2026-07-06")

    payload = common._airflow_failure_payload({"dag_run": dag_run})

    assert payload["dag_id"] == "dag_daily_gps"
    assert payload["run_id"] == "scheduled__2026-07-06"
    assert payload["task_id"] is None


def test_transient_retry_default_contract() -> None:
    common = _load_common_module()

    assert common.AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS["retries"] == 2
    assert common.AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS["retry_delay"].total_seconds() == 300
    assert common.AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS["on_failure_callback"] is common.airflow_failure_alert


@pytest.mark.parametrize(
    ("processing_date", "reason"),
    [
        (date(2026, 6, 25), None),
        (date(2026, 6, 26), "incomplete_raw_gps_archive"),
        (date(2026, 6, 27), None),
        (date(2026, 7, 4), None),
        (date(2026, 7, 5), "degraded_raw_gps_archive"),
        (date(2026, 7, 6), "degraded_raw_gps_archive"),
        (date(2026, 7, 7), "degraded_raw_gps_archive"),
        (date(2026, 7, 8), None),
        (date(2026, 7, 12), None),
    ],
)
def test_historical_daily_exclusion_policy_boundaries(processing_date: date, reason: str | None) -> None:
    common = _load_common_module()

    assert common.historical_daily_exclusion_reason(processing_date) == reason


def test_historical_daily_exclusion_policy_has_only_known_bad_dates() -> None:
    common = _load_common_module()

    assert date(2026, 6, 27) == common.HISTORICAL_DAILY_ELIGIBLE_START_DATE
    assert {
        date(2026, 6, 26): "incomplete_raw_gps_archive",
        date(2026, 7, 5): "degraded_raw_gps_archive",
        date(2026, 7, 6): "degraded_raw_gps_archive",
        date(2026, 7, 7): "degraded_raw_gps_archive",
    } == common.HISTORICAL_DAILY_EXCLUSION_REASONS


def test_airflow_failure_alert_rejects_non_https_webhook(monkeypatch: Any) -> None:
    common = _load_common_module()

    def fail_urlopen(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("non-HTTPS webhook should not be called")

    monkeypatch.setenv("AIRFLOW_FAILURE_WEBHOOK_URL", "http://example.test/hook")
    monkeypatch.setattr(common, "urlopen", fail_urlopen)

    common.airflow_failure_alert({})


def test_airflow_failure_alert_posts_https_webhook(monkeypatch: Any) -> None:
    common = _load_common_module()
    calls = []

    def fake_urlopen(request: Any, *, timeout: float) -> FakeWebhookResponse:
        calls.append((request, timeout))
        return FakeWebhookResponse()

    monkeypatch.setenv("AIRFLOW_FAILURE_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setattr(common, "urlopen", fake_urlopen)

    common.airflow_failure_alert({"task_instance": types.SimpleNamespace(dag_id="dag", task_id="task")})

    request, timeout = calls[0]
    assert request.full_url == "https://example.test/hook"
    assert request.get_method() == "POST"
    assert request.headers["Content-type"] == "application/json"
    assert b'"dag_id": "dag"' in request.data
    assert b'"task_id": "task"' in request.data
    assert timeout == common.AIRFLOW_FAILURE_WEBHOOK_TIMEOUT_SECONDS


def test_airflow_failure_alert_swallows_webhook_errors(monkeypatch: Any) -> None:
    common = _load_common_module()

    def failing_urlopen(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("webhook down")

    monkeypatch.setenv("AIRFLOW_FAILURE_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setattr(common, "urlopen", failing_urlopen)

    common.airflow_failure_alert({})


def _load_common_module() -> types.ModuleType:
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    airflow_sdk_module.Asset = FakeAsset
    sys.modules["airflow"] = types.ModuleType("airflow")
    sys.modules["airflow.sdk"] = airflow_sdk_module

    dag_dir = Path(__file__).parents[1] / "dags"
    module_path = dag_dir / "ztm_airflow_common.py"
    module_name = "ztm_airflow_common_under_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load common module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class FakeAsset:
    def __init__(self, uri: str) -> None:
        self.uri = uri


class FakeWebhookResponse:
    def __enter__(self) -> FakeWebhookResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None
