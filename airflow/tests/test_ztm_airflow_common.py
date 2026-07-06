from __future__ import annotations

import importlib.util
import sys
import types
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


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
