from __future__ import annotations

import copy
import importlib.util
import json
import sys
import types
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from .test_poller_health import HOUR, START, failing, health, row, samples, summary
from .test_poller_health_public import public


class NotFound(Exception):
    pass


class PreconditionFailed(Exception):
    pass


class FakeBlob:
    def __init__(self, bucket: FakeBucket, path: str) -> None:
        self.bucket = bucket
        self.path = path
        self.size = None
        self.generation = None
        self.cache_control = None

    def reload(self) -> None:
        self.bucket.reads.append(self.path)
        if self.path not in self.bucket.objects:
            raise NotFound
        data, generation = self.bucket.objects[self.path]
        self.size, self.generation = len(data), generation

    def download_as_bytes(self, **kwargs: Any) -> bytes:
        data, generation = self.bucket.objects[self.path]
        assert kwargs["if_generation_match"] == generation
        assert kwargs["end"] <= public.HISTORY_MAX_BYTES
        return data[kwargs["start"] : kwargs["end"] + 1]

    def upload_from_string(self, payload: str, **kwargs: Any) -> None:
        generation = self.bucket.objects.get(self.path, (b"", 0))[1]
        if kwargs.get("if_generation_match", generation) != generation:
            raise PreconditionFailed
        self.generation = generation + 1
        self.bucket.objects[self.path] = (payload.encode(), self.generation)
        self.bucket.uploads.append(self.path)
        self.bucket.cache_control[self.path] = self.cache_control


class FakeBucket:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, int]] = {}
        self.uploads: list[str] = []
        self.list_calls: list[str] = []
        self.list_parameters: list[dict[str, Any]] = []
        self.reads: list[str] = []
        self.cache_control: dict[str, str | None] = {}

    def blob(self, path: str) -> FakeBlob:
        return FakeBlob(self, path)

    def list_blobs(  # noqa: PLR0913 - mirrors the bounded storage listing API
        self,
        *,
        prefix: str,
        max_results: int,
        start_offset: str | None = None,
        end_offset: str | None = None,
        page_size: int | None = None,
        fields: str | None = None,
    ) -> list[Any]:
        self.list_calls.append(prefix)
        self.list_parameters.append(
            {
                "prefix": prefix,
                "max_results": max_results,
                "start_offset": start_offset,
                "end_offset": end_offset,
                "page_size": page_size,
                "fields": fields,
            }
        )
        assert 1 <= max_results <= 90 * 24 + 1
        if prefix != "health/poller/reports/":
            assert max_results == 1
        return [
            types.SimpleNamespace(name=name)
            for name in sorted(self.objects)
            if name.startswith(prefix)
            and (start_offset is None or name >= start_offset)
            and (end_offset is None or name < end_offset)
        ][:max_results]

    def put(self, path: str, data: dict[str, Any]) -> None:
        generation = self.objects.get(path, (b"", 0))[1]
        self.objects[path] = (json.dumps(data).encode(), generation + 1)

    def get(self, path: str) -> dict[str, Any]:
        return json.loads(self.objects[path][0])


class FakeClient:
    def __init__(self) -> None:
        self.tables: list[Any] = []
        self.queries: list[Any] = []
        self.result_calls = 0
        self.fail = False

    def create_table(self, table: Any, *, exists_ok: bool) -> None:
        assert exists_ok
        self.tables.append(table)

    def query(self, query: str, **kwargs: Any) -> FakeClient:
        self.queries.append((query, kwargs))
        return self

    def result(self) -> None:
        self.result_calls += 1
        if self.fail:
            raise RuntimeError("fake BQ transport")


class FakeDAG:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        pass


class FakeTask:
    def __init__(self, func: Any) -> None:
        self.function = func
        self.calls: list[Any] = []

    def __call__(self, *args: Any) -> FakeTask:
        self.calls.append(args)
        return self


@pytest.fixture
def dag_module(monkeypatch: Any) -> types.ModuleType:
    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG = FakeDAG
    sdk.task = FakeTask
    sdk.get_current_context = lambda: {"data_interval_end": HOUR.replace(minute=25)}
    exceptions = types.ModuleType("google.api_core.exceptions")
    exceptions.NotFound = NotFound
    exceptions.PreconditionFailed = PreconditionFailed
    bq = types.ModuleType("google.cloud.bigquery")
    bq.SchemaField = lambda name, kind: types.SimpleNamespace(name=name, field_type=kind)
    bq.Table = lambda name, schema: types.SimpleNamespace(table_id=name, schema=schema)
    bq.TimePartitioningType = types.SimpleNamespace(DAY="DAY")
    bq.TimePartitioning = types.SimpleNamespace
    bq.QueryJobConfig = types.SimpleNamespace
    bq.ScalarQueryParameter = lambda name, kind, value: types.SimpleNamespace(name=name, type_=kind, value=value)
    cloud = types.ModuleType("google.cloud")
    cloud.bigquery = bq
    cloud.storage = types.ModuleType("google.cloud.storage")
    common = types.ModuleType("ztm_airflow_common")
    for key, value in {
        "AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS": {"retries": 2},
        "BIGQUERY_LOCATION": "europe-north1",
        "BIGQUERY_RAW_DATASET": "ztm_raw",
        "GCP_PROJECT": "ztm-data",
        "GCS_BUCKET": "bucket",
        "airflow_failure_alert": lambda context: None,
    }.items():
        setattr(common, key, value)
    requests = types.ModuleType("requests")
    requests.RequestException = type("RequestException", (Exception,), {})
    requests.codes = types.SimpleNamespace(ok=200, multiple_choices=300)
    requests.post = lambda *args, **kwargs: types.SimpleNamespace(status_code=200)
    for key, module in {
        "airflow": types.ModuleType("airflow"),
        "airflow.sdk": sdk,
        "google": types.ModuleType("google"),
        "google.api_core": types.ModuleType("google.api_core"),
        "google.api_core.exceptions": exceptions,
        "google.cloud": cloud,
        "google.cloud.bigquery": bq,
        "google.cloud.storage": cloud.storage,
        "ztm_airflow_common": common,
        "poller_health": health,
        "poller_health_public": public,
        "requests": requests,
    }.items():
        monkeypatch.setitem(sys.modules, key, module)
    spec = importlib.util.spec_from_file_location(
        "dag_poller_health_under_test", Path(__file__).parents[1] / "dags/dag_poller_health.py"
    )
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def seed_history(bucket: FakeBucket, hour: Any = HOUR, vehicles: int = 100) -> None:
    for source in samples(hour, vehicles=vehicles):
        bucket.put(health.hour_path("hourly", health.timestamp(source["hour_start"])), source)


def history_for(report: dict[str, Any]) -> dict[str, Any]:
    """A published history whose newest hour is ``report``'s."""
    lookahead = {mode: [health.empty_baseline()] * 3 for mode in health.MODES}
    return public.feed_history(report, [None] * 23, lookahead, health.Config())


def test_dag_import_schedule_and_failure_callback(dag_module: Any, monkeypatch: Any) -> None:
    dag = dag_module.dag
    assert dag.kwargs["schedule"] == "25 * * * *"
    assert dag.kwargs["start_date"].utcoffset() == timedelta(0)
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["on_failure_callback"] is dag_module.airflow_failure_alert
    assert dag.kwargs["default_args"] == {"retries": 2}
    assert dag_module.alert_transitions.calls == [(dag_module.evaluate_hour,)]
    assert dag_module.publish_hour.calls == [(dag_module.evaluate_hour,)]
    assert dag_module.persist_hour.calls == [(dag_module.evaluate_hour,)]
    hours = []
    monkeypatch.setattr(
        dag_module.storage,
        "Client",
        lambda **kwargs: types.SimpleNamespace(bucket=lambda name: FakeBucket()),
        raising=False,
    )
    monkeypatch.setattr(
        dag_module.bigquery,
        "Client",
        lambda **kwargs: pytest.fail("evaluation must not contact BigQuery"),
        raising=False,
    )
    monkeypatch.setattr(dag_module, "freeze_report", lambda bucket, hour, config, **kwargs: hours.append(hour))
    assert dag_module.evaluate_hour.function() == health.iso(HOUR - timedelta(hours=1))
    assert hours == [HOUR - timedelta(hours=1)]


def test_hourly_report_raw_partition_transaction_and_public_history(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    seed_history(bucket)
    previous = HOUR - timedelta(hours=1)
    previous_source = summary(previous)
    bucket.put(health.hour_path("hourly", previous), previous_source)
    bucket.put(health.hour_path("reports", previous), health.evaluate(previous, previous_source, []))
    bucket.put(health.hour_path("hourly", HOUR), summary())
    report = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert row(report)["status"] == "healthy"
    assert bucket.get(health.hour_path("reports", HOUR)) == report
    history = bucket.get(dag_module.HISTORY_PATH)
    assert dag_module.HISTORY_PATH == "health/poller/public/feed-history.json"
    assert bucket.cache_control[dag_module.HISTORY_PATH] == "no-cache"
    assert history["hour_start"] == health.iso(HOUR)
    assert history["vehicle_types"]["bus"]["fresh"][-120:] == [100] * 120
    assert history["vehicle_types"]["bus"]["usual"][1380:1440] == [100] * 60
    assert bucket.cache_control[health.hour_path("reports", HOUR)] is None
    table = client.tables[0]
    assert table.table_id == "ztm-data.ztm_raw.raw_poller_hourly_health"
    assert table.time_partitioning.field == "hour_start"
    assert table.time_partitioning.type_ == "DAY"
    assert table.time_partitioning.expiration_ms == 90 * 24 * 60 * 60 * 1000
    assert table.clustering_fields == ["mode"]
    assert {field.name: field.field_type for field in table.schema}["intervals"] == "STRING"
    query, kwargs = client.queries[0]
    assert "BEGIN TRANSACTION" in query
    assert "COMMIT TRANSACTION" in query
    assert "WHERE hour_start = @hour_start" in query
    assert kwargs["location"] == "europe-north1"
    params = {param.name: param.value for param in kwargs["job_config"].query_parameters}
    assert params["hour_start"] == HOUR
    rows = json.loads(params["rows"])
    assert [entry["mode"] for entry in rows] == ["bus", "tram"]
    assert json.loads(rows[0]["reasons"]) == []
    assert rows[0]["mean_accepted_vehicles"] == 100
    assert client.result_calls == 1


def test_absent_and_invalid_summaries_nullable_metrics(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    bucket.put(
        sys.modules["poller_health_gcs"].HEARTBEAT_PATH, {"collection_started_at": START, "hostname": "secret-host"}
    )
    report = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert row(report)["status"] == "monitoring_gap"
    params = client.queries[0][1]["job_config"].query_parameters
    assert json.loads(params[1].value)[0]["parsed_rows"] is None
    assert "secret-host" not in json.dumps(report)
    bucket = FakeBucket()
    source = summary()
    source["version"] = 2
    bucket.put(health.hour_path("hourly", HOUR), source)
    bucket.put(sys.modules["poller_health_gcs"].HEARTBEAT_PATH, {"collection_started_at": START})
    report = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert row(report)["reasons"] == ["invalid_summary"]


def test_retry_reuses_frozen_evaluation_after_warehouse_failure(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    bucket.put(health.hour_path("hourly", HOUR), failing())
    client.fail = True
    with pytest.raises(RuntimeError, match="BQ transport"):
        dag_module.run_monitor(bucket, client, HOUR, health.Config())
    original = bucket.get(health.hour_path("reports", HOUR))
    assert dag_module.HISTORY_PATH not in bucket.objects
    bucket.put(health.hour_path("hourly", HOUR), summary())
    client.fail = False
    retried = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert retried == original
    assert row(retried)["status"] == "degraded"
    assert client.result_calls == 2
    assert bucket.uploads.count(health.hour_path("reports", HOUR)) == 1


def test_alerts_and_history_are_published_while_warehouse_publication_fails(dag_module: Any, monkeypatch: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    bucket.put(health.hour_path("hourly", HOUR), failing())
    client.fail = True
    monkeypatch.setattr(
        dag_module.storage, "Client", lambda **kwargs: types.SimpleNamespace(bucket=lambda name: bucket), raising=False
    )
    monkeypatch.setattr(dag_module.bigquery, "Client", lambda **kwargs: client, raising=False)
    monkeypatch.setattr(
        dag_module, "get_current_context", lambda: {"data_interval_end": HOUR + timedelta(hours=1, minutes=25)}
    )
    monkeypatch.setenv("POLLER_HEALTH_WEBHOOK_URL", "https://example.test/alerts")
    delivered = []
    monkeypatch.setattr(
        dag_module.requests,
        "post",
        lambda *args, **kwargs: delivered.append(kwargs["json"]) or types.SimpleNamespace(status_code=200),
    )
    hour = dag_module.evaluate_hour.function()
    assert hour == health.iso(HOUR)
    assert client.queries == []
    with pytest.raises(RuntimeError, match="BQ transport"):
        dag_module.persist_hour.function(hour)
    dag_module.publish_hour.function(hour)
    assert bucket.get(dag_module.HISTORY_PATH)["hour_start"] == health.iso(HOUR)
    dag_module.alert_transitions.function(hour)
    stored = bucket.get(health.hour_path("reports", HOUR))
    assert len(delivered) == 2
    assert all(entry["delivered_at"] is not None for entry in stored["events"])
    assert all("delivered_at" not in entry for entry in delivered)
    # Publication retries must not overwrite the alert task's acknowledgements.
    client.fail = False
    dag_module.persist_hour.function(hour)
    dag_module.publish_hour.function(hour)
    assert bucket.get(health.hour_path("reports", HOUR)) == stored
    history = bucket.get(dag_module.HISTORY_PATH)
    assert history["hour_start"] == stored["hour_start"]
    assert history["vehicle_types"]["bus"]["status"] == "degraded"


def test_backfill_uses_previous_hour_not_latest_state_and_cannot_regress_history(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    previous_hour = HOUR - timedelta(hours=1)
    previous = failing(previous_hour)
    previous_report = health.evaluate(previous_hour, previous, [])
    bucket.put(health.hour_path("hourly", previous_hour), previous)
    bucket.put(health.hour_path("reports", previous_hour), previous_report)
    bucket.put(health.hour_path("hourly", HOUR), failing())
    future_hour = HOUR + timedelta(days=1)
    future = history_for(health.evaluate(future_hour, summary(future_hour), []))
    bucket.put(dag_module.HISTORY_PATH, future)
    report = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert row(report)["status"] == "degraded"
    assert report["events"] == []
    assert row(report)["state"]["active"]["start_at"] == health.iso(previous_hour)
    assert bucket.get(dag_module.HISTORY_PATH) == future
    assert dag_module.HISTORY_PATH not in bucket.uploads


def test_alert_transport_retry_ack_and_disabled_not_claiming_delivery(
    dag_module: Any, monkeypatch: Any, caplog: Any
) -> None:
    bucket = FakeBucket()
    report = health.evaluate(HOUR, failing(), [])
    path = health.hour_path("reports", HOUR)
    bucket.put(path, report)
    dag_module.deliver_alerts(bucket, HOUR, "")
    assert "webhook disabled" in caplog.text
    assert all(entry["delivered_at"] is None for entry in bucket.get(path)["events"])
    assert all(entry["logged_at"] for entry in bucket.get(path)["events"])
    logged_count = caplog.text.count("Poller health transition (webhook disabled)")
    dag_module.deliver_alerts(bucket, HOUR, "")
    assert caplog.text.count("Poller health transition (webhook disabled)") == logged_count
    # A separate pending outbox exercises HTTP retries; log acknowledgements are terminal.
    bucket = FakeBucket()
    bucket.put(path, report)
    calls = []

    def failed_post(*args: Any, **kwargs: Any) -> None:
        calls.append(copy.deepcopy(kwargs["json"]))
        raise dag_module.requests.RequestException("private webhook URL must not leak")

    monkeypatch.setattr(dag_module.requests, "post", failed_post)
    with pytest.raises(RuntimeError, match="remains pending") as exc:
        dag_module.deliver_alerts(bucket, HOUR, "https://example.test/private-token")
    assert "private-token" not in str(exc.value)
    assert all(entry["delivered_at"] is None for entry in bucket.get(path)["events"])

    def successful_post(*args: Any, **kwargs: Any) -> Any:
        assert kwargs["allow_redirects"] is False
        calls.append(copy.deepcopy(kwargs["json"]))
        return types.SimpleNamespace(status_code=204)

    monkeypatch.setattr(dag_module.requests, "post", successful_post)
    dag_module.deliver_alerts(bucket, HOUR, "https://example.test/private-token")
    assert calls[0]["event_id"] == calls[1]["event_id"]
    assert all(entry["delivered_at"] for entry in bucket.get(path)["events"])
    assert len(calls) == 3
    dag_module.deliver_alerts(bucket, HOUR, "https://example.test/private-token")
    assert len(calls) == 3


@pytest.mark.parametrize("status", [301, 400, 500])
def test_non_success_response_remains_pending(dag_module: Any, monkeypatch: Any, status: int) -> None:
    bucket = FakeBucket()
    report = health.evaluate(HOUR, failing(), [])
    path = health.hour_path("reports", HOUR)
    bucket.put(path, report)
    monkeypatch.setattr(dag_module.requests, "post", lambda *args, **kwargs: types.SimpleNamespace(status_code=status))
    with pytest.raises(RuntimeError, match="pending"):
        dag_module.deliver_alerts(bucket, HOUR, "https://example.test/health")
    assert all(entry["delivered_at"] is None for entry in bucket.get(path)["events"])


@pytest.mark.parametrize("url", ["http://example.test", "https://user:password@example.test", "file:///tmp/alert"])
def test_webhook_requires_https(dag_module: Any, url: str) -> None:
    bucket = FakeBucket()
    bucket.put(health.hour_path("reports", HOUR), health.evaluate(HOUR, summary(), []))
    with pytest.raises(ValueError, match="HTTPS"):
        dag_module.deliver_alerts(bucket, HOUR, url)


def test_cloud_read_bounds_payload_and_validates_versions(dag_module: Any) -> None:
    bucket = FakeBucket()
    path = health.hour_path("hourly", HOUR)
    bucket.objects[path] = (b" " * (health.SUMMARY_MAX_BYTES + 1), 1)
    assert dag_module.read_summary(bucket, HOUR) == (None, "invalid_summary")
    for version in (2, 3):
        bucket.put(health.hour_path("reports", HOUR), {"version": version})
        with pytest.raises(ValueError, match="persisted"):
            dag_module.read_report(bucket, HOUR)


def test_runtime_config_bounded(dag_module: Any, monkeypatch: Any) -> None:
    monkeypatch.setenv("POLLER_HEALTH_DURATION_MINUTES", "20")
    monkeypatch.setenv("POLLER_HEALTH_THRESHOLD", "0.4")
    assert dag_module.config_from_env().duration_minutes == 20
    assert dag_module.config_from_env().threshold == 0.4
    assert dag_module.config_from_env().minimum_fleet == 20
    monkeypatch.setenv("POLLER_HEALTH_MINIMUM_FLEET", "5")
    assert dag_module.config_from_env().minimum_fleet == 5
    for value in ("0", "1001"):
        monkeypatch.setenv("POLLER_HEALTH_MINIMUM_FLEET", value)
        with pytest.raises(ValueError, match="minimum_fleet"):
            dag_module.config_from_env()
    monkeypatch.setenv("POLLER_HEALTH_MINIMUM_FLEET", "20")
    monkeypatch.setenv("POLLER_HEALTH_LOOKBACK_DAYS", "365")
    with pytest.raises(ValueError, match="bounded"):
        dag_module.config_from_env()


def test_first_retained_summary_confirms_rollout_without_current_source(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    older_hour = HOUR - timedelta(days=2)
    bucket.put(health.hour_path("hourly", older_hour), summary(older_hour))
    report = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert report["collection_started_at"] == START
    assert row(report)["status"] == "monitoring_gap"
    assert row(report)["accepted_rows"] is None
    empty = dag_module.run_monitor(FakeBucket(), FakeClient(), HOUR, health.Config())
    assert row(empty)["status"] == "not_monitored"


def test_concurrent_report_create_uses_winning_frozen_report(dag_module: Any, monkeypatch: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    bucket.put(health.hour_path("hourly", HOUR), summary())
    winner = health.evaluate(HOUR, failing(), [])
    path = health.hour_path("reports", HOUR)
    original_write = dag_module.write_object

    def race_write(target: Any, name: str, data: Any, generation: int, **kwargs: Any) -> int:
        if name == path and generation == 0:
            bucket.put(path, winner)
            raise PreconditionFailed
        return original_write(target, name, data, generation, **kwargs)

    monkeypatch.setattr(dag_module, "write_object", race_write)
    result = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert result == winner
    assert bucket.get(dag_module.HISTORY_PATH)["vehicle_types"]["bus"]["status"] == "degraded"


def test_alert_success_with_failed_ack_still_retries_same_event(dag_module: Any, monkeypatch: Any) -> None:
    bucket = FakeBucket()
    path = health.hour_path("reports", HOUR)
    bucket.put(path, health.evaluate(HOUR, failing(), []))
    calls = []
    monkeypatch.setattr(
        dag_module.requests,
        "post",
        lambda *args, **kwargs: calls.append(kwargs["json"]) or types.SimpleNamespace(status_code=200),
    )
    original_write = dag_module.write_object

    def failed_ack(*args: Any, **kwargs: Any) -> None:
        raise PreconditionFailed

    monkeypatch.setattr(dag_module, "write_object", failed_ack)
    with pytest.raises(PreconditionFailed):
        dag_module.deliver_alerts(bucket, HOUR, "https://example.test/health")
    assert bucket.get(path)["events"][0]["delivered_at"] is None
    monkeypatch.setattr(dag_module, "write_object", original_write)
    dag_module.deliver_alerts(bucket, HOUR, "https://example.test/health")
    assert calls[0]["event_id"] == calls[1]["event_id"]
    assert all(entry["delivered_at"] for entry in bucket.get(path)["events"])


def test_configurable_summary_prefix_and_first_retained_marker(dag_module: Any, monkeypatch: Any) -> None:
    monkeypatch.setenv("POLLER_HEALTH_GCS_PREFIX", " /custom/[poller].hourly/ ")
    bucket = FakeBucket()
    path = "custom/[poller].hourly/2026-10-05/12.json"
    bucket.put(path, summary())
    assert sys.modules["poller_health_gcs"].summary_path(HOUR) == path
    assert dag_module.read_summary(bucket, HOUR)[0] == summary()
    assert dag_module.first_collection_start(bucket) == START
    assert bucket.list_calls == ["custom/[poller].hourly/"]
    report = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config())
    assert row(report)["parsed_rows"] == 720000
    assert health.hour_path("reports", HOUR) in bucket.objects
    assert dag_module.HISTORY_PATH in bucket.objects


def test_configurable_heartbeat_root_collection_marker(dag_module: Any, monkeypatch: Any) -> None:
    monkeypatch.setenv("POLLER_HEARTBEAT_GCS_PATH", " /custom/heartbeat.json/ ")
    bucket = FakeBucket()
    bucket.put("custom/heartbeat.json", {"collection_started_at": START})
    assert dag_module.collection_start(bucket) == START
    report = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config())
    assert row(report)["status"] == "monitoring_gap"
    assert report["collection_started_at"] == START


@pytest.mark.parametrize("value", ["", "/", " /// ", " \t "])
def test_empty_configured_paths_fail_explicitly(dag_module: Any, monkeypatch: Any, value: str) -> None:
    bucket = FakeBucket()
    monkeypatch.setenv("POLLER_HEALTH_GCS_PREFIX", value)
    with pytest.raises(ValueError, match="POLLER_HEALTH_GCS_PREFIX must not be empty"):
        dag_module.read_summary(bucket, HOUR)
    with pytest.raises(ValueError, match="POLLER_HEALTH_GCS_PREFIX must not be empty"):
        dag_module.first_collection_start(bucket)
    monkeypatch.setenv("POLLER_HEARTBEAT_GCS_PATH", value)
    with pytest.raises(ValueError, match="POLLER_HEARTBEAT_GCS_PATH must not be empty"):
        dag_module.collection_start(bucket)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda report: report.update(vehicle_types=None),
        lambda report: report.update(vehicle_types=1),
        lambda report: report.update(vehicle_types=[{}]),
        lambda report: report["vehicle_types"].update(bus=None),
        lambda report: report["vehicle_types"]["bus"].update(state=None),
        lambda report: report["vehicle_types"]["bus"].update(state=[]),
        lambda report: report["vehicle_types"]["bus"]["state"].update(active=1),
        lambda report: report["vehicle_types"]["bus"]["state"].update(active={}),
        lambda report: report["vehicle_types"]["bus"]["state"].update(bad_tail=[None]),
        lambda report: report["vehicle_types"]["bus"]["state"].update(good_tail=[None]),
        lambda report: report["vehicle_types"]["bus"].update(baseline=None),
        lambda report: report["vehicle_types"]["bus"].update(reasons=[{}]),
        lambda report: report["vehicle_types"]["bus"].update(status=[]),
        lambda report: report["vehicle_types"]["bus"].update(intervals=[{}]),
        lambda report: report["vehicle_types"]["bus"].update(monitored_minutes=True),
        lambda report: report["vehicle_types"]["bus"].update(parsed_rows=1),
        lambda report: report.update(events=[{}]),
        lambda report: report.update(recent_intervals=[None]),
        lambda report: report.update(collection_started_at=1),
    ],
)
def test_persisted_report_malformed_types_are_value_errors(dag_module: Any, mutation: Any) -> None:
    bucket = FakeBucket()
    report = health.evaluate(HOUR, summary(), [])
    mutation(report)
    bucket.put(health.hour_path("reports", HOUR), report)
    with pytest.raises(ValueError, match=r".+"):
        dag_module.read_report(bucket, HOUR)


def test_persisted_report_outbox_identity_and_tail_timestamps(dag_module: Any) -> None:
    bucket = FakeBucket()
    path = health.hour_path("reports", HOUR)
    report = health.evaluate(HOUR, failing(), [])
    report["events"][0]["event_id"] = "changed"
    bucket.put(path, report)
    with pytest.raises(ValueError, match="event identity"):
        dag_module.read_report(bucket, HOUR)
    report = health.evaluate(HOUR, summary(), [])
    report["vehicle_types"]["bus"]["state"]["good_tail"][0] = health.iso(HOUR - timedelta(days=1))
    bucket.put(path, report)
    with pytest.raises(ValueError, match="tail timestamp"):
        dag_module.read_report(bucket, HOUR)


def test_skipped_monitor_hour_preserves_active_incident_and_history(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    older_hour = HOUR - timedelta(hours=1)
    closed_source = failing(older_hour)
    healthy_tail = summary(older_hour)
    for mode in health.MODES:
        closed_source["vehicle_types"][mode]["minutes"][45:] = healthy_tail["vehicle_types"][mode]["minutes"][45:]
    closed_report = health.evaluate(older_hour, closed_source, [])
    bucket.put(health.hour_path("hourly", older_hour), closed_source)
    bucket.put(health.hour_path("reports", older_hour), closed_report)
    first = failing()
    bucket.put(health.hour_path("hourly", HOUR), first)
    original = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    first_path = health.hour_path("reports", HOUR)
    original_ids = [entry["event_id"] for entry in original["events"]]
    # The collector still ran, but the next monitor evaluation never produced a report.
    missed_hour = HOUR + timedelta(hours=1)
    bucket.put(health.hour_path("hourly", missed_hour), failing(missed_hour))
    resumed_hour = HOUR + timedelta(hours=2)
    bucket.put(health.hour_path("hourly", resumed_hour), failing(resumed_hour))
    resumed = dag_module.run_monitor(bucket, client, resumed_hour, health.Config())
    assert row(resumed)["status"] == "degraded"
    assert row(resumed)["state"]["active"] == row(original)["state"]["active"]
    assert resumed["events"] == []
    assert "monitoring_evaluation_gap" in row(resumed)["reasons"]
    assert {entry["start_at"] for entry in resumed["recent_intervals"]} == {health.iso(older_hour), health.iso(HOUR)}
    assert [
        entry for entry in resumed["recent_intervals"] if entry["start_at"] == health.iso(older_hour)
    ] == closed_report["recent_intervals"]
    assert resumed["collection_started_at"] == original["collection_started_at"]
    assert [entry["event_id"] for entry in bucket.get(first_path)["events"]] == original_ids
    assert bucket.get(first_path) == original
    assert health.hour_path("reports", missed_hour) not in bucket.objects
    health.validate_report(resumed, resumed_hour)


@pytest.mark.parametrize("active", [False, True])
def test_evaluation_gap_cannot_bridge_detection_or_recovery_tails(dag_module: Any, active: bool) -> None:
    bucket, client = FakeBucket(), FakeClient()
    first = failing() if active else summary()
    other = summary() if active else failing()
    for mode in health.MODES:
        first["vehicle_types"][mode]["minutes"][50:] = other["vehicle_types"][mode]["minutes"][50:]
    bucket.put(health.hour_path("hourly", HOUR), first)
    original = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert len(row(original)["state"]["good_tail" if active else "bad_tail"]) == 10
    missed_hour = HOUR + timedelta(hours=1)
    bucket.put(
        health.hour_path("hourly", missed_hour),
        summary(missed_hour) if active else failing(missed_hour),
    )
    resumed_hour = HOUR + timedelta(hours=2)
    current = failing(resumed_hour) if active else summary(resumed_hour)
    prefix = summary(resumed_hour) if active else failing(resumed_hour)
    for mode in health.MODES:
        current["vehicle_types"][mode]["minutes"][:5] = prefix["vehicle_types"][mode]["minutes"][:5]
    bucket.put(health.hour_path("hourly", resumed_hour), current)
    resumed = dag_module.run_monitor(bucket, client, resumed_hour, health.Config())
    assert resumed["events"] == []
    assert row(resumed)["state"]["active"] == row(original)["state"]["active"]
    assert "monitoring_evaluation_gap" in row(resumed)["reasons"]


def test_state_fallback_lists_bounded_names_and_reads_newest_durable_not_public_history(dag_module: Any) -> None:
    bucket = FakeBucket()
    older_hour = HOUR - timedelta(hours=4)
    older = health.evaluate(older_hour, summary(older_hour), [])
    newest_hour = HOUR - timedelta(hours=2)
    newest = health.evaluate(newest_hour, failing(newest_hour), [])
    future_hour = HOUR + timedelta(hours=1)
    future = health.evaluate(future_hour, summary(future_hour), [])
    for hour, report in ((older_hour, older), (newest_hour, newest), (future_hour, future)):
        bucket.put(health.hour_path("reports", hour), report)
    bucket.put(dag_module.HISTORY_PATH, history_for(older))
    restored, gap = dag_module.preceding_report(bucket, HOUR)
    assert gap
    assert restored["hour_start"] == health.iso(HOUR - timedelta(hours=1))
    assert row(restored)["state"]["active"] == row(newest)["state"]["active"]
    assert not row(restored)["state"]["bad_tail"]
    assert not row(restored)["state"]["good_tail"]
    assert bucket.reads == [
        health.hour_path("reports", HOUR - timedelta(hours=1)),
        health.hour_path("reports", newest_hour),
    ]
    assert bucket.list_parameters == [
        {
            "prefix": "health/poller/reports/",
            "start_offset": health.hour_path("reports", HOUR - timedelta(days=90)),
            "end_offset": health.hour_path("reports", HOUR),
            "max_results": 90 * 24 + 1,
            "page_size": 1024,
            "fields": "items(name),nextPageToken",
        }
    ]
    bucket.put(dag_module.HISTORY_PATH, history_for(future))
    bucket.reads.clear()
    restored, gap = dag_module.preceding_report(bucket, HOUR)
    assert gap
    assert row(restored)["state"]["active"] == row(newest)["state"]["active"]
    assert health.hour_path("reports", future_hour) not in bucket.reads


def test_later_hour_retries_original_failed_outbox_without_copying_events(dag_module: Any, monkeypatch: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    bucket.put(health.hour_path("hourly", HOUR), failing())
    original = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    calls = []

    def failed(*args: Any, **kwargs: Any) -> None:
        calls.append(kwargs["json"])
        raise dag_module.requests.RequestException("transport failed")

    monkeypatch.setattr(dag_module.requests, "post", failed)
    with pytest.raises(RuntimeError, match="pending"):
        dag_module.retry_alert_outbox(bucket, HOUR, "https://example.test/health")
    later = HOUR + timedelta(hours=2)
    bucket.put(health.hour_path("hourly", later), failing(later))
    current = dag_module.run_monitor(bucket, client, later, health.Config())
    assert current["events"] == []
    monkeypatch.setattr(
        dag_module.requests,
        "post",
        lambda *args, **kwargs: calls.append(kwargs["json"]) or types.SimpleNamespace(status_code=204),
    )
    dag_module.retry_alert_outbox(bucket, later, "https://example.test/health")
    assert calls[0]["event_id"] == calls[1]["event_id"]
    assert [entry["event_id"] for entry in calls[1:]] == [entry["event_id"] for entry in original["events"]]
    assert all(entry["delivered_at"] for entry in bucket.get(health.hour_path("reports", HOUR))["events"])
    assert all("logged_at" not in entry for entry in calls)
    dag_module.retry_alert_outbox(bucket, later, "https://example.test/health")
    assert len(calls) == 3


def test_disabled_webhook_log_ack_is_terminal_and_bounded(dag_module: Any, monkeypatch: Any, caplog: Any) -> None:
    bucket = FakeBucket()
    report = health.evaluate(HOUR, failing(), [])
    path = health.hour_path("reports", HOUR)
    bucket.put(path, report)
    dag_module.retry_alert_outbox(bucket, HOUR, "")
    logged_count = caplog.text.count("Poller health transition (webhook disabled)")
    assert logged_count == 2
    later = HOUR + timedelta(hours=1)
    bucket.put(health.hour_path("reports", later), health.evaluate(later, summary(later), []))
    dag_module.retry_alert_outbox(bucket, later, "")
    assert caplog.text.count("Poller health transition (webhook disabled)") == logged_count
    monkeypatch.setattr(
        dag_module.requests, "post", lambda *args, **kwargs: pytest.fail("log-acknowledged events must not be sent")
    )
    dag_module.retry_alert_outbox(bucket, later, "https://example.test/health")
    stored = bucket.get(path)
    assert len(stored["events"]) == 2
    assert all(entry["delivered_at"] is None and entry["logged_at"] for entry in stored["events"])
    assert bucket.get(health.hour_path("reports", later))["events"] == []


def test_outbox_horizon_warns_without_deleting_pending_and_supports_manual_replay(
    dag_module: Any, monkeypatch: Any, caplog: Any
) -> None:
    bucket = FakeBucket()
    current_hour = HOUR + timedelta(hours=dag_module.OUTBOX_RETRY_HOURS - 1)
    original = health.evaluate(HOUR, failing(), [])
    original_path = health.hour_path("reports", HOUR)
    bucket.put(original_path, original)
    bucket.put(health.hour_path("reports", current_hour), health.evaluate(current_hour, summary(current_hour), []))
    monkeypatch.setattr(dag_module.requests, "post", lambda *args, **kwargs: types.SimpleNamespace(status_code=500))
    with pytest.raises(RuntimeError, match="pending"):
        dag_module.retry_alert_outbox(bucket, current_hour, "https://example.test/health")
    assert "manually replay deliver_alerts" in caplog.text
    assert bucket.get(original_path) == original
    following = current_hour + timedelta(hours=1)
    bucket.put(health.hour_path("reports", following), health.evaluate(following, summary(following), []))
    bucket.reads.clear()
    dag_module.retry_alert_outbox(bucket, following, "https://example.test/health")
    assert len(bucket.reads) == dag_module.OUTBOX_RETRY_HOURS
    assert original_path not in bucket.reads
    assert all(entry["delivered_at"] is None for entry in bucket.get(original_path)["events"])
    monkeypatch.setattr(dag_module.requests, "post", lambda *args, **kwargs: types.SimpleNamespace(status_code=200))
    dag_module.deliver_alerts(bucket, HOUR, "https://example.test/health")
    assert all(entry["delivered_at"] for entry in bucket.get(original_path)["events"])


def test_gap_beyond_outbox_horizon_warns_about_older_manual_handling(dag_module: Any, caplog: Any) -> None:
    bucket = FakeBucket()
    older_hour = HOUR - timedelta(hours=60)
    older = health.evaluate(older_hour, failing(older_hour), [])
    bucket.put(health.hour_path("reports", older_hour), older)
    bucket.put(dag_module.HISTORY_PATH, history_for(older))
    restored, gap = dag_module.preceding_report(bucket, HOUR)
    assert gap
    assert row(restored)["state"]["active"] == row(older)["state"]["active"]
    assert "older reports may contain pending alerts" in caplog.text
    assert "manually replay deliver_alerts" in caplog.text


def test_legacy_outbox_events_without_logged_at_still_replay(dag_module: Any, monkeypatch: Any) -> None:
    bucket = FakeBucket()
    report = health.evaluate(HOUR, failing(), [])
    for entry in report["events"]:
        entry.pop("logged_at")
    bucket.put(health.hour_path("reports", HOUR), report)
    monkeypatch.setattr(dag_module.requests, "post", lambda *args, **kwargs: types.SimpleNamespace(status_code=200))
    dag_module.retry_alert_outbox(bucket, HOUR, "https://example.test/health")
    assert all(entry["delivered_at"] for entry in bucket.get(health.hour_path("reports", HOUR))["events"])


@pytest.mark.parametrize("already_acknowledged", [False, True])
def test_outbox_deduplicates_shared_event_ids_across_reports(
    dag_module: Any, monkeypatch: Any, already_acknowledged: bool
) -> None:
    bucket = FakeBucket()
    original = health.evaluate(HOUR, failing(), [])
    later = HOUR + timedelta(hours=1)
    duplicate = health.evaluate(later, summary(later), [])
    duplicate["events"] = copy.deepcopy(original["events"])
    if already_acknowledged:
        for entry in duplicate["events"]:
            entry["delivered_at"] = health.iso(later + timedelta(hours=1))
    bucket.put(health.hour_path("reports", HOUR), original)
    bucket.put(health.hour_path("reports", later), duplicate)
    calls = []
    monkeypatch.setattr(
        dag_module.requests,
        "post",
        lambda *args, **kwargs: calls.append(kwargs["json"]) or types.SimpleNamespace(status_code=200),
    )
    dag_module.retry_alert_outbox(bucket, later, "https://example.test/health")
    assert len(calls) == (0 if already_acknowledged else 2)
    older_events = bucket.get(health.hour_path("reports", HOUR))["events"]
    newer_events = bucket.get(health.hour_path("reports", later))["events"]
    assert [entry["delivered_at"] for entry in older_events] == [entry["delivered_at"] for entry in newer_events]
    assert all(entry["delivered_at"] for entry in older_events)


def test_duplicate_event_ids_in_one_report_rejected(dag_module: Any) -> None:
    bucket = FakeBucket()
    report = health.evaluate(HOUR, failing(), [])
    report["events"].append(copy.deepcopy(report["events"][0]))
    bucket.put(health.hour_path("reports", HOUR), report)
    with pytest.raises(ValueError, match="duplicate event_id"):
        dag_module.read_report(bucket, HOUR)


def test_durable_report_discovery_rejects_resource_overflow(dag_module: Any) -> None:
    bucket = FakeBucket()
    prefix = health.hour_path("reports", HOUR - timedelta(hours=1))
    for index in range(dag_module.REPORT_LIST_LIMIT):
        bucket.put(f"{prefix}.invalid-{index:04d}", {})
    with pytest.raises(ValueError, match="bounded lookback"):
        dag_module.preceding_report(bucket, HOUR)
    assert bucket.reads == [health.hour_path("reports", HOUR - timedelta(hours=1))]


def test_durable_report_discovery_rejects_corrupt_newest_referent(dag_module: Any) -> None:
    bucket = FakeBucket()
    older = HOUR - timedelta(hours=3)
    newest = HOUR - timedelta(hours=2)
    bucket.put(health.hour_path("reports", older), health.evaluate(older, summary(older), []))
    bucket.put(health.hour_path("reports", newest), {"version": 2})
    with pytest.raises(ValueError, match="persisted poller report"):
        dag_module.preceding_report(bucket, HOUR)
    assert health.hour_path("reports", older) not in bucket.reads


def test_immediate_predecessor_avoids_report_listing(dag_module: Any) -> None:
    bucket = FakeBucket()
    previous = HOUR - timedelta(hours=1)
    report = health.evaluate(previous, summary(previous), [])
    bucket.put(health.hour_path("reports", previous), report)
    assert dag_module.preceding_report(bucket, HOUR) == (report, False)
    assert bucket.list_calls == []


def active_fleet_bucket(dag_module: Any) -> tuple[FakeBucket, dict[str, Any]]:
    bucket = FakeBucket()
    previous = HOUR - timedelta(hours=1)
    report = health.evaluate(previous, summary(previous, vehicles=1, lines=1), samples(previous))
    bucket.put(health.hour_path("reports", previous), report)
    bucket.put(health.hour_path("hourly", previous), summary(previous, vehicles=1, lines=1))
    bucket.put(dag_module.HISTORY_PATH, history_for(report))
    bucket.put(health.hour_path("hourly", HOUR), summary())
    return bucket, report


def test_operator_rebaseline_persists_distinct_action_and_retries_frozen_result(dag_module: Any) -> None:
    bucket, prior = active_fleet_bucket(dag_module)
    client = FakeClient()
    client.fail = True
    with pytest.raises(RuntimeError, match="BQ transport"):
        dag_module.run_monitor(bucket, client, HOUR, health.Config(), reset_modes=["bus"])
    frozen = bucket.get(health.hour_path("reports", HOUR))
    assert [entry["transition"] for entry in frozen["events"]] == ["rebaseline"]
    assert frozen["events"][0]["at"] == health.iso(HOUR)
    assert frozen["events"][0]["start_at"] == row(prior)["state"]["active"]["start_at"]
    assert row(frozen)["state"]["active"] is None
    assert row(frozen)["state"]["baseline_reset_at"] == health.iso(HOUR)
    assert row(frozen, "tram")["state"]["active"] is not None
    assert row(frozen)["status"] == "warming_up"
    assert "baseline_reset" in row(frozen)["reasons"]
    assert row(frozen)["intervals"] == []
    assert [entry for entry in frozen["recent_intervals"] if entry["mode"] == "bus"] == [
        entry for entry in prior["recent_intervals"] if entry["mode"] == "bus"
    ]
    client.fail = False
    assert dag_module.run_monitor(bucket, client, HOUR, health.Config(), reset_modes=["bus"]) == frozen
    assert bucket.uploads.count(health.hour_path("reports", HOUR)) == 1
    assert bucket.get(health.hour_path("reports", HOUR - timedelta(hours=1))) == prior
    with pytest.raises(ValueError, match="frozen report"):
        dag_module.run_monitor(bucket, client, HOUR, health.Config(), reset_modes=["tram"])


def test_operator_rebaseline_rejects_other_existing_frozen_report(dag_module: Any) -> None:
    bucket, _ = active_fleet_bucket(dag_module)
    frozen = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config())
    with pytest.raises(ValueError, match="frozen report"):
        dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config(), reset_modes=["bus"])
    assert bucket.get(health.hour_path("reports", HOUR)) == frozen


@pytest.mark.parametrize("published", [False, True])
def test_operator_rebaseline_rejects_backfill_even_if_newer_durable_not_published(
    dag_module: Any, published: bool
) -> None:
    bucket, _ = active_fleet_bucket(dag_module)
    future = HOUR + timedelta(hours=1)
    report = health.evaluate(future, summary(future), [])
    if published:
        bucket.put(dag_module.HISTORY_PATH, history_for(report))
    else:
        bucket.put(health.hour_path("reports", future), report)
    with pytest.raises(ValueError, match="chronological"):
        dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config(), reset_modes=["bus"])
    assert health.hour_path("reports", HOUR) not in bucket.objects


@pytest.mark.parametrize("value", [True, False, None, "bus", [True], ["metro"], ["bus", "bus"], {"bus": True}])
def test_operator_rebaseline_input_is_explicit_unique_known_modes(dag_module: Any, value: Any) -> None:
    bucket, _ = active_fleet_bucket(dag_module)
    with pytest.raises(ValueError, match="rebaseline_modes"):
        dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config(), reset_modes=value)
    assert health.hour_path("reports", HOUR) not in bucket.objects


def test_dag_run_conf_passes_validated_rebaseline_modes(dag_module: Any, monkeypatch: Any) -> None:
    context = {
        "data_interval_end": HOUR.replace(minute=25),
        "dag_run": types.SimpleNamespace(conf={"rebaseline_modes": ["tram", "bus"]}),
    }
    monkeypatch.setattr(dag_module, "get_current_context", lambda: context)
    monkeypatch.setattr(
        dag_module.storage,
        "Client",
        lambda **kwargs: types.SimpleNamespace(bucket=lambda name: FakeBucket()),
        raising=False,
    )
    monkeypatch.setattr(dag_module.bigquery, "Client", lambda **kwargs: FakeClient(), raising=False)
    requests = []
    monkeypatch.setattr(dag_module, "freeze_report", lambda *args, **kwargs: requests.append(kwargs["reset_modes"]))
    dag_module.evaluate_hour.function()
    assert requests == [("bus", "tram")]
    for value in (True, ("bus",), None):
        context["dag_run"].conf = {"rebaseline_modes": value}
        with pytest.raises(ValueError, match="must be a list"):
            dag_module.reset_modes_from_context(context)


def test_rebaseline_epoch_survives_durable_gap_and_excludes_old_baseline(dag_module: Any) -> None:
    bucket, _ = active_fleet_bucket(dag_module)
    seed_history(bucket)
    reset = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config(), reset_modes=("bus",))
    assert row(reset)["baseline_samples"] == 0
    assert row(reset)["status"] == "warming_up"
    assert row(reset, "tram")["baseline_samples"] == 3
    later = HOUR + timedelta(hours=2)
    seed_history(bucket, later)
    bucket.put(health.hour_path("hourly", later), summary(later))
    resumed = dag_module.run_monitor(bucket, FakeClient(), later, health.Config())
    assert row(resumed)["baseline_samples"] == 0
    assert row(resumed, "tram")["baseline_samples"] == 3
    assert row(resumed)["state"]["baseline_reset_at"] == health.iso(HOUR)
    assert row(resumed)["state"]["active"] is None
    assert resumed["events"] == []
    assert "monitoring_evaluation_gap" in row(resumed)["reasons"]
    assert "baseline_reset" not in row(resumed)["reasons"]


def test_durable_discovery_rejects_listed_but_missing_report(dag_module: Any, monkeypatch: Any) -> None:
    bucket = FakeBucket()
    missing = health.hour_path("reports", HOUR - timedelta(hours=2))
    monkeypatch.setattr(bucket, "list_blobs", lambda **kwargs: [types.SimpleNamespace(name=missing)])
    with pytest.raises(ValueError, match="listed poller report is missing"):
        dag_module.preceding_report(bucket, HOUR)
