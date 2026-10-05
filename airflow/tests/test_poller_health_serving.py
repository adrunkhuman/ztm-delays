from __future__ import annotations

import copy
import json
from datetime import timedelta
from typing import Any

import pytest

from .test_dag_serving_export import FakeBlob, FakeStorageClient, _load_dag_module
from .test_poller_health import HOUR, health, samples, summary


def payload(*, degraded: bool = False) -> dict[str, Any]:
    source = summary(vehicles=20, lines=2, ratio=0.2) if degraded else summary()
    report = health.evaluate(HOUR, source, samples(), evaluated_at=HOUR + timedelta(hours=1, minutes=25))
    return health.snapshot(report)


def exported(data: dict[str, Any], now: Any = None) -> dict[str, Any]:
    dag = _load_dag_module()
    client = FakeStorageClient([FakeBlob("health/poller/feed-status.json", data=json.dumps(data).encode())])
    return dag._poller_status(client, now or HOUR + timedelta(hours=1, minutes=30), "bucket")


def test_feed_status_is_optional_with_legacy_heartbeat() -> None:
    dag = _load_dag_module()
    heartbeat = {"status": "ok", "updated_at": health.iso(HOUR), "vehicle_types": {}}
    result = dag._poller_status(
        FakeStorageClient([FakeBlob("health/poller/latest.json", data=json.dumps(heartbeat).encode())]),
        HOUR,
        "bucket",
    )
    assert result["status"] == "ok"
    assert "feed_health" not in result


def test_feed_status_survives_missing_request_heartbeat_and_is_allowlisted() -> None:
    data = payload(degraded=True)
    data["hostname"] = "private-host"
    data["events"] = [{"webhook": "private-url"}]
    data["vehicle_types"]["bus"]["private_state"] = "private-host"
    data["vehicle_types"]["bus"]["reasons"].append("private-secret")
    result = exported(data)
    assert result["status"] == "unknown"
    assert result["feed_health"]["vehicle_types"]["bus"]["status"] == "degraded"
    assert result["feed_health"]["recent_intervals"][0]["reason"] == "stale_heavy"
    assert "private-" not in json.dumps(result)


def test_healthy_hour_maps_to_fresh_and_old_hour_is_stale_even_after_reevaluation() -> None:
    data = payload()
    assert exported(data)["feed_health"]["vehicle_types"]["bus"]["status"] == "fresh"
    now = HOUR + timedelta(hours=4)
    data["evaluated_at"] = health.iso(now)
    modes = exported(data, now)["feed_health"]["vehicle_types"]
    assert modes["bus"]["status"] == "stale"
    assert modes["bus"]["status_at_evaluation"] == "healthy"


@pytest.mark.parametrize("version", [None, True, 2, "1"])
def test_unknown_snapshot_version_does_not_prevent_export(version: object) -> None:
    data = payload()
    data["version"] = version
    assert exported(data)["feed_health"] == {"status": "unknown", "vehicle_types": {}}


@pytest.mark.parametrize("field", ["monitored_minutes", "parsed_rows", "mean_accepted_lines"])
def test_invalid_metrics_do_not_prevent_export(field: str) -> None:
    data = payload()
    data["vehicle_types"]["bus"][field] = -1
    assert exported(data)["feed_health"]["status"] == "unknown"


def test_future_evaluation_and_oversized_payload_are_unknown() -> None:
    data = payload()
    data["evaluated_at"] = health.iso(HOUR + timedelta(hours=2))
    assert exported(data)["feed_health"]["status"] == "unknown"
    data = payload()
    data["extra"] = "x" * health.REPORT_MAX_BYTES
    assert exported(data)["feed_health"]["status"] == "unknown"


def test_partial_hours_preserve_nullable_counts_not_zero() -> None:
    report = health.evaluate(
        HOUR,
        None,
        [],
        collection_started_at=health.iso(HOUR + timedelta(minutes=30)),
        evaluated_at=HOUR + timedelta(hours=1, minutes=25),
    )
    result = exported(health.snapshot(report))["feed_health"]["vehicle_types"]["bus"]
    assert result["status"] == "partial"
    assert result["parsed_rows"] is None
    assert result["accepted_rows"] is None


def test_source_snapshot_is_not_mutated() -> None:
    data = payload()
    original = copy.deepcopy(data)
    exported(data)
    assert data == original
