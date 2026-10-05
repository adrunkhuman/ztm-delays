from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from . import test_dag_poller_health as helpers
from .test_dag_poller_health import FakeBucket, FakeClient
from .test_poller_health import HOUR, health, row, summary

dag_module = helpers.dag_module


def test_resume_discovers_durable_incident_even_when_warehouse_publication_failed(dag_module: Any) -> None:
    bucket = FakeBucket()
    client = FakeClient()
    before = HOUR - timedelta(hours=1)
    bucket.put(health.hour_path("hourly", before), summary(before))
    dag_module.run_monitor(bucket, client, before, health.Config())
    bucket.put(health.hour_path("hourly", HOUR), summary(HOUR, ratio=0.2, vehicles=20, lines=2))
    client.fail = True
    with pytest.raises(RuntimeError, match="BQ transport"):
        dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert row(bucket.get(health.hour_path("reports", HOUR)))["state"]["active"]
    assert bucket.get(dag_module.SNAPSHOT_PATH)["hour_start"] == health.iso(before)
    client.fail = False
    resumed_hour = HOUR + timedelta(hours=2)
    bucket.put(health.hour_path("hourly", resumed_hour), summary(resumed_hour))
    resumed = dag_module.run_monitor(bucket, client, resumed_hour, health.Config())
    assert [entry["transition"] for entry in resumed["events"]] == ["recovered", "recovered"]
    assert {entry["start_at"] for entry in resumed["events"]} == {health.iso(HOUR)}
    assert "monitoring_evaluation_gap" in row(resumed)["reasons"]


@pytest.mark.parametrize("active", [False, True])
def test_unevaluated_hours_break_telemetry_loss_and_restoration_tails(dag_module: Any, active: bool) -> None:
    bucket = FakeBucket()
    previous = summary()
    for mode in health.MODES:
        minutes = previous["vehicle_types"][mode]["minutes"]
        previous["vehicle_types"][mode]["minutes"] = minutes[50:] if active else minutes[:50]
    prior = health.evaluate(HOUR, previous, [])
    bucket.put(health.hour_path("reports", HOUR), prior)
    resumed_hour = HOUR + timedelta(hours=2)
    current = summary(resumed_hour)
    for mode in health.MODES:
        minutes = current["vehicle_types"][mode]["minutes"]
        current["vehicle_types"][mode]["minutes"] = minutes[:5] if active else minutes[5:]
    bucket.put(health.hour_path("hourly", resumed_hour), current)
    resumed = dag_module.freeze_report(bucket, resumed_hour, health.Config())
    health.validate_report(resumed, resumed_hour)
    assert resumed["events"] == []
    assert row(resumed)["state"]["monitoring"]["active"] == row(prior)["state"]["monitoring"]["active"]
