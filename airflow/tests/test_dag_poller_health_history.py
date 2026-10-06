from __future__ import annotations

import sys
from datetime import timedelta
from typing import Any

import pytest

from . import test_dag_poller_health as helpers
from .test_dag_poller_health import FakeBucket, FakeClient, history_for, seed_history
from .test_poller_health import HOUR, START, health, row, summary
from .test_poller_health_baseline import legacy_v1, seed, seeds

dag_module = helpers.dag_module


def put_seeds(bucket: FakeBucket, sources: list[dict[str, Any]]) -> None:
    for source in sources:
        bucket.put(health.hour_path("baseline-seed", health.timestamp(source["hour_start"])), source)


def test_read_sample_prefers_summary_then_seed_and_skips_bad_seeds(dag_module: Any, caplog: Any) -> None:
    bucket = FakeBucket()
    assert dag_module.read_sample(bucket, HOUR) == (None, None)
    put_seeds(bucket, [seed(bus=7)])
    assert dag_module.read_sample(bucket, HOUR) == (None, seed(bus=7))
    bucket.put(health.hour_path("hourly", HOUR), summary())
    assert dag_module.read_sample(bucket, HOUR) == (summary(), summary())
    bad = seed()
    bad["source"] = "poller"
    other = HOUR + timedelta(hours=1)
    bucket.put(health.hour_path("baseline-seed", other), bad)
    assert dag_module.read_sample(bucket, other) == (None, None)
    assert "Invalid poller baseline seed" in caplog.text
    # The seed must describe the hour it is stored under.
    bucket.put(health.hour_path("baseline-seed", other), seed())
    assert dag_module.read_sample(bucket, other) == (None, None)
    bucket.objects[health.hour_path("baseline-seed", other)] = (b" " * (health.SEED_MAX_BYTES + 1), 9)
    assert dag_module.read_sample(bucket, other) == (None, None)


def test_seeds_give_a_baseline_from_the_first_monitored_hour(dag_module: Any) -> None:
    bucket, client = FakeBucket(), FakeClient()
    bucket.put(sys.modules["poller_health_gcs"].HEARTBEAT_PATH, {"collection_started_at": health.iso(HOUR)})
    put_seeds(bucket, seeds())
    current = summary()
    current["collection_started_at"] = health.iso(HOUR)
    for item in current["vehicle_types"]["bus"]["minutes"]:
        item["accepted_vehicle_count_sum"] = 0
    bucket.put(health.hour_path("hourly", HOUR), current)
    report = dag_module.run_monitor(bucket, client, HOUR, health.Config())
    assert report["collection_started_at"] == health.iso(HOUR)
    assert row(report)["baseline_samples"] == 3
    assert row(report)["reasons"] == ["no_accepted"]
    assert row(report, "tram")["status"] == "healthy"
    assert row(report, "tram")["baseline"]["vehicles"] == [50] * 60


def test_summary_wins_over_seed_for_the_same_hour(dag_module: Any) -> None:
    bucket = FakeBucket()
    put_seeds(bucket, seeds(bus=1000))
    seed_history(bucket)
    bucket.put(health.hour_path("hourly", HOUR), summary())
    report = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config())
    assert row(report)["baseline"]["vehicles"] == [100] * 60
    assert row(report)["status"] == "healthy"


def test_publish_history_writes_lookahead_baselines_and_previous_reports(dag_module: Any) -> None:
    bucket = FakeBucket()
    bucket.put(sys.modules["poller_health_gcs"].HEARTBEAT_PATH, {"collection_started_at": START})
    previous_hour = HOUR - timedelta(hours=1)
    bucket.put(health.hour_path("reports", previous_hour), health.evaluate(previous_hour, summary(previous_hour), []))
    put_seeds(bucket, seeds(HOUR + timedelta(hours=1), bus=77))
    seed_history(bucket, HOUR + timedelta(hours=3), vehicles=33)
    bucket.put(health.hour_path("hourly", HOUR), summary())
    report = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config(minimum_fleet=40))
    history = bucket.get(dag_module.HISTORY_PATH)
    assert bucket.cache_control[dag_module.HISTORY_PATH] == "no-cache"
    assert history["hour_start"] == health.iso(HOUR)
    assert history["rules"]["minimum_fleet"] == 40
    bus = history["vehicle_types"]["bus"]
    assert len(bus["fresh"]) == 1440
    assert bus["fresh"][:1320] == [None] * 1320
    assert bus["fresh"][1320:] == [100] * 120
    assert len(bus["usual"]) == len(bus["low"]) == len(bus["high"]) == 1620
    assert bus["usual"][1440:1500] == [77] * 60
    assert bus["usual"][1500:1560] == [None] * 60
    assert bus["usual"][1560:] == [33] * 60
    assert history["vehicle_types"]["tram"]["usual"][1440:1500] == [50] * 60
    # A retry of the same hour may refresh it; an older hour may not.
    dag_module.publish_history(bucket, report, health.Config())
    assert bucket.get(dag_module.HISTORY_PATH)["rules"]["minimum_fleet"] == 20
    older = health.evaluate(previous_hour, summary(previous_hour), [])
    uploads = len(bucket.uploads)
    dag_module.publish_history(bucket, older, health.Config())
    assert len(bucket.uploads) == uploads
    assert bucket.get(dag_module.HISTORY_PATH)["hour_start"] == health.iso(HOUR)


def test_corrupt_past_report_or_public_object_does_not_block_history(dag_module: Any, caplog: Any) -> None:
    bucket = FakeBucket()
    corrupt_hour = HOUR - timedelta(hours=1)
    bad = health.evaluate(corrupt_hour, summary(corrupt_hour), [])
    bad["vehicle_types"]["bus"]["fresh"] = [-1] * 60
    bucket.put(health.hour_path("reports", corrupt_hour), bad)
    bucket.put(dag_module.HISTORY_PATH, {"version": 1, "hour_start": "not a time"})
    report = health.evaluate(HOUR, summary(), [])
    dag_module.publish_history(bucket, report, health.Config())
    history = bucket.get(dag_module.HISTORY_PATH)
    assert history["hour_start"] == health.iso(HOUR)
    assert history["vehicle_types"]["bus"]["fresh"][1320:1380] == [None] * 60
    assert "shown as unknown" in caplog.text


def test_rebaseline_rejects_invalid_public_history(dag_module: Any) -> None:
    bucket = FakeBucket()
    history = history_for(health.evaluate(HOUR - timedelta(hours=2), summary(HOUR - timedelta(hours=2)), []))
    history["version"] = 2
    bucket.put(dag_module.HISTORY_PATH, history)
    with pytest.raises(ValueError, match="public history version"):
        dag_module.ensure_chronological_reset(bucket, HOUR)


def test_v1_report_with_stale_heavy_incident_is_read_and_recovers(dag_module: Any) -> None:
    bucket = FakeBucket()
    previous_hour = HOUR - timedelta(hours=1)
    # Under the old rules a mostly stale hour was an incident; now it is normal service.
    previous = summary(previous_hour, ratio=0.2)
    v1 = legacy_v1(health.evaluate(previous_hour, previous, []), stale_mode="bus")
    bucket.put(health.hour_path("hourly", previous_hour), previous)
    bucket.put(health.hour_path("reports", previous_hour), v1)
    upgraded, _ = dag_module.read_report(bucket, previous_hour)
    assert upgraded["version"] == 2
    assert row(upgraded)["state"]["active"]["reason"] == "stale_heavy"
    seed_history(bucket)
    bucket.put(health.hour_path("hourly", HOUR), summary(ratio=0.2))
    report = dag_module.run_monitor(bucket, FakeClient(), HOUR, health.Config())
    health.validate_report(report, HOUR)
    assert row(report)["status"] == "healthy"
    assert row(report)["state"]["active"] is None
    assert row(report)["intervals"] == [
        {"start_at": health.iso(previous_hour), "end_at": health.iso(HOUR), "reason": "stale_heavy"}
    ]
    assert [(entry["mode"], entry["transition"], entry["reason"]) for entry in report["events"]] == [
        ("bus", "recovered", "stale_heavy")
    ]
    assert "monitoring_evaluation_gap" not in row(report)["reasons"]
    # The stored v1 report is not rewritten; the public history hides the legacy incident.
    assert bucket.get(health.hour_path("reports", previous_hour)) == v1
    history = bucket.get(dag_module.HISTORY_PATH)
    assert history["incidents"] == []
    assert history["vehicle_types"]["bus"]["fresh"][1320:1380] == [None] * 60
    assert history["vehicle_types"]["bus"]["fresh"][1380:] == [100] * 60
