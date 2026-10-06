# ruff: noqa: PLR2004 -- Synthetic expected counters remain beside their inputs.
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import poller
from poller_health_counters import DEFAULT_PREFIX, HealthCounters
from tests.test_poller_health import HealthBucket

if TYPE_CHECKING:
    from types import ModuleType

    import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _collect(tmp_path: Path, hour: datetime, *, low: bool = False, restart: bool = False) -> dict[str, object]:
    counters = HealthCounters(tmp_path, 60)
    for minute in range(60):
        if restart and minute == 30:
            counters = HealthCounters(tmp_path, 60)
        for mode in ("bus", "tram"):
            counters.record(
                poller.PollResult(
                    vehicle_type_name=mode,
                    attempted_at=hour + timedelta(minutes=minute),
                    succeeded=True,
                    parsed_rows=100,
                    accepted_rows=20 if low else 100,
                    dropped_stale=80 if low else 0,
                    accepted_vehicle_count=20 if low else 100,
                    accepted_line_count=2 if low else 10,
                )
            )
    bucket = HealthBucket()
    assert counters.upload(bucket, DEFAULT_PREFIX, hour + timedelta(hours=1))
    assert len(bucket.objects) == 1
    return json.loads(next(iter(bucket.objects.values())))


def test_collector_restart_to_monitor_to_public_history_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monitor = _load("poller_health", ROOT / "airflow/dags/poller_health.py", monkeypatch)
    public = _load("poller_health_public", ROOT / "airflow/dags/poller_health_public.py", monkeypatch)
    hour = datetime(2026, 10, 5, 12, tzinfo=UTC)
    samples = []
    for days in (21, 14, 7):
        candidate = hour - timedelta(days=days)
        source = _collect(tmp_path / str(days), candidate)
        monitor.validate_summary(source, candidate)
        samples.append(source)
    current = _collect(tmp_path / "current", hour, low=True, restart=True)
    monitor.validate_summary(current, hour)
    report = monitor.evaluate(hour, current, samples, evaluated_at=hour + timedelta(hours=1, minutes=25))
    monitor.validate_report(json.loads(json.dumps(report)), hour)
    config = monitor.DEFAULT_CONFIG
    ahead = {mode: [monitor.baseline(samples, mode, config)] * 3 for mode in monitor.MODES}
    history = public.feed_history(report, [None] * 23, ahead, config)
    bus = history["vehicle_types"]["bus"]
    assert bus["status"] == "degraded"
    assert bus["baseline_samples"] == len(samples)
    assert bus["fresh"][-60:] == [20.0] * 60
    assert bus["usual"][-240:-180] == [100.0] * 60
    assert len(bus["usual"]) == 24 * 60 + 180
    assert history["incidents"][0]["reason"] == "low_fleet"
    assert history["incidents"][0]["ongoing"] is True
    assert "state" not in json.dumps(history)
