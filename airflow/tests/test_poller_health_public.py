from __future__ import annotations

import importlib.util
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from .test_poller_health import HOUR, failing, health, row, samples, summary

_SPEC = importlib.util.spec_from_file_location(
    "poller_health_public_under_test", Path(__file__).parents[1] / "dags/poller_health_public.py"
)
assert _SPEC
assert _SPEC.loader
public = importlib.util.module_from_spec(_SPEC)
with mock.patch.dict(sys.modules, {"poller_health": health}):
    _SPEC.loader.exec_module(public)
sys.modules[_SPEC.name] = public
END = HOUR + timedelta(hours=1)


def report_at(hour: Any, source: Any = None) -> dict[str, Any]:
    return health.evaluate(hour, summary(hour) if source is None else source, samples(hour), evaluated_at=END)


def previous_reports() -> list[dict[str, Any] | None]:
    return [report_at(HOUR - timedelta(hours=offset)) for offset in range(23, 0, -1)]


def lookahead(vehicles: float | None = 80) -> dict[str, list[dict[str, Any]]]:
    expected = {"samples": 3, **{key: [vehicles] * 60 for key in ("vehicles", "low", "high")}}
    return {mode: [expected] * 3 for mode in health.MODES}


def test_feed_history_shape_series_lengths_and_rules() -> None:
    report = report_at(HOUR)
    config = health.Config(minimum_fleet=30)
    history = public.feed_history(report, previous_reports(), lookahead(), config)
    assert set(history) == {
        "version",
        "evaluated_at",
        "hour_start",
        "series_start",
        "history_minutes",
        "rules",
        "vehicle_types",
        "incidents",
    }
    assert history["version"] == 1
    assert history["hour_start"] == health.iso(HOUR)
    assert history["evaluated_at"] == report["evaluated_at"]
    assert history["series_start"] == health.iso(END - timedelta(hours=24))
    assert history["history_minutes"] == 1440
    assert history["rules"] == {"threshold": 0.5, "duration_minutes": 15, "minimum_fleet": 30, "lookback_days": 28}
    assert set(history["vehicle_types"]) == set(health.MODES)
    for mode in health.MODES:
        series = history["vehicle_types"][mode]
        assert set(series) == {"status", "baseline_samples", "fresh", "usual", "low", "high"}
        assert series["status"] == "healthy"
        assert series["baseline_samples"] == 3
        assert series["fresh"] == [100] * 1440
        for key in ("usual", "low", "high"):
            assert series[key] == [100] * 1440 + [80] * 180
    assert history["incidents"] == []
    payload = json.dumps(history, allow_nan=False)
    assert "state" not in payload
    assert "tail" not in payload


def test_missing_reports_are_unknown_never_zero() -> None:
    previous = previous_reports()
    previous[0] = None
    previous[-1] = None
    history = public.feed_history(report_at(HOUR), previous, lookahead(None), health.Config())
    bus = history["vehicle_types"]["bus"]
    assert bus["fresh"][:60] == [None] * 60
    assert bus["fresh"][60:1320] == [100] * 1260
    assert bus["fresh"][1320:1380] == [None] * 60
    assert bus["fresh"][1380:] == [100] * 60
    assert bus["usual"][1320:1380] == [None] * 60
    assert bus["usual"][1440:] == [None] * 180


@pytest.mark.parametrize("problem", ["shifted", "short_history", "short_lookahead"])
def test_feed_history_rejects_inconsistent_inputs(problem: str) -> None:
    previous = previous_reports()
    ahead = lookahead()
    if problem == "shifted":
        previous[5] = report_at(HOUR - timedelta(hours=19))
    elif problem == "short_history":
        previous = previous[1:]
    else:
        ahead["tram"] = ahead["tram"][:2]
    with pytest.raises(ValueError, match="feed history"):
        public.feed_history(report_at(HOUR), previous, ahead, health.Config())


def test_incidents_filter_legacy_and_old_and_flag_the_ongoing_one() -> None:
    report = report_at(HOUR, failing())
    active = row(report)["state"]["active"]
    assert active == {"start_at": health.iso(HOUR), "reason": "api_failures"}
    cutoff = END - timedelta(days=14)

    def interval(mode: str, start: Any, end: Any, reason: str) -> dict[str, str]:
        return {"mode": mode, "start_at": health.iso(start), "end_at": health.iso(end), "reason": reason}

    report["recent_intervals"] = [
        interval("bus", cutoff - timedelta(hours=2), cutoff - timedelta(minutes=1), "low_fleet"),
        interval("tram", cutoff - timedelta(hours=1), cutoff, "no_accepted"),
        interval("bus", HOUR - timedelta(days=2), HOUR - timedelta(days=1), "stale_heavy"),
        interval("bus", HOUR - timedelta(hours=3), END, "low_fleet"),
        interval("bus", HOUR, END, "api_failures"),
        interval("tram", HOUR, END, "api_failures"),
    ]
    history = public.feed_history(report, previous_reports(), lookahead(), health.Config())
    assert history["incidents"] == [
        {**report["recent_intervals"][1], "ongoing": False},
        {**report["recent_intervals"][3], "ongoing": False},
        {**report["recent_intervals"][4], "ongoing": True},
        {**report["recent_intervals"][5], "ongoing": True},
    ]
    assert history["vehicle_types"]["bus"]["status"] == "degraded"
    assert history["vehicle_types"]["bus"]["fresh"][-60:] == [None] * 60


def test_closed_incident_in_evaluated_hour_is_not_ongoing() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    prior = health.evaluate(previous_hour, failing(previous_hour), [])
    report = health.evaluate(HOUR, summary(), samples(), previous_summary=failing(previous_hour), previous_report=prior)
    assert row(report)["state"]["active"] is None
    history = public.feed_history(report, [*previous_reports()[:-1], prior], lookahead(), health.Config())
    assert {entry["ongoing"] for entry in history["incidents"]} == {False}
    assert [entry["end_at"] for entry in history["incidents"]] == [health.iso(HOUR)] * 2
