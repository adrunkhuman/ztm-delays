from __future__ import annotations

import copy
from datetime import datetime, timedelta
from typing import Any

import pytest

from .test_poller_health import HOUR, failing, health, row, samples, set_fleet, summary


def seed(hour: datetime = HOUR, bus: Any = 100, tram: Any = 50) -> dict[str, Any]:
    """Raw-GPS seed; ``bus``/``tram`` map minute -> fresh fleet, None leaving the minute out."""
    fleets = {"bus": bus, "tram": tram}
    return {
        "version": 1,
        "hour_start": health.iso(hour),
        "source": "raw_gps_pings",
        "vehicle_types": {
            mode: {
                "minutes": [
                    {"minute": minute, "fresh_vehicles": value}
                    for minute in range(60)
                    if (value := fleets[mode](minute) if callable(fleets[mode]) else fleets[mode]) is not None
                ]
            }
            for mode in health.MODES
        },
    }


def seeds(hour: datetime = HOUR, **fleets: Any) -> list[dict[str, Any]]:
    return [seed(hour - timedelta(days=days), **fleets) for days in (7, 14, 21)]


def legacy_v1(report: dict[str, Any], *, stale_mode: str | None = None) -> dict[str, Any]:
    """Shape a report as the stale-share rules persisted it, optionally mid stale_heavy incident."""
    old = copy.deepcopy(report)
    old["version"] = 1
    hour = health.timestamp(old["hour_start"])
    for mode, mode_row in old["vehicle_types"].items():
        mode_row.pop("fresh")
        mode_row["baseline"] = {"samples": mode_row["baseline_samples"], "vehicles": 100.0, "lines": 10.0, "ratio": 0.9}
        if mode == stale_mode:
            active = {"start_at": health.iso(hour), "reason": "stale_heavy"}
            interval = {**active, "end_at": health.iso(hour + timedelta(hours=1))}
            mode_row.update(status="degraded", reasons=["stale_heavy"], intervals=[interval])
            mode_row["state"].update(active=active, bad_tail=[], good_tail=[])
            old["recent_intervals"].append({"mode": mode, **interval})
            entry = health.event(mode, active, "degraded", health.iso(hour + timedelta(minutes=15)))
            old["events"].append({**entry, "delivered_at": health.iso(hour + timedelta(hours=1))})
    return old


def ramp(minute: int) -> int:
    return 20 + 2 * minute


def test_baseline_is_per_minute_so_ramps_judge_each_minute_against_its_own_usual() -> None:
    history = [set_fleet(source, ramp) for source in samples()]
    expected = health.baseline(history, "bus", health.Config())
    assert expected["vehicles"] == [ramp(minute) for minute in range(60)]
    assert expected["low"] == expected["high"] == expected["vehicles"]
    assert row(health.evaluate(HOUR, set_fleet(summary(), ramp), history))["status"] == "healthy"
    # A flat 40 is fine early in the ramp, but below half the usual fleet after minute 30.
    report = health.evaluate(HOUR, set_fleet(summary(), lambda _minute: 40), history)
    assert row(report)["status"] == "degraded"
    assert row(report)["intervals"] == [
        {
            "start_at": health.iso(HOUR + timedelta(minutes=31)),
            "end_at": health.iso(HOUR + timedelta(hours=1)),
            "reason": "low_fleet",
        }
    ]


def test_baseline_drops_outage_weeks_per_minute_and_rounds() -> None:
    history = samples()
    for item in history[0]["vehicle_types"]["bus"]["minutes"][:20]:
        item["accepted_vehicle_count_sum"] = 0
    history.append(summary(HOUR - timedelta(days=28)))
    for item in history[-1]["vehicle_types"]["bus"]["minutes"]:
        item["accepted_vehicle_count_sum"] = 1213
    expected = health.baseline(history, "bus", health.Config())
    assert expected["samples"] == 4
    # Minutes 0-19: [0, 100, 100, 101.08] -> the outage week is dropped.
    assert expected["vehicles"][0] == 100
    assert expected["low"][0] == 100
    assert expected["high"][0] == 101.1
    # Minutes 20-59: [100, 100, 100, 101.08] -> nothing dropped.
    assert expected["vehicles"][20] == 100
    assert expected["high"][20] == 101.1
    # When most weeks are low, the low value is the usual one and nothing is dropped.
    mostly_low = [summary(HOUR - timedelta(days=days), vehicles=10) for days in (7, 14)] + samples()[2:]
    assert health.baseline(mostly_low, "bus", health.Config())["vehicles"][0] == 10


@pytest.mark.parametrize(("fleet", "reason"), [(0, "no_accepted"), (49, "low_fleet"), (50, None), (100, None)])
def test_no_accepted_and_low_fleet_boundaries(fleet: int, reason: str | None) -> None:
    item = summary()["vehicle_types"]["bus"]["minutes"][0]
    item["accepted_vehicle_count_sum"] = fleet * item["successes"]
    assert health.bad_reason(item, 100, health.Config()) == reason
    report = health.evaluate(HOUR, set_fleet(summary(), lambda _minute: fleet), samples())
    assert row(report)["reasons"] == ([reason] if reason else [])


def test_api_failures_need_no_baseline_and_precede_fleet_rules() -> None:
    item = summary()["vehicle_types"]["bus"]["minutes"][0]
    item.update(successes=5, accepted_vehicle_count_sum=500)
    for expected in (None, 0, 100):
        assert health.bad_reason(item, expected, health.Config()) == "api_failures"
    item.update(successes=6, accepted_vehicle_count_sum=0)
    assert health.bad_reason(item, None, health.Config()) is None
    assert health.bad_reason(item, 100, health.Config()) == "no_accepted"


def test_quiet_night_below_the_floor_never_flags() -> None:
    report = health.evaluate(HOUR, summary(vehicles=0), samples(vehicles=10))
    assert row(report)["status"] == "healthy"
    assert row(report)["reasons"] == []
    assert row(report)["baseline"]["vehicles"] == [10] * 60
    assert report["events"] == []


def test_tram_night_with_zero_fleet_is_healthy_when_usual_is_below_the_floor() -> None:
    history = [set_fleet(source, lambda _minute: 15, ("tram",)) for source in samples()]
    current = set_fleet(summary(), lambda _minute: 0, ("tram",))
    report = health.evaluate(HOUR, current, history)
    assert row(report)["status"] == "healthy"
    assert row(report, "tram")["status"] == "healthy"
    assert row(report, "tram")["fresh"] == [0] * 60
    assert report["events"] == []
    lowered = health.evaluate(HOUR, current, history, config=health.Config(minimum_fleet=15))
    assert row(lowered, "tram")["reasons"] == ["no_accepted"]
    assert row(lowered)["status"] == "healthy"


def test_minimum_fleet_floor_is_inclusive_and_bounded() -> None:
    assert row(health.evaluate(HOUR, summary(vehicles=0), samples(vehicles=20)))["reasons"] == ["no_accepted"]
    assert row(health.evaluate(HOUR, summary(vehicles=0), samples(vehicles=19)))["reasons"] == []
    for value in (0, 1001):
        with pytest.raises(ValueError, match="minimum_fleet"):
            health.Config(minimum_fleet=value)


def test_partial_baseline_is_not_warming_up() -> None:
    history = samples()
    del history[0]["vehicle_types"]["bus"]["minutes"][:30]
    report = health.evaluate(HOUR, summary(), history)
    assert row(report)["baseline"]["vehicles"] == [None] * 30 + [100] * 30
    assert row(report)["status"] == "healthy"
    assert row(health.evaluate(HOUR, summary(), samples()[:2]))["status"] == "warming_up"


def evening(minute: int) -> int:
    """Usual fleet falls below the floor from minute 21."""
    return 40 if minute < 21 else 10


def test_fleet_incident_recovery_needs_minutes_with_judgeable_usual_fleet() -> None:
    history = [set_fleet(source, evening) for source in samples()]
    report = health.evaluate(HOUR, summary(vehicles=0), history)
    assert row(report)["intervals"][0]["start_at"] == health.iso(HOUR)
    assert row(report)["intervals"][0]["reason"] == "no_accepted"
    # Minutes 21-59 are not bad, but cannot show that the fleet is back.
    assert row(report)["status"] == "degraded"
    assert row(report)["state"]["active"] is not None
    assert row(report)["state"]["good_tail"] == []
    assert [entry["transition"] for entry in report["events"]] == ["degraded", "degraded"]


def test_api_incident_recovers_on_answered_polls_below_the_floor() -> None:
    history = [set_fleet(source, evening) for source in samples()]
    current = summary(vehicles=0)
    broken = failing()
    for mode in health.MODES:
        current["vehicle_types"][mode]["minutes"][:21] = broken["vehicle_types"][mode]["minutes"][:21]
    report = health.evaluate(HOUR, current, history)
    assert row(report)["state"]["active"] is None
    assert row(report)["intervals"] == [
        {
            "start_at": health.iso(HOUR),
            "end_at": health.iso(HOUR + timedelta(minutes=21)),
            "reason": "api_failures",
        }
    ]
    assert row(report)["status"] == "healthy"


def test_fleet_tail_crosses_hours_with_the_preceding_hours_baseline() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    previous = summary(previous_hour)
    for mode in health.MODES:
        for item in previous["vehicle_types"][mode]["minutes"][50:]:
            item["accepted_vehicle_count_sum"] = 0
    prior = health.evaluate(previous_hour, previous, samples(previous_hour))
    assert row(prior)["state"]["bad_tail"][0]["reason"] == "no_accepted"
    assert prior["events"] == []
    report = health.evaluate(HOUR, summary(vehicles=0), samples(), previous_summary=previous, previous_report=prior)
    assert row(report)["intervals"][0]["start_at"] == health.iso(previous_hour + timedelta(minutes=50))
    assert {entry["at"] for entry in report["events"]} == {health.iso(HOUR + timedelta(minutes=5))}


def test_seeds_stand_in_for_summaries_and_absent_minutes_mean_zero() -> None:
    history = seeds(bus=100, tram=lambda minute: 40 if minute < 30 else None)
    for source in history:
        health.validate_seed(source, health.timestamp(source["hour_start"]))
        del source["vehicle_types"]["bus"]["minutes"][50:]
    bus = health.baseline(history, "bus", health.Config())
    tram = health.baseline(history, "tram", health.Config())
    assert bus["samples"] == tram["samples"] == 3
    assert bus["vehicles"] == [100] * 50 + [None] * 10
    # A minute collected for buses had no fresh trams; one collected for neither is unknown.
    assert tram["vehicles"] == [40] * 30 + [0] * 20 + [None] * 10
    report = health.evaluate(HOUR, summary(vehicles=0), history)
    assert row(report)["reasons"] == ["no_accepted"]
    mixed = [*samples()[:2], seeds()[2]]
    assert health.baseline(mixed, "bus", health.Config())["vehicles"] == [100] * 60
    epoch = health.iso(HOUR - timedelta(days=14))
    assert health.baseline(seeds(), "bus", health.Config(), reset_at=epoch)["samples"] == 2


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data.update(extra=1),
        lambda data: data.update(source="poller"),
        lambda data: data.update(version=2),
        lambda data: data.update(version=True),
        lambda data: data.update(hour_start=health.iso(HOUR + timedelta(hours=1))),
        lambda data: data.update(hour_start="2026-10-05T12:00:00"),
        lambda data: data["vehicle_types"].pop("tram"),
        lambda data: data["vehicle_types"].update(bus=[]),
        lambda data: data["vehicle_types"]["bus"].update(minutes={}),
        lambda data: data["vehicle_types"]["bus"].update(extra=[]),
        lambda data: data["vehicle_types"]["bus"]["minutes"].append({"minute": 0, "fresh_vehicles": 1}),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(minute=60),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(minute=True),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(fresh_vehicles=-1),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(fresh_vehicles=1.5),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(fresh_vehicles=health.MAX_COUNTER + 1),
        lambda data: data["vehicle_types"]["bus"]["minutes"][0].update(extra=0),
        lambda data: data["vehicle_types"]["bus"]["minutes"].__setitem__(0, None),
    ],
)
def test_malformed_seed_rejected(mutation: Any) -> None:
    data = seed()
    health.validate_seed(data, HOUR)
    mutation(data)
    with pytest.raises(ValueError, match=r".+"):
        health.validate_seed(data, HOUR)


def test_build_seeds_groups_rows_by_utc_hour_and_mode() -> None:
    warsaw = HOUR.astimezone(health.WARSAW)
    rows = [
        ("tram", HOUR + timedelta(hours=1, minutes=5), 7),
        ("bus", warsaw + timedelta(minutes=59), 120),
        ("bus", HOUR, 100),
        ("tram", HOUR, 30),
        ("bus", HOUR + timedelta(minutes=1), 101),
    ]
    built = health.build_seeds(rows)
    later = HOUR + timedelta(hours=1)
    assert list(built) == [HOUR, later]
    assert built[HOUR] == {
        "version": 1,
        "hour_start": health.iso(HOUR),
        "source": "raw_gps_pings",
        "vehicle_types": {
            "bus": {
                "minutes": [
                    {"minute": 0, "fresh_vehicles": 100},
                    {"minute": 1, "fresh_vehicles": 101},
                    {"minute": 59, "fresh_vehicles": 120},
                ]
            },
            "tram": {"minutes": [{"minute": 0, "fresh_vehicles": 30}]},
        },
    }
    assert built[later]["vehicle_types"] == {
        "bus": {"minutes": []},
        "tram": {"minutes": [{"minute": 5, "fresh_vehicles": 7}]},
    }
    assert health.build_seeds([]) == {}
    with pytest.raises(ValueError, match="unknown seed mode"):
        health.build_seeds([("metro", HOUR, 1)])
    with pytest.raises(ValueError, match="invalid seed minute"):
        health.build_seeds([("bus", HOUR, 1), ("bus", HOUR.replace(second=30), 2)])
    with pytest.raises(ValueError, match="invalid seed minute"):
        health.build_seeds([("bus", HOUR, -1)])


@pytest.mark.parametrize(
    "mutation",
    [
        lambda mode_row: mode_row.update(fresh=[None] * 59),
        lambda mode_row: mode_row.update(fresh=[float("nan")] * 60),
        lambda mode_row: mode_row.update(fresh=[-1] * 60),
        lambda mode_row: mode_row.update(fresh=[True] * 60),
        lambda mode_row: mode_row.pop("fresh"),
        lambda mode_row: mode_row["baseline"].pop("low"),
        lambda mode_row: mode_row["baseline"].update(high=None),
        lambda mode_row: mode_row["baseline"].update(vehicles=["100"] * 60),
        lambda mode_row: mode_row["baseline"].update(lines=None),
        lambda mode_row: mode_row["baseline"].update(samples=2),
    ],
)
def test_persisted_minute_series_are_validated(mutation: Any) -> None:
    report = health.evaluate(HOUR, summary(), samples())
    health.validate_report(report, HOUR)
    mutation(row(report))
    with pytest.raises(ValueError, match="persisted poller report"):
        health.validate_report(report, HOUR)


def test_v1_report_upgrades_to_v2_with_unknown_minute_series() -> None:
    v1 = legacy_v1(health.evaluate(HOUR, summary(), samples()), stale_mode="bus")
    with pytest.raises(ValueError, match="persisted poller report"):
        health.validate_report(v1, HOUR)
    original = copy.deepcopy(v1)
    upgraded = health.upgrade_report(v1)
    assert v1 == original
    health.validate_report(upgraded, HOUR)
    assert upgraded["version"] == 2
    assert row(upgraded)["fresh"] == [None] * 60
    assert row(upgraded)["baseline"] == {**health.empty_baseline(), "samples": 3}
    assert row(upgraded)["state"]["active"]["reason"] == "stale_heavy"
    assert upgraded["events"] == v1["events"]
    current = health.evaluate(HOUR, summary(), samples())
    assert health.upgrade_report(current) is current


def test_legacy_incident_is_retired_so_a_real_outage_opens_its_own() -> None:
    previous_hour = HOUR - timedelta(hours=1)
    prior = health.upgrade_report(
        legacy_v1(health.evaluate(previous_hour, summary(previous_hour), []), stale_mode="bus")
    )
    report = health.evaluate(
        HOUR,
        summary(vehicles=0, lines=0),
        samples(),
        previous_summary=summary(previous_hour),
        previous_report=prior,
    )
    bus = row(report)
    assert bus["intervals"][0] == {
        "start_at": health.iso(previous_hour),
        "end_at": health.iso(HOUR),
        "reason": "stale_heavy",
    }
    assert bus["state"]["active"] == {"start_at": health.iso(HOUR), "reason": "no_accepted"}
    transitions = [(entry["mode"], entry["transition"], entry["reason"]) for entry in report["events"]]
    assert ("bus", "recovered", "stale_heavy") in transitions
    assert ("bus", "degraded", "no_accepted") in transitions
