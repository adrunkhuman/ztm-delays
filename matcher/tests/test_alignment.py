from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from ztm_matcher.alignment import extract_evidence, settle_duty

BASE = datetime(2026, 1, 15, 9, tzinfo=UTC)


def _course(
    trip_id: str,
    order: int = 1,
    *,
    line: str = "10",
    settled: bool = True,
    service_date: date = date(2026, 1, 15),
) -> dict[str, Any]:
    return {
        "service_date": service_date,
        "processing_date": date(2026, 1, 15),
        "gtfs_snapshot_id": "synthetic",
        "duty_chain_id": "duty",
        "duty_chain_source": "block_id",
        "duty_chain_source_id": "block",
        "trip_id": trip_id,
        "trip_order": order,
        "line": line,
        "brigade": "1",
        "mode": "bus",
        "scheduled_start_time": BASE + timedelta(minutes=(order - 1) * 20),
        "scheduled_end_time": BASE + timedelta(minutes=(order - 1) * 20 + 15),
        "are_passenger_boundaries_settled": settled,
        "origin_lat": 52.0,
        "origin_lon": 21.0,
        "destination_lat": 52.0,
        "destination_lon": 21.02,
    }


def _ping(minutes: int, lon: float, *, vehicle: str = "100", line: str = "10") -> dict[str, Any]:
    return {
        "line": line,
        "brigade": "1",
        "lat": 52.0,
        "lon": 21.0 + lon,
        "gps_time": BASE + timedelta(minutes=minutes),
        "vehicle_number": vehicle,
        "vehicle_type": 1,
    }


def _evidence(course: dict[str, Any], pings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return extract_evidence(course, pings)


def test_terminal_dwell_loop_and_repeated_delayed_traversals_are_distinct() -> None:
    course = _course("repeat")
    pings = [
        _ping(30, 0.0),
        _ping(31, 0.0),
        _ping(32, 0.01),
        _ping(33, 0.02),
        _ping(34, 0.02),
        _ping(40, 0.0),
        _ping(41, 0.01),
        _ping(42, 0.02),
    ]
    candidates = [item for item in _evidence(course, pings) if item["candidate_kind"] == "candidate"]

    assert [(item["origin_event_time"], item["destination_event_time"]) for item in candidates] == [
        (BASE + timedelta(minutes=30), BASE + timedelta(minutes=33)),
        (BASE + timedelta(minutes=40), BASE + timedelta(minutes=42)),
    ]
    first, second = _course("first"), _course("second", 2)
    both = _evidence(first, pings) + _evidence(second, pings)
    outcomes = settle_duty([first, second], both)
    assert [(row["execution_status"], row["ownership_interval_start_time"]) for row in outcomes] == [
        ("executed", BASE + timedelta(minutes=32)),
        ("executed", BASE + timedelta(minutes=41)),
    ]
    assert settle_duty([first, second], list(reversed(both))) == outcomes
    gap_candidates = [
        item
        for item in _evidence(course, [_ping(0, 0.0), _ping(4, 0.0), _ping(5, 0.01), _ping(6, 0.02)])
        if item["candidate_kind"] == "candidate"
    ]
    assert gap_candidates[0]["origin_event_time"] == BASE + timedelta(minutes=4)


def test_loop_terminal_requires_departure_and_return() -> None:
    course = {**_course("loop"), "destination_lon": 21.0}
    evidence = _evidence(course, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.0)])

    assert len([item for item in evidence if item["candidate_kind"] == "candidate"]) == 1
    assert settle_duty([course], evidence)[0]["execution_status"] == "executed"


def test_missing_first_middle_and_partial_gps_have_conservative_outcomes() -> None:
    first, middle, last = _course("first"), _course("middle", 2), _course("last", 3)
    pings = [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02), _ping(40, 0.0), _ping(41, 0.01), _ping(42, 0.02)]
    outcomes = settle_duty([first, middle, last], _evidence(first, pings) + _evidence(last, pings))
    assert [row["execution_status"] for row in outcomes] == ["executed", "skipped", "executed"]

    missing_first, observed_second = _course("missing-first"), _course("observed-second", 2)
    missing_first_outcomes = settle_duty(
        [missing_first, observed_second],
        _evidence(observed_second, [_ping(20, 0.0), _ping(21, 0.01), _ping(22, 0.02)]),
    )
    assert [row["execution_status"] for row in missing_first_outcomes] == ["missed", "executed"]

    partial = settle_duty([_course("partial")], _evidence(_course("partial"), [_ping(0, 0.01)]))
    assert partial[0]["execution_status"] == "uncertain"
    assert partial[0]["source_ping_count"] == 1


def test_line_change_same_line_handoff_short_turn_unknown_and_overnight() -> None:
    changed_a, changed_b = _course("a"), _course("b", 2, line="20")
    pings = [
        _ping(0, 0.0),
        _ping(1, 0.01),
        _ping(2, 0.02),
        _ping(20, 0.0, line="20"),
        _ping(21, 0.01, line="20"),
        _ping(22, 0.02, line="20"),
    ]
    assert [
        row["execution_status"]
        for row in settle_duty([changed_a, changed_b], _evidence(changed_a, pings) + _evidence(changed_b, pings))
    ] == ["executed", "executed"]

    short, following = _course("short"), _course("following", 2, line="20")
    short_pings = [
        _ping(0, 0.0),
        _ping(1, 0.01),
        _ping(20, 0.0, line="20"),
        _ping(21, 0.01, line="20"),
        _ping(22, 0.02, line="20"),
    ]
    assert [
        row["execution_status"]
        for row in settle_duty([short, following], _evidence(short, short_pings) + _evidence(following, short_pings))
    ] == ["short_turned", "executed"]

    unknown = _course("unknown", settled=False)
    assert (
        settle_duty([unknown], _evidence(unknown, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]))[0]["confidence"]
        == "low"
    )

    fallback = {**_course("fallback"), "duty_chain_source": "line_brigade"}
    fallback_outcome = settle_duty([fallback], _evidence(fallback, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]))[0]
    assert fallback_outcome["confidence"] == "low"
    assert "line_brigade_fallback" in fallback_outcome["execution_evidence"]

    overnight = _course("overnight", service_date=date(2026, 1, 14))
    assert (
        settle_duty([overnight], _evidence(overnight, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]))[0][
            "execution_status"
        ]
        == "executed"
    )


def test_ambiguous_vehicles_emit_signal_without_ownership() -> None:
    course = _course("ambiguous")
    evidence = _evidence(course, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]) + _evidence(
        course, [_ping(0, 0.0, vehicle="200"), _ping(1, 0.01, vehicle="200"), _ping(2, 0.02, vehicle="200")]
    )
    outcome = settle_duty([course], evidence)[0]

    assert outcome["execution_status"] == "vehicle_change_signal"
    assert outcome["competing_candidate_count"] == 2
    assert outcome["ownership_interval_start_time"] is None
