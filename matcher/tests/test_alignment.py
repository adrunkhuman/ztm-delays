from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from ztm_matcher.alignment import extract_evidence, resolve_competing_ownership, settle_duty

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


def test_return_to_origin_ends_partial_before_later_complete_traversal() -> None:
    course = _course("return")
    evidence = _evidence(
        course,
        [
            _ping(0, 0.0),
            _ping(1, 0.01),
            _ping(2, 0.0),
            _ping(10, 0.01),
            _ping(11, 0.02),
        ],
    )
    partials = [item for item in evidence if item["candidate_kind"] == "partial"]
    candidates = [item for item in evidence if item["candidate_kind"] == "candidate"]
    assert len(partials) == 1
    assert candidates[0]["origin_event_time"] == BASE + timedelta(minutes=2)
    assert candidates[0]["destination_event_time"] == BASE + timedelta(minutes=11)


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
    unknown_outcome = settle_duty([unknown], _evidence(unknown, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]))[0]
    assert unknown_outcome["execution_status"] == "executed"
    assert unknown_outcome["confidence"] == "high"
    assert unknown_outcome["execution_reason"] == "terminal_progression"
    assert unknown_outcome["vehicle_number"] == "100"
    assert unknown_outcome["ownership_interval_start_time"] == BASE + timedelta(minutes=1)
    assert "passenger_boundaries_unknown" in unknown_outcome["execution_evidence"]

    weak_unknown = settle_duty([unknown], _evidence(unknown, [_ping(0, 0.01)]))[0]
    assert (weak_unknown["execution_status"], weak_unknown["confidence"]) == ("uncertain", "low")
    no_traversal_unknown = settle_duty([unknown], [])[0]
    assert (no_traversal_unknown["execution_status"], no_traversal_unknown["confidence"]) == ("uncertain", "low")

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


def test_exact_path_ties_emit_signal_without_ownership() -> None:
    course = _course("ambiguous")
    evidence = _evidence(course, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]) + _evidence(
        course, [_ping(0, 0.0, vehicle="200"), _ping(1, 0.01, vehicle="200"), _ping(2, 0.02, vehicle="200")]
    )
    outcome = settle_duty([course], evidence)[0]

    assert outcome["execution_status"] == "vehicle_change_signal"
    assert outcome["competing_candidate_count"] == 2
    assert outcome["ownership_interval_start_time"] is None
    assert settle_duty([course], list(reversed(evidence)))[0] == outcome


def test_coherent_vehicle_path_beats_a_competing_single_course_candidate() -> None:
    first, second, third = _course("first"), _course("second", 2), _course("third", 3)
    coherent_pings = [
        _ping(0, 0.0),
        _ping(1, 0.01),
        _ping(2, 0.02),
        _ping(20, 0.0),
        _ping(21, 0.01),
        _ping(22, 0.02),
        _ping(40, 0.0),
        _ping(41, 0.01),
        _ping(42, 0.02),
    ]
    competing_pings = [_ping(20, 0.0, vehicle="200"), _ping(21, 0.01, vehicle="200"), _ping(22, 0.02, vehicle="200")]
    evidence = [
        item
        for course in (first, second, third)
        for item in _evidence(course, coherent_pings) + _evidence(course, competing_pings)
    ]

    outcomes = settle_duty([first, second, third], evidence)

    assert [row["execution_status"] for row in outcomes] == ["executed", "executed", "executed"]
    assert [row["vehicle_number"] for row in outcomes] == ["100", "100", "100"]
    assert outcomes[1]["competing_candidate_count"] == 4


def test_long_consistent_delay_and_skipped_middle_retain_later_traversal() -> None:
    first, missing, last = _course("first"), _course("missing", 2), _course("last", 3)
    delayed_pings = [
        _ping(100, 0.0),
        _ping(101, 0.01),
        _ping(102, 0.02),
        _ping(140, 0.0),
        _ping(141, 0.01),
        _ping(142, 0.02),
    ]
    outcomes = settle_duty([first, missing, last], _evidence(first, delayed_pings) + _evidence(last, delayed_pings))

    assert [row["execution_status"] for row in outcomes] == ["executed", "skipped", "executed"]
    assert outcomes[0]["ownership_interval_start_time"] == BASE + timedelta(minutes=101)
    assert outcomes[2]["ownership_interval_start_time"] == BASE + timedelta(minutes=141)


def test_competing_duties_cannot_own_the_same_physical_interval() -> None:
    course = _course("one")
    pings = [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)]
    first = settle_duty([course], _evidence(course, pings))[0]
    second = {**first, "trip_id": "two", "duty_chain_id": "other-duty"}
    resolved = resolve_competing_ownership([first, second])
    assert [row["execution_status"] for row in resolved] == ["uncertain", "uncertain"]
    assert all(row["ownership_interval_start_time"] is None for row in resolved)
    assert all("competing_duty_ownership" in row["execution_evidence"] for row in resolved)


def test_delay_state_uses_departure_not_terminal_arrival() -> None:
    course = _course("dwell")
    template = next(
        item
        for item in _evidence(course, [_ping(0, 0.0), _ping(1, 0.01), _ping(2, 0.02)])
        if item["candidate_kind"] == "candidate"
    )
    long_dwell = {
        **template,
        "traversal_id": "long-dwell",
        "origin_event_time": BASE - timedelta(minutes=30),
        "departure_event_time": BASE + timedelta(minutes=10),
        "destination_event_time": BASE + timedelta(minutes=20),
    }
    later_departure = {
        **template,
        "traversal_id": "later-departure",
        "origin_event_time": BASE,
        "departure_event_time": BASE + timedelta(minutes=20),
        "destination_event_time": BASE + timedelta(minutes=30),
    }
    outcome = settle_duty([course], [long_dwell, later_departure])[0]
    assert outcome["ownership_interval_start_time"] == BASE + timedelta(minutes=10)
