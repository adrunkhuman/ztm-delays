from __future__ import annotations

from typing import Any

import pytest

from ztm_matcher.facts import classify_trip, expected_status


def _metrics(**changes: object) -> dict[str, Any]:
    values: dict[str, Any] = {
        "passenger_stops_expected": 5,
        "passenger_stops_detected": 5,
        "first_required_stop_sequence": 0,
        "last_required_stop_sequence": 4,
        "first_detected_stop_sequence": 0,
        "last_detected_stop_sequence": 4,
        "max_stop_sequence_gap": 1,
        "max_ping_gap_seconds": 60,
        "max_speed_mps": 10.0,
        "has_non_monotonic_stop_progression": False,
        "start_delay_seconds": 0,
        "end_delay_seconds": 0,
    }
    values.update(changes)
    return values


@pytest.mark.parametrize(
    ("name", "changes", "quality", "service_class", "required_flags"),
    [
        ("complete", {}, "complete", "regular", set()),
        ("partial", {"passenger_stops_detected": 3}, "partial", "regular", {"low_stop_coverage"}),
        (
            "broken_low_coverage",
            {"passenger_stops_detected": 1, "last_detected_stop_sequence": 0},
            "broken",
            "matching_failure",
            {"low_stop_coverage", "likely_wrong_trip_assignment"},
        ),
        (
            "missing_first",
            {"passenger_stops_detected": 3, "first_detected_stop_sequence": 3},
            "partial",
            "truncated",
            {"missing_first_stop"},
        ),
        (
            "missing_last",
            {"passenger_stops_detected": 3, "last_detected_stop_sequence": 1, "end_delay_seconds": -121},
            "partial",
            "truncated",
            {"missing_last_stop"},
        ),
        (
            "zero_based_terminal_tolerance",
            {
                "passenger_stops_expected": 11,
                "passenger_stops_detected": 9,
                "last_required_stop_sequence": 10,
                "first_detected_stop_sequence": 2,
                "last_detected_stop_sequence": 8,
            },
            "complete",
            "regular",
            set(),
        ),
        (
            "large_internal_gap",
            {"passenger_stops_detected": 4, "max_stop_sequence_gap": 5},
            "partial",
            "modified",
            {"large_stop_sequence_gap"},
        ),
        ("large_ping_gap", {"max_ping_gap_seconds": 901}, "partial", "regular", {"large_ping_gap"}),
        (
            "impossible_speed",
            {"max_speed_mps": 50.1, "impossible_speed_event_count": 2, "impossible_speed_segment_count": 2},
            "broken",
            "matching_failure",
            {"impossible_speed_jump"},
        ),
        ("extreme_delay", {"start_delay_seconds": 3601}, "complete", "regular", {"extreme_delay"}),
        (
            "stale_progress",
            {"passenger_stops_detected": 4, "last_detected_stop_sequence": 1, "end_delay_seconds": -120},
            "partial",
            "modified",
            {"stale_stop_progression"},
        ),
        (
            "nonmonotone",
            {"has_non_monotonic_stop_progression": True},
            "broken",
            "matching_failure",
            {"non_monotonic_stop_progression", "likely_wrong_trip_assignment"},
        ),
        (
            "request_only",
            {
                "passenger_stops_expected": 0,
                "passenger_stops_detected": 0,
                "first_required_stop_sequence": None,
                "last_required_stop_sequence": None,
                "first_detected_stop_sequence": None,
                "last_detected_stop_sequence": None,
                "start_delay_seconds": None,
                "end_delay_seconds": None,
            },
            "partial",
            "truncated",
            {"missing_first_stop", "missing_last_stop"},
        ),
    ],
)
def test_trip_quality_fixtures_cover_canonical_policy(
    name: str, changes: dict[str, object], quality: str, service_class: str, required_flags: set[str]
) -> None:
    result = classify_trip(_metrics(**changes))

    assert result["trip_quality"] == quality, name
    assert result["service_observation_class"] == service_class, name
    assert required_flags <= set(result["quality_flags"]), name


@pytest.mark.parametrize(
    ("direct_confidence", "stop_service_class", "trip_service_class", "expected", "evidence"),
    [
        ("high", "regular", "matching_failure", "observed", []),
        ("medium", "regular", "matching_failure", "uncertain", ["alignment_ambiguous_or_medium"]),
        (None, "regular", "matching_failure", "uncertain", ["unreliable_trip_assignment"]),
        (None, "request", "matching_failure", "uncertain", ["unreliable_trip_assignment"]),
        (None, "request", "regular", "skipped_optional", []),
        (None, "regular", "regular", "missed", []),
    ],
)
def test_expected_event_states_are_explicit(
    direct_confidence: str | None,
    stop_service_class: str,
    trip_service_class: str,
    expected: str,
    evidence: list[str],
) -> None:
    assert expected_status(
        direct_confidence=direct_confidence,
        stop_service_class=stop_service_class,
        trip_service_observation_class=trip_service_class,
    ) == (
        expected,
        evidence,
    )


def test_overnight_quality_fixture_is_independent_of_service_date() -> None:
    result = classify_trip(_metrics(start_delay_seconds=3_599, end_delay_seconds=3_599))

    assert result["trip_quality"] == "complete"
    assert "extreme_delay" not in result["quality_flags"]


def test_speed_outlier_tolerance_keeps_flag_without_failing_assignment() -> None:
    result = classify_trip(
        _metrics(max_speed_mps=80.0, impossible_speed_event_count=1, impossible_speed_segment_count=2)
    )

    assert result["trip_quality"] == "complete"
    assert result["service_observation_class"] == "regular"
    assert result["has_impossible_speed_jump"]
    assert "impossible_speed_jump" in result["quality_flags"]
    assert "bad_assignment_evidence" not in result["service_observation_flags"]


def test_speed_outlier_tolerance_does_not_hide_repeated_events() -> None:
    result = classify_trip(
        _metrics(max_speed_mps=80.0, impossible_speed_event_count=2, impossible_speed_segment_count=2)
    )

    assert result["trip_quality"] == "broken"
    assert result["service_observation_class"] == "matching_failure"


def test_speed_outlier_tolerance_does_not_hide_long_bursts() -> None:
    result = classify_trip(
        _metrics(max_speed_mps=80.0, impossible_speed_event_count=1, impossible_speed_segment_count=4)
    )

    assert result["trip_quality"] == "broken"
    assert result["service_observation_class"] == "matching_failure"
