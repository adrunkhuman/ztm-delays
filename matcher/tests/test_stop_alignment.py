from __future__ import annotations

import math
import random
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from ztm_matcher import stop_alignment
from ztm_matcher.alignment import _distance_meters
from ztm_matcher.stop_alignment import (
    AMBIGUITY_COST,
    EXPANDED_RADIUS_METERS,
    MAX_ALIGNMENT_STATES,
    REGULAR_RADIUS_METERS,
    CrossingCandidate,
    _align,
    _candidate_cost,
    _missing_cost,
    _scheduled_time,
    _segment_distance_m,
    align_stop_crossings,
    crossing_candidates,
)

BASE = datetime(2026, 1, 15, tzinfo=UTC)


def _execution(**changes: object) -> dict[str, Any]:
    row: dict[str, Any] = {
        "gtfs_snapshot_id": "snapshot-a",
        "service_date": date(2026, 1, 15),
        "processing_date": date(2026, 1, 15),
        "duty_chain_id": "duty-a",
        "trip_id": "trip-a",
        "line": "145",
        "brigade": "1",
        "mode": "bus",
        "vehicle_number": "100",
        "vehicle_type": 1,
        "execution_status": "executed",
        "confidence": "high",
        "ownership_interval_start_time": BASE,
        "ownership_interval_end_time": BASE + timedelta(minutes=20),
        "source_ping_start_time": BASE,
        "source_ping_end_time": BASE + timedelta(minutes=20),
    }
    row.update(changes)
    return row


def _stop(
    sequence: int, lon: float, *, kind: str = "passenger", settled: bool = True, request: bool = False
) -> dict[str, Any]:
    return {
        "stop_id": f"same-post-{sequence % 2}",
        "stop_group_id": "same",
        "stop_sequence": sequence,
        "stop_lat": 52.0,
        "stop_lon": 21.0 + lon,
        "arrival_time_seconds": 3600 + sequence * 60,
        "departure_time_seconds": 3600 + sequence * 60 + 20,
        "pickup_type": 0,
        "drop_off_type": 0,
        "stop_service_class": "request" if request else "regular",
        "stop_execution_class": kind,
        "classification_confidence": "high" if kind != "unknown" else "low",
        "classification_reason": kind,
        "classification_evidence": [kind],
        "are_passenger_boundaries_settled": settled,
        "is_passenger_stop": kind == "passenger" and settled,
        "first_passenger_stop_sequence": 1 if settled else None,
        "last_passenger_stop_sequence": 3 if settled else None,
    }


def _ping(seconds: int, lon: float, *, line: str = "145") -> dict[str, Any]:
    return {
        "lat": 52.0,
        "lon": 21.0 + lon,
        "gps_time": BASE + timedelta(seconds=seconds),
        "vehicle_number": "100",
        "vehicle_type": 1,
        "line": line,
        "brigade": "1",
    }


def _with_schedule(stops: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for index, stop in enumerate(stops):
        stop["arrival_time_seconds"] = 25 * 3600 + index * 60
        stop["departure_time_seconds"] = stop["arrival_time_seconds"]
    return stops


def _candidate(stop_index: int, segment_index: int, distance_m: float = 0.0) -> CrossingCandidate:
    actual_time = BASE + timedelta(seconds=segment_index * 60)
    segment_start = {"gps_time": actual_time, "lat": 52.0, "lon": 21.0}
    segment_end = {"gps_time": actual_time + timedelta(seconds=10), "lat": 52.0, "lon": 21.0}
    return CrossingCandidate(
        stop_index=stop_index,
        segment_index=segment_index,
        actual_time=actual_time,
        scheduled_time=BASE,
        distance_m=distance_m,
        start_distance_m=distance_m,
        end_distance_m=distance_m,
        radius_m=REGULAR_RADIUS_METERS,
        segment_start=segment_start,
        segment_end=segment_end,
    )


def _signature(selected: tuple[CrossingCandidate | None, ...]) -> tuple[tuple[int, int], ...]:
    return tuple(
        (-1, -1) if candidate is None else (candidate.segment_index, int(candidate.actual_time.timestamp()))
        for candidate in selected
    )


def _exhaustive_align(
    stops: list[dict[str, Any]], candidates: list[list[CrossingCandidate]]
) -> tuple[tuple[CrossingCandidate | None, ...], bool]:
    """The pre-optimization transition expansion, retained as a test oracle."""
    states: list[tuple[int, float, tuple[CrossingCandidate | None, ...]]] = [(0, 0.0, ())]
    for stop, alternatives in zip(stops, candidates, strict=True):
        next_states = [
            (selected_count, cost + _missing_cost(stop), selected + (None,))
            for selected_count, cost, selected in states
        ]
        for selected_count, cost, selected in states:
            previous = next((candidate for candidate in reversed(selected) if candidate is not None), None)
            for candidate in alternatives:
                if previous and (
                    candidate.segment_index <= previous.segment_index or candidate.actual_time < previous.actual_time
                ):
                    continue
                next_states.append((selected_count + 1, cost + _candidate_cost(candidate), selected + (candidate,)))
        next_states.sort(key=lambda state: (-state[0], state[1], _signature(state[2])))
        retained: dict[int | None, tuple[int, float, tuple[CrossingCandidate | None, ...]]] = {}
        for state in next_states:
            last = next((candidate.segment_index for candidate in reversed(state[2]) if candidate is not None), None)
            retained.setdefault(last, state)
            if len(retained) >= MAX_ALIGNMENT_STATES:
                break
        states = list(retained.values())
    states.sort(key=lambda state: (-state[0], state[1], _signature(state[2])))
    best_count, best_cost, best = states[0]
    ambiguous = any(
        selected_count == best_count and cost - best_cost <= AMBIGUITY_COST and selected != best
        for selected_count, cost, selected in states[1:]
    )
    return best, ambiguous


def _unpruned_align(
    stops: list[dict[str, Any]], candidates: list[list[CrossingCandidate]]
) -> tuple[tuple[CrossingCandidate | None, ...], bool]:
    """Small regression oracle without beam pruning."""
    states: list[tuple[int, float, tuple[CrossingCandidate | None, ...]]] = [(0, 0.0, ())]
    for stop, alternatives in zip(stops, candidates, strict=True):
        next_states = [
            (selected_count, cost + _missing_cost(stop), selected + (None,))
            for selected_count, cost, selected in states
        ]
        for selected_count, cost, selected in states:
            previous = next((candidate for candidate in reversed(selected) if candidate is not None), None)
            for candidate in alternatives:
                if previous and (
                    candidate.segment_index <= previous.segment_index or candidate.actual_time < previous.actual_time
                ):
                    continue
                next_states.append((selected_count + 1, cost + _candidate_cost(candidate), selected + (candidate,)))
        states = next_states
    states.sort(key=lambda state: (-state[0], state[1], _signature(state[2])))
    best_count, best_cost, best = states[0]
    ambiguous = any(
        selected_count == best_count and cost - best_cost <= AMBIGUITY_COST and selected != best
        for selected_count, cost, selected in states[1:]
    )
    return best, ambiguous


def _objective(
    stops: list[dict[str, Any]], selected: tuple[CrossingCandidate | None, ...]
) -> tuple[int, float, tuple[tuple[int, int], ...]]:
    return (
        sum(candidate is not None for candidate in selected),
        sum(
            _missing_cost(stop) if candidate is None else _candidate_cost(candidate)
            for stop, candidate in zip(stops, selected, strict=True)
        ),
        _signature(selected),
    )


def test_ports_segment_interpolation_uneven_interpolation_and_snapshot_identity() -> None:
    execution = _execution(service_date=date(2026, 1, 14))
    stop = _stop(1, 0.0025)
    stop["arrival_time_seconds"] = stop["departure_time_seconds"] = 25 * 3600
    result = align_stop_crossings(execution, [stop], [_ping(0, 0.0), _ping(80, 0.01)])

    row = result.operational_crossings[0]
    assert row["actual_arrival_time"] == BASE + timedelta(seconds=20)
    assert row["scheduled_arrival_time"] == datetime(2026, 1, 15, tzinfo=UTC)
    assert row["gtfs_snapshot_id"] == "snapshot-a"
    assert row["arrival_delay_seconds"] == 20
    assert row["detection_method"] == "segment_within_250m"
    assert (row["stop_group_id"], row["pickup_type"], row["drop_off_type"]) == ("same", 0, 0)


@pytest.mark.parametrize(
    ("service_date", "seconds", "expected"),
    [
        (date(2026, 3, 29), 2 * 3600 + 30 * 60, datetime(2026, 3, 29, 1, 30, tzinfo=UTC)),
        (date(2026, 10, 25), 2 * 3600 + 30 * 60, datetime(2026, 10, 25, 0, 30, tzinfo=UTC)),
        (date(2026, 1, 15), 25 * 3600, datetime(2026, 1, 16, tzinfo=UTC)),
    ],
)
def test_scheduled_time_uses_warsaw_wall_clock(service_date: date, seconds: int, expected: datetime) -> None:
    assert _scheduled_time(service_date, seconds) == expected


def test_global_path_uses_stop_sequence_not_stop_id_and_supports_loop() -> None:
    stops = _with_schedule([_stop(1, 0.0), _stop(2, 0.01), _stop(3, 0.0)])
    execution = _execution(
        service_date=date(2026, 1, 14),
        ownership_interval_end_time=BASE + timedelta(seconds=180),
        source_ping_end_time=BASE + timedelta(seconds=180),
    )
    result = align_stop_crossings(
        execution, stops, [_ping(0, 0.0), _ping(60, 0.005), _ping(120, 0.01), _ping(180, 0.0)]
    )

    assert [row["stop_sequence"] for row in result.operational_crossings] == [1, 2, 3]
    assert len({row["stop_id"] for row in result.operational_crossings}) < len(result.operational_crossings)
    assert [row["actual_arrival_time"] for row in result.operational_crossings] == sorted(
        row["actual_arrival_time"] for row in result.operational_crossings
    )


def test_service_class_and_terminal_radius_follow_legacy_rules() -> None:
    execution = _execution(service_date=date(2026, 1, 14))
    regular, request, technical = (
        _stop(2, 0.005),
        _stop(2, 0.005, request=True),
        _stop(2, 0.005, kind="technical_prefix"),
    )
    for stop in (regular, request, technical):
        stop["first_passenger_stop_sequence"], stop["last_passenger_stop_sequence"] = 1, 3
    candidates = crossing_candidates(execution, [regular, request, technical], [_ping(0, 0.004), _ping(60, 0.006)])

    assert candidates[0][0].radius_m == REGULAR_RADIUS_METERS
    assert candidates[1][0].radius_m == EXPANDED_RADIUS_METERS
    assert candidates[2][0].radius_m == EXPANDED_RADIUS_METERS


def test_vectorized_candidates_preserve_scalar_metrics_and_segment_order() -> None:
    execution = _execution(service_date=date(2026, 1, 14), ownership_interval_end_time=BASE + timedelta(seconds=240))
    stops = [_stop(1, 0.0), _stop(2, 0.004), _stop(3, 0.008)]
    pings = [
        _ping(seconds, longitude)
        for seconds, longitude in ((0, 0.0), (60, 0.002), (120, 0.004), (180, 0.006), (240, 0.008))
    ]

    candidates = crossing_candidates(execution, stops, pings)

    assert [[candidate.segment_index for candidate in alternatives] for alternatives in candidates] == [
        [0, 1],
        [1, 2],
        [2, 3],
    ]
    for stop, alternatives in zip(stops, candidates, strict=True):
        for candidate in alternatives:
            scalar_distance = _segment_distance_m(candidate.segment_start, candidate.segment_end, stop)
            scalar_start_distance = _distance_meters(
                candidate.segment_start["lat"], candidate.segment_start["lon"], stop["stop_lat"], stop["stop_lon"]
            )
            scalar_end_distance = _distance_meters(
                candidate.segment_end["lat"], candidate.segment_end["lon"], stop["stop_lat"], stop["stop_lon"]
            )
            proportion = (
                scalar_start_distance / (scalar_start_distance + scalar_end_distance)
                if scalar_start_distance + scalar_end_distance
                else 0.5
            )
            expected_seconds = math.floor(
                (candidate.segment_end["gps_time"] - candidate.segment_start["gps_time"]).total_seconds() * proportion
                + 0.5
            )

            assert candidate.distance_m == pytest.approx(scalar_distance, rel=1e-12, abs=1e-9)
            assert candidate.start_distance_m == pytest.approx(scalar_start_distance, rel=1e-12, abs=1e-9)
            assert candidate.end_distance_m == pytest.approx(scalar_end_distance, rel=1e-12, abs=1e-9)
            assert candidate.actual_time == candidate.segment_start["gps_time"] + timedelta(seconds=expected_seconds)


def test_candidate_generation_chunks_segments_without_changing_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    segment_count = 17
    execution = _execution(
        service_date=date(2026, 1, 14),
        ownership_interval_end_time=BASE + timedelta(seconds=segment_count * 60),
        source_ping_end_time=BASE + timedelta(seconds=segment_count * 60),
    )
    stops = [_stop(1, 0.0), _stop(2, 0.004)]
    pings = [_ping(index * 60, index * 0.0005) for index in range(segment_count + 1)]

    monkeypatch.setattr(stop_alignment, "SEGMENT_CANDIDATE_CHUNK_SIZE", segment_count)
    unchunked = crossing_candidates(execution, stops, pings)

    matrix_widths: list[int] = []
    matrix_shapes: list[tuple[int, ...]] = []
    original_matrices = stop_alignment._candidate_matrices

    def bounded_matrices(
        matrix_stops: list[dict[str, Any]], matrix_segments: list[tuple[int, dict[str, Any], dict[str, Any]]]
    ) -> tuple[object, object, object, object]:
        matrix_widths.append(len(matrix_segments))
        matrices = original_matrices(matrix_stops, matrix_segments)
        matrix_shapes.append(matrices[0].shape)
        return matrices

    monkeypatch.setattr(stop_alignment, "SEGMENT_CANDIDATE_CHUNK_SIZE", 4)
    monkeypatch.setattr(stop_alignment, "_candidate_matrices", bounded_matrices)
    chunked = crossing_candidates(execution, stops, pings)

    assert chunked == unchunked
    assert matrix_widths == [4, 4, 4, 4, 1]
    assert matrix_shapes == [(2, 4), (2, 4), (2, 4), (2, 4), (2, 1)]
    assert max(matrix_widths) <= stop_alignment.SEGMENT_CANDIDATE_CHUNK_SIZE


def test_technical_crossings_are_lineage_only_and_unknown_boundaries_emit_no_passenger_arrival() -> None:
    execution = _execution(service_date=date(2026, 1, 14))
    technical, passenger, unknown = _with_schedule(
        [_stop(1, 0.0, kind="technical_prefix"), _stop(2, 0.01), _stop(3, 0.02, kind="unknown", settled=False)]
    )
    result = align_stop_crossings(
        execution,
        [technical, passenger, unknown],
        [_ping(0, 0.0), _ping(60, 0.005), _ping(120, 0.01), _ping(180, 0.02)],
    )

    assert {row["stop_execution_class"] for row in result.operational_crossings} >= {"technical_prefix", "passenger"}
    assert [row["stop_execution_class"] for row in result.passenger_arrivals] == ["passenger"]
    assert all(row["are_passenger_boundaries_settled"] for row in result.passenger_arrivals)


def test_missing_middle_never_uses_interpolation_across_an_invalid_gap() -> None:
    execution = _execution(
        service_date=date(2026, 1, 14),
        ownership_interval_end_time=BASE + timedelta(seconds=720),
        source_ping_end_time=BASE + timedelta(seconds=720),
    )
    stops = _with_schedule([_stop(1, 0.0), _stop(2, 0.01), _stop(3, 0.02)])
    result = align_stop_crossings(
        execution, stops, [_ping(0, 0.0), _ping(60, 0.005), _ping(660, 0.015), _ping(720, 0.02)]
    )

    assert [row["stop_sequence"] for row in result.operational_crossings] == [1, 3]
    assert result.missing_stop_count == 1


def test_multi_hour_delayed_coherent_path_beats_missing_stops() -> None:
    execution = _execution(
        service_date=date(2026, 1, 14),
        ownership_interval_end_time=BASE + timedelta(hours=3, minutes=5),
        source_ping_end_time=BASE + timedelta(hours=3, minutes=5),
    )
    stops = _with_schedule([_stop(1, 0.0), _stop(2, 0.01), _stop(3, 0.02)])
    result = align_stop_crossings(
        execution,
        stops,
        [
            _ping(3 * 3600, 0.0),
            _ping(3 * 3600 + 60, 0.005),
            _ping(3 * 3600 + 120, 0.01),
            _ping(3 * 3600 + 180, 0.015),
            _ping(3 * 3600 + 240, 0.02),
        ],
    )

    assert [row["stop_sequence"] for row in result.operational_crossings] == [1, 2, 3]
    assert result.missing_stop_count == 0
    assert all(row["arrival_delay_seconds"] >= 3 * 3600 for row in result.operational_crossings)


def test_early_origin_destination_chronology_and_ambiguous_candidates_are_deterministic() -> None:
    execution = _execution(service_date=date(2026, 1, 14))
    first, last = _with_schedule([_stop(1, 0.0), _stop(2, 0.01)])
    chronology = align_stop_crossings(execution, [first, last], [_ping(0, 0.01), _ping(60, 0.0)])
    assert len(chronology.operational_crossings) == 1

    lone = _stop(1, 0.0)
    lone["arrival_time_seconds"] = 25 * 3600
    ambiguous = align_stop_crossings(execution, [lone], [_ping(0, 0.0), _ping(60, 0.01), _ping(120, 0.0)])
    assert ambiguous.ambiguous
    assert "alignment_ambiguous" in ambiguous.operational_crossings[0]["alignment_evidence"]
    assert align_stop_crossings(execution, [lone], [_ping(0, 0.0), _ping(60, 0.01), _ping(120, 0.0)]) == ambiguous


def test_prefix_best_alignment_matches_exhaustive_beam_on_deterministic_matrices() -> None:
    randomizer = random.Random(131)
    for matrix_number in range(150):
        stop_count = randomizer.randint(1, 7)
        stops = [{} for _ in range(stop_count)]
        candidates = [
            [
                _candidate(stop_index, randomizer.randrange(7), randomizer.choice((0.0, 7.5, 15.0, 30.0)))
                for _ in range(randomizer.randrange(5))
            ]
            for stop_index in range(stop_count)
        ]
        # Same segment and timestamp candidates must compete, not be chained. They also
        # exercise stable tie selection when their objective and signature are identical.
        if matrix_number % 3 == 0:
            candidates[0].extend([_candidate(0, 2, 7.5), _candidate(0, 2, 7.5)])

        expected, expected_ambiguous = _exhaustive_align(stops, candidates)
        actual, actual_ambiguous = _align(stops, candidates)

        assert actual == expected, f"matrix {matrix_number}"
        assert _objective(stops, actual) == _objective(stops, expected), f"matrix {matrix_number}"
        assert actual_ambiguous == expected_ambiguous, f"matrix {matrix_number}"


def test_beam_preserves_missing_and_early_frontiers_against_unpruned_oracle() -> None:
    stops = [{}, {}, {}]
    candidates = [
        [_candidate(0, 1)],
        [_candidate(1, segment_index, distance_m=1_000_000.0) for segment_index in range(2, 66)],
        [_candidate(2, 2)],
    ]

    expected, expected_ambiguous = _unpruned_align(stops, candidates)
    actual, actual_ambiguous = _align(stops, candidates)

    assert len(candidates[1]) == MAX_ALIGNMENT_STATES
    assert _signature(expected) == (
        (1, int((BASE + timedelta(minutes=1)).timestamp())),
        (-1, -1),
        (2, int((BASE + timedelta(minutes=2)).timestamp())),
    )
    assert actual == expected
    assert actual_ambiguous == expected_ambiguous


def test_ownership_bounds_exclude_handoff_and_partial_gps_stays_bounded() -> None:
    execution = _execution(
        service_date=date(2026, 1, 14),
        ownership_interval_start_time=BASE + timedelta(seconds=60),
        source_ping_start_time=BASE + timedelta(seconds=60),
        ownership_interval_end_time=BASE + timedelta(seconds=120),
        source_ping_end_time=BASE + timedelta(seconds=120),
    )
    stop = _stop(1, 0.0)
    stop["arrival_time_seconds"] = 25 * 3600
    result = align_stop_crossings(
        execution, [stop], [_ping(0, 0.0, line="118"), _ping(60, 0.01, line="118"), _ping(120, 0.0, line="145")]
    )

    assert result.operational_crossings[0]["segment_start_time"] >= execution["ownership_interval_start_time"]
    assert result.operational_crossings[0]["line"] == "145"
