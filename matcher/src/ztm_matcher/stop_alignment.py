"""Bounded global alignment of scheduled stop occurrences to GPS segments."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

REGULAR_RADIUS_METERS = 75.0
EXPANDED_RADIUS_METERS = 250.0
MAX_SEGMENT_GAP_SECONDS = 180
MAX_ALIGNMENT_STATES = 64
MAX_CANDIDATES_PER_STOP = 64
AMBIGUITY_COST = 0.25
WARSAW = ZoneInfo("Europe/Warsaw")


@dataclass(frozen=True)
class CrossingCandidate:
    """One direct segment-to-occurrence observation, before global selection."""

    stop_index: int
    segment_index: int
    actual_time: datetime
    scheduled_time: datetime
    distance_m: float
    start_distance_m: float
    end_distance_m: float
    radius_m: float
    segment_start: dict[str, Any]
    segment_end: dict[str, Any]


@dataclass(frozen=True)
class AlignmentResult:
    """Selected direct operational crossings and their passenger-only subset."""

    operational_crossings: list[dict[str, Any]]
    passenger_arrivals: list[dict[str, Any]]
    missing_stop_count: int
    ambiguous: bool


@dataclass(frozen=True)
class _AlignmentState:
    """One bounded DP history; ``candidate`` is this stop's selection."""

    selected_count: int
    cost: float
    last_segment: int | None
    parent: _AlignmentState | None
    candidate: CrossingCandidate | None
    signature_rank: int


def _segment_distance_m(start: dict[str, Any], end: dict[str, Any], stop: dict[str, Any]) -> float:
    """Return point-to-finite-segment distance using a local metric projection."""
    latitude = math.radians((float(start["lat"]) + float(end["lat"]) + float(stop["stop_lat"])) / 3)
    scale_x = 111_320 * math.cos(latitude)
    scale_y = 110_540.0
    start_x, start_y = float(start["lon"]) * scale_x, float(start["lat"]) * scale_y
    end_x, end_y = float(end["lon"]) * scale_x, float(end["lat"]) * scale_y
    stop_x, stop_y = float(stop["stop_lon"]) * scale_x, float(stop["stop_lat"]) * scale_y
    dx, dy = end_x - start_x, end_y - start_y
    denominator = dx * dx + dy * dy
    fraction = (
        0.0
        if denominator == 0
        else max(0.0, min(1.0, ((stop_x - start_x) * dx + (stop_y - start_y) * dy) / denominator))
    )
    return math.hypot(stop_x - (start_x + fraction * dx), stop_y - (start_y + fraction * dy))


def _scheduled_time(service_date: date, seconds: int | None) -> datetime | None:
    if seconds is None:
        return None
    # Add GTFS elapsed seconds after converting local midnight to an absolute instant.
    midnight = datetime.combine(service_date, time(), WARSAW).astimezone(UTC)
    return midnight + timedelta(seconds=int(seconds))


def _radius(stop: dict[str, Any]) -> float:
    first, last, sequence = (
        stop.get("first_passenger_stop_sequence"),
        stop.get("last_passenger_stop_sequence"),
        stop["stop_sequence"],
    )
    if (
        stop.get("stop_service_class") == "request"
        or sequence == first
        or sequence == last
        or stop.get("stop_execution_class") in {"technical_prefix", "technical_suffix", "unknown"}
    ):
        return EXPANDED_RADIUS_METERS
    return REGULAR_RADIUS_METERS


def _candidate_cost(candidate: CrossingCandidate) -> float:
    residual = abs((candidate.actual_time - candidate.scheduled_time).total_seconds())
    expanded_penalty = 0.25 if candidate.radius_m == EXPANDED_RADIUS_METERS else 0.0
    return 1.0 + candidate.distance_m / candidate.radius_m + residual / 900.0 + expanded_penalty


def _missing_cost(stop: dict[str, Any]) -> float:
    # Every operational occurrence is lineage; absence is allowed but never silently preferred.
    return 3.0


def _segments(pings: list[dict[str, Any]]) -> list[tuple[int, dict[str, Any], dict[str, Any]]]:
    return [
        (index, start, end)
        for index, (start, end) in enumerate(zip(pings, pings[1:], strict=False))
        if 1 <= (end["gps_time"] - start["gps_time"]).total_seconds() <= MAX_SEGMENT_GAP_SECONDS
    ]


def _candidate_matrices(
    stops: list[dict[str, Any]], segments: list[tuple[int, dict[str, Any], dict[str, Any]]]
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Calculate bounded stop-by-segment crossing metrics for one execution."""
    stop_latitudes = np.asarray([float(stop["stop_lat"]) for stop in stops])[:, np.newaxis]
    stop_longitudes = np.asarray([float(stop["stop_lon"]) for stop in stops])[:, np.newaxis]
    start_latitudes = np.asarray([float(start["lat"]) for _, start, _ in segments])[np.newaxis, :]
    start_longitudes = np.asarray([float(start["lon"]) for _, start, _ in segments])[np.newaxis, :]
    end_latitudes = np.asarray([float(end["lat"]) for _, _, end in segments])[np.newaxis, :]
    end_longitudes = np.asarray([float(end["lon"]) for _, _, end in segments])[np.newaxis, :]

    latitude = np.radians((start_latitudes + end_latitudes + stop_latitudes) / 3)
    scale_x = 111_320 * np.cos(latitude)
    scale_y = 110_540.0
    start_x, start_y = start_longitudes * scale_x, start_latitudes * scale_y
    end_x, end_y = end_longitudes * scale_x, end_latitudes * scale_y
    stop_x, stop_y = stop_longitudes * scale_x, stop_latitudes * scale_y
    dx, dy = end_x - start_x, end_y - start_y
    denominator = dx * dx + dy * dy
    fraction = np.divide(
        (stop_x - start_x) * dx + (stop_y - start_y) * dy,
        denominator,
        out=np.zeros_like(denominator),
        where=denominator != 0,
    )
    np.clip(fraction, 0.0, 1.0, out=fraction)
    distances = np.hypot(stop_x - (start_x + fraction * dx), stop_y - (start_y + fraction * dy))

    start_latitudes_radians, start_longitudes_radians = np.radians(start_latitudes), np.radians(start_longitudes)
    end_latitudes_radians, end_longitudes_radians = np.radians(end_latitudes), np.radians(end_longitudes)
    stop_latitudes_radians, stop_longitudes_radians = np.radians(stop_latitudes), np.radians(stop_longitudes)

    def haversine_distances(latitudes: np.ndarray, longitudes: np.ndarray) -> np.ndarray:
        latitude_delta = stop_latitudes_radians - latitudes
        longitude_delta = stop_longitudes_radians - longitudes
        a = (
            np.sin(latitude_delta / 2) ** 2
            + np.cos(latitudes) * np.cos(stop_latitudes_radians) * np.sin(longitude_delta / 2) ** 2
        )
        return 12_742_000 * np.arcsin(np.sqrt(a))

    start_distances = haversine_distances(start_latitudes_radians, start_longitudes_radians)
    end_distances = haversine_distances(end_latitudes_radians, end_longitudes_radians)
    endpoint_distances = start_distances + end_distances
    proportions = np.divide(
        start_distances,
        endpoint_distances,
        out=np.full_like(start_distances, 0.5),
        where=endpoint_distances != 0,
    )
    segment_seconds = np.asarray([(end["gps_time"] - start["gps_time"]).total_seconds() for _, start, end in segments])[
        np.newaxis, :
    ]
    arrival_seconds = np.floor(segment_seconds * proportions + 0.5).astype(np.int64)
    return distances, start_distances, end_distances, arrival_seconds


def crossing_candidates(
    execution: dict[str, Any], stops: list[dict[str, Any]], pings: list[dict[str, Any]]
) -> list[list[CrossingCandidate]]:
    """Generate direct candidates inside the settled ownership and source bounds."""
    lower = max(
        execution["ownership_interval_start_time"],
        execution["source_ping_start_time"] or execution["ownership_interval_start_time"],
    )
    upper = min(
        execution["ownership_interval_end_time"],
        execution["source_ping_end_time"] or execution["ownership_interval_end_time"],
    )
    bounded = [ping for ping in pings if lower <= ping["gps_time"] <= upper]
    result: list[list[CrossingCandidate]] = [[] for _ in stops]
    eligible_stops: list[tuple[int, dict[str, Any], datetime, float]] = []
    for stop_index, stop in enumerate(stops):
        if stop.get("stop_lat") is None or stop.get("stop_lon") is None:
            continue
        scheduled = _scheduled_time(execution["service_date"], stop.get("arrival_time_seconds"))
        if scheduled is None:
            continue
        radius = _radius(stop)
        eligible_stops.append((stop_index, stop, scheduled, radius))
    if not eligible_stops:
        return result
    segments = _segments(bounded)
    if not segments:
        return result

    distances, start_distances, end_distances, arrival_seconds = _candidate_matrices(
        [stop for _, stop, _, _ in eligible_stops], segments
    )
    radii = np.asarray([radius for _, _, _, radius in eligible_stops])[:, np.newaxis]
    within_radius = distances <= radii
    for matrix_stop_index, (stop_index, _, scheduled, radius) in enumerate(eligible_stops):
        for matrix_segment_index in np.flatnonzero(within_radius[matrix_stop_index]):
            segment_index, start, end = segments[int(matrix_segment_index)]
            distance = float(distances[matrix_stop_index, matrix_segment_index])
            start_distance = float(start_distances[matrix_stop_index, matrix_segment_index])
            end_distance = float(end_distances[matrix_stop_index, matrix_segment_index])
            seconds = int(arrival_seconds[matrix_stop_index, matrix_segment_index])
            result[stop_index].append(
                CrossingCandidate(
                    stop_index,
                    segment_index,
                    start["gps_time"] + timedelta(seconds=seconds),
                    scheduled,
                    distance,
                    start_distance,
                    end_distance,
                    radius,
                    start,
                    end,
                )
            )
            if len(result[stop_index]) >= MAX_CANDIDATES_PER_STOP * 2:
                result[stop_index] = sorted(
                    result[stop_index],
                    key=lambda item: (item.actual_time, item.distance_m, item.segment_index),
                )[:MAX_CANDIDATES_PER_STOP]
        # Bound dense terminal dwell evidence before the global beam consumes it.
        result[stop_index].sort(
            key=lambda item: (item.actual_time, item.distance_m, item.segment_index, item.segment_end["gps_time"])
        )
        if len(result[stop_index]) > MAX_CANDIDATES_PER_STOP:
            result[stop_index] = sorted(
                result[stop_index],
                key=lambda item: (item.actual_time, item.distance_m, item.segment_index),
            )[:MAX_CANDIDATES_PER_STOP]
    return result


def _signature_token(candidate: CrossingCandidate | None) -> tuple[int, int]:
    if candidate is None:
        return (-1, -1)
    return (candidate.segment_index, int(candidate.actual_time.timestamp()))


def _state_key(state: _AlignmentState) -> tuple[int, float, int]:
    return (-state.selected_count, state.cost, state.signature_rank)


def _transition_key(state: _AlignmentState) -> tuple[int, float, int, tuple[int, int]]:
    """Order a pending extension without materializing its complete signature."""
    parent = state.parent
    assert parent is not None
    return (-state.selected_count, state.cost, parent.signature_rank, _signature_token(state.candidate))


def _parent_signature_key(state: _AlignmentState) -> tuple[int, tuple[int, int]]:
    parent = state.parent
    assert parent is not None
    return (parent.signature_rank, _signature_token(state.candidate))


def _rank_signatures(states: list[_AlignmentState]) -> list[_AlignmentState]:
    """Give retained backpointer paths compact ranks in lexical signature order."""
    ranked: list[_AlignmentState | None] = [None] * len(states)
    previous_key: tuple[int, tuple[int, int]] | None = None
    rank = -1
    for index in sorted(range(len(states)), key=lambda item: _parent_signature_key(states[item])):
        state = states[index]
        key = _parent_signature_key(state)
        if key != previous_key:
            rank += 1
            previous_key = key
        ranked[index] = _AlignmentState(
            state.selected_count,
            state.cost,
            state.last_segment,
            state.parent,
            state.candidate,
            rank,
        )
    return [state for state in ranked if state is not None]


def _selected(state: _AlignmentState) -> tuple[CrossingCandidate | None, ...]:
    selected: list[CrossingCandidate | None] = []
    while state.parent is not None:
        selected.append(state.candidate)
        state = state.parent
    return tuple(reversed(selected))


def _align(
    stops: list[dict[str, Any]], candidates: list[list[CrossingCandidate]]
) -> tuple[tuple[CrossingCandidate | None, ...], bool]:
    states = [_AlignmentState(0, 0.0, None, None, None, 0)]
    for stop, alternatives in zip(stops, candidates, strict=True):
        next_states = [
            _AlignmentState(
                state.selected_count,
                state.cost + _missing_cost(stop),
                state.last_segment,
                state,
                None,
                0,
            )
            for state in states
        ]

        # Segments are chronological, so every candidate at segment N can only follow the
        # best retained state whose last direct segment is less than N.
        no_segment_state = next((state for state in states if state.last_segment is None), None)
        segment_states = sorted(
            (state for state in states if state.last_segment is not None), key=lambda item: item.last_segment
        )
        alternatives_by_segment: dict[int, list[CrossingCandidate]] = {}
        for candidate in alternatives:
            alternatives_by_segment.setdefault(candidate.segment_index, []).append(candidate)
        predecessor = no_segment_state
        state_index = 0
        for segment_index in sorted(alternatives_by_segment):
            while state_index < len(segment_states):
                contender = segment_states[state_index]
                last_segment = contender.last_segment
                assert last_segment is not None
                if last_segment >= segment_index:
                    break
                if predecessor is None or _state_key(contender) < _state_key(predecessor):
                    predecessor = contender
                state_index += 1
            if predecessor is None:
                continue
            predecessor_state = predecessor
            candidate = min(
                alternatives_by_segment[segment_index],
                key=lambda item: (
                    -(predecessor_state.selected_count + 1),
                    predecessor_state.cost + _candidate_cost(item),
                    predecessor_state.signature_rank,
                    _signature_token(item),
                ),
            )
            next_states.append(
                _AlignmentState(
                    predecessor_state.selected_count + 1,
                    predecessor_state.cost + _candidate_cost(candidate),
                    candidate.segment_index,
                    predecessor_state,
                    candidate,
                    0,
                )
            )
        # A direct, monotone crossing is stronger evidence than schedule adherence. Cost only
        # breaks ties between paths that explain the same number of scheduled occurrences.
        next_states.sort(key=_transition_key)
        # Equivalent histories have the same last physical segment and retain only the deterministic winner.
        retained: dict[int | None, _AlignmentState] = {}
        for state in next_states:
            retained.setdefault(state.last_segment, state)
            if len(retained) >= MAX_ALIGNMENT_STATES:
                break
        states = _rank_signatures(list(retained.values()))
    states.sort(key=_state_key)
    best = states[0]
    selected = _selected(best)
    ambiguous = any(
        state.selected_count == best.selected_count
        and state.cost - best.cost <= AMBIGUITY_COST
        and _selected(state) != selected
        for state in states[1:]
    )
    return selected, ambiguous


def align_stop_crossings(
    execution: dict[str, Any], semantics: list[dict[str, Any]], pings: list[dict[str, Any]]
) -> AlignmentResult:
    """Select one monotone, no-reuse path for a confident executed course."""
    if (
        execution.get("execution_status") != "executed"
        or execution.get("confidence") != "high"
        or not execution.get("ownership_interval_start_time")
        or not execution.get("ownership_interval_end_time")
    ):
        return AlignmentResult([], [], 0, False)
    stops = sorted(semantics, key=lambda item: int(item["stop_sequence"]))
    candidates = crossing_candidates(execution, stops, pings)
    selected, ambiguous = _align(stops, candidates)
    operational: list[dict[str, Any]] = []
    passenger: list[dict[str, Any]] = []
    for stop, candidate in zip(stops, selected, strict=True):
        if candidate is None:
            continue
        evidence = ["direct_segment_crossing", "global_monotone_path"]
        if ambiguous:
            evidence.append("alignment_ambiguous")
        confidence = "medium" if ambiguous else "high"
        scheduled_departure = _scheduled_time(execution["service_date"], stop.get("departure_time_seconds"))
        row = {
            **{
                name: execution[name]
                for name in (
                    "gtfs_snapshot_id",
                    "service_date",
                    "processing_date",
                    "duty_chain_id",
                    "trip_id",
                    "line",
                    "brigade",
                    "mode",
                    "vehicle_number",
                    "vehicle_type",
                )
            },
            **{
                name: stop.get(name)
                for name in (
                    "stop_id",
                    "stop_group_id",
                    "stop_sequence",
                    "pickup_type",
                    "drop_off_type",
                    "stop_service_class",
                    "stop_execution_class",
                    "classification_confidence",
                    "classification_reason",
                    "classification_evidence",
                    "are_passenger_boundaries_settled",
                    "is_passenger_stop",
                )
            },
            "scheduled_arrival_time": candidate.scheduled_time,
            "scheduled_departure_time": scheduled_departure,
            "actual_arrival_time": candidate.actual_time,
            "arrival_delay_seconds": int((candidate.actual_time - candidate.scheduled_time).total_seconds()),
            "segment_start_time": candidate.segment_start["gps_time"],
            "segment_end_time": candidate.segment_end["gps_time"],
            "segment_duration_seconds": int(
                (candidate.segment_end["gps_time"] - candidate.segment_start["gps_time"]).total_seconds()
            ),
            "segment_distance_m": float(candidate.distance_m),
            "segment_start_distance_m": float(candidate.start_distance_m),
            "segment_end_distance_m": float(candidate.end_distance_m),
            "stop_match_radius_m": candidate.radius_m,
            "detection_method": f"segment_within_{int(candidate.radius_m)}m",
            "alignment_confidence": confidence,
            "alignment_evidence": evidence,
        }
        operational.append(row)
        if (
            stop.get("stop_execution_class") == "passenger"
            and stop.get("is_passenger_stop")
            and stop.get("are_passenger_boundaries_settled")
        ):
            passenger.append(row)
    return AlignmentResult(operational, passenger, len(stops) - len(operational), ambiguous)
