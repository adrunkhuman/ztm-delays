"""Deterministic, bounded terminal traversal and duty allocation."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

TERMINAL_RADIUS_METERS = 250
TERMINAL_EPISODE_GAP_SECONDS = 180
MAX_PATH_STATES = 64
DELAY_DIVERSE_STATES = 32
DELAY_BUCKET_SECONDS = 300
PATH_TIMING_TIE_SECONDS = 30


def _distance_meters(lat: float, lon: float, stop_lat: float, stop_lon: float) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (lat, lon, stop_lat, stop_lon))
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 12_742_000 * math.asin(math.sqrt(a))


def terminal_courses(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse one schedule/semantic join into one course with usable endpoints."""
    grouped: dict[tuple[object, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (row["service_date"], row["processing_date"], row["gtfs_snapshot_id"], row["duty_chain_id"], row["trip_id"])
        ].append(row)
    courses = []
    for key in sorted(grouped):
        stops = sorted(grouped[key], key=lambda item: (item["stop_sequence"], item["stop_id"]))
        base = {
            name: stops[0][name]
            for name in stops[0]
            if name not in {"stop_sequence", "stop_id", "stop_lat", "stop_lon", "is_passenger_stop"}
        }
        settled = bool(stops[0]["are_passenger_boundaries_settled"])
        endpoints = [row for row in stops if row["is_passenger_stop"]] if settled else stops
        endpoints = [row for row in endpoints if row["stop_lat"] is not None and row["stop_lon"] is not None]
        origin, destination = (endpoints[0], endpoints[-1]) if endpoints else (None, None)
        courses.append(
            {
                **base,
                "are_passenger_boundaries_settled": settled,
                "origin_lat": origin["stop_lat"] if origin else None,
                "origin_lon": origin["stop_lon"] if origin else None,
                "destination_lat": destination["stop_lat"] if destination else None,
                "destination_lon": destination["stop_lon"] if destination else None,
            }
        )
    return courses


def extract_evidence(course: dict[str, Any], pings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract terminal episodes and complete or short traversals for one vehicle/course."""
    selected = [
        ping
        for ping in pings
        if ping["line"] == course["line"]
        and ping["brigade"] == course["brigade"]
        and ping["vehicle_type"] == (1 if course["mode"] == "bus" else 2 if course["mode"] == "tram" else -1)
    ]
    common = {
        name: course[name]
        for name in ("service_date", "processing_date", "gtfs_snapshot_id", "duty_chain_id", "trip_id")
    }
    if not selected:
        return []
    source_start, source_end = selected[0]["gps_time"], selected[-1]["gps_time"]
    observation = {
        **common,
        "vehicle_number": selected[0]["vehicle_number"],
        "vehicle_type": selected[0]["vehicle_type"],
        "candidate_kind": "observation",
        "traversal_id": None,
        "origin_event_time": None,
        "departure_event_time": None,
        "destination_event_time": None,
        "source_ping_start_time": source_start,
        "source_ping_end_time": source_end,
        "source_ping_count": len(selected),
    }
    if course["origin_lat"] is None or course["destination_lat"] is None:
        return [observation]

    states: list[tuple[str, dict[str, Any]]] = []
    for ping in selected:
        origin = (
            _distance_meters(ping["lat"], ping["lon"], course["origin_lat"], course["origin_lon"])
            <= TERMINAL_RADIUS_METERS
        )
        destination = (
            _distance_meters(ping["lat"], ping["lon"], course["destination_lat"], course["destination_lon"])
            <= TERMINAL_RADIUS_METERS
        )
        state = (
            "origin_destination"
            if origin and destination
            else "origin"
            if origin
            else "destination"
            if destination
            else "outside"
        )
        states.append((state, ping))
    episodes: list[dict[str, Any]] = []
    for state, ping in states:
        if (
            not episodes
            or episodes[-1]["state"] != state
            or (ping["gps_time"] - episodes[-1]["end"]).total_seconds() > TERMINAL_EPISODE_GAP_SECONDS
        ):
            episodes.append({"state": state, "start": ping["gps_time"], "end": ping["gps_time"]})
        else:
            episodes[-1]["end"] = ping["gps_time"]

    result = [observation]
    for origin_index, origin_episode in enumerate(episodes):
        if origin_episode["state"] not in {"origin", "origin_destination"}:
            continue
        if origin_index + 1 >= len(episodes) or episodes[origin_index + 1]["state"] != "outside":
            continue
        departure_index = origin_index + 1
        departure = episodes[departure_index]
        destination = None
        for episode in episodes[departure_index + 1 :]:
            if episode["state"] in {"destination", "origin_destination"}:
                destination = episode
                break
            if episode["state"] == "origin":
                break
        kind = "candidate" if destination else "partial"
        end = destination["start"] if destination else departure["start"]
        bounded = [ping for ping in selected if origin_episode["start"] <= ping["gps_time"] <= end]
        result.append(
            {
                **observation,
                "candidate_kind": kind,
                "traversal_id": "|".join(
                    (str(observation["vehicle_number"]), origin_episode["start"].isoformat(), end.isoformat())
                ),
                "origin_event_time": origin_episode["start"],
                "departure_event_time": departure["start"],
                "destination_event_time": destination["start"] if destination else None,
                "source_ping_start_time": bounded[0]["gps_time"],
                "source_ping_end_time": bounded[-1]["gps_time"],
                "source_ping_count": len(bounded),
            }
        )
    return result


def settle_duty(courses: list[dict[str, Any]], evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Allocate a duty by comparing coherent, vehicle-specific paths."""
    by_trip: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        by_trip[item["trip_id"]].append(item)
    ordered = sorted(courses, key=lambda row: (row["trip_order"], row["trip_id"]))
    vehicles = sorted(
        {
            item["vehicle_number"]
            for items in by_trip.values()
            for item in items
            if item["candidate_kind"] == "candidate"
        }
    )

    def path_signature(path: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        return tuple(
            (course["trip_id"], str(path["selected"][course["trip_id"]]["traversal_id"]))
            for course in ordered
            if course["trip_id"] in path["selected"]
        )

    def better_path(left: dict[str, Any], right: dict[str, Any]) -> bool:
        return (-left["executed"], left["timing_cost"], path_signature(left)) < (
            -right["executed"],
            right["timing_cost"],
            path_signature(right),
        )

    paths: list[dict[str, Any]] = []
    for vehicle in vehicles:
        states: dict[tuple[str | None, float | None], dict[str, Any]] = {
            (None, None): {
                "selected": {},
                "used_traversal_ids": frozenset(),
                "previous_destination": None,
                "delay": None,
                "executed": 0,
                "timing_cost": 0.0,
            }
        }
        for course in ordered:
            next_states = dict(states)
            candidates = sorted(
                (
                    item
                    for item in by_trip[course["trip_id"]]
                    if item["candidate_kind"] == "candidate" and item["vehicle_number"] == vehicle
                ),
                key=lambda item: (item["origin_event_time"], str(item["traversal_id"])),
            )
            for state in states.values():
                for candidate in candidates:
                    traversal_id = str(candidate["traversal_id"])
                    if traversal_id in state["used_traversal_ids"] or (
                        state["previous_destination"] is not None
                        and candidate["origin_event_time"] <= state["previous_destination"]
                    ):
                        continue
                    delay = (candidate["departure_event_time"] - course["scheduled_start_time"]).total_seconds()
                    timing_cost = state["timing_cost"] + abs(
                        delay if state["delay"] is None else delay - state["delay"]
                    )
                    path = {
                        "selected": {**state["selected"], course["trip_id"]: candidate},
                        "used_traversal_ids": state["used_traversal_ids"] | {traversal_id},
                        "previous_destination": candidate["destination_event_time"],
                        "delay": delay,
                        "executed": state["executed"] + 1,
                        "timing_cost": timing_cost,
                    }
                    key = (traversal_id, delay)
                    if key not in next_states or better_path(path, next_states[key]):
                        next_states[key] = path
            # A skipped course deliberately retains timing, destination, and traversal state.
            ranked_states = sorted(
                next_states.items(),
                key=lambda item: (
                    -item[1]["executed"],
                    item[1]["timing_cost"],
                    path_signature(item[1]),
                    str(item[0]),
                ),
            )
            by_delay_bucket: dict[int | None, tuple[tuple[str | None, float | None], dict[str, Any]]] = {}
            for item in ranked_states:
                delay = item[1]["delay"]
                bucket = None if delay is None else round(delay / DELAY_BUCKET_SECONDS)
                by_delay_bucket.setdefault(bucket, item)
            diverse = sorted(
                by_delay_bucket.items(),
                key=lambda item: (
                    math.inf if item[0] is None else abs(item[0]),
                    -item[1][1]["executed"],
                    item[1][1]["timing_cost"],
                    path_signature(item[1][1]),
                ),
            )[:DELAY_DIVERSE_STATES]
            retained = [item for _, item in diverse]
            retained_keys = {item[0] for item in retained}
            retained.extend(item for item in ranked_states if item[0] not in retained_keys)
            states = dict(retained[:MAX_PATH_STATES])
        best = min(states.values(), key=lambda path: (-path["executed"], path["timing_cost"], path_signature(path)))
        if best["executed"]:
            paths.append({**best, "vehicle_number": vehicle})

    paths.sort(key=lambda path: (-path["executed"], path["timing_cost"], path["vehicle_number"], path_signature(path)))
    winning_path = paths[0] if paths else None
    selected = winning_path["selected"] if winning_path else {}
    ambiguous_trip_ids: set[str] = set()
    if winning_path:
        for path in paths[1:]:
            if path["executed"] != winning_path["executed"]:
                break
            if path["timing_cost"] - winning_path["timing_cost"] > PATH_TIMING_TIE_SECONDS:
                break
            for course in ordered:
                winner = winning_path["selected"].get(course["trip_id"])
                contender = path["selected"].get(course["trip_id"])
                if winner != contender:
                    ambiguous_trip_ids.add(course["trip_id"])

    outcomes = []
    for index, course in enumerate(ordered):
        items = by_trip[course["trip_id"]]
        candidates = [item for item in items if item["candidate_kind"] == "candidate"]
        observations = [item for item in items if item["candidate_kind"] == "observation"]
        partials = [item for item in items if item["candidate_kind"] == "partial"]
        chosen = selected.get(course["trip_id"])
        evidence_flags: list[str] = []
        source = chosen or (
            candidates[0] if candidates else partials[0] if partials else observations[0] if observations else None
        )
        if not course["are_passenger_boundaries_settled"]:
            status, confidence, reason = "uncertain", "low", "passenger_boundaries_unknown"
            evidence_flags.append("passenger_boundaries_unknown")
        elif course["trip_id"] in ambiguous_trip_ids:
            status, confidence, reason = "vehicle_change_signal", "low", "multiple_vehicles_terminal_progression"
            evidence_flags.append("multiple_vehicles")
        elif chosen:
            status, confidence, reason = "executed", "high", "terminal_progression"
            evidence_flags.append("origin_departure_destination_progression")
        elif index + 1 < len(ordered) and any(
            partial["vehicle_number"]
            == (following := selected.get(ordered[index + 1]["trip_id"], {})).get("vehicle_number")
            and partial["departure_event_time"] < following["origin_event_time"]
            and (
                index == 0
                or (preceding := selected.get(ordered[index - 1]["trip_id"])) is None
                or partial["origin_event_time"] > preceding["destination_event_time"]
            )
            for partial in partials
        ):
            status, confidence, reason = "short_turned", "medium", "next_course_origin_before_destination"
            evidence_flags.append("next_course_origin_before_destination")
        elif (
            index > 0
            and index + 1 < len(ordered)
            and (before := selected.get(ordered[index - 1]["trip_id"])) is not None
            and (after := selected.get(ordered[index + 1]["trip_id"])) is not None
            and before["vehicle_number"] == after["vehicle_number"]
        ):
            status, confidence, reason = "skipped", "medium", "adjacent_courses_terminal_progression"
            evidence_flags.append("adjacent_course_executions")
        elif observations:
            status, confidence, reason = "uncertain", "low", "terminal_progression_incomplete"
            evidence_flags.append("line_observation_without_terminal_progression")
        else:
            status, confidence, reason = "missed", "medium", "no_line_observation"
        if course["duty_chain_source"] == "line_brigade":
            confidence = "low"
            evidence_flags.append("line_brigade_fallback")
        outcomes.append(
            {
                **{
                    name: course[name]
                    for name in (
                        "service_date",
                        "processing_date",
                        "gtfs_snapshot_id",
                        "duty_chain_id",
                        "duty_chain_source",
                        "duty_chain_source_id",
                        "trip_order",
                        "trip_id",
                        "line",
                        "brigade",
                        "mode",
                        "scheduled_start_time",
                        "scheduled_end_time",
                        "are_passenger_boundaries_settled",
                    )
                },
                "has_terminal_coordinates": course["origin_lat"] is not None and course["destination_lat"] is not None,
                "vehicle_number": chosen["vehicle_number"] if status == "executed" and chosen else None,
                "vehicle_type": chosen["vehicle_type"] if status == "executed" and chosen else None,
                "execution_status": status,
                "confidence": confidence,
                "execution_reason": reason,
                "execution_evidence": sorted(evidence_flags),
                "competing_candidate_count": len(candidates),
                "source_ping_start_time": source["source_ping_start_time"] if source else None,
                "source_ping_end_time": source["source_ping_end_time"] if source else None,
                "source_ping_count": source["source_ping_count"] if source else 0,
                "ownership_interval_start_time": chosen["departure_event_time"]
                if status == "executed" and chosen
                else None,
                "ownership_interval_end_time": chosen["destination_event_time"]
                if status == "executed" and chosen
                else None,
            }
        )
    return outcomes


def resolve_competing_ownership(outcomes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Downgrade physical intervals claimed by more than one scheduled course."""
    by_vehicle: dict[tuple[str, int], list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for index, outcome in enumerate(outcomes):
        if outcome["execution_status"] != "executed":
            continue
        by_vehicle[(outcome["vehicle_number"], outcome["vehicle_type"])].append((index, outcome))
    conflicts: set[int] = set()
    for intervals in by_vehicle.values():
        intervals.sort(
            key=lambda item: (
                item[1]["ownership_interval_start_time"],
                item[1]["ownership_interval_end_time"],
                item[1]["service_date"],
                item[1]["duty_chain_id"],
                item[1]["trip_order"],
                item[1]["trip_id"],
            )
        )
        active: list[tuple[int, dict[str, Any]]] = []
        for current in intervals:
            start = current[1]["ownership_interval_start_time"]
            active = [item for item in active if item[1]["ownership_interval_end_time"] >= start]
            if active:
                conflicts.add(current[0])
                conflicts.update(item[0] for item in active)
            active.append(current)
    for index in conflicts:
        outcome = outcomes[index]
        outcomes[index] = {
            **outcome,
            "vehicle_number": None,
            "vehicle_type": None,
            "execution_status": "uncertain",
            "confidence": "low",
            "execution_reason": "competing_duty_ownership",
            "execution_evidence": sorted({*outcome["execution_evidence"], "competing_duty_ownership"}),
            "ownership_interval_start_time": None,
            "ownership_interval_end_time": None,
        }
    return outcomes
