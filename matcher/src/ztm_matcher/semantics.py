"""Ports of dbt duty-chain and stop-boundary semantics."""

import math
import re
from collections.abc import Iterator
from typing import Any

from ztm_matcher.gtfs import Snapshot, StopTime, chain_id


def _depot(name: str | None) -> bool:
    return bool(name and re.search(r"(^R-[0-9]+\s+Zajezdnia|^Zajezdnia|\sZajezdnia)", name, re.IGNORECASE))


def _distance(left: dict[str, Any] | None, right: dict[str, Any] | None) -> float | None:
    if not left or not right:
        return None
    lat1, lon1, lat2, lon2 = map(
        math.radians, (left["stop_lat"], left["stop_lon"], right["stop_lat"], right["stop_lon"])
    )
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 12_742_000 * math.asin(math.sqrt(a))


def duties(schedule: list[dict[str, Any]], snapshot: Snapshot) -> list[dict[str, Any]]:
    """Prefer block IDs, otherwise use namespaced line:brigade duty groups."""
    groups: dict[tuple[object, ...], list[dict[str, Any]]] = {}
    for trip in schedule:
        source = "block_id" if trip["block_id"] else "line_brigade"
        source_id = trip["block_id"] or f"{trip['line']}:{trip['brigade']}"
        groups.setdefault((trip["service_date"], source, source_id), []).append(
            {**trip, "duty_chain_source": source, "duty_chain_source_id": source_id}
        )
    result = []
    for (_, source_value, source_id), group in sorted(groups.items()):
        source = str(source_value)
        group.sort(key=lambda row: (row["trip_start_seconds"], row["trip_end_seconds"], row["trip_id"]))
        for index, trip in enumerate(group):
            endpoints = sorted(snapshot.stop_times.get(trip["trip_id"], []), key=lambda row: row.stop_sequence)
            origin, destination = (endpoints[0].stop_id, endpoints[-1].stop_id) if endpoints else (None, None)
            previous, next_trip = (
                (group[index - 1] if index else None),
                (group[index + 1] if index + 1 < len(group) else None),
            )
            origin_name, destination_name = (
                snapshot.stops.get(origin or "", {}).get("stop_name"),
                snapshot.stops.get(destination or "", {}).get("stop_name"),
            )
            missing = len(endpoints) < 2 or not origin or not destination
            overlap = bool(previous and trip["trip_start_seconds"] < previous["trip_end_seconds"])
            negative = trip["trip_end_seconds"] < trip["trip_start_seconds"]
            depot = _depot(origin_name) or _depot(destination_name)
            result.append(
                {
                    **trip,
                    "duty_chain_id": chain_id(trip["gtfs_snapshot_id"], trip["service_date"], source, str(source_id)),
                    "trip_order": index + 1,
                    "previous_trip_id": previous["trip_id"] if previous else None,
                    "next_trip_id": next_trip["trip_id"] if next_trip else None,
                    "origin_stop_id": origin,
                    "destination_stop_id": destination,
                    "origin_stop_name": origin_name,
                    "destination_stop_name": destination_name,
                    "is_depot_segment": depot,
                    "is_public_service_segment": not depot,
                    "layover_from_previous_seconds": trip["trip_start_seconds"] - previous["trip_end_seconds"]
                    if previous
                    else None,
                    "layover_to_next_seconds": next_trip["trip_start_seconds"] - trip["trip_end_seconds"]
                    if next_trip
                    else None,
                    "line_changed_from_previous": bool(previous and trip["line"] != previous["line"]),
                    "line_changes_to_next": bool(next_trip and trip["line"] != next_trip["line"]),
                    "overlaps_previous_trip": overlap,
                    "has_negative_duration": negative,
                    "has_missing_stops": missing,
                    "is_malformed_duty_segment": overlap or negative or missing,
                }
            )
    return result


def iter_stop_semantics(duty_rows: list[dict[str, Any]], snapshot: Snapshot) -> Iterator[dict[str, Any]]:
    """Keep every operational stop; unknown endpoint evidence cannot be passenger output."""
    duty = {(row["service_date"], row["trip_id"]): row for row in duty_rows}
    for (service_date, trip_id), current in sorted(duty.items()):
        rows = sorted(snapshot.stop_times.get(trip_id, []), key=lambda row: row.stop_sequence)
        eligible = [row for row in rows if row.stop_service_class != "not_in_passenger_service"]
        classified = []
        for index, row in enumerate(rows):
            before, after = rows[index - 1] if index else None, rows[index + 1] if index + 1 < len(rows) else None
            prior = duty.get((service_date, current["previous_trip_id"]))
            following = duty.get((service_date, current["next_trip_id"]))
            prefix = bool(
                index == 0
                and prior
                and row.stop_id == prior["destination_stop_id"]
                and after
                and after.stop_id != row.stop_id
                and after.stop_id[:4] == row.stop_id[:4]
            )
            suffix = bool(
                index == len(rows) - 1
                and following
                and row.stop_id == following["origin_stop_id"]
                and before
                and before.stop_id != row.stop_id
                and before.stop_id[:4] == row.stop_id[:4]
            )
            explicit = row.stop_service_class == "not_in_passenger_service"
            if current["is_depot_segment"] or not eligible:
                kind, reason = (
                    "technical_trip",
                    "depot_segment" if current["is_depot_segment"] else "no_gtfs_passenger_stops",
                )
            elif explicit:
                before_passenger = any(item.stop_service_class != "not_in_passenger_service" for item in rows[:index])
                after_passenger = any(
                    item.stop_service_class != "not_in_passenger_service" for item in rows[index + 1 :]
                )
                kind = (
                    "technical_prefix"
                    if not before_passenger
                    else "technical_suffix"
                    if not after_passenger
                    else "unknown"
                )
                reason = (
                    "explicit_non_passenger_prefix"
                    if kind == "technical_prefix"
                    else "explicit_non_passenger_suffix"
                    if kind == "technical_suffix"
                    else "explicit_internal_non_passenger"
                )
            elif (
                prefix
                and current["duty_chain_source"] == "block_id"
                and (current["layover_from_previous_seconds"] or 0) >= 0
                and (_distance(snapshot.stops.get(row.stop_id), snapshot.stops.get(after.stop_id)) or math.inf) <= 250
            ):
                kind, reason = "technical_prefix", "adjacent_duty_origin_handoff"
            elif (
                suffix
                and current["duty_chain_source"] == "block_id"
                and (current["layover_to_next_seconds"] or 0) >= 0
                and (_distance(snapshot.stops.get(before.stop_id), snapshot.stops.get(row.stop_id)) or math.inf) <= 250
            ):
                kind, reason = "technical_suffix", "adjacent_duty_destination_handoff"
            elif prefix or suffix:
                kind, reason = "unknown", "ambiguous_terminal_handoff"
            else:
                kind, reason = "passenger", "gtfs_passenger_stop"
            classified.append((row, kind, reason))
        settled = not any(
            kind == "unknown" and row.stop_sequence in {rows[0].stop_sequence, rows[-1].stop_sequence}
            for row, kind, _ in classified
        )
        passenger = [row.stop_sequence for row, kind, _ in classified if kind == "passenger"]
        for row, kind, reason in classified:
            yield {
                "gtfs_snapshot_id": current["gtfs_snapshot_id"],
                "service_date": current["service_date"],
                "processing_date": current["processing_date"],
                "trip_id": trip_id,
                "stop_id": row.stop_id,
                "stop_group_id": row.stop_id[:4],
                "stop_sequence": row.stop_sequence,
                "arrival_time_seconds": row.arrival_time_seconds,
                "departure_time_seconds": row.departure_time_seconds,
                "pickup_type": row.pickup_type,
                "drop_off_type": row.drop_off_type,
                "stop_service_class": row.stop_service_class,
                "duty_chain_id": current["duty_chain_id"],
                "duty_chain_source": current["duty_chain_source"],
                "duty_chain_source_id": current["duty_chain_source_id"],
                "trip_order": current["trip_order"],
                "previous_trip_id": current["previous_trip_id"],
                "next_trip_id": current["next_trip_id"],
                "stop_execution_class": kind,
                "classification_confidence": "low" if kind == "unknown" else "high",
                "classification_reason": reason,
                "classification_evidence": _classification_evidence(current, row, kind),
                "is_passenger_stop": kind == "passenger" and settled,
                "are_passenger_boundaries_settled": settled,
                "first_passenger_stop_sequence": min(passenger) if settled and passenger else None,
                "last_passenger_stop_sequence": max(passenger) if settled and passenger else None,
            }


def stop_semantics(duty_rows: list[dict[str, Any]], snapshot: Snapshot) -> list[dict[str, Any]]:
    """Materialize compact fixture output; production uses the bounded iterator."""
    return list(iter_stop_semantics(duty_rows, snapshot))


def _classification_evidence(current: dict[str, Any], row: StopTime, kind: str) -> list[str]:
    if current["is_depot_segment"]:
        return ["depot_terminal_name"]
    if kind == "technical_trip":
        return ["explicit_non_passenger_service"]
    if row.stop_service_class == "not_in_passenger_service":
        return ["explicit_non_passenger_service"]
    if kind in {"technical_prefix", "technical_suffix"}:
        return sorted(
            [
                "adjacent_trip_exact_stop_post",
                "block_id_duty_chain",
                "non_negative_layover",
                "same_stop_group_terminal_movement",
                "short_terminal_movement",
            ]
        )
    if kind == "unknown":
        return ["incomplete_terminal_handoff_evidence"]
    return ["gtfs_passenger_eligible"]
