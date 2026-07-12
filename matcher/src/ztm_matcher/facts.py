"""Bounded local reconstruction facts with dbt quality-policy parity."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ztm_matcher.schemas import (
    RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
    RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_TRIP_FACT_SCHEMA,
)

COMPLETE_STOP_RATIO = 0.80
BROKEN_STOP_RATIO = 0.30
LARGE_PING_GAP_SECONDS = 900
TERMINAL_STOP_TOLERANCE = 2
TERMINAL_PROGRESS_LAG_SECONDS = 120
LARGE_STOP_SEQUENCE_GAP = 4
IMPOSSIBLE_SPEED_MPS = 50.0
EXTREME_DELAY_SECONDS = 3600
FACT_ROW_GROUP_ROWS = 25_000


def classify_trip(metrics: dict[str, Any]) -> dict[str, Any]:
    """Port dbt ``int_trip_summary`` quality and service-observation policy."""
    expected = int(metrics["passenger_stops_expected"])
    detected = int(metrics["passenger_stops_detected"])
    ratio = detected / expected if expected else None
    first_required = metrics["first_required_stop_sequence"]
    last_required = metrics["last_required_stop_sequence"]
    first_detected = metrics["first_detected_stop_sequence"]
    last_detected = metrics["last_detected_stop_sequence"]
    first_observed = bool(
        first_required is not None
        and first_detected is not None
        and first_detected <= first_required + TERMINAL_STOP_TOLERANCE
    )
    last_observed = bool(
        last_required is not None
        and last_detected is not None
        and last_detected >= last_required - TERMINAL_STOP_TOLERANCE
    )
    max_gap = int(metrics["max_stop_sequence_gap"] or 0)
    max_ping_gap = int(metrics["max_ping_gap_seconds"] or 0)
    max_speed = float(metrics["max_speed_mps"] or 0.0)
    non_monotonic = bool(metrics["has_non_monotonic_stop_progression"])
    start_delay = metrics["start_delay_seconds"]
    end_delay = metrics["end_delay_seconds"]
    stale = bool(
        last_required is not None
        and last_detected is not None
        and last_detected < last_required - TERMINAL_STOP_TOLERANCE
        and end_delay is not None
        and end_delay >= -TERMINAL_PROGRESS_LAG_SECONDS
    )
    impossible_speed = max_speed > IMPOSSIBLE_SPEED_MPS
    flags = [
        *([] if first_observed else ["missing_first_stop"]),
        *([] if last_observed else ["missing_last_stop"]),
        *([] if ratio is None or ratio >= COMPLETE_STOP_RATIO else ["low_stop_coverage"]),
        *([] if max_ping_gap <= LARGE_PING_GAP_SECONDS else ["large_ping_gap"]),
        *([] if not non_monotonic else ["non_monotonic_stop_progression"]),
        *([] if not impossible_speed else ["impossible_speed_jump"]),
        *([] if max_gap <= LARGE_STOP_SEQUENCE_GAP else ["large_stop_sequence_gap"]),
        *(
            []
            if not (
                (first_observed and start_delay is not None and abs(start_delay) > EXTREME_DELAY_SECONDS)
                or (last_observed and end_delay is not None and abs(end_delay) > EXTREME_DELAY_SECONDS)
            )
            else ["extreme_delay"]
        ),
        *([] if not stale else ["stale_stop_progression"]),
        *(
            []
            if not ((ratio is not None and ratio < BROKEN_STOP_RATIO) or non_monotonic)
            else ["likely_wrong_trip_assignment"]
        ),
    ]
    bad_assignment = (
        (ratio is not None and ratio < BROKEN_STOP_RATIO)
        or max_ping_gap > LARGE_PING_GAP_SECONDS * 2
        or non_monotonic
        or impossible_speed
    )
    service_flags = [
        *([] if first_observed else ["short_start"]),
        *([] if last_observed else ["short_end"]),
        *([] if max_gap <= LARGE_STOP_SEQUENCE_GAP else ["large_internal_gap"]),
        *([] if not stale else ["stale_progress"]),
        *([] if not bad_assignment else ["bad_assignment_evidence"]),
    ]
    if bad_assignment:
        quality = "broken"
    elif (
        ratio is not None
        and ratio >= COMPLETE_STOP_RATIO
        and first_observed
        and last_observed
        and max_gap <= LARGE_STOP_SEQUENCE_GAP
        and max_ping_gap <= LARGE_PING_GAP_SECONDS
    ):
        quality = "complete"
    else:
        quality = "partial"
    service_class = (
        "matching_failure"
        if "bad_assignment_evidence" in service_flags
        else "modified"
        if "large_internal_gap" in service_flags or "stale_progress" in service_flags
        else "regular"
        if first_observed and last_observed
        else "truncated"
        if not first_observed or not last_observed
        else "modified"
    )
    return {
        "detected_stop_ratio": ratio,
        "is_first_stop_observed": first_observed,
        "is_last_stop_observed": last_observed,
        "has_impossible_speed_jump": impossible_speed,
        "has_stale_stop_progression": stale,
        "trip_quality": quality,
        "quality_flags": flags,
        "service_observation_class": service_class,
        "service_observation_flags": service_flags,
    }


def expected_status(
    *,
    direct_confidence: str | None,
    stop_service_class: str,
    trip_service_observation_class: str,
) -> tuple[str, list[str]]:
    """Classify an accepted settled passenger occurrence without interpolating it."""
    if direct_confidence == "high":
        return "observed", []
    if direct_confidence is not None:
        return "uncertain", ["alignment_ambiguous_or_medium"]
    if trip_service_observation_class == "matching_failure":
        return "uncertain", ["unreliable_trip_assignment"]
    if stop_service_class == "request":
        return "skipped_optional", []
    return "missed", []


def _quoted(path: Path) -> str:
    return str(path).replace("'", "''")


def _assert_unique(connection: duckdb.DuckDBPyConnection, query: str, label: str) -> None:
    if connection.execute(query).fetchone() is not None:
        raise ValueError(f"duplicate {label} grain")


def _install_warsaw_scheduled_time_macro(connection: duckdb.DuckDBPyConnection) -> None:
    """Install Python-authoritative Warsaw wall-clock conversion, not legacy dbt elapsed UTC."""
    connection.execute(
        """
        create or replace temp macro warsaw_scheduled_time(service_date, seconds) as (
            with wall_time as (
                select service_date::timestamp + seconds * interval '1 second' as local_time
            ), resolved as (
                select local_time, timezone('Europe/Warsaw', local_time) as resolved_time,
                    local_time - interval '1 hour' as previous_local_time
                from wall_time
            ), offsets as (
                select *, local_time - timezone('UTC', resolved_time) as resolved_offset,
                    previous_local_time
                        - timezone('UTC', timezone('Europe/Warsaw', previous_local_time)) as previous_offset,
                    timezone('Europe/Warsaw', resolved_time) as resolved_local_time,
                    timezone('Europe/Warsaw', timezone('Europe/Warsaw', previous_local_time))
                        as previous_resolved_local_time
                from resolved
            )
            select case
                -- DuckDB selects the second fall-back occurrence by default. Choose the
                -- first one, as stop_alignment does with Python's fold=0.
                when resolved_local_time = local_time
                    and previous_resolved_local_time = previous_local_time
                    and previous_offset > resolved_offset
                    then resolved_time - (previous_offset - resolved_offset)
                else resolved_time
            end
            from offsets
        )
        """
    )


def _trip_input_query(executions: Path, semantics: Path, arrivals: Path, gps: Path) -> str:
    return f"""
        with accepted as (
            select * from read_parquet('{_quoted(executions)}')
            where execution_status = 'executed' and confidence = 'high'
              and duty_chain_source != 'line_brigade'
              and ownership_interval_start_time is not null and ownership_interval_end_time is not null
        ), regular_stops as (
            select accepted.gtfs_snapshot_id, accepted.processing_date, accepted.service_date, accepted.trip_id,
                accepted.vehicle_number, semantics.stop_sequence,
                warsaw_scheduled_time(semantics.service_date, semantics.arrival_time_seconds)
                    as scheduled_arrival_time
            from accepted inner join read_parquet('{_quoted(semantics)}') semantics
                using (gtfs_snapshot_id, processing_date, service_date, duty_chain_id, trip_id)
            where semantics.are_passenger_boundaries_settled and semantics.is_passenger_stop
              and semantics.stop_execution_class = 'passenger' and semantics.stop_service_class = 'regular'
        ), optional_stops as (
            select accepted.gtfs_snapshot_id, accepted.processing_date, accepted.service_date, accepted.trip_id,
                accepted.vehicle_number, semantics.stop_sequence
            from accepted inner join read_parquet('{_quoted(semantics)}') semantics
                using (gtfs_snapshot_id, processing_date, service_date, duty_chain_id, trip_id)
            where semantics.are_passenger_boundaries_settled and semantics.is_passenger_stop
              and semantics.stop_execution_class = 'passenger' and semantics.stop_service_class = 'request'
        ), regular_arrivals_base as (
            select accepted.gtfs_snapshot_id, accepted.processing_date, accepted.service_date, accepted.trip_id,
                accepted.vehicle_number, arrivals.stop_sequence, arrivals.actual_arrival_time,
                arrivals.arrival_delay_seconds
            from accepted inner join read_parquet('{_quoted(arrivals)}') arrivals
                using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
            where arrivals.are_passenger_boundaries_settled and arrivals.is_passenger_stop
              and arrivals.stop_execution_class = 'passenger' and arrivals.stop_service_class = 'regular'
              and arrivals.alignment_confidence = 'high'
        ), regular_arrivals as (
            select *, lag(stop_sequence) over (
                partition by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                order by actual_arrival_time, stop_sequence
            ) previous_by_time,
            lag(stop_sequence) over (
                partition by gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number
                order by stop_sequence
            ) previous_by_sequence
            from regular_arrivals_base
        ), regular_metrics as (
            select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number,
                count(*)::bigint passenger_stops_detected, min(stop_sequence)::bigint first_detected_stop_sequence,
                max(stop_sequence)::bigint last_detected_stop_sequence,
                arg_min(actual_arrival_time, stop_sequence) actual_start_time,
                arg_max(actual_arrival_time, stop_sequence) actual_end_time,
                arg_min(arrival_delay_seconds, stop_sequence)::bigint start_delay_seconds,
                arg_max(arrival_delay_seconds, stop_sequence)::bigint end_delay_seconds,
                coalesce(max(stop_sequence - previous_by_sequence), 0)::bigint max_stop_sequence_gap,
                coalesce(bool_or(stop_sequence < previous_by_time), false) has_non_monotonic_stop_progression
            from regular_arrivals group by all
        ), ping_segments as (
            select accepted.gtfs_snapshot_id, accepted.processing_date, accepted.service_date, accepted.trip_id,
                accepted.vehicle_number, gps.gps_date,
                date_diff('second', lag(gps.gps_time) over trip_window, gps.gps_time)::bigint ping_gap_seconds,
                12742000 * asin(sqrt(
                    pow(sin(radians(gps.lat - lag(gps.lat) over trip_window) / 2), 2)
                    + cos(radians(lag(gps.lat) over trip_window)) * cos(radians(gps.lat))
                    * pow(sin(radians(gps.lon - lag(gps.lon) over trip_window) / 2), 2)
                )) / nullif(date_diff('second', lag(gps.gps_time) over trip_window, gps.gps_time), 0) speed_mps
            from accepted inner join read_parquet('{_quoted(gps)}') gps
                on accepted.vehicle_number = gps.vehicle_number
                and gps.gps_time between accepted.ownership_interval_start_time and accepted.ownership_interval_end_time
            window trip_window as (
                partition by accepted.gtfs_snapshot_id, accepted.processing_date, accepted.service_date,
                    accepted.trip_id, accepted.vehicle_number order by gps.gps_time
            )
        ), ping_metrics as (
            select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number,
                max(gps_date) gps_date, coalesce(max(ping_gap_seconds), 0)::bigint max_ping_gap_seconds,
                coalesce(max(speed_mps), 0.0)::double max_speed_mps
            from ping_segments where ping_gap_seconds is not null group by all
        ), regular_stop_metrics as (
            select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number,
                count(*)::bigint passenger_stops_expected, min(stop_sequence)::bigint first_required_stop_sequence,
                max(stop_sequence)::bigint last_required_stop_sequence,
                arg_min(scheduled_arrival_time, stop_sequence) scheduled_start_time,
                arg_max(scheduled_arrival_time, stop_sequence) scheduled_end_time
            from regular_stops group by all
        ), optional_metrics as (
            select gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number,
                count(*)::bigint optional_passenger_stops_expected
            from optional_stops group by all
        ), optional_arrivals as (
            select accepted.gtfs_snapshot_id, accepted.processing_date, accepted.service_date, accepted.trip_id,
                accepted.vehicle_number, count(*)::bigint optional_passenger_stops_detected
            from accepted inner join read_parquet('{_quoted(arrivals)}') arrivals
                using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
            where arrivals.are_passenger_boundaries_settled and arrivals.is_passenger_stop
              and arrivals.stop_execution_class = 'passenger' and arrivals.stop_service_class = 'request'
              and arrivals.alignment_confidence = 'high'
            group by all
        )
        select accepted.gtfs_snapshot_id, accepted.processing_date,
            coalesce(ping_metrics.gps_date, accepted.processing_date) gps_date,
            accepted.service_date, accepted.trip_id, accepted.vehicle_number, accepted.line, accepted.brigade,
            accepted.mode,
            regular_stop_metrics.scheduled_start_time, regular_stop_metrics.scheduled_end_time,
            regular_metrics.actual_start_time, regular_metrics.actual_end_time, regular_metrics.start_delay_seconds,
            regular_metrics.end_delay_seconds,
            coalesce(regular_stop_metrics.passenger_stops_expected, 0)::bigint passenger_stops_expected,
            coalesce(regular_metrics.passenger_stops_detected, 0)::bigint passenger_stops_detected,
            coalesce(optional_metrics.optional_passenger_stops_expected, 0)::bigint optional_passenger_stops_expected,
            coalesce(optional_arrivals.optional_passenger_stops_detected, 0)::bigint optional_passenger_stops_detected,
            regular_stop_metrics.first_required_stop_sequence, regular_stop_metrics.last_required_stop_sequence,
            regular_metrics.first_detected_stop_sequence, regular_metrics.last_detected_stop_sequence,
            coalesce(regular_metrics.max_stop_sequence_gap, 0)::bigint max_stop_sequence_gap,
            coalesce(ping_metrics.max_ping_gap_seconds, 0)::bigint max_ping_gap_seconds,
            coalesce(ping_metrics.max_speed_mps, 0.0)::double max_speed_mps,
            coalesce(regular_metrics.has_non_monotonic_stop_progression, false) has_non_monotonic_stop_progression
        from accepted
        left join regular_stop_metrics using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
        left join regular_metrics using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
        left join optional_metrics using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
        left join optional_arrivals using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
        left join ping_metrics using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
        order by gtfs_snapshot_id, service_date, trip_id, vehicle_number
    """


def _write_trip_facts(connection: duckdb.DuckDBPyConnection, query: str, output: Path) -> int:
    writer = pq.ParquetWriter(output, RECONSTRUCTION_TRIP_FACT_SCHEMA, compression="zstd")
    count = 0
    try:
        reader = connection.execute(query).to_arrow_reader(FACT_ROW_GROUP_ROWS)
        for batch in reader:
            rows = []
            for row in batch.to_pylist():
                quality = classify_trip(row)
                rows.append(
                    {
                        name: row[name]
                        for name in RECONSTRUCTION_TRIP_FACT_SCHEMA.names
                        if name in row and name not in quality
                    }
                    | quality
                )
            if rows:
                table = pa.Table.from_pylist(rows, schema=RECONSTRUCTION_TRIP_FACT_SCHEMA)
                writer.write_table(table)
                count += table.num_rows
    finally:
        writer.close()
    return count


def build_facts(
    connection: duckdb.DuckDBPyConnection,
    *,
    executions: Path,
    semantics: Path,
    arrivals: Path,
    normalized_gps: Path,
    output_dir: Path,
) -> dict[str, int]:
    """Write deterministic fact adapters, scanning raw GPS only through owned intervals."""
    _install_warsaw_scheduled_time_macro(connection)
    accepted = f"""select gtfs_snapshot_id, service_date, trip_id, vehicle_number, count(*) count
        from read_parquet('{_quoted(executions)}')
        where execution_status = 'executed' and confidence = 'high' and duty_chain_source != 'line_brigade'
        group by all having count(*) > 1"""
    direct = f"""select gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence, count(*) count
        from read_parquet('{_quoted(arrivals)}') group by all having count(*) > 1"""
    _assert_unique(connection, accepted, "accepted trip")
    _assert_unique(connection, direct, "direct passenger arrival")
    trip_path = output_dir / "reconstruction_trip_facts.parquet"
    stop_path = output_dir / "reconstruction_stop_arrivals.parquet"
    expected_path = output_dir / "reconstruction_expected_stop_events.parquet"
    trip_count = _write_trip_facts(
        connection, _trip_input_query(executions, semantics, arrivals, normalized_gps), trip_path
    )
    trip_sql, arrival_sql, semantic_sql = _quoted(trip_path), _quoted(arrivals), _quoted(semantics)
    connection.execute(
        f"""
        copy (
            select trips.gtfs_snapshot_id, trips.processing_date, trips.gps_date,
                coalesce(segment.gps_date, trips.gps_date) source_gps_date, trips.service_date, trips.trip_id,
                trips.vehicle_number, trips.line, trips.brigade, trips.mode, arrivals.stop_id, arrivals.stop_group_id,
                arrivals.stop_sequence::bigint stop_sequence, arrivals.pickup_type::bigint pickup_type,
                arrivals.drop_off_type::bigint drop_off_type, arrivals.stop_service_class,
                arrivals.scheduled_arrival_time, arrivals.scheduled_departure_time, arrivals.actual_arrival_time,
                arrivals.arrival_delay_seconds::bigint delay_seconds, arrivals.detection_method,
                arrivals.stop_match_radius_m,
                arrivals.segment_distance_m stop_distance_m, arrivals.segment_start_distance_m prev_ping_distance_m,
                arrivals.segment_end_distance_m next_ping_distance_m, arrivals.segment_start_time,
                arrivals.segment_end_time,
                arrivals.segment_duration_seconds::bigint segment_duration_seconds, arrivals.alignment_confidence,
                arrivals.alignment_evidence, trips.trip_quality, trips.quality_flags, trips.service_observation_class,
                trips.service_observation_flags
            from read_parquet('{arrival_sql}') arrivals
            inner join read_parquet('{trip_sql}') trips
                using (gtfs_snapshot_id, processing_date, service_date, trip_id, vehicle_number)
            left join read_parquet('{_quoted(normalized_gps)}') segment
                on arrivals.vehicle_number = segment.vehicle_number and arrivals.segment_start_time = segment.gps_time
            where arrivals.alignment_confidence = 'high'
            order by trips.gtfs_snapshot_id, trips.service_date, trips.trip_id, trips.vehicle_number,
                arrivals.stop_sequence
        ) to '{_quoted(stop_path)}' (format parquet, compression zstd, row_group_size {FACT_ROW_GROUP_ROWS})
        """
    )
    connection.execute(
        f"""
        copy (
            select trips.gtfs_snapshot_id, trips.processing_date, trips.gps_date,
                case when arrivals.alignment_confidence is not null then segment.gps_date end source_gps_date,
                trips.service_date, trips.trip_id, trips.vehicle_number, trips.line, trips.brigade, trips.mode,
                semantics.stop_id, semantics.stop_group_id, semantics.stop_sequence::bigint stop_sequence,
                semantics.pickup_type::bigint pickup_type, semantics.drop_off_type::bigint drop_off_type,
                semantics.stop_service_class,
                warsaw_scheduled_time(semantics.service_date, semantics.arrival_time_seconds)
                    as scheduled_arrival_time,
                warsaw_scheduled_time(semantics.service_date, semantics.departure_time_seconds)
                    as scheduled_departure_time,
                case when arrivals.alignment_confidence = 'high' then 'observed'
                    when arrivals.alignment_confidence is not null then 'uncertain'
                    when trips.service_observation_class = 'matching_failure' then 'uncertain'
                    when semantics.stop_service_class = 'request' then 'skipped_optional'
                    else 'missed' end observation_status,
                case when arrivals.alignment_confidence = 'high'
                    then arrivals.actual_arrival_time end actual_arrival_time,
                case when arrivals.alignment_confidence = 'high'
                    then arrivals.arrival_delay_seconds end delay_seconds,
                case
                    when arrivals.alignment_confidence is not null and arrivals.alignment_confidence != 'high'
                        then ['alignment_ambiguous_or_medium']
                    when arrivals.alignment_confidence is null
                        and trips.service_observation_class = 'matching_failure'
                        then ['unreliable_trip_assignment']
                    else []::varchar[]
                end uncertainty_evidence,
                trips.trip_quality, trips.quality_flags, trips.service_observation_class,
                trips.service_observation_flags
            from read_parquet('{trip_sql}') trips
            inner join read_parquet('{semantic_sql}') semantics
                on trips.gtfs_snapshot_id = semantics.gtfs_snapshot_id
                and trips.processing_date = semantics.processing_date and trips.service_date = semantics.service_date
                and trips.trip_id = semantics.trip_id
            left join read_parquet('{_quoted(arrivals)}') arrivals
                on trips.gtfs_snapshot_id = arrivals.gtfs_snapshot_id
                and trips.processing_date = arrivals.processing_date
                and trips.service_date = arrivals.service_date
                and trips.trip_id = arrivals.trip_id
                and trips.vehicle_number = arrivals.vehicle_number
                and semantics.stop_sequence = arrivals.stop_sequence
                and arrivals.are_passenger_boundaries_settled
                and arrivals.is_passenger_stop
                and arrivals.stop_execution_class = 'passenger'
            left join read_parquet('{_quoted(normalized_gps)}') segment
                on arrivals.vehicle_number = segment.vehicle_number and arrivals.segment_start_time = segment.gps_time
            where semantics.are_passenger_boundaries_settled and semantics.is_passenger_stop
              and semantics.stop_execution_class = 'passenger'
            order by trips.gtfs_snapshot_id, trips.service_date, trips.trip_id, trips.vehicle_number,
                semantics.stop_sequence
        ) to '{_quoted(expected_path)}' (format parquet, compression zstd, row_group_size {FACT_ROW_GROUP_ROWS})
        """
    )
    for path, schema in (
        (trip_path, RECONSTRUCTION_TRIP_FACT_SCHEMA),
        (stop_path, RECONSTRUCTION_STOP_ARRIVAL_SCHEMA),
        (expected_path, RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA),
    ):
        if pq.read_schema(path) != schema:
            raise ValueError(f"fact artifact schema validation failed: {path.name}")
    return {
        "reconstruction_trip_facts": trip_count,
        "reconstruction_stop_arrivals": pq.ParquetFile(stop_path).metadata.num_rows,
        "reconstruction_expected_stop_events": pq.ParquetFile(expected_path).metadata.num_rows,
    }
