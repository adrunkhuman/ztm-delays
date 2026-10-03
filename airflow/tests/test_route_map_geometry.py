from __future__ import annotations

import json
import sys
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import route_map_geometry as geometry

MONTH = "2026-09"
A = [21.00, 52.23]
B = [21.01, 52.23]
C = [21.01, 52.24]
D = [21.00, 52.24]


def segment(a: list[float] = A, b: list[float] = B, **overrides: Any) -> dict[str, Any]:
    count = int(overrides.get("observation_count", 1))
    delta = float(overrides.pop("delta", 30))
    row = {
        "period": "weekday",
        "gtfs_snapshot_id": "s1",
        "shape_id": "shape",
        "line": "101",
        "mode": "bus",
        "direction_id": 0,
        "from_stop_id": "a",
        "to_stop_id": "b",
        "from_stop_sequence": 1,
        "to_stop_sequence": 2,
        "from_stop_name": "Alpha",
        "to_stop_name": "Beta",
        "from_stop_post_code": "01",
        "to_stop_post_code": "02",
        "from_lon": a[0],
        "from_lat": a[1],
        "to_lon": b[0],
        "to_lat": b[1],
        "observation_count": count,
        "observed_days": 1,
        "sum_delta_seconds": delta * count,
        "sum_gain_seconds": max(delta, 0) * count,
        "sum_recovery_seconds": max(-delta, 0) * count,
        "gain_count": count if delta > 0 else 0,
        "recovery_count": count if delta < 0 else 0,
        "unchanged_count": count if delta == 0 else 0,
        "mean_from_delay_seconds": -100,
        "mean_to_delay_seconds": -100 + delta,
        "mean_scheduled_elapsed_seconds": 300,
        "mean_actual_elapsed_seconds": 300 + delta,
    }
    return {**row, **overrides}


def shape(coords: list[list[float]] | None = None, **overrides: Any) -> dict[str, Any]:
    coords = [A, B] if coords is None else coords
    return {
        "gtfs_snapshot_id": "s1",
        "shape_id": "shape",
        "geometry": json.dumps({"type": "LineString", "coordinates": coords}),
        **overrides,
    }


def payload(
    rows: list[dict[str, Any]] | None = None,
    shapes: list[dict[str, Any]] | None = None,
    coverage: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "segments": rows if rows is not None else [segment()],
        "shapes": shapes if shapes is not None else [shape()],
        "coverage": coverage or [],
    }


def coords_set(routes: dict[str, Any]) -> set[tuple[tuple[float, ...], ...]]:
    return {tuple(map(tuple, f["geometry"]["coordinates"])) for f in routes["features"]}


def loop_rows() -> list[dict[str, Any]]:
    return [
        segment(A, B),
        segment(B, C, from_stop_id="b", to_stop_id="c", from_stop_sequence=2, to_stop_sequence=3),
        segment(C, D, from_stop_id="c", to_stop_id="d", from_stop_sequence=3, to_stop_sequence=4),
        segment(D, A, from_stop_id="d", to_stop_id="a", from_stop_sequence=4, to_stop_sequence=5),
        segment(A, B, from_stop_sequence=5, to_stop_sequence=6),
    ]


def test_bend_clipped_on_shape_not_chord_or_tails() -> None:
    start, end = [21.005, 52.23], [21.01, 52.235]
    routes, report = geometry.process(payload([segment(start, end)], [shape([A, B, C, D])]), MONTH)

    assert routes["features"][0]["geometry"]["coordinates"] == [start, B, end]
    assert report["periods"]["weekday"]["mapped_traversals"] == 1


def test_loop_repeated_stop_occurrences_follow_sequence() -> None:
    routes, report = geometry.process(payload(loop_rows(), [shape([A, B, C, D, A, B])]), MONTH)

    assert report["exclusions"] == {}
    assert report["periods"]["weekday"]["mapped_segment_rows"] == 5
    first = next(f for f in routes["features"] if f["geometry"]["coordinates"] == [A, B])
    assert first["properties"]["stats"]["weekday"]["all"]["observation_count"] == 2


def test_loop_without_occurrence_evidence_rejected() -> None:
    routes, report = geometry.process(payload(shapes=[shape([A, B, C, D, A, B])]), MONTH)

    assert routes["features"] == []
    assert report["exclusions"] == {"ambiguous_snap": 1}
    assert report["excluded_traversals_by_reason"] == {"ambiguous_snap": 1}


@pytest.mark.parametrize(
    ("shapes", "reason"),
    [
        ([], "missing_shape"),
        ([shape([A, A])], "invalid_shape"),
        ([shape([[20, 51], [20.001, 51]])], "failed_snap"),
        ([shape([B, A])], "nonmonotonic_snap"),
        ([shape([A, [21.1, 52.23]])], "invalid_shape"),
    ],
)
def test_missing_invalid_distant_and_reversed_shapes_never_fallback(shapes: list[dict[str, Any]], reason: str) -> None:
    routes, report = geometry.process(payload(shapes=shapes), MONTH)

    assert routes["features"] == []
    assert report["exclusions"] == {reason: 1}


def test_duplicate_vertices_and_collinear_vertices_canonicalized() -> None:
    rows = [segment(), segment(gtfs_snapshot_id="s2", line="102")]
    shapes = [shape([A, A, B]), shape([A, [21.005, 52.23], B], gtfs_snapshot_id="s2")]
    routes, report = geometry.process(payload(rows, shapes), MONTH)

    assert len(routes["features"]) == 1
    assert report["exclusions"] == {}
    assert routes["features"][0]["geometry"]["coordinates"] == [A, B]


def test_weighted_signed_net_and_mode_period_filters() -> None:
    rows = [
        segment(observation_count="1", delta=90, observed_days="1"),
        segment(observation_count="9", delta=-30, mode="tram", line="T1", gtfs_snapshot_id="s2"),
        segment(period="weekend", delta=60),
    ]
    routes, report = geometry.process(payload(rows, [shape(), shape(gtfs_snapshot_id="s2")]), MONTH)
    stats = routes["features"][0]["properties"]["stats"]
    combined = stats["weekday"]["all"]

    assert combined["mean_delta_seconds"] == -18
    assert combined["observation_count"] == 10
    assert combined["mean_from_delay_seconds"] == -100
    assert combined["mean_to_delay_seconds"] == -118
    assert combined["mean_gain_seconds"] == 90
    assert combined["mean_recovery_seconds"] == 30
    assert combined["gain_count"] == 1
    assert combined["recovery_count"] == 9
    assert combined["lines"] == ["101", "T1"]
    assert stats["weekday"]["bus"]["mean_delta_seconds"] == 90
    assert stats["weekday"]["tram"]["mean_delta_seconds"] == -30
    assert stats["weekend"]["all"]["mean_delta_seconds"] == 60
    assert report["periods"]["weekday"]["mapped_traversals"] == 10


def test_days_are_bounds_or_actual_union_never_summed_as_unique() -> None:
    rows = [
        segment(observation_count=4, observed_days=3),
        segment(observation_count=4, observed_days=2, line="102"),
    ]
    routes, _ = geometry.process(payload(rows), MONTH)
    stats = routes["features"][0]["properties"]["stats"]["weekday"]["all"]

    assert stats["observed_days"] is None
    assert (stats["observed_days_lower"], stats["observed_days_upper"]) == (3, 5)

    rows[0]["observed_service_dates"] = ["2026-09-01", "2026-09-02", "2026-09-03"]
    rows[1]["observed_service_dates"] = ["2026-09-02", "2026-09-04"]
    routes, _ = geometry.process(payload(rows), MONTH)

    assert routes["features"][0]["properties"]["stats"]["weekday"]["all"]["observed_days"] == 4


def test_segments_without_observed_service_dates_report_bounds() -> None:
    # The production extract omits observed_service_dates; days become bounds, never an exact union.
    rows = [segment(observation_count=2, observed_days=2), segment(observation_count=1, observed_days=1, line="102")]
    assert all("observed_service_dates" not in row for row in rows)
    routes, report = geometry.process(payload(rows), MONTH)
    stats = routes["features"][0]["properties"]["stats"]["weekday"]["all"]

    assert stats["observed_days"] is None
    assert (stats["observed_days_lower"], stats["observed_days_upper"]) == (2, 3)
    assert stats["observation_count"] == 3
    assert report["exclusions"] == {}

    single, _ = geometry.process(payload([segment(observed_days=1)]), MONTH)
    single_stats = single["features"][0]["properties"]["stats"]["weekday"]["all"]
    assert single_stats["observed_days"] == 1


def test_mixed_known_and_unknown_service_dates() -> None:
    known = segment(observation_count=2, observed_days=2, observed_service_dates=["2026-09-01", "2026-09-02"])
    unknown = segment(observation_count=3, observed_days=3, line="102")
    routes, _ = geometry.process(payload([known, unknown]), MONTH)
    stats = routes["features"][0]["properties"]["stats"]["weekday"]["all"]

    assert (stats["observed_days_lower"], stats["observed_days_upper"]) == (3, 5)
    assert stats["observed_days"] is None


def test_31_day_month_counts_all_calendar_days() -> None:
    # October 2026 has 31 days: 22 weekdays and 9 weekend days. A 30-day assumption caps these at 21 and 8.
    weekday = segment(observation_count=22, observed_days=22)
    weekend = segment(period="weekend", observation_count=9, observed_days=9, line="102")
    routes, _ = geometry.process(payload([weekday, weekend]), "2026-10")
    stats = routes["features"][0]["properties"]["stats"]

    assert stats["weekday"]["all"]["observed_days"] == 22
    assert stats["weekday"]["all"]["observed_days_upper"] == 22
    assert stats["weekend"]["all"]["observed_days"] == 9
    assert stats["weekend"]["all"]["observed_days_upper"] == 9


def test_february_calendar_days() -> None:
    # February 2026: 20 weekdays and 8 weekend days; 29 days in a leap year must not be assumed.
    routes, _ = geometry.process(payload([segment(observation_count=20, observed_days=20)]), "2026-02")
    assert routes["features"][0]["properties"]["stats"]["weekday"]["all"]["observed_days"] == 20

    with pytest.raises(ValueError, match="observed_days exceeds calendar days"):
        geometry.process(payload([segment(observation_count=21, observed_days=21)]), "2026-02")


@pytest.mark.parametrize("month", ["2026-9", "2026-13", "2026-00", "0000-01", "2026-09-01", "September"])
def test_invalid_month_rejected(month: str) -> None:
    with pytest.raises(ValueError, match="month must be YYYY-MM"):
        geometry.process(payload(), month)


@pytest.mark.parametrize("bad", [[], [payload()], {"segments": [], "shapes": []}, {**payload(), "coverage": None}])
def test_input_requires_segment_shape_coverage_arrays(bad: Any) -> None:
    with pytest.raises(ValueError, match="segments, shapes, coverage"):
        geometry.process(bad, MONTH)


def test_near_identical_paths_of_one_stop_pair_merge() -> None:
    # Two shapes of the same road, 3 m apart; the busier one's geometry wins.
    shifted = [[A[0], A[1] + 0.000027], [B[0], B[1] + 0.000027]]
    rows = [
        segment(observation_count=3, delta=30),
        segment(observation_count=1, delta=-30, gtfs_snapshot_id="s2", line="102"),
    ]
    routes, _ = geometry.process(payload(rows, [shape(), shape(shifted, gtfs_snapshot_id="s2")]), MONTH)

    assert len(routes["features"]) == 1
    feature = routes["features"][0]
    assert feature["geometry"]["coordinates"] == [A, B]
    stats = feature["properties"]["stats"]["weekday"]["all"]
    assert stats["observation_count"] == 4
    assert stats["mean_delta_seconds"] == 15
    assert stats["lines"] == ["101", "102"]


def test_distinct_paths_or_stop_pairs_do_not_merge() -> None:
    # 30 m apart: a different path. 3 m apart but another stop pair: not the same segment.
    far = [[A[0], A[1] + 0.00027], [B[0], B[1] + 0.00027]]
    near = [[A[0], A[1] + 0.000027], [B[0], B[1] + 0.000027]]
    rows = [segment(), segment(gtfs_snapshot_id="s2"), segment(gtfs_snapshot_id="s3", from_stop_id="x")]
    shapes = [shape(), shape(far, gtfs_snapshot_id="s2"), shape(near, gtfs_snapshot_id="s3")]
    routes, _ = geometry.process(payload(rows, shapes), MONTH)

    assert len(routes["features"]) == 3


def test_paths_within_requires_same_orientation() -> None:
    scale = 111320.0
    forward = geometry.path_samples([A, B], scale)
    backward = geometry.path_samples([B, A], scale)

    assert geometry.paths_within(forward, forward, geometry.MERGE_TOLERANCE)
    assert not geometry.paths_within(forward, backward, geometry.MERGE_TOLERANCE)


def test_opposite_directions_remain_distinct() -> None:
    rows = [segment(), segment(B, A, shape_id="reverse", direction_id=1, delta=-30)]
    routes, _ = geometry.process(payload(rows, [shape(), shape([B, A], shape_id="reverse")]), MONTH)

    assert len(routes["features"]) == 2
    assert coords_set(routes) == {tuple(map(tuple, [A, B])), tuple(map(tuple, [B, A]))}


def test_unobserved_stop_gap_is_not_drawn() -> None:
    rows = [segment(), segment(C, D, from_stop_id="c", to_stop_id="d", from_stop_sequence=4, to_stop_sequence=5)]
    routes, _ = geometry.process(payload(rows, [shape([A, B, C, D])]), MONTH)

    assert len(routes["features"]) == 2
    assert coords_set(routes) == {tuple(map(tuple, [A, B])), tuple(map(tuple, [C, D]))}


def test_conflicting_order_rejected_not_guessed() -> None:
    routes, report = geometry.process(payload([segment(), segment(to_stop_id="different")]), MONTH)

    assert routes["features"] == []
    assert report["exclusions"] == {"conflicting_stop_order": 2}


def test_bad_endpoint_does_not_remove_other_valid_intervals() -> None:
    rows = [
        segment(),
        segment(
            B,
            [21.02, 52.25],
            from_stop_id="b",
            to_stop_id="bad",
            from_stop_sequence=2,
            to_stop_sequence=3,
        ),
    ]
    routes, report = geometry.process(payload(rows), MONTH)

    assert len(routes["features"]) == 1
    assert report["exclusions"] == {"failed_snap": 1}


def test_pathological_detour_and_duplicate_shape_conflict_reported() -> None:
    end = [21.0002, 52.2317]
    routes, report = geometry.process(payload([segment(A, end)], [shape([A, B, C, D, end])]), MONTH)

    assert routes["features"] == []
    assert report["exclusions"] == {"pathological_interval": 1}

    routes, report = geometry.process(payload(shapes=[shape(), shape([A, C])]), MONTH)

    assert routes["features"] == []
    assert report["exclusions"] == {"invalid_shape": 1}
    assert "conflicting duplicate shapes" in report["shape_errors"][0]["reason"]


def test_local_order_conflict_keeps_safe_pairs_without_bridging() -> None:
    points = [A, [21.002, 52.23], [21.004, 52.23], [21.003, 52.23], [21.006, 52.23], B]
    rows = [
        segment(a, b, from_stop_id=f"s{i}", to_stop_id=f"s{i + 1}", from_stop_sequence=i, to_stop_sequence=i + 1)
        for i, (a, b) in enumerate(pairwise(points))
    ]
    routes, report = geometry.process(payload(rows), MONTH)

    assert report["exclusions"] == {"nonmonotonic_snap": 3}
    assert report["periods"]["weekday"]["mapped_segment_rows"] == 2
    assert coords_set(routes) == {
        tuple(map(tuple, [points[0], points[1]])),
        tuple(map(tuple, [points[4], points[5]])),
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"sum_delta_seconds": "NaN"},
        {"gain_count": 0},
        {"mean_to_delay_seconds": 30},
        {"observation_count": True},
        {"from_lat": None},
        {"sum_recovery_seconds": -1},
        {"to_stop_sequence": 1},
        {"observed_days": 2},
    ],
)
def test_invalid_statistics_fail_loudly(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError):  # noqa: PT011
        geometry.process(payload([segment(**overrides)]), MONTH)


def test_observed_service_dates_must_match_month_period_and_days() -> None:
    for dates in (["2026-08-31"], ["2026-09-05"], ["2026-09-01", "2026-09-02"]):
        with pytest.raises(ValueError, match="observed_service_dates"):
            geometry.process(payload([segment(observed_service_dates=dates)]), MONTH)


def test_hour_buckets_weighted_stats_days_and_report() -> None:
    day = segment(
        observation_count=2,
        observed_days=2,
        delta=30,
        time_window="daytime",
        observed_service_dates=["2026-09-01", "2026-09-02"],
    )
    outside = segment(
        observation_count=8,
        observed_days=3,
        delta=-30,
        time_window="outside",
        observed_service_dates=["2026-09-02", "2026-09-03", "2026-09-04"],
    )
    daytime_tram = segment(
        mode="tram", line="T1", delta=-60, time_window="daytime", observed_service_dates=["2026-09-04"]
    )
    missing = segment(shape_id="missing", time_window="daytime", delta=0, observation_count=3)
    daily = {
        "service_date": "2026-09-01",
        "period": "weekday",
        **dict.fromkeys(geometry.COVERAGE_FIELDS, 0),
        "candidate_pairs": "16",
        "usable_pairs": "14",
        "daytime_candidate_pairs": "7",
        "daytime_usable_pairs": "6",
        "outside_candidate_pairs": "9",
        "outside_usable_pairs": "8",
        "daytime_missing_endpoint_count": "1",
    }
    routes, report = geometry.process(payload([day, outside, daytime_tram, missing], coverage=[daily]), MONTH)

    assert report["has_time_window_data"]
    assert len(routes["features"]) == 1
    properties = routes["features"][0]["properties"]
    all_bus = properties["stats"]["weekday"]["bus"]
    day_bus = properties["daytime_stats"]["weekday"]["bus"]
    assert all_bus["observation_count"] == 10
    assert all_bus["mean_delta_seconds"] == -18
    assert all_bus["mean_from_delay_seconds"] == -100
    assert all_bus["mean_to_delay_seconds"] == -118
    assert all_bus["mean_actual_elapsed_seconds"] == 282
    assert all_bus["observed_days"] == 4
    assert (all_bus["gain_count"], all_bus["recovery_count"]) == (2, 8)
    assert (all_bus["mean_gain_seconds"], all_bus["mean_recovery_seconds"]) == (30, 30)
    assert day_bus["observation_count"] == 2
    assert day_bus["mean_delta_seconds"] == 30
    assert day_bus["mean_to_delay_seconds"] == -70
    assert day_bus["mean_actual_elapsed_seconds"] == 330
    assert day_bus["observed_days"] == 2
    assert (day_bus["gain_count"], day_bus["recovery_count"]) == (2, 0)
    assert day_bus["mean_gain_seconds"] == 30
    assert day_bus["mean_recovery_seconds"] is None
    day_all = properties["daytime_stats"]["weekday"]["all"]
    assert day_all["mean_delta_seconds"] == 0
    assert day_all["observation_count"] == 3
    assert day_all["observed_days"] == 3
    assert (day_all["mean_gain_seconds"], day_all["mean_recovery_seconds"]) == (30, 60)
    assert report["periods"]["weekday"]["input_traversals"] == 14
    assert report["periods"]["weekday"]["mapped_traversals"] == 11
    window = report["time_windows"]["daytime"]["periods"]["weekday"]
    assert (window["input_traversals"], window["mapped_traversals"], window["excluded_traversals"]) == (6, 3, 3)
    assert window["excluded_traversals_by_reason"] == {"missing_shape": 3}
    assert window["coverage"]["candidate_pairs"] == 7
    assert window["coverage"]["missing_endpoint_count"] == 1
    assert window["coverage"]["invalid_time_count"] is None
    assert report["issues"][0]["time_window"] == "daytime"


def test_hours_never_filter_geometry_alignment_or_change_loop_keys() -> None:
    rows = loop_rows()
    raw_shape = [shape([A, B, C, D, A, B])]
    legacy, legacy_report = geometry.process(payload(rows, raw_shape), MONTH)
    windowed = [{**row, "time_window": "daytime" if i == 0 else "outside"} for i, row in enumerate(rows)]
    bucketed, report = geometry.process(payload(windowed, raw_shape), MONTH)

    assert not legacy_report["has_time_window_data"]
    assert report["exclusions"] == {}
    assert report["periods"]["weekday"]["mapped_traversals"] == legacy_report["periods"]["weekday"]["mapped_traversals"]
    for a, b in zip(legacy["features"], bucketed["features"], strict=True):
        assert a["id"] == b["id"]
        assert a["geometry"] == b["geometry"]
        assert a["properties"]["stats"] == b["properties"]["stats"]
        assert a["properties"]["daytime_stats"] == {}
    assert report["time_windows"]["daytime"]["periods"]["weekday"]["mapped_traversals"] == 1
    alone, alone_report = geometry.process(payload([{**rows[0], "time_window": "daytime"}], raw_shape), MONTH)
    assert alone["features"] == []
    assert alone_report["exclusions"] == {"ambiguous_snap": 1}


@pytest.mark.parametrize("value", [None, "", "all", "night", 1])
def test_time_window_enum_rejects_unknown_values(value: Any) -> None:
    with pytest.raises(ValueError, match="time_window"):
        geometry.process(payload([segment(time_window=value)]), MONTH)


def test_outside_only_window_and_coverage_partition_validation() -> None:
    outside, report = geometry.process(payload([segment(time_window="outside")]), MONTH)

    assert report["has_time_window_data"]
    assert outside["features"][0]["properties"]["daytime_stats"] == {}
    assert report["time_windows"]["daytime"]["periods"]["weekday"]["input_traversals"] == 0
    row = {
        "service_date": "2026-09-01",
        "period": "weekday",
        **dict.fromkeys(geometry.COVERAGE_FIELDS, 10),
        "daytime_candidate_pairs": 6,
        "daytime_usable_pairs": 6,
        "outside_candidate_pairs": 4,
        "outside_usable_pairs": 4,
    }
    _, report = geometry.process(payload(coverage=[row]), MONTH)
    assert report["coverage"][0]["outside_usable_pairs"] == 4
    for bad in (
        {**row, "outside_usable_pairs": 5},
        {**row, "daytime_candidate_pairs": -1},
        {**row, "daytime_invalid_time_count": "NaN"},
    ):
        with pytest.raises(ValueError):  # noqa: PT011
            geometry.process(payload(coverage=[bad]), MONTH)


def test_hours_scheduled_guard_uses_all_day_weighted_elapsed() -> None:
    end = [21.01, 52.235]
    # Daytime's short interval alone would reject this >2km subpath;
    # splitting the data must preserve the all-day geometry decision.
    far = [21.025, 52.23]
    rows = [
        segment(A, end, time_window="daytime", mean_scheduled_elapsed_seconds=10, mean_actual_elapsed_seconds=40),
        segment(A, end, time_window="outside", mean_scheduled_elapsed_seconds=300),
    ]
    routes, report = geometry.process(payload(rows, [shape([A, B, far, [21.025, 52.235], end])]), MONTH)
    properties = routes["features"][0]["properties"]

    assert report["exclusions"] == {}
    assert properties["stats"]["weekday"]["all"]["observation_count"] == 2
    assert properties["daytime_stats"]["weekday"]["all"]["observation_count"] == 1


def test_coverage_numeric_strings_and_calendar() -> None:
    coverage = [{"service_date": "2026-09-05", "period": "weekend", **dict.fromkeys(geometry.COVERAGE_FIELDS, "10")}]
    _, report = geometry.process(payload(coverage=coverage), MONTH)

    assert report["coverage"][0]["usable_pairs"] == 10
    for day, period in (("2026-08-05", "weekday"), ("2026-09-05", "weekday"), ("2026-09-31", "weekday")):
        with pytest.raises(ValueError):  # noqa: PT011
            geometry.process(payload(coverage=[{**coverage[0], "service_date": day, "period": period}]), MONTH)
    with pytest.raises(ValueError, match="duplicate"):
        geometry.process(payload(coverage=[coverage[0], coverage[0]]), MONTH)


def test_process_leaves_input_unchanged() -> None:
    data = payload([segment(from_stop_name="Łódź <b>")])
    original = json.dumps(data, sort_keys=True)
    routes, _ = geometry.process(data, MONTH)

    assert json.dumps(data, sort_keys=True) == original
    assert "Łódź <b>" in routes["features"][0]["properties"]["stats"]["weekday"]["all"]["endpoints"][0]
