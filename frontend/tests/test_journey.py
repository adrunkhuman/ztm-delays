# ruff: noqa: PLR2004 - literal seconds are the behavioral expectations
from __future__ import annotations

import math
import os
import random
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import duckdb
import pytest
from flask import Flask

from ztm_frontend import db, journey
from ztm_frontend.journey import Journey, Network, Ride, Walk

if TYPE_CHECKING:
    from pathlib import Path

DAY = date(2026, 9, 23)


@dataclass(frozen=True)
class Stop:
    post: str
    scheduled: int
    late: int = 0
    margin: int | None = 0
    usual: int = 0
    cumulative: float | None = None
    can_alight: bool = True
    expected: int | None = None  # default: the timetable for metro and rail, else the first departure plus the ride
    shape_dist: int | None = None


@dataclass(frozen=True)
class Trip:
    key: int
    stops: tuple[Stop, ...]
    day: date = DAY
    mode: str = "bus"
    line: str = "test"
    brigade: str | None = None
    duty: str | None = None
    shape: str | None = None


def _write(
    connection: duckdb.DuckDBPyConnection,
    trips: tuple[Trip, ...],
    walks: tuple = (),
    ranges: tuple = (),
    *,
    build_id: str | None = None,
) -> None:
    connection.execute(
        """
        create table planner_metadata (build_id varchar not null);
        create table planner_trip (
            trip_key bigint, service_date date, mode varchar, line varchar, headsign varchar,
            duty_id varchar, brigade varchar, shape_id varchar
        );
        create table planner_stop (
            trip_key bigint, stop_sequence integer, stop_id varchar, stop_group_id varchar,
            stop_name varchar, scheduled_sod integer, usual_delay_s integer, late_delay_s integer,
            leave_by_offset_s integer, ride_from_start_s double, expected_sod integer, can_alight boolean not null,
            shape_dist_m integer
        );
        create table planner_footpath (
            from_stop_id varchar, to_stop_id varchar, distance_m integer, walk_s integer
        );
        create table planner_range (
            is_tram boolean, weekday boolean, hour integer, min_ride_s double, max_ride_s double,
            low_ratio double, high_ratio double
        );
        """
    )
    connection.execute("insert into planner_metadata values (?)", [build_id or str(uuid4())])
    for trip in trips:
        connection.execute(
            "insert into planner_trip values (?, ?, ?, ?, 'Destination', ?, ?, ?)",
            [trip.key, trip.day, trip.mode, trip.line, trip.duty, trip.brigade, trip.shape],
        )
        first = trip.stops[0]
        start = first.scheduled + max(first.usual, first.usual if first.margin is None else first.margin)
        start -= first.cumulative or 0.0
        for i, stop in enumerate(trip.stops):
            cumulative = stop.cumulative if stop.cumulative is not None else float(stop.scheduled - first.scheduled)
            expected = stop.expected
            if expected is None:
                expected = stop.scheduled if trip.mode in {"metro", "rail"} else round(start + cumulative)
            connection.execute(
                "insert into planner_stop values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    trip.key,
                    10 * (i + 1),
                    stop.post,
                    stop.post.partition(":")[0],
                    stop.post,
                    stop.scheduled,
                    stop.usual,
                    stop.late,
                    stop.margin if i < len(trip.stops) - 1 else None,
                    cumulative,
                    expected,
                    stop.can_alight,
                    stop.shape_dist,
                ],
            )
    if walks:
        connection.executemany("insert into planner_footpath values (?, ?, 100, ?)", walks)
    if ranges:
        connection.executemany("insert into planner_range values (?, ?, ?, ?, ?, 1, ?)", ranges)


def _network(*trips: Trip, walks: tuple = (), ranges: tuple = (), posts: tuple = ()) -> Network:
    with duckdb.connect() as connection:
        _write(connection, trips, walks, ranges)
        connection.execute("create table planner_stop_post (stop_id varchar, lat double, lon double)")
        if posts:
            connection.executemany("insert into planner_stop_post values (?, ?, ?)", posts)
        return Network(connection, DAY)


def _keys(result: Journey) -> list[int]:
    return [leg.trip_key for leg in result.legs if isinstance(leg, Ride)]


def test_boarding_model_not_alighting_quantile_and_signed_artifact_sequences() -> None:
    net = _network(
        Trip(
            -5,
            (
                Stop("O:1", 100, usual=15, late=40, margin=-10, cumulative=50),
                Stop("D:1", 200, usual=300, late=900, cumulative=250),
            ),
        )
    )
    ride = Ride(-5, 10, 20, 90, 115, 315, 360)
    assert net.timing(-5, 10, 20) == ride
    assert net.search("O", "D", 90) == [Journey(90, 360, (ride,))]
    assert net.search("O", "D", 91) == []
    assert net.search("missing", "D", 0) == []
    assert net.search("O", "missing", 0) == []
    assert net.search("D", "O", 0) == []


@pytest.mark.parametrize("walk_s", [0, 30])
@pytest.mark.parametrize("slack", [-1, 0, 1])
def test_impossible_stop_quantile_transfer_and_exact_safe_boundary(walk_s: int, slack: int) -> None:
    # The obsolete stop-quantile arrival is 200, but the boarded ride arrives by 250.
    board_by = 250 + walk_s + slack
    post = "X:2" if walk_s else "X:1"
    net = _network(
        Trip(1, (Stop("O:1", 100, late=40), Stop("X:1", 200))),
        Trip(2, (Stop(post, board_by + 10, margin=-10), Stop("D:1", 400))),
        walks=(("X:1", post, walk_s),) if walk_s else (),
    )
    results = net.search("O", "D", 90)
    if slack < 0:
        assert results == []
    else:
        assert _keys(results[0]) == [1, 2]
        assert results[0].legs[0] == Ride(1, 10, 20, 100, 100, 200, 250)
        if walk_s:
            assert results[0].legs[1] == Walk("X:1", "X:2", walk_s)
        assert results[0].arrive_late == math.ceil(board_by + 10 + (390 - board_by) * 1.1)


def test_boarding_dependent_overtaking_at_different_posts() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100, late=200), Stop("X:1", 200), Stop("D:1", 300))),
        Trip(2, (Stop("O:1", 110), Stop("X:1", 210, late=200), Stop("D:1", 310))),
    )
    assert _keys(net.search("O", "D", 0)[0]) == [2]
    assert _keys(net.search("X", "D", 0)[0]) == [1]
    assert len(net.patterns) == 1


def test_downstream_deadline_uses_latest_label_and_keeps_overtaking_candidates() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 110, margin=None), Stop("D:1", 500))),
        Trip(2, (Stop("O:1", 120, late=1000), Stop("X:1", 130, margin=None), Stop("D:1", 600))),
        Trip(3, (Stop("O:1", 150), Stop("X:1", 160, margin=None), Stop("D:1", 300))),
    )
    # The last trip boards after X's best arrival; it still improves D. The
    # intervening trip's very late boarding model must not terminate the scan.
    assert net.search("O", "D", 0) == [Journey(150, 315, (Ride(3, 10, 30, 150, 150, 300, 315),))]


def test_later_round_scans_newly_feasible_earlier_boardings() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 400))),
        Trip(2, (Stop("O:1", 110), Stop("Y:1", 120))),
        Trip(3, (Stop("Y:1", 200), Stop("X:1", 250))),
        Trip(4, (Stop("X:1", 450), Stop("D:1", 600))),
        Trip(5, (Stop("X:1", 300), Stop("D:1", 350))),
    )
    results = net.search("O", "D", 0)
    assert [(j.vehicles, j.arrive_late) for j in results] == [(2, 615), (3, 355)]
    assert [_keys(j) for j in results] == [[1, 4], [2, 3, 5]]
    for result in results:
        _assert_feasible(net, result)


def test_board_by_sorting_is_specific_to_each_pattern_position() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 300), Stop("D:1", 400))),
        Trip(2, (Stop("O:1", 110), Stop("X:1", 200), Stop("D:1", 500))),
    )
    assert _keys(net.search("X", "D", 250)[0]) == [1]
    assert _keys(net.search("X", "D", 190)[0]) == [1]


def test_no_walking_ahead_to_board_a_trip_that_stops_at_the_origin() -> None:
    # Boarding at X gives a smaller late spread on the same vehicle (an artefact of the boarding-dependent
    # bound); the rider should still board where they are.
    net = _network(
        Trip(1, (Stop("O:1", 100, late=300), Stop("X:1", 200), Stop("D:1", 300))),
        walks=(("O:1", "X:1", 20),),
    )
    (result,) = net.search("O", "D", 0)
    assert result.legs == (net.timing(1, 10, 30),)
    assert result.depart == 100


def test_reboarding_same_trip_in_another_round_is_not_fifo_pruned() -> None:
    # Reach X before boarding closes; its new hour resets the ride calibration.
    net = _network(
        Trip(
            1,
            (
                Stop("O:1", 3500, late=50, cumulative=0),
                Stop("X:1", 3600, cumulative=10),
                Stop("D:1", 4600, cumulative=1010),
            ),
        ),
        ranges=((False, True, 0, 0, 2000, 2),),
    )
    results = net.search("O", "D", 0)
    assert [(j.vehicles, j.arrive_late) for j in results] == [(1, 5570), (2, 4700)]
    assert _keys(results[1]) == [1, 1]


@pytest.mark.parametrize(("duration", "late"), [(100, 300), (100.001, 300), (150, 300), (201, 322)])
def test_ratio_downward_boundary_uses_monotone_envelope(duration: float, late: int) -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100, cumulative=0), Stop("D:1", 200, cumulative=duration))),
        ranges=((False, True, 0, 0, 100, 2), (False, True, 0, 100, 200, 1)),
    )
    assert net.timing(1, 10, 20).arrive_late == late
    assert net.search("O", "D", 0)[0].arrive_late == late


@pytest.mark.parametrize(("duration", "late"), [(50, 155), (75, 175), (100, 200), (150, 265), (200, 300), (210, 331)])
def test_ratio_gaps_and_absent_cells_default_and_ratios_floor_at_one(duration: float, late: int) -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100, cumulative=0), Stop("D:1", 200, cumulative=duration))),
        ranges=((False, True, 0, 50, 100, 0.5), (False, True, 0, 150, 200, 0.5)),
    )
    assert net.timing(1, 10, 20).arrive_late == late


def test_float_just_after_transfer_boundary_is_ceiled_not_truncated() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100, cumulative=0), Stop("X:1", 200, cumulative=100.0000001))),
        Trip(2, (Stop("X:1", 200), Stop("D:1", 300))),
        ranges=((False, True, 0, 0, 1000, 1),),
    )
    assert net.timing(1, 10, 20).arrive_late == 201
    assert net.search("O", "D", 0) == []


@pytest.mark.parametrize("mode", ["metro", "rail"])
def test_metro_and_skm_keep_timetable_and_ignore_bus_ranges(mode: str) -> None:
    late = 120 if mode == "rail" else 0
    net = _network(
        Trip(1, (Stop("O:1", 100, late=late, cumulative=0), Stop("D:1", 200, late=999, cumulative=900)), mode=mode),
        ranges=((False, True, 0, 0, 1000, 9),),
    )
    assert net.timing(1, 10, 20) == Ride(1, 10, 20, 100, 100, 200, 200 + late)
    assert net.search("O", "D", 0)[0].arrive_late == 200 + late


def test_service_weekday_scheduled_hour_modulo_and_tram_select_range() -> None:
    # Monday's query includes Sunday's 24h trip: Sunday calibration, hour zero.
    sunday = date(2026, 9, 20)
    with duckdb.connect() as connection:
        _write(
            connection,
            (Trip(1, (Stop("O:1", 86400), Stop("D:1", 86500)), sunday, "tram"),),
            ranges=(
                (True, False, 0, 0, 1000, 2),
                (True, True, 0, 0, 1000, 3),
                (False, False, 0, 0, 1000, 4),
            ),
        )
        net = Network(connection, sunday + timedelta(days=1))
    assert net.timing(1, 10, 20) == Ride(1, 10, 20, 0, 0, 100, 200)


def test_departure_bases_clamped_to_board_by_and_late_at_least_expected() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100, usual=-50, late=-40, margin=-10), Stop("D:1", 200))),
        Trip(2, (Stop("O:1", 100, usual=50, late=20), Stop("D:1", 200))),
    )
    assert net.timing(1, 10, 20) == Ride(1, 10, 20, 90, 90, 190, 200)
    assert net.timing(2, 10, 20) == Ride(2, 10, 20, 100, 150, 250, 260)


@pytest.mark.parametrize("walk_s", [0, 30])
def test_pickup_only_stop_cannot_alight_or_transfer_but_allows_boarding_and_through_rides(walk_s: int) -> None:
    next_post = "X:2" if walk_s else "X:1"
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 200, can_alight=False), Stop("T:1", 300))),
        Trip(2, (Stop(next_post, 250), Stop("D:1", 400))),
        walks=(("X:1", next_post, walk_s),) if walk_s else (),
    )
    assert net.search("O", "X", 0) == []
    assert net.search("O", "D", 0) == []
    assert net.search("O", "T", 0) == [Journey(100, 320, (Ride(1, 10, 30, 100, 100, 300, 320),))]
    assert net.search("X", "T", 0) == [Journey(200, 310, (Ride(1, 20, 30, 200, 200, 300, 310),))]


@pytest.mark.parametrize("first_allowed", [True, False])
def test_identical_stop_sequences_with_different_alighting_permissions(first_allowed: bool) -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("D:1", 200, can_alight=first_allowed))),
        Trip(2, (Stop("O:1", 110), Stop("D:1", 300, can_alight=not first_allowed))),
    )
    result = net.search("O", "D", 0)[0]
    assert _keys(result) == ([1] if first_allowed else [2])
    _assert_feasible(net, result)


def test_all_no_alighting_pattern_has_no_arrivals_or_walking_transfers() -> None:
    net = _network(
        Trip(1, tuple(Stop(post, time, can_alight=False) for post, time in [("O:1", 100), ("X:1", 200), ("D:1", 300)])),
        Trip(2, (Stop("W:1", 400), Stop("T:1", 500))),
        walks=(("X:1", "W:1", 30),),
    )
    assert net.search("O", "D", 0) == []
    assert net.search("X", "D", 0) == []
    assert net.search("O", "T", 0) == []


def test_pickup_prohibition_and_previous_day_cutoff() -> None:
    net = _network(
        Trip(-7, (Stop("O:1", 86300), Stop("X:1", 86500, margin=None), Stop("D:1", 86700)), DAY - timedelta(days=1)),
        Trip(8, (Stop("O:1", 100), Stop("D:1", 200)), DAY - timedelta(days=1)),
    )
    assert net.search("X", "D", 0) == []
    assert net.search("O", "D", 0) == []
    assert _keys(net.search("O", "D", -100)[0]) == [-7]
    assert net.search("O", "X", -100)[0].arrive_late == 121


def test_endpoint_walk_reconstruction_and_no_chained_footpaths() -> None:
    net = _network(
        Trip(1, (Stop("B:1", 100, margin=-10), Stop("C:1", 200))),
        Trip(2, (Stop("O:1", 500), Stop("D:1", 600))),
        walks=(("O:1", "B:1", 30), ("C:1", "D:1", 40)),
    )
    assert net.search("O", "D", 60) == [
        Journey(60, 250, (Walk("O:1", "B:1", 30), Ride(1, 10, 20, 90, 100, 200, 210), Walk("C:1", "D:1", 40)))
    ]
    assert _keys(net.search("O", "D", 61)[0]) == [2]
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 200))),
        Trip(2, (Stop("Y:1", 1000), Stop("D:1", 1100))),
        walks=(("X:1", "Y:1", 20), ("Y:1", "D:1", 20)),
    )
    assert net.search("O", "D", 0)[0].arrive_late == 1110


def test_walked_label_cannot_dominate_ride_for_another_walk() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 200))),
        Trip(2, (Stop("O:1", 100), Stop("Y:1", 220))),
        Trip(3, (Stop("D:1", 1000), Stop("Z:1", 1100))),
        walks=(("X:1", "Y:1", 0), ("Y:1", "D:1", 10)),
    )
    result = net.search("O", "D", 0)[0]
    assert _keys(result) == [2]
    assert result.legs[-1] == Walk("Y:1", "D:1", 10)
    assert result.arrive_late == 242


@pytest.mark.parametrize("restricted_departure", [100, 120])
def test_walk_cannot_hide_unrestricted_boarding_arrival(restricted_departure: int) -> None:
    # The earlier arrival at B walked from A, so it cannot board trip 3 there.
    # With a later departure on trip 1, plan() also scans that restriction in a prior profile run.
    trips = (
        Trip(1, (Stop("O:1", restricted_departure), Stop("A:1", 200))),
        Trip(2, (Stop("O:1", 100), Stop("B:1", 250))),
        Trip(3, (Stop("A:1", 100), Stop("B:1", 300), Stop("D:1", 400))),
    )
    for walks in ((), (("A:1", "B:1", 30),)):
        net = _network(*trips, walks=walks)
        for results in (net.search("O", "D", 0), journey.plan(net, "O", "D", 0, results=10)):
            assert len(results) == 1
            (result,) = results
            assert _keys(result) == [2, 3]
            assert (result.depart, result.arrive_late, result.vehicles) == (100, 410, 2)
            _assert_feasible(net, result)


@pytest.mark.parametrize("restricted_departure", [100, 120])
def test_walks_from_different_origins_keep_distinct_boarding_restrictions(restricted_departure: int) -> None:
    # Both arrivals at B walked, but only the walk from A forbids boarding trip 3.
    net = _network(
        Trip(1, (Stop("O:1", restricted_departure), Stop("A:1", 200))),
        Trip(2, (Stop("O:1", 100), Stop("C:1", 250))),
        Trip(3, (Stop("A:1", 100), Stop("B:1", 300), Stop("D:1", 400))),
        walks=(("A:1", "B:1", 30), ("C:1", "B:1", 30)),
    )
    for results in (net.search("O", "D", 0), journey.plan(net, "O", "D", 0, results=10)):
        assert len(results) == 1
        (result,) = results
        assert _keys(result) == [2, 3]
        assert result.legs[1] == Walk("C:1", "B:1", 30)
        assert (result.depart, result.arrive_late, result.vehicles) == (100, 410, 2)
        _assert_feasible(net, result)


def test_distinct_posts_and_five_vehicle_limit() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("X:1", 200))),
        Trip(2, (Stop("X:2", 300), Stop("D:1", 400))),
    )
    assert net.search("O", "D", 0) == []
    net = _network(*(Trip(i, (Stop(f"S{i}:1", i * 100), Stop(f"S{i + 1}:1", i * 100 + 50))) for i in range(6)))
    assert net.search("S0", "S5", 0)[0].vehicles == 5
    assert net.search("S0", "S6", 0) == []


def test_search_frontier_and_plan_challenges_early_departure() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("D:1", 600))),
        Trip(2, (Stop("O:1", 100), Stop("X:1", 200))),
        Trip(3, (Stop("O:1", 150), Stop("X:1", 250))),
        Trip(4, (Stop("X:1", 300), Stop("D:1", 400))),
    )
    first = net.search("O", "D", 0)
    assert [(j.vehicles, j.arrive_late) for j in first] == [(1, 650), (2, 410)]
    assert first == net.search("O", "D", 0)
    assert first[1].depart == 100
    planned = journey.plan(net, "O", "D", 0, results=2)
    assert [(j.depart, j.arrive_late) for j in planned] == [(100, 650), (150, 410)]
    assert journey.plan(net, "O", "D", 0, results=1)[0] == planned[0]
    assert journey.plan(net, "O", "D", 101, results=2) == [planned[1]]


def test_dominance_needs_strict_improvement_and_ride_constructor_compatibility() -> None:
    first = Journey(100, 200, (Ride(1, 10, 20, 100),))
    tied = Journey(100, 200, (Ride(2, 10, 20, 100),))
    later = Journey(110, 200, (Ride(3, 10, 20, 110),))
    assert not first.dominates(tied)
    assert not first.dominates(first)
    assert later.dominates(first)


def test_artifact_cache_reuses_network_and_invalidates_on_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = OrderedDict()
    monkeypatch.setattr(journey, "_cache", cache)
    path, replacement = tmp_path / "planner.duckdb", tmp_path / "replacement.duckdb"
    with duckdb.connect(str(path)) as connection:
        _write(connection, (Trip(1, (Stop("O:1", 100), Stop("D:1", 200))),), build_id="old")
    old = journey.network(path, DAY)
    assert journey.network(path, DAY) is old
    with duckdb.connect(str(replacement)) as connection:
        _write(connection, (Trip(2, (Stop("O:1", 300), Stop("D:1", 400))),), build_id="new")
    replacement.replace(path)
    new = journey.network(path, DAY)
    assert new is not old
    assert _keys(new.search("O", "D", 0)[0]) == [2]
    assert _keys(old.search("O", "D", 0)[0]) == [1]
    assert list(cache) == [(str(path), "old", DAY), (str(path), "new", DAY)]
    next_day = journey.network(path, DAY + timedelta(days=1))
    assert journey.network(path, DAY) is new  # refresh the build/day LRU entry
    journey.network(path, DAY + timedelta(days=2))
    assert list(cache) == [(str(path), "new", DAY), (str(path), "new", DAY + timedelta(days=2))]
    assert journey.network(path, DAY + timedelta(days=1)) is not next_day
    assert len(cache) == journey.CACHED_DAYS


@pytest.mark.skipif(os.name == "nt", reason="Windows cannot replace an open DuckDB file")
def test_network_cache_identity_and_leg_details_follow_request_snapshot_during_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = OrderedDict()
    monkeypatch.setattr(journey, "_cache", cache)
    path, replacement = tmp_path / "planner.duckdb", tmp_path / "replacement.duckdb"
    with duckdb.connect(str(path)) as connection:
        _write(connection, (Trip(1, (Stop("O:1", 100), Stop("D:1", 200))),), build_id="old")
    with duckdb.connect(str(replacement)) as connection:
        _write(connection, (Trip(2, (Stop("O:1", 300), Stop("D:1", 500))),), build_id="new")
    app = Flask(__name__)
    app.teardown_appcontext(db.close_request_connections)
    with app.test_request_context():
        assert db.fetch_one(path, "select build_id from planner_metadata") == {"build_id": "old"}
        with db.read_connection(path) as held:
            replacement.replace(path)
            old = journey.network(path, DAY)
            with db.read_connection(path) as reused:
                assert reused is held
        assert list(cache) == [(str(path), "old", DAY)]
        assert journey.network(path, DAY) is old
        result = old.search("O", "D", 0)[0]
        assert result.legs == (Ride(1, 10, 20, 100, 100, 200, 210),)
        _assert_leg_details(path, result)
    with app.test_request_context():
        new = journey.network(path, DAY)
        assert new is not old
        assert journey.network(path, DAY) is new
        assert db.fetch_one(path, "select build_id from planner_metadata") == {"build_id": "new"}
        assert list(cache) == [(str(path), "old", DAY), (str(path), "new", DAY)]
        result = new.search("O", "D", 0)[0]
        assert result.legs == (Ride(2, 10, 20, 300, 300, 500, 520),)
        _assert_leg_details(path, result)


def _assert_leg_details(path: Path, result: Journey) -> None:
    for leg in result.legs:
        assert isinstance(leg, Ride)
        rows = db.fetch_all(
            path,
            "select stop_sequence, scheduled_sod from planner_stop where trip_key = ? order by stop_sequence",
            [leg.trip_key],
        )
        assert rows == [
            {"stop_sequence": leg.board_sequence, "scheduled_sod": leg.depart},
            {"stop_sequence": leg.alight_sequence, "scheduled_sod": leg.arrive},
        ]


def _assert_feasible(net: Network, result: Journey) -> None:
    clock = result.depart
    walked = False
    for leg in result.legs:
        if isinstance(leg, Walk):
            assert not walked
            clock += leg.walk_s
            walked = True
        else:
            assert clock <= leg.board_by
            assert leg == net.timing(leg.trip_key, leg.board_sequence, leg.alight_sequence)
            assert leg.arrive_late is not None
            clock = leg.arrive_late
            walked = False
    assert clock == result.arrive_late


def _enumerate_journeys(trips: tuple[Trip, ...], walks: tuple) -> list[tuple[int, int]]:  # noqa: C901 - brute force
    """Every journey from O to D, explicitly, with the router's rules; (vehicles, late arrival) per count that
    arrives earlier than all smaller counts."""
    footpaths: dict[str, list[tuple[str, int]]] = {}
    for source, dest, seconds in walks:
        footpaths.setdefault(source, []).append((dest, seconds))

    def boardable(trip: Trip, pos: int) -> bool:
        alights = [i for i, stop in enumerate(trip.stops) if stop.can_alight]
        return bool(alights) and pos < alights[-1]

    def late(trip: Trip, board: int, alight: int) -> int:
        b, a = trip.stops[board], trip.stops[alight]
        assert a.cumulative is not None
        assert b.cumulative is not None
        return math.ceil(b.scheduled + b.late + max(0, a.cumulative - b.cumulative) * 1.1)

    best: dict[int, int] = {}

    def ride_from(post: str, clock: int, vehicles: int, walked_from: tuple[str, ...]) -> None:
        if vehicles == journey.MAX_VEHICLES:
            return
        for trip in trips:
            for pos, board in enumerate(trip.stops):
                if board.post != post or clock > board.scheduled or not boardable(trip, pos):
                    continue
                if any(s.post in walked_from and boardable(trip, i) for i, s in enumerate(trip.stops[:pos])):
                    continue  # could board this trip where the walk started
                for alight in range(pos + 1, len(trip.stops)):
                    if trip.stops[alight].can_alight:
                        arrive(trip, pos, alight, vehicles + 1)

    def arrive(trip: Trip, board: int, alight: int, vehicles: int) -> None:
        post, clock = trip.stops[alight].post, late(trip, board, alight)
        if post == "D:1":
            best[vehicles] = min(best.get(vehicles, journey.INF), clock)
        ride_from(post, clock, vehicles, ())
        for dest, seconds in footpaths.get(post, []):
            if dest == "D:1":
                best[vehicles] = min(best.get(vehicles, journey.INF), clock + seconds)
            ride_from(dest, clock + seconds, vehicles, (post,))

    ride_from("O:1", 0, 0, ())
    for dest, seconds in footpaths.get("O:1", []):
        ride_from(dest, seconds, 0, ("O:1",))
    expected, record = [], journey.INF
    for count in range(1, journey.MAX_VEHICLES + 1):
        if best.get(count, journey.INF) < record:
            record = best[count]
            expected.append((count, record))
    return expected


@pytest.mark.parametrize("seed", [7301, 901, 73])
def test_random_small_networks_match_unpruned_round_oracle(seed: int) -> None:
    rng = random.Random(seed)  # noqa: S311 - reproducible test inputs
    for _ in range(20):
        trips = tuple(
            Trip(
                key,
                tuple(
                    Stop(
                        post,
                        100 + key * 10 + pos * 100,
                        late=rng.randrange(60),
                        cumulative=pos * rng.randrange(60, 100),
                        can_alight=rng.choice([True, False]),
                    )
                    for pos, post in enumerate(rng.sample(["O:1", "A:1", "B:1", "D:1"], 3))
                ),
            )
            for key in range(8)
        )
        walks = (("O:1", "A:1", 30), ("B:1", "D:1", 20))
        net = _network(*trips, walks=walks)
        expected = _enumerate_journeys(trips, walks)
        results = net.search("O", "D", 0)
        assert [(j.vehicles, j.arrive_late) for j in results] == expected
        for result in results:
            _assert_feasible(net, result)
            assert all(
                trips[leg.trip_key].stops[leg.alight_sequence // 10 - 1].can_alight
                for leg in result.legs
                if isinstance(leg, Ride)
            )


@pytest.mark.parametrize(("previous_departure", "winner"), [(None, 1), (10, 2)])
def test_initial_walk_prefix_improvements_preserve_dynamic_tie_order(
    previous_departure: int | None, winner: int
) -> None:
    net = _network(
        Trip(1, (Stop("X:1", 55), Stop("D:1", 155)), mode="metro"),
        Trip(2, (Stop("Y:1", 55), Stop("D:1", 155)), mode="metro"),
        Trip(3, (Stop("O:1", 1000), Stop("Q:1", 1100)), mode="metro"),
        Trip(4, (Stop("O:2", 1000), Stop("Q:2", 1100)), mode="metro"),
        walks=(("O:1", "X:1", 100), ("O:1", "Y:1", 50), ("O:2", "X:1", 50), ("O:2", "X:1", 70)),
    )
    profile = net.profile("O", "D")
    assert len(profile.origin_walks) == 3
    if previous_departure is not None:
        assert profile.run(previous_departure) == []
    # From scratch the longer X walk inserts X first, then the shorter one replaces its value. After a run
    # from 10 s, that longer walk cannot improve X's bound, so Y is inserted before X instead.
    result = journey._journey(net, profile.run(0)[0])  # noqa: SLF001
    assert _keys(result) == [winner]
    assert (result.depart, result.arrive_late) == (5, 155)


@pytest.mark.parametrize("budget", [0, 1, journey.WALK_PERMISSION_WORDS])
def test_equivalent_walk_winner_keeps_its_own_position_among_equal_time_labels(
    monkeypatch: pytest.MonkeyPatch, budget: int
) -> None:
    monkeypatch.setattr(journey, "WALK_PERMISSION_WORDS", budget)
    net = _network(
        Trip(1, (Stop("O:1", 100), Stop("A:1", 200)), mode="metro"),
        Trip(2, (Stop("O:1", 100), Stop("B:1", 100)), mode="metro"),
        Trip(3, (Stop("O:1", 100), Stop("C:1", 100)), mode="metro"),
        Trip(4, (Stop("X:1", 250), Stop("D:1", 350)), mode="metro"),
        Trip(5, (Stop("Y:1", 250), Stop("D:1", 350)), mode="metro"),
        walks=(("A:1", "X:1", 100), ("B:1", "Y:1", 100), ("C:1", "X:1", 100)),
    )
    # X-from-A is inserted first, but X-from-C beats it. X-from-C must not inherit X-from-A's position before Y.
    expected = Journey(100, 350, (net.timing(2, 10, 20), Walk("B:1", "Y:1", 100), net.timing(5, 10, 20)))
    assert net.search("O", "D", 0) == [expected]
    assert journey.plan(net, "O", "D", 0, results=10) == [expected]


@pytest.mark.parametrize("budget", [0, 2, journey.WALK_PERMISSION_WORDS])
def test_capped_windows_keep_ties_boundaries_count_and_filter(monkeypatch: pytest.MonkeyPatch, budget: int) -> None:
    monkeypatch.setattr(journey, "WALK_PERMISSION_WORDS", budget)
    monkeypatch.setattr(journey, "WINDOW_EVENTS", 2)
    start = 1000
    departures = [start + offset for offset in (0, 0, 1, 1799, 1800, 1801, 3599, 3600, 28_799, 28_800)]
    net = _network(
        *(
            Trip(key, (Stop("O:1", departure), Stop("D:1", departure + 100)), mode="metro")
            for key, departure in enumerate(departures, start=1)
        )
    )
    expected = [
        Journey(departure, departure + 100, (net.timing(key, 10, 20),))
        for key, departure in enumerate(departures, start=1)
        if key not in {2, 10}  # first tied trip wins; the 8-hour span's upper boundary is excluded
    ]
    assert journey.plan(net, "O", "D", start, results=100) == expected
    assert journey.plan(net, "O", "D", start, results=3) == expected[:3]

    def useful(found: list[Journey]) -> list[Journey]:
        return [result for result in found if _keys(result)[0] % 2]

    assert journey.plan(net, "O", "D", start, results=3, useful=useful) == useful(expected)[:3]


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_three_hour_horizon_is_strict(offset: int) -> None:
    departure = 100
    arrival = departure + journey.MAX_JOURNEY_S + offset
    net = _network(Trip(1, (Stop("O:1", departure), Stop("D:1", arrival)), mode="metro"))
    expected = [Journey(departure, arrival, (net.timing(1, 10, 20),))] if offset < 0 else []
    assert net.search("O", "D", departure) == expected
    assert journey.plan(net, "O", "D", departure, results=10) == expected


def _front_of_searches(
    net: Network, until: int, origin: str | journey.Point = "O", destination: str | journey.Point = "D"
) -> list[tuple[int, int, int]]:
    """(depart, late arrival, vehicles) of every search from each second, less the dominated."""
    found = {(j.depart, j.arrive_late, j.vehicles) for t in range(until) for j in net.search(origin, destination, t)}
    return sorted(j for j in found if not any(o != j and o[0] >= j[0] and o[1] <= j[1] and o[2] <= j[2] for o in found))


def test_profile_plan_drops_journeys_a_later_window_beats() -> None:
    # Timetabled trips arriving together: a search from 100 s keeps the first, which the second beats by leaving
    # later, but the second leaves in the next departure window.
    slow = Trip(1, (Stop("O:1", 100, cumulative=0), Stop("D:1", 4800, cumulative=4700)), mode="metro")
    late = journey.PROFILE_WINDOW_S + 200
    fast = Trip(2, (Stop("O:1", late, cumulative=0), Stop("D:1", 4800, cumulative=4800 - late)), mode="metro")
    planned = journey.plan(_network(slow, fast), "O", "D", 0, results=2)
    assert [_keys(j) for j in planned] == [[2]]


@pytest.mark.parametrize("seed", [11, 512, 2026])
def test_profile_plan_is_the_front_of_searches_from_every_departure(seed: int) -> None:
    rng = random.Random(seed)  # noqa: S311 - reproducible test inputs
    for _ in range(10):
        trips = tuple(
            Trip(
                key,
                tuple(
                    Stop(
                        post,
                        100 + rng.randrange(600) + pos * 100,
                        late=rng.randrange(60),
                        cumulative=pos * rng.randrange(60, 100),
                        can_alight=rng.random() < 0.8,
                    )
                    for pos, post in enumerate(rng.sample(["O:1", "O:2", "A:1", "B:1", "D:1"], 3))
                ),
            )
            for key in range(12)
        )
        walks = (("O:1", "A:1", 30), ("O:2", "B:1", 40), ("B:1", "D:1", 20), ("A:1", "B:1", 50))
        net = _network(*trips, walks=walks)
        planned = journey.plan(net, "O", "D", 0, results=100)
        assert [(j.depart, j.arrive_late, j.vehicles) for j in planned] == _front_of_searches(net, 1000)
        for result in planned:
            _assert_feasible(net, result)


@pytest.mark.parametrize(("far_arrival", "winner"), [(700, 2), (1000, 1)])
def test_point_access_considers_multiple_posts_not_the_nearest_group(far_arrival: int, winner: int) -> None:
    net = _network(
        Trip(1, (Stop("O:1", 500), Stop("D:1", 900))),
        Trip(2, (Stop("A:1", 600), Stop("D:1", far_arrival))),
        posts=(("O:1", 52.0, 21.0), ("A:1", 52.001, 21.0)),
    )
    origin = journey.Point(52, 21)
    walks = net.point_walks(origin, access=True)
    assert set(walks) == {net.stop_index["O:1"], net.stop_index["A:1"]}
    result = net.search(origin, "D", 0)[0]
    assert _keys(result) == [winner]
    access, ride = result.legs[:2]
    assert isinstance(access, Walk)
    assert isinstance(ride, Ride)
    assert result.depart == ride.board_by - access.walk_s
    planned = journey.plan(net, origin, "D", 0, results=10)
    assert winner in {_keys(j)[0] for j in planned}
    assert all(j.depart >= 0 for j in planned)
    assert net.search(origin, "D", result.depart) == [result]
    assert result not in net.search(origin, "D", result.depart + 1)


def test_point_egress_costs_choose_a_later_alighting_post_and_bound_profiles() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 300), Stop("D:1", 500), Stop("D:2", 520))),
        posts=(("O:1", 52, 21), ("D:1", 52.012, 21), ("D:2", 52.01, 21)),
    )
    start, end = journey.Point(52, 21), journey.Point(52.01, 21)
    egress = net.point_walks(end, access=False)
    assert len(egress) == 2
    profile = net.profile(start, end)
    assert profile.to_target[net.stop_index["D:2"]] == 30
    result = Journey(
        270,
        572,
        (
            Walk(journey.ORIGIN_POINT, "O:1", 30, 0),
            Ride(1, 10, 30, 300, 300, 520, 542),
            Walk("D:2", journey.DESTINATION_POINT, 30, 0),
        ),
    )
    assert net.search(start, end, 270) == [result]
    assert journey.plan(net, start, end, 270, results=10) == [result]
    assert journey.plan(net, start, end, 271, results=10) == []
    assert journey.plan(net, "O", end, 300, results=10)[0].arrive_late == 572


@pytest.mark.parametrize(("mode", "seconds"), [("bus", 30), ("metro", 90), ("rail", 90)])
def test_point_walk_station_access_and_minimum_time(mode: str, seconds: int) -> None:
    net = _network(
        Trip(1, (Stop("O:1", 300), Stop("D:1", 500)), mode=mode),
        posts=(("O:1", 52, 21), ("D:1", 52.01, 21)),
    )
    origin, destination = journey.Point(52, 21), journey.Point(52.01, 21)
    result = net.search(origin, destination, 0)[0]
    assert result.legs[0] == Walk(journey.ORIGIN_POINT, "O:1", seconds, 0)
    assert result.legs[-1] == Walk("D:1", journey.DESTINATION_POINT, seconds, 0)
    assert result.depart == 300 - seconds
    ride = result.legs[1]
    assert isinstance(ride, Ride)
    assert ride.arrive_late is not None
    assert result.arrive_late == ride.arrive_late + seconds


@pytest.mark.parametrize("access", [True, False])
def test_point_radius_is_estimated_distance_and_only_served_posts_are_used(*, access: bool) -> None:
    metres_per_degree = 1.3 * 6_371_000 * math.pi / 180
    net = _network(
        Trip(1, (Stop("O:1", 1000), Stop("D:1", 2000))),
        posts=(("O:1", 999 / metres_per_degree, 0), ("D:1", 1001 / metres_per_degree, 0), ("unused", 0, 0)),
    )
    walks = net.point_walks(journey.Point(0, 0), access=access)
    assert list(walks) == [net.stop_index["O:1"]]
    walk = walks[net.stop_index["O:1"]]
    assert walk.distance_m == 999
    assert walk.walk_s == math.ceil(999 / 1.2)
    assert journey.plan(net, journey.Point(0, 0), journey.Point(-10, -10), 0, results=10) == []
    assert journey.plan(net, journey.Point(-10, -10), "D", 0, results=10) == []


def test_point_walks_cannot_chain_artifact_footpaths() -> None:
    net = _network(
        Trip(1, (Stop("B:1", 300), Stop("X:1", 500))),
        Trip(2, (Stop("A:1", 100), Stop("Y:1", 200))),
        walks=(("A:1", "B:1", 30), ("X:1", "Y:1", 30)),
        posts=(("A:1", 52, 21), ("B:1", 52.1, 21), ("X:1", 52.2, 21), ("Y:1", 52.3, 21)),
    )
    assert journey.plan(net, journey.Point(52, 21), "X", 0, results=10) == []
    assert journey.plan(net, "B", journey.Point(52.3, 21), 0, results=10) == []
    assert _keys(journey.plan(net, journey.Point(52.1, 21), journey.Point(52.2, 21), 0, results=10)[0]) == [1]


@pytest.mark.parametrize(("lat", "lon"), [(math.nan, 21), (52, math.inf), (-91, 0), (0, 181)])
def test_direct_point_api_rejects_invalid_coordinates(lat: float, lon: float) -> None:
    with pytest.raises(ValueError, match="invalid geographic coordinates"):
        journey.Point(lat, lon)


def test_point_queries_do_not_leak_into_cached_network(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    with duckdb.connect(str(path)) as connection:
        _write(connection, (Trip(1, (Stop("O:1", 300), Stop("D:1", 500))),))
        connection.execute("create table planner_stop_post (stop_id varchar, lat double, lon double)")
        connection.execute("insert into planner_stop_post values ('O:1', 52, 21), ('D:1', 52.01, 21)")
    net = journey.network(path, DAY)
    stops, footpaths = list(net.stop_ids), [list(walks) for walks in net.footpaths]
    baseline = journey.plan(net, "O", "D", 0, results=10)
    for _ in range(2):
        assert journey.plan(net, journey.Point(52, 21), journey.Point(52.01, 21), 0, results=10)
        assert journey.plan(net, journey.Point(0, 0), "D", 0, results=10) == []
        assert journey.plan(net, "O", "D", 0, results=10) == baseline
        assert journey.network(path, DAY) is net
        assert (net.stop_ids, net.footpaths) == (stops, footpaths)


@pytest.mark.parametrize(("point_origin", "point_destination"), [(True, False), (False, True), (True, True)])
def test_point_profiles_match_independent_searches_from_every_departure(
    point_origin: bool, point_destination: bool
) -> None:
    rng = random.Random(2026)  # noqa: S311 - reproducible test inputs
    posts = (("O:1", 52, 21), ("O:2", 52.001, 21), ("A:1", 52.01, 21),
             ("B:1", 52.02, 21), ("D:1", 52.03, 21), ("D:2", 52.031, 21))  # fmt: skip
    origin = journey.Point(52, 21) if point_origin else "O"
    destination = journey.Point(52.03, 21) if point_destination else "D"
    for _ in range(5):
        trips = tuple(
            Trip(
                key,
                tuple(
                    Stop(
                        post,
                        200 + rng.randrange(600) + pos * 100,
                        late=rng.randrange(60),
                        cumulative=pos * rng.randrange(60, 100),
                        can_alight=rng.random() < 0.8,
                    )
                    for pos, post in enumerate(rng.sample([p[0] for p in posts], 3))
                ),
            )
            for key in range(12)
        )
        net = _network(*trips, walks=(("A:1", "B:1", 50), ("O:1", "A:1", 30), ("B:1", "D:1", 20)), posts=posts)
        planned = journey.plan(net, origin, destination, 0, results=100)
        assert [(j.depart, j.arrive_late, j.vehicles) for j in planned] == _front_of_searches(
            net, 1100, origin, destination
        )
        for result in planned:
            _assert_feasible(net, result)
