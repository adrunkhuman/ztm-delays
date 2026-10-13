# ruff: noqa: PLR2004, SLF001 - exact timing boundaries and private index contracts
from __future__ import annotations

from array import array
from typing import TYPE_CHECKING

import pytest

from tests.test_journey import Stop, Trip, _network
from ztm_frontend import journey

if TYPE_CHECKING:
    from collections.abc import Callable

    from ztm_frontend.journey import Journey, Network


def _reference_boarding(net: Network, pattern: int, pos: int) -> tuple[int, int, array, array]:
    ordered = sorted(
        (net.board[net.trip_rows[t] + pos], t)
        for t in net.pattern_trips[pattern]
        if net.board[net.trip_rows[t] + pos] < journey.NO_BOARD
    )
    return pattern, pos, array("i", (b for b, _ in ordered)), array("i", (t for _, t in ordered))


def _complete_departures(profile: journey._Profile, start: int, end: int, _limit: int) -> list[tuple[int, set[int]]]:
    """Use the original full enumeration; plan still applies its original cap and cutoff."""
    return profile.departures(start, end)


def test_boarding_integer_sort_preserves_unsorted_times_duplicates_and_prohibition() -> None:
    net = _network(
        Trip(40, (Stop("O:1", 100), Stop("X:1", 300), Stop("D:1", 500))),
        Trip(10, (Stop("O:1", 200), Stop("X:1", 250), Stop("D:1", 500))),
        Trip(30, (Stop("O:1", 100), Stop("X:1", 250), Stop("D:1", 500))),
        Trip(20, (Stop("O:1", 50, margin=None), Stop("X:1", 400, margin=None), Stop("D:1", 500))),
    )
    assert list(net.trip_keys) == [10, 20, 30, 40]
    assert isinstance(net.pattern_row_starts, tuple)
    for pattern, trips in enumerate(net.pattern_trips):
        assert trips == sorted(trips)
        assert net.pattern_row_starts[pattern] == tuple(net.trip_rows[t] for t in trips)
    for entries in net.incidence:
        for pattern, pos, times, trips in entries:
            assert (pattern, pos, times, trips) == _reference_boarding(net, pattern, pos)
    assert list(net.incidence[net.stop_index["O:1"]][0][2]) == [100, 100, 200]
    assert list(net.incidence[net.stop_index["O:1"]][0][3]) == [2, 3, 0]
    assert list(net.incidence[net.stop_index["X:1"]][0][3]) == [0, 2, 3]
    tied = _network(
        Trip(40, (Stop("O:1", 100), Stop("D:1", 200)), mode="metro"),
        Trip(30, (Stop("O:1", 100), Stop("D:1", 200)), mode="metro"),
    )
    assert journey.plan(tied, "O", "D", 100, 1) == [journey.Journey(100, 200, (tied.timing(30, 10, 20),))]


def _tie_network() -> Network:
    return _network(
        *(
            Trip(i, (Stop("O:1", t), Stop("D:1", t + 100)), mode="metro")
            for i, t in enumerate((99, 100, 100, 101, 102, 103), start=1)
        ),
        Trip(10, (Stop("O:2", 100), Stop("D:2", 200)), mode="metro"),
        Trip(11, (Stop("X:1", 120), Stop("D:1", 220)), mode="metro"),
        Trip(12, (Stop("X:1", 120), Stop("D:1", 220)), mode="metro"),
        Trip(13, (Stop("X:2", 130), Stop("D:2", 230)), mode="metro"),
        # Walking downstream to X:3 is forbidden even though its board time would create an event at 100.
        Trip(14, (Stop("O:1", 1000), Stop("X:3", 120), Stop("D:1", 200)), mode="metro"),
        walks=(("O:1", "X:1", 20), ("O:2", "X:2", 30), ("O:1", "X:3", 20)),
    )


@pytest.mark.parametrize("limit", [0, 1, 2, 3, 10, journey.WINDOW_EVENTS + 1])
@pytest.mark.parametrize(("start", "end"), [(99, 104), (100, 103), (101, 102), (104, 104), (2000, 3000)])
def test_capped_events_union_all_streams_at_exact_cutoff(limit: int, start: int, end: int) -> None:
    net = _tie_network()
    profile = net.profile("O", "D")
    full = profile.departures(start, end)
    assert profile.capped_departures(start, end, limit) == (full[-limit:] if limit else [])
    if start <= 100 < end:
        assert dict(full)[100] == {net.stop_index[s] for s in ("O:1", "O:2", "X:1", "X:2")}
    assert all(net.stop_index["X:3"] not in posts for _, posts in full)
    assert profile.departures(start, end) == full  # complete enumeration is unchanged after the capped call


def test_point_access_offsets_and_no_chained_origin_walks() -> None:
    net = _network(
        Trip(1, (Stop("A:1", 130), Stop("D:1", 500)), mode="bus"),
        Trip(2, (Stop("B:1", 190), Stop("D:2", 500)), mode="metro"),
        Trip(3, (Stop("X:1", 150), Stop("D:1", 250)), mode="metro"),
        posts=(("A:1", 52.0, 21.0), ("B:1", 52.0, 21.0)),
        walks=(("A:1", "X:1", 50),),
    )
    profile = net.profile(journey.Point(52.0, 21.0), "D")
    assert {net.stop_ids[s]: walk.walk_s for s, walk in profile.access.items()} == {"A:1": 30, "B:1": 90}
    expected = [(100, {net.stop_index["A:1"], net.stop_index["B:1"]})]
    assert profile.departures(100, 101) == expected
    assert profile.capped_departures(100, 101, 1) == expected
    assert profile.capped_departures(0, 100, 1) == []
    assert profile.capped_departures(101, 200, 1) == []


def test_redundant_origin_walks_still_contribute_window_events() -> None:
    net = _network(
        Trip(1, (Stop("O:1", 1000), Stop("D:1", 1100)), mode="metro"),
        Trip(2, (Stop("X:1", 130), Stop("D:1", 230)), mode="metro"),
        walks=(("O:1", "X:1", 20), ("O:1", "X:1", 30), ("O:1", "X:1", 30)),
    )
    profile = net.profile("O", "D")
    assert profile.origin_walks == [(net.stop_index["O:1"], net.stop_index["X:1"], 20)]
    expected = [(110, {net.stop_index["X:1"]}), (100, {net.stop_index["X:1"]})]
    assert profile.departures(0, 200) == expected
    assert profile.capped_departures(0, 200, 2) == expected


@pytest.mark.parametrize("cap", [1, 2, journey.WINDOW_EVENTS])
@pytest.mark.parametrize("point_origin", [False, True])
def test_plan_multiple_windows_matches_complete_enumeration_and_reuses_only_streams(
    monkeypatch: pytest.MonkeyPatch, cap: int, *, point_origin: bool
) -> None:
    monkeypatch.setattr(journey, "WINDOW_EVENTS", cap)
    after = journey.DAY_SECONDS - 30
    offset = 90 if point_origin else 0
    departures = [after + delta for delta in (0, 0, 1, 1799, 1800, 1801, 3599, 3600, 28_799, 28_800)]
    net = _network(
        *(
            Trip(key, (Stop("O:1", t + offset), Stop("D:1", t + offset + 100)), mode="metro")
            for key, t in enumerate(departures, start=1)
        ),
        posts=(("O:1", 52.0, 21.0),),
    )
    origin = journey.Point(52.0, 21.0) if point_origin else "O"

    def useful(found: list[Journey]) -> list[Journey]:
        return [j for j in found if isinstance(j.legs[-1], journey.Ride) and j.legs[-1].trip_key % 2]

    real_run = journey._Profile.run
    runs: list[tuple[journey._Profile, int, set[int] | None]] = []

    def record(profile: journey._Profile, when: int, boarding: set[int] | None = None) -> list[journey._Label]:
        runs.append((profile, when, boarding))
        return real_run(profile, when, boarding)

    monkeypatch.setattr(journey._Profile, "run", record)
    actual = journey.plan(net, origin, "D", after, 8, useful=useful)
    optimized_runs = list(runs)
    profiles = list(dict.fromkeys(p for p, _, _ in runs))
    assert len(profiles) > 1
    assert all(p._departure_streams is profiles[0]._departure_streams for p in profiles)
    assert len({id(p.state) for p in profiles}) == len(profiles)
    assert all(p.query is profiles[0].query for p in profiles)
    for profile in profiles:
        window = [(t, b) for p, t, b in runs if p is profile]
        assert window[0][1] is None
        assert all(b is not None for _, b in window[1:])
        assert [t for t, _ in window] == sorted((t for t, _ in window), reverse=True)
        assert len(window) <= cap
    runs.clear()
    monkeypatch.setattr(journey._Profile, "capped_departures", _complete_departures)
    assert journey.plan(net, origin, "D", after, 8, useful=useful) == actual
    assert [(t, b) for _, t, b in runs] == [(t, b) for _, t, b in optimized_runs]
    # Force horizon exhaustion: the event at exactly after + 8h must never run.
    assert max(t for _, t, _ in runs) == after + 28_799
    assert [j.depart for j in actual] == [departures[i - 1] for i in (1, 3, 5, 7, 9)]


def test_eight_filtered_candidates_and_departure_pagination_match_complete_enumeration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(journey, "WINDOW_EVENTS", 2)
    net = _network(
        *(
            Trip(key, (Stop("O:1", 1000 + key * 901), Stop("D:1", 1100 + key * 901)), mode="metro")
            for key in range(1, 26)
        )
    )

    def useful(found: list[Journey]) -> list[Journey]:
        return [j for j in found if isinstance(j.legs[0], journey.Ride) and j.legs[0].trip_key % 2]

    first = journey.plan(net, "O", "D", 1000, 8, useful=useful)
    assert len(first) == 8
    after = first[-1].depart + 1
    second = journey.plan(net, "O", "D", after, 8, useful=useful)
    assert first + second == [
        journey.Journey(1000 + key * 901, 1100 + key * 901, (net.timing(key, 10, 20),)) for key in range(1, 26, 2)
    ]
    monkeypatch.setattr(journey._Profile, "capped_departures", _complete_departures)
    assert journey.plan(net, "O", "D", 1000, 8, useful=useful) == first
    assert journey.plan(net, "O", "D", after, 8, useful=useful) == second


def test_live_patched_indexes_streams_and_prepared_networks_leave_original_immutable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    net = _tie_network()
    before_columns = [array("i", getattr(net, name)) for name in ("board", "expected", "depart", "late_base")]
    before_incidence = [
        [(p, pos, array("i", times), array("i", trips)) for p, pos, times, trips in entries]
        for entries in net.incidence
    ]
    profile = net.profile("O", "D")
    before_events = profile.capped_departures(0, 2000, 100)
    before_plan = journey.plan(net, "O", "D", 0, 8)
    row = net.trip_index[2][0]
    patched = net.patched({row: (journey.NO_BOARD, 120, 140), net.trip_index[3][0]: (80, 100, 120)})
    assert patched.pattern_row_starts is net.pattern_row_starts
    assert patched.incidence[net.stop_index["X:1"]] is net.incidence[net.stop_index["X:1"]]
    assert patched.incidence[net.stop_index["O:1"]] is not net.incidence[net.stop_index["O:1"]]
    for entries in patched.incidence:
        for p, pos, times, trips in entries:
            assert (p, pos, times, trips) == _reference_boarding(patched, p, pos)
    patched_profile = patched.profile("O", "D")
    assert patched_profile.capped_departures(0, 2000, 100) == patched_profile.departures(0, 2000)
    assert patched_profile._departure_streams is not profile._departure_streams
    assert journey._prepared_network(patched) is not journey._prepared_network(net)
    optimized = journey.plan(patched, "O", "D", 0, 8)
    with monkeypatch.context() as patch:
        patch.setattr(journey._Profile, "capped_departures", _complete_departures)
        patch.setattr(journey.Network, "_boarding", _reference_boarding)
        reference = net.patched({row: (journey.NO_BOARD, 120, 140), net.trip_index[3][0]: (80, 100, 120)})
        assert journey.plan(reference, "O", "D", 0, 8) == optimized
    assert before_columns == [getattr(net, name) for name in ("board", "expected", "depart", "late_base")]
    assert net.incidence == before_incidence
    assert profile.capped_departures(0, 2000, 100) == before_events
    assert journey.plan(net, "O", "D", 0, 8) == before_plan


def test_capped_enumeration_does_not_scan_the_rest_of_a_dense_window(monkeypatch: pytest.MonkeyPatch) -> None:
    net = _network(Trip(1, (Stop("O:1", 0), Stop("D:1", 100)), mode="metro"))
    stop = net.stop_index["O:1"]
    pattern, pos, _, _ = net.incidence[stop][0]
    net.incidence[stop][0] = (pattern, pos, array("i", range(28_800)), array("i", [0] * 28_800))
    profile = net.profile("O", "D")
    popped = 0
    original: Callable = journey.heapq.heappop

    def pop(heap: list) -> tuple:
        nonlocal popped
        popped += 1
        return original(heap)

    monkeypatch.setattr(journey.heapq, "heappop", pop)
    assert profile.capped_departures(0, 28_800, 3) == [(t, {stop}) for t in (2, 1, 0)]
    assert popped == 3
