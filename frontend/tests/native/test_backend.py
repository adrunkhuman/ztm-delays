"""Native ownership, failure boundaries, and ordinary threaded requests (no backend switch)."""

# ruff: noqa: SLF001, PLR2004
from __future__ import annotations

import gc
import math
import subprocess
import sys
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.test_journey import DAY, Stop, Trip, _network
from tests.test_planner import _client
from ztm_frontend import journey
from ztm_frontend.routing import NativeState, PreparedNet, PreparedQuery

if TYPE_CHECKING:
    from typing import NoReturn

    from werkzeug.test import TestResponse


def small() -> journey.Network:
    return _network(Trip(1, (Stop("O:1", 100), Stop("D:1", 200))))


@pytest.mark.parametrize(
    "damage",
    [
        "short_column",
        "nonfinite_duration",
        "bad_cell",
        "bad_trip_row",
        "bad_incidence_trip",
        "bad_incidence_lengths",
        "bad_ratio",
        "bad_pattern_stop",
        "bad_footpath",
        "bad_adjacency",
    ],
)
def test_invalid_network_rejected(damage: str) -> None:  # noqa: C901 - one case per native boundary
    net = small()
    if damage == "short_column":
        net.late_base.pop()
    elif damage == "nonfinite_duration":
        net.cumulative[1] = math.nan
    elif damage == "bad_cell":
        net.range_ids[0] = 99
    elif damage == "bad_trip_row":
        net.trip_rows[0] = 2**31 - 1
    elif damage == "bad_incidence_trip":
        net.incidence[net.stop_index["O:1"]][0][3][0] = 99
    elif damage == "bad_incidence_lengths":
        net.incidence[net.stop_index["O:1"]][0][3].pop()
    elif damage == "bad_ratio":
        net.envelopes[0].ratio[0] = math.inf
    elif damage == "bad_pattern_stop":
        net.patterns[0][0] = -1
    elif damage == "bad_footpath":
        net.footpaths[0].append((0, -1))
    elif damage == "bad_adjacency":
        net.incidence.pop()
    with pytest.raises((ValueError, IndexError)):
        PreparedNet(net)


def test_late_cast_overflow_and_invalid_index_rejected() -> None:
    net = small()
    net.cumulative[1] = 1e300
    prepared = PreparedNet(net)
    with pytest.raises(OverflowError):
        prepared.late(0, 1)
    with pytest.raises(IndexError):
        prepared.late(-1, 1)


@pytest.mark.parametrize(("after", "error"), [(2**40, ValueError), (2**100, OverflowError)])
def test_unsupported_request_time_rejected(after: int, error: type[Exception]) -> None:
    profile = small().profile("O", "D")
    with pytest.raises(error):
        profile.run(after)
    assert profile.state is not None
    assert profile.state.stats()["live_labels_after_return"] == 0


def test_native_exception_clears_labels_and_invalidates_partial_bounds() -> None:
    net = small()
    profile = net.profile("O", "D")
    profile.origin_walks = [(net.stop_index["O:1"], net.stop_index["D:1"], 2**63 - 1)]
    with pytest.raises(OverflowError, match="overflow"):
        profile.run(100)
    assert profile.state is not None
    assert profile.state.stats()["live_labels_after_return"] == 0
    with pytest.raises(RuntimeError, match="cannot be reused"):
        profile.run(0)
    # A fresh ordinary request is unaffected by the failed window.
    assert net.search("O", "D", 0)


@pytest.mark.parametrize("error", [RuntimeError, MemoryError])
def test_materialization_failure_clears_labels(monkeypatch: pytest.MonkeyPatch, error: type[Exception]) -> None:
    profile = small().profile("O", "D")

    def fail(*_args: object) -> NoReturn:
        raise error("materialization failed")

    with monkeypatch.context() as patch:
        patch.setattr(journey, "_Label", fail)
        with pytest.raises(error, match="materialization failed"):
            profile.run(0)
    assert profile.state is not None
    stats = profile.state.stats()
    assert stats["peak_labels"] > 0
    assert stats["live_labels_after_return"] == 0
    with pytest.raises(RuntimeError, match="cannot be reused"):
        profile.run(0)


def test_labels_are_independent_of_later_runs_and_native_owners() -> None:
    net = small()
    profile = net.profile("O", "D")
    labels = profile.run(100)
    expected = net.search("O", "D", 100)
    assert profile.state is not None
    assert profile.query is not None
    for after in range(99, -1, -1):
        assert profile.run(after) == []
        assert profile.state.stats()["live_labels_after_return"] == 0
    refs = [weakref.ref(owner) for owner in (profile, profile.state, profile.query, profile.query.network)]
    del profile
    gc.collect()
    assert all(ref() is None for ref in refs[:3])
    # The weak network cache retains prepared metadata only while the Network lives.
    assert refs[3]() is not None
    assert [journey._journey(net, label) for label in labels] == expected
    net_ref = weakref.ref(net)
    del net
    gc.collect()
    assert net_ref() is None
    assert refs[3]() is None
    assert labels[0].previous is not None


def test_prepared_network_copies_inputs_and_patched_network_gets_own_metadata() -> None:
    net = small()
    original = journey._prepared_network(net)
    before = original.late(0, 1)
    patched = net.patched({0: (300, 300, 300)})
    replacement = journey._prepared_network(patched)
    assert replacement is not original
    assert original.late(0, 1) == before
    assert replacement.late(0, 1) > before
    assert journey._prepared_network(net) is original
    net.late_base[0] += 100  # Deliberate unsupported input mutation proves the native copy owns its data.
    assert original.late(0, 1) == before
    assert PreparedNet(net).late(0, 1) > before


def test_query_shared_only_across_its_own_windows_and_state_released() -> None:
    net = small()
    first = net.profile("O", "D")
    assert first.run(0)
    assert first.query is not None
    assert first.state is not None
    second = journey._Profile(net, first.origins, first.targets, first.to_target, query=first.query)
    assert second.run(0)
    assert second.state is not None
    other = net.profile("O", "D")
    assert other.run(0)
    assert other.query is not None
    assert second.query is first.query
    assert second.state is not first.state
    assert other.query is not first.query
    assert other.query.network is first.query.network
    old = weakref.ref(first.state)
    del first
    gc.collect()
    assert old() is None
    assert second.state.stats()["live_labels_after_return"] == 0


@pytest.mark.parametrize("bounds", [[math.nan, 0], [-1, 0], [0]])
def test_invalid_query_bounds_rejected(bounds: list[float]) -> None:
    with pytest.raises(ValueError, match="target bound"):
        PreparedQuery(PreparedNet(small()), bounds, 0)


def test_null_native_owners_and_negative_budget_rejected() -> None:
    with pytest.raises(TypeError):
        PreparedQuery(None, [], 0)  # ty: ignore[invalid-argument-type] - exercise the native boundary
    with pytest.raises(TypeError):
        NativeState(None, None)  # ty: ignore[invalid-argument-type] - exercise the native boundary
    with pytest.raises(OverflowError):
        PreparedQuery(PreparedNet(small()), [0, 0], -1)


@pytest.mark.parametrize("budget", [0, 1, 2, 3, journey.WALK_PERMISSION_WORDS])
def test_permission_cache_multiword_budget_preserves_results(monkeypatch: pytest.MonkeyPatch, budget: int) -> None:
    # More than 64 incidences at X requires a two-word mask for each transfer origin.
    net = _network(
        Trip(100, (Stop("O:1", 100), Stop("A:1", 200)), mode="metro"),
        Trip(101, (Stop("O:1", 100), Stop("B:1", 200)), mode="metro"),
        *(Trip(key, (Stop("X:1", 400), Stop(f"D:{key}", 500)), mode="metro") for key in range(65)),
        walks=(("A:1", "X:1", 100), ("B:1", "X:1", 100)),
    )
    expected = net.search("O", "D", 0)
    monkeypatch.setattr(journey, "WALK_PERMISSION_WORDS", budget)
    profile = net.profile("O", "D")
    assert [journey._journey(net, label) for label in profile.run(0)] == expected
    assert profile.query is not None
    peak = profile.query.peak_permission_words()
    assert peak <= budget
    assert peak >= 2 if budget >= 2 else peak == 0


def test_ieee_late_matches_python_at_fractional_and_bucket_boundaries() -> None:
    values = sorted(
        {
            0.0,
            0.001,
            0.1,
            1.0,
            10.0,
            100.0,
            300.0,
            900.0,
            1000.0,
            *(
                x
                for base in (1 / 1.1, 10 / 1.1, 100 / 1.1, 300.0, 900.0)
                for x in (math.nextafter(base, -math.inf), base, math.nextafter(base, math.inf))
            ),
        }
    )
    net = _network(
        Trip(1, tuple(Stop(f"S:{i}", 100 + i, cumulative=value) for i, value in enumerate(values))),
        ranges=((False, True, 0, 0.0, 300.0, 1.2), (False, True, 0, 300.0, 900.0, 1.05)),
    )
    prepared = PreparedNet(net)
    for board in range(len(values)):
        for alight in range(board, len(values)):
            assert prepared.late(board, alight) == net._late(board, alight)


def test_four_threads_prepare_one_network_and_keep_query_state_private() -> None:
    net = small()
    barrier = threading.Barrier(4)

    def request(_: int) -> tuple[journey._Profile, list[journey._Label]]:
        barrier.wait(timeout=10)
        profile = net.profile("O", "D")
        labels = profile.run(0)
        return profile, labels

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(request, range(4)))
    profiles = [profile for profile, _ in responses]
    assert all(labels == responses[0][1] for _, labels in responses)
    assert all(profile.query is not None for profile in profiles)
    assert len({id(profile.query.network) for profile in profiles if profile.query is not None}) == 1
    assert len({id(profile.query) for profile in profiles}) == 4
    assert len({id(profile.state) for profile in profiles}) == 4


def test_shared_query_windows_and_getters_are_safe_across_threads() -> None:
    # Transfers exercise shared multiword permission masks, not just immutable metadata.
    net = _network(
        Trip(100, (Stop("O:1", 100), Stop("A:1", 200)), mode="metro"),
        *(Trip(key, (Stop("X:1", 400), Stop(f"D:{key}", 500)), mode="metro") for key in range(65)),
        walks=(("A:1", "X:1", 100),),
    )
    expected = net.search("O", "D", 0)
    template = net.profile("O", "D")
    query = PreparedQuery(journey._prepared_network(net), template.to_target, journey.WALK_PERMISSION_WORDS)
    barrier = threading.Barrier(4)

    def window(_: int) -> list[journey.Journey]:
        profile = journey._Profile(net, template.origins, template.targets, template.to_target, query=query)
        barrier.wait(timeout=10)
        labels = profile.run(0)
        assert profile.state is not None
        assert profile.state.stats()["live_labels_after_return"] == 0
        assert 2 <= query.peak_permission_words() <= journey.WALK_PERMISSION_WORDS
        return [journey._journey(net, label) for label in labels]

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert all(result == expected for result in pool.map(window, range(4)))


def test_four_threads_ordinary_planner_requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _client(tmp_path, monkeypatch).application
    path = tmp_path / "planner" / "planner.duckdb"
    net = journey.network(path, DAY)
    query = "/planner?from=1001&to=4004&date=2026-09-23&time=07:00"

    # Create no shadow, backend switch, or shared Flask request context.
    def request(_: int) -> TestResponse:
        with app.test_client() as client:
            return client.get(query, headers={"Accept-Language": "en"})

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(request, range(16)))
    assert all(response.status_code == 200 for response in responses)
    assert all(b"110" in response.data and b"M1" in response.data for response in responses)
    assert journey.network(path, DAY) is net
    assert net in journey._prepared_networks


def test_missing_extension_is_a_hard_import_failure() -> None:
    script = """
import sys
from importlib.abc import MetaPathFinder
class MissingNative(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'ztm_routing._native':
            raise ModuleNotFoundError('native deliberately unavailable')
sys.meta_path.insert(0, MissingNative())
import ztm_frontend.journey
"""
    result = subprocess.run(  # noqa: S603 - fixed interpreter and test-owned script
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "native deliberately unavailable" in result.stderr
