# ruff: noqa: PLR2004 - literal keys, counts and timeouts are behavioral expectations
from __future__ import annotations

import gc
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from threading import Barrier, Event
from typing import TYPE_CHECKING

import pytest

from ztm_frontend import route_cache
from ztm_frontend.journey import Journey, Network, Point, Ride

if TYPE_CHECKING:
    from ztm_frontend.route_cache import Key

RESULT = Journey(100, 200, (Ride(1, 0, 1, 100, 100, 190, 200),))


def _useful(found: list[Journey]) -> list[Journey]:
    return found


def _key(after: int = 0) -> Key:
    return "O", "D", after, 8, _useful


@pytest.fixture
def net() -> Network:
    # The cache uses identity only, so unit tests need no routing columns or DuckDB artifact.
    return Network.__new__(Network)


@pytest.fixture(autouse=True)
def _isolated_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(route_cache, "_caches", weakref.WeakKeyDictionary())


def test_results_are_fresh_lists_of_frozen_values(net: Network) -> None:
    original = [RESULT]
    first = route_cache.get(net, _key(), lambda: original)
    original.clear()
    second = route_cache.get(net, _key(), lambda: pytest.fail("cache miss"))
    assert first == second == [RESULT]
    assert first is not second
    first.clear()
    assert route_cache.get(net, _key(), lambda: pytest.fail("cache miss")) == [RESULT]
    with pytest.raises(FrozenInstanceError):
        setattr(second[0], "depart", 0)  # noqa: B010 - exercise the frozen value contract


def test_none_network_stub_bypasses_caching() -> None:
    assert route_cache.get(None, _key(), lambda: [RESULT]) == [RESULT]
    assert route_cache.get(None, _key(), list) == []


def test_empty_results_are_cached(net: Network) -> None:
    assert route_cache.get(net, _key(), list) == []
    assert route_cache.get(net, _key(), lambda: pytest.fail("empty result not cached")) == []


def test_completed_lru_is_bounded_and_hits_refresh_recency(net: Network) -> None:
    for after in range(route_cache.COMPLETED_LIMIT):
        route_cache.get(net, _key(after), list)
    route_cache.get(net, _key(), lambda: pytest.fail("cache miss"))
    route_cache.get(net, _key(route_cache.COMPLETED_LIMIT), list)
    assert route_cache.get(net, _key(), lambda: pytest.fail("recent entry evicted")) == []
    assert route_cache.get(net, _key(1), lambda: [RESULT]) == [RESULT]
    assert len(route_cache._caches[net].completed) == route_cache.COMPLETED_LIMIT  # noqa: SLF001


@pytest.mark.parametrize(
    "key",
    [
        ("other", "D", 0, 8, _useful),
        ("O", "other", 0, 8, _useful),
        ("O", "D", 1, 8, _useful),
        ("O", "D", 0, 9, _useful),
        ("O", "D", 0, 8, lambda found: found),
        (Point(52, 21), "D", 0, 8, _useful),
        ("O", Point(52, 21), 0, 8, _useful),
    ],
)
def test_routing_parameters_are_distinct_keys(net: Network, key: Key) -> None:
    route_cache.get(net, _key(), list)
    assert route_cache.get(net, key, lambda: [RESULT]) == [RESULT]


def test_coordinates_are_exact_but_point_names_do_not_affect_keys(net: Network) -> None:
    key = (Point(52, 21, "old"), Point(53, 22, "end"), 0, 8, _useful)
    route_cache.get(net, key, lambda: [RESULT])
    renamed = (Point(52, 21, "new"), Point(53, 22, "renamed end"), 0, 8, _useful)
    assert route_cache.get(net, renamed, lambda: pytest.fail("names changed key")) == [RESULT]
    for different in (
        (Point(52.000000001, 21), key[1], 0, 8, _useful),
        (key[0], Point(53, 22.000000001), 0, 8, _useful),
    ):
        assert route_cache.get(net, different, list) == []


def test_network_identity_scopes_entries(net: Network) -> None:
    other = Network.__new__(Network)
    route_cache.get(net, _key(), lambda: [RESULT])
    assert route_cache.get(other, _key(), list) == []
    assert route_cache.get(net, _key(), lambda: pytest.fail("original entry lost")) == [RESULT]


def test_completed_cache_does_not_pin_network_or_compute_closure() -> None:
    net = Network.__new__(Network)

    def compute(captured: Network = net) -> list[Journey]:
        assert captured is not None
        return [RESULT]

    reference, closure = weakref.ref(net), weakref.ref(compute)
    route_cache.get(net, _key(), compute)
    del net, compute
    gc.collect()
    assert closure() is None
    assert reference() is None
    assert len(route_cache._caches) == 0  # noqa: SLF001


@pytest.mark.parametrize("error_type", [None, ValueError, KeyboardInterrupt])
def test_identical_requests_share_owner_and_failure_but_do_not_block_other_keys(
    net: Network, monkeypatch: pytest.MonkeyPatch, error_type: type[BaseException] | None
) -> None:
    entered, release = Event(), Event()
    waiting = Barrier(4)
    calls = 0

    def compute() -> list[Journey]:
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(5)
        if error_type is not None:
            raise error_type("transient")
        return [RESULT]

    with ThreadPoolExecutor(max_workers=5) as executor:
        owner = executor.submit(route_cache.get, net, _key(), compute)
        try:
            assert entered.wait(5)
            flight = route_cache._caches[net].inflight[_key()]  # noqa: SLF001
            original_result = flight.result.result

            def wait_for_result(timeout: float | None = None) -> tuple[Journey, ...]:
                waiting.wait(5)
                return original_result(timeout)

            monkeypatch.setattr(flight.result, "result", wait_for_result)
            waiters = [executor.submit(route_cache.get, net, _key(), compute) for _ in range(3)]
            waiting.wait(5)
            independent = executor.submit(route_cache.get, net, _key(1), lambda: [RESULT])
            assert independent.result(5) == [RESULT]
        finally:
            release.set()
        if error_type is None:
            results = [future.result(5) for future in [owner, *waiters]]
            assert results == [[RESULT]] * 4
            assert len({id(result) for result in results}) == 4
        else:
            for future in [owner, *waiters]:
                with pytest.raises(error_type, match="transient"):
                    future.result(5)
            assert route_cache.get(net, _key(), lambda: [RESULT]) == [RESULT]
        assert calls == 1
        assert not route_cache._caches[net].inflight  # noqa: SLF001


def test_full_inflight_cache_bypasses_distinct_requests_without_storing_them(
    net: Network, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(route_cache, "INFLIGHT_LIMIT", 2)
    entered, release = Barrier(3), Event()

    def compute() -> list[Journey]:
        entered.wait(5)
        assert release.wait(5)
        return [RESULT]

    with ThreadPoolExecutor(max_workers=3) as executor:
        owners = [executor.submit(route_cache.get, net, _key(after), compute) for after in range(2)]
        try:
            entered.wait(5)
            assert len(route_cache._caches[net].inflight) == 2  # noqa: SLF001
            bypass = executor.submit(route_cache.get, net, _key(2), lambda: [RESULT])
            assert bypass.result(5) == [RESULT]
            assert route_cache.get(net, _key(2), list) == []
            assert _key(2) not in route_cache._caches[net].completed  # noqa: SLF001
        finally:
            release.set()
        assert [future.result(5) for future in owners] == [[RESULT], [RESULT]]
    assert route_cache.get(net, _key(2), lambda: [RESULT]) == [RESULT]


def test_recursive_same_key_raises_and_cleans_up_for_retry(net: Network) -> None:
    with pytest.raises(RuntimeError, match="recursive"):
        route_cache.get(net, _key(), lambda: route_cache.get(net, _key(), list))
    assert not route_cache._caches[net].inflight  # noqa: SLF001
    assert route_cache.get(net, _key(), lambda: [RESULT]) == [RESULT]


def test_recursive_different_key_is_allowed(net: Network) -> None:
    assert route_cache.get(net, _key(), lambda: route_cache.get(net, _key(1), lambda: [RESULT])) == [RESULT]
