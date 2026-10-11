"""Bounded raw journeys and duplicate-request coalescing, scoped to network identity."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary

from ztm_frontend.journey import Journey, Network, Point

COMPLETED_LIMIT = 128
INFLIGHT_LIMIT = 16

type Key = tuple[str | Point, str | Point, int, int, Callable[[list[Journey]], list[Journey]]]


@dataclass
class _Flight:
    owner: int
    result: Future[tuple[Journey, ...]] = field(default_factory=Future)


@dataclass
class _Cache:
    completed: OrderedDict[Key, tuple[Journey, ...]] = field(default_factory=OrderedDict)
    inflight: dict[Key, _Flight] = field(default_factory=dict)


_caches: WeakKeyDictionary[Network, _Cache] = WeakKeyDictionary()
_lock = threading.Lock()


def get(net: Network | None, key: Key, compute: Callable[[], list[Journey]]) -> list[Journey]:  # noqa: C901
    """Return a fresh list; identical in-flight calls share results, never failures on later calls.

    Network identity scopes build, path, day and live patches. Stored values hold neither networks nor compute
    closures. Distinct requests bypass a full in-flight cache rather than waiting for unrelated searches.
    A None network sentinel (used by router stubs) bypasses caching.
    """
    if net is None:
        return list(compute())
    with _lock:
        cache = _caches.get(net)
        if cache is None:
            cache = _caches[net] = _Cache()
        if key in cache.completed:
            cache.completed.move_to_end(key)
            return list(cache.completed[key])
        flight = cache.inflight.get(key)
        owner = flight is None
        if flight is not None and flight.owner == threading.get_ident():
            raise RuntimeError("recursive route-cache request for the same key")
        if owner and len(cache.inflight) < INFLIGHT_LIMIT:
            flight = cache.inflight[key] = _Flight(threading.get_ident())

    if flight is None:
        return list(compute())
    if not owner:
        return list(flight.result.result())

    try:
        results = tuple(compute())
    except BaseException as error:
        # Remove before publishing the failure: future callers may retry, existing waiters still see this error.
        with _lock:
            del cache.inflight[key]
        flight.result.set_exception(error)
        raise
    else:
        with _lock:
            cache.completed[key] = results
            while len(cache.completed) > COMPLETED_LIMIT:
                cache.completed.popitem(last=False)
            del cache.inflight[key]
        flight.result.set_result(results)
        return list(results)
