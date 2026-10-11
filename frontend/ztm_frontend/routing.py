"""Translate frontend objects to the required Rust engine's primitive boundary.

Native owners never retain a Python Network or Profile. Returned paths alone are
turned into Python labels after the Rust search has finished and reacquired the GIL.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import ztm_routing

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ztm_frontend.journey import Network, _Label, _Profile


class PreparedNet:
    """Immutable native copy of a service-day network; eligible for weak references."""

    def __init__(self, net: Network) -> None:
        """Copy columns and replace frontend envelopes with primitive sequences."""
        self._native = ztm_routing.PreparedNet(
            len(net.stop_ids),
            net.late_base,
            net.expected,
            net.depart,
            net.range_ids,
            net.trip_rows,
            net.trip_pattern,
            net.cumulative,
            net.patterns,
            net.pattern_alights,
            net.pattern_prefix,
            net.incidence,
            net.footpaths,
            [(envelope.upper, envelope.ratio, envelope.floor) for envelope in net.envelopes],
        )

    def payload_bytes(self) -> int:
        """Return the native network's owned payload size."""
        return self._native.payload_bytes()

    def late(self, board: int, alight: int) -> int:
        """Return the conservative arrival bound for a ride."""
        return self._native.late(board, alight)


class PreparedQuery:
    """Own the network and native permission cache shared by a search's windows."""

    def __init__(self, network: PreparedNet, to_target: list[float], permission_words: int) -> None:
        """Pass bounds and the unsigned permission budget through to Rust validation."""
        if not isinstance(network, PreparedNet):
            raise TypeError("network must be a PreparedNet")
        self._network = network
        self._native = ztm_routing.PreparedQuery(network._native, to_target, permission_words)  # noqa: SLF001

    @property
    def network(self) -> PreparedNet:
        """Keep prepared metadata alive without retaining its Python source network."""
        return self._network

    def peak_permission_words(self) -> int:
        """Return the permission cache's high-water mark."""
        return self._native.peak_permission_words()


class NativeState:
    """Own one mutable departure window and materialize its returned paths."""

    def __init__(self, query: PreparedQuery, profile: _Profile) -> None:
        """Copy endpoint metadata, preserving insertion order at the native boundary."""
        from ztm_frontend import journey  # noqa: PLC0415 - journey imports this facade

        if not isinstance(query, PreparedQuery):
            raise TypeError("query must be a PreparedQuery")
        if journey.MAX_VEHICLES != 5 or journey.INF != 2**31 - 1:  # noqa: PLR2004
            raise ValueError("native routing requires six vehicle-count columns and int32 INF")
        self._query = query
        self._failed = False
        # Protect the whole run, including Python materialization after Rust returns.
        # A competing call must not invalidate the departure already in progress.
        self._run_lock = threading.Lock()
        self._native = ztm_routing.NativeState(
            query._native,  # noqa: SLF001
            profile.origins,
            [(stop, walk.walk_s) for stop, walk in profile.access.items()],
            profile.origin_walks,
            [(dest, profile.egress[dest].walk_s if dest in profile.egress else -1) for dest in profile.targets],
            bool(profile.egress),
            journey.MAX_JOURNEY_S,
        )

    @property
    def query(self) -> PreparedQuery:
        """Keep the native query and its network alive for this window."""
        return self._query

    def run(self, profile: _Profile, after: int, boarding: Iterable[int] | None = None) -> list[_Label]:
        """Search with the GIL detached, then rebuild only complete returned paths."""
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("native window is already running")
        try:
            if self._failed:
                raise RuntimeError("native window cannot be reused after a failed run")
            try:
                paths = self._native.run(after, None if boarding is None else list(boarding))
                return [self._materialize(profile, path) for path in paths]
            except BaseException:
                # A Python conversion failure also leaves numeric bounds partially advanced.
                self._failed = True
                self._native.invalidate()
                raise
        finally:
            self._run_lock.release()

    @staticmethod
    def _materialize(profile: _Profile, path: list[tuple[int, int, int, int, int, int, int, int]]) -> _Label:
        from ztm_frontend import journey  # noqa: PLC0415

        previous = None
        for time, kind, trip, board, alight, source, dest, seconds in path:
            if kind == 0:
                leg = None
            elif kind == 1:
                leg = (trip, board, alight)
            elif kind == 2:  # noqa: PLR2004
                leg = journey.Walk(profile.net.stop_ids[source], profile.net.stop_ids[dest], seconds)
            elif kind == 3:  # noqa: PLR2004
                leg = profile.access[dest]
            elif kind == 4:  # noqa: PLR2004
                leg = profile.egress[dest]
            else:
                raise RuntimeError("invalid native label kind")
            previous = journey._Label(time, previous, leg)  # noqa: SLF001
        if previous is None:
            raise RuntimeError("empty native path")
        return previous

    def stats(self) -> dict[str, int]:
        """Return the engine's arena, permission cache, and state payload counters."""
        return self._native.stats()
