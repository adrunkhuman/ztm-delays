"""Exercise the primitive facade boundary without a compiled engine or routing oracle."""

# ruff: noqa: SLF001
from __future__ import annotations

import importlib.util
import sys
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class Label:
    time: int
    previous: Label | None = None
    leg: object = None


@dataclass
class Walk:
    from_stop: str
    to_stop: str
    walk_s: int


class EngineNet:
    def __init__(self, *args: object) -> None:
        self.args = args


class EngineQuery:
    def __init__(self, *args: object) -> None:
        self.args = args


class EngineState:
    def __init__(self, *args: object) -> None:
        self.args = args
        self.invalidations = 0
        self.paths = [
            [
                (10, 0, 0, 0, 0, 0, 0, 0),
                (20, 3, 0, 0, 0, 0, 0, 10),
                (30, 1, 7, 0, 1, 0, 1, 0),
                (40, 2, 0, 0, 0, 0, 1, 10),
                (50, 4, 0, 0, 0, 0, 1, 10),
            ]
        ]

    def run(self, after: int, boarding: list[int] | None) -> list[list[tuple[int, ...]]]:
        self.request = (after, boarding)
        return self.paths

    def invalidate(self) -> None:
        self.invalidations += 1

    def stats(self) -> dict[str, int]:
        return {"live_labels_after_return": 0}


@pytest.fixture
def facade(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # Load a separate module, so test doubles never replace the app's real facade.
    monkeypatch.setitem(
        sys.modules,
        "ztm_routing",
        SimpleNamespace(
            PreparedNet=EngineNet,
            PreparedQuery=EngineQuery,
            NativeState=EngineState,
        ),
    )
    fake_journey = SimpleNamespace(MAX_VEHICLES=5, INF=2**31 - 1, MAX_JOURNEY_S=10800, _Label=Label, Walk=Walk)
    monkeypatch.setitem(sys.modules, "ztm_frontend", SimpleNamespace(journey=fake_journey))
    spec = importlib.util.spec_from_file_location(
        "facade_under_test",
        Path(__file__).resolve().parents[1] / "ztm_frontend" / "routing.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SourceNetwork(SimpleNamespace):
    """Weak-referenceable source object that must not be retained by the facade."""


def network() -> SourceNetwork:
    return SourceNetwork(
        stop_ids=["O", "D"],
        late_base=[10, 20],
        expected=[10, 20],
        depart=[10, 20],
        range_ids=[0, 0],
        trip_rows=[0],
        trip_pattern=[0],
        cumulative=[0.0, 10.0],
        patterns=[[0, 1]],
        pattern_alights=[[0, 1]],
        pattern_prefix=[[0.0, 10.0]],
        incidence=[[(0, 0, [10], [0])], []],
        footpaths=[[(1, 10)], []],
        envelopes=[SimpleNamespace(upper=[float("inf")], ratio=[1.1], floor=[0.0])],
    )


def profile(net: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(
        net=net,
        origins=[0],
        access={0: Walk("@origin", "O", 10)},
        origin_walks=[(0, 1, 10)],
        targets={1},
        egress={1: Walk("D", "@destination", 10)},
    )


def test_primitive_inputs_and_owner_lifetime(facade: ModuleType) -> None:
    net = network()
    prepared = facade.PreparedNet(net)
    query = facade.PreparedQuery(prepared, [10.0, 0.0], 12)
    state = facade.NativeState(query, profile(net))
    assert prepared._native.args == (
        2,
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
        [([float("inf")], [1.1], [0.0])],
    )
    assert query._native.args == (prepared._native, [10.0, 0.0], 12)
    assert state._native.args == (query._native, [0], [(0, 10)], [(0, 1, 10)], [(1, 10)], True, 10800)
    assert query.network is prepared
    assert state.query is query
    refs = [weakref.ref(owner) for owner in (net, prepared, query, state)]
    del net, prepared, query
    assert refs[0]() is None  # Native facade ownership must not keep the source network alive.
    assert refs[1]() is not None
    assert refs[2]() is not None
    del state
    assert all(ref() is None for ref in refs)


def test_materializes_every_leg_kind_and_iterable_boarding(facade: ModuleType) -> None:
    context = profile(network())
    state = facade.NativeState(facade.PreparedQuery(facade.PreparedNet(context.net), [0, 0], 0), context)
    result = state.run(context, 10, iter([0, 1]))
    assert state._native.request == (10, [0, 1])
    assert result == [
        Label(
            50,
            Label(40, Label(30, Label(20, Label(10), context.access[0]), (7, 0, 1)), Walk("O", "D", 10)),
            context.egress[1],
        )
    ]
    assert result[0].leg is context.egress[1]
    assert state.stats() == {"live_labels_after_return": 0}


def test_python_boundary_failure_invalidates_window(facade: ModuleType) -> None:
    context = profile(network())
    state = facade.NativeState(facade.PreparedQuery(facade.PreparedNet(context.net), [0, 0], 0), context)

    def broken_boarding() -> Iterator[int]:
        yield 0
        raise MemoryError("boarding conversion failed")

    with pytest.raises(MemoryError, match="boarding conversion failed"):
        state.run(context, 10, broken_boarding())
    assert state._native.invalidations == 1
    with pytest.raises(RuntimeError, match="cannot be reused"):
        state.run(context, 0)


def test_unknown_native_kind_invalidates_window(facade: ModuleType) -> None:
    context = profile(network())
    state = facade.NativeState(facade.PreparedQuery(facade.PreparedNet(context.net), [0, 0], 0), context)
    state._native.paths[0][1] = (20, 99, 0, 0, 0, 0, 0, 0)
    with pytest.raises(RuntimeError, match="invalid native label kind"):
        state.run(context, 10)
    assert state._native.invalidations == 1


def test_concurrent_window_rejection_does_not_invalidate_active_run(
    facade: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = profile(network())
    state = facade.NativeState(facade.PreparedQuery(facade.PreparedNet(context.net), [0, 0], 0), context)
    entered, release = threading.Event(), threading.Event()
    original = state._native.run

    def blocked(after: int, boarding: list[int] | None) -> list[list[tuple[int, ...]]]:
        entered.set()
        assert release.wait(timeout=10)
        return original(after, boarding)

    monkeypatch.setattr(state._native, "run", blocked)
    with ThreadPoolExecutor(max_workers=1) as pool:
        active = pool.submit(state.run, context, 10)
        try:
            assert entered.wait(timeout=10)
            with pytest.raises(RuntimeError, match="already running"):
                state.run(context, 0)
            assert state._native.invalidations == 0
        finally:
            release.set()
        assert active.result(timeout=10)
    assert state.run(context, 0)


@pytest.mark.parametrize(("constant", "value"), [("MAX_VEHICLES", 6), ("INF", 2**63 - 1)])
def test_frontend_constant_mismatch_rejected(
    facade: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    constant: str,
    value: int,
) -> None:
    from ztm_frontend import journey  # noqa: PLC0415

    context = profile(network())
    monkeypatch.setattr(journey, constant, value)
    with pytest.raises(ValueError, match="six vehicle-count columns"):
        facade.NativeState(facade.PreparedQuery(facade.PreparedNet(context.net), [0, 0], 0), context)
