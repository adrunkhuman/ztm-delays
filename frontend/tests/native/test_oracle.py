"""Run the journey behavior suite again with an independent shadow for every departure.

The test-only wrapper is scoped to this module. Production and ordinary threaded
request tests never install it; there is no runtime backend switch.
"""

# ruff: noqa: F403, SLF001
from __future__ import annotations

from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

import pytest

from tests.test_journey import *
from ztm_frontend import journey

from . import oracle

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(scope="module", autouse=True)
def _exact_shadow() -> Iterator[dict[str, int]]:
    original = journey._Profile.run
    shadows: WeakKeyDictionary[journey._Profile, oracle._Profile] = WeakKeyDictionary()
    counts = {"departures": 0, "labels": 0}

    def checked(profile: journey._Profile, after: int, boarding: set[int] | None = None) -> list[journey._Label]:
        shadow = shadows.get(profile)
        if shadow is None:
            shadow = oracle._Profile(
                profile.net,
                profile.origins,
                profile.targets,
                profile.to_target,
                access=profile.access,
                egress=profile.egress,
            )
            shadows[profile] = shadow
        oracle.WALK_PERMISSION_WORDS = journey.WALK_PERMISSION_WORDS
        oracle.MAX_JOURNEY_S = journey.MAX_JOURNEY_S
        expected = shadow.run(after, boarding)
        actual = original(profile, after, boarding)
        assert actual == expected, (after, actual, expected)
        counts["departures"] += 1
        counts["labels"] += len(actual)
        return actual

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(journey._Profile, "run", checked)
        yield counts
    print(f"Exact recursive native/Python label comparisons: {counts}")
