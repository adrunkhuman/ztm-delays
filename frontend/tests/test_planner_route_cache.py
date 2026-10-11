# ruff: noqa: PLR2004 - literal seconds and counts are behavioral expectations
from __future__ import annotations

from collections import OrderedDict
from datetime import timedelta
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

import duckdb
import pytest
from flask import Flask

from tests.test_live import _at, _ping, _sod, publish_artifact, publish_feed
from tests.test_live_times import _calibrate
from tests.test_planner import DAY, _write_artifact
from ztm_frontend import db, journey, live, planner, route_cache

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@pytest.fixture
def plans(monkeypatch: pytest.MonkeyPatch) -> list[journey.Network]:
    monkeypatch.setattr(route_cache, "_caches", WeakKeyDictionary())
    monkeypatch.setattr(journey, "_cache", OrderedDict())
    called: list[journey.Network] = []
    original = journey.plan

    def counted(  # noqa: PLR0913
        net: journey.Network,
        origin: str | journey.Point,
        destination: str | journey.Point,
        after: int,
        results: int,
        useful: Callable[[list[journey.Journey]], list[journey.Journey]] | None = None,
    ) -> list[journey.Journey]:
        called.append(net)
        return original(net, origin, destination, after, results, useful=useful)

    monkeypatch.setattr(journey, "plan", counted)
    return called


def test_search_caches_only_raw_routes_and_renders_current_point_labels(
    tmp_path: Path, plans: list[journey.Network]
) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    origin = journey.Point(52.332, 20.921, "Home")
    destination = journey.Point(52.2705, 20.97, "Office")
    first, later = planner.search(path, origin, destination, DAY, 7 * 3600)
    assert first
    assert first[0]["timeline"][0]["name"] == "Home"
    assert first[0]["timeline"][-1]["name"] == "Office"
    first[0]["chips"].clear()
    renamed = journey.Point(origin.lat, origin.lon, "New home")
    renamed_end = journey.Point(destination.lat, destination.lon, "New office")
    second, next_page = planner.search(path, renamed, renamed_end, DAY, 7 * 3600)
    assert len(plans) == 1
    assert second[0]["timeline"][0]["name"] == "New home"
    assert second[0]["timeline"][-1]["name"] == "New office"
    assert second[0]["chips"]
    assert next_page == later
    # Artifact lookups are still per request, even on a raw-route hit.
    with duckdb.connect(str(path)) as connection:
        connection.execute("update planner_stop set stop_name = 'Current stop' where stop_group_id = '1001'")
    refreshed, _ = planner.search(path, renamed, renamed_end, DAY, 7 * 3600)
    assert refreshed[0]["from_name"] == "Current stop"
    assert len(plans) == 1


def test_search_key_includes_exact_time_result_limit_and_filter_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plans: list[journey.Network]
) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    baseline = planner.search(path, "1001", "2002", DAY, 7 * 3600)
    assert planner.search(path, "1001", "2002", DAY, 7 * 3600) == baseline
    planner.search(path, "1001", "2002", DAY, 7 * 3600 + 1)
    assert len(plans) == 2
    monkeypatch.setattr(planner, "EXTRA_CANDIDATES", planner.EXTRA_CANDIDATES + 1)
    planner.search(path, "1001", "2002", DAY, 7 * 3600)
    assert len(plans) == 3
    original = planner._unbeaten  # noqa: SLF001
    monkeypatch.setattr(planner, "_unbeaten", lambda found: original(found))  # noqa: PLW0108 - new filter identity
    planner.search(path, "1001", "2002", DAY, 7 * 3600)
    assert len(plans) == 4
    assert len({id(net) for net in plans}) == 1


def test_same_build_at_another_path_or_day_has_separate_results(tmp_path: Path, plans: list[journey.Network]) -> None:
    paths = [tmp_path / name for name in ("first.duckdb", "second.duckdb")]
    for path in paths:
        _write_artifact(path)
        planner.search(path, "1001", "2002", DAY, 7 * 3600)
    planner.search(paths[0], "1001", "2002", DAY + timedelta(days=1), 7 * 3600)
    assert len(plans) == 3
    assert len({id(net) for net in plans}) == 3


def test_warm_cache_follows_held_request_snapshot_and_new_build_after_publication(
    tmp_path: Path, plans: list[journey.Network]
) -> None:
    path, replacement = tmp_path / "planner.duckdb", tmp_path / "replacement.duckdb"
    _write_artifact(path)
    _write_artifact(replacement)
    with duckdb.connect(str(replacement)) as connection:
        connection.execute("update planner_metadata set build_id = 'b2'")
        connection.execute("update planner_stop set stop_name = 'New build stop' where stop_group_id = '1001'")
        connection.execute(
            "update planner_stop set scheduled_sod = scheduled_sod + 600, expected_sod = expected_sod + 600"
        )
    app = Flask(__name__)
    app.teardown_appcontext(db.close_request_connections)
    with app.test_request_context():
        old = planner.search(path, "1001", "2002", DAY, 7 * 3600)
        replacement.replace(path)
        assert planner.search(path, "1001", "2002", DAY, 7 * 3600) == old
        assert len(plans) == 1
    with app.test_request_context():
        new = planner.search(path, "1001", "2002", DAY, 7 * 3600)
        assert new[0][0]["from_name"] == "New build stop"
        assert new[0][0]["depart"] == old[0][0]["depart"] + 600
        assert planner.search(path, "1001", "2002", DAY, 7 * 3600) == new
        assert len(plans) == 2
        assert plans[0] is not plans[1]


@pytest.mark.parametrize("broken", ["missing", "metadata", "invalid"])
def test_failed_or_missing_snapshot_never_reuses_warm_results(
    tmp_path: Path, plans: list[journey.Network], broken: str
) -> None:
    path, saved = tmp_path / "planner.duckdb", tmp_path / "saved.duckdb"
    _write_artifact(path)
    baseline = planner.search(path, "1001", "2002", DAY, 7 * 3600)
    path.replace(saved)
    if broken == "metadata":
        with duckdb.connect(str(path)) as connection:
            connection.execute("create table planner_metadata (build_id varchar)")
    elif broken == "invalid":
        path.write_text("not a DuckDB snapshot")
    with pytest.raises((duckdb.Error, RuntimeError)):
        planner.search(path, "1001", "2002", DAY, 7 * 3600)
    assert len(plans) == 1
    saved.replace(path)
    assert planner.search(path, "1001", "2002", DAY, 7 * 3600) == baseline
    assert len(plans) == 1


def test_live_feed_patched_networks_are_cached_separately_and_stale_feed_uses_schedule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, plans: list[journey.Network]
) -> None:
    path, scheduled = publish_artifact(tmp_path, monkeypatch)
    _calibrate(path)
    baseline = planner.search(path, "101", "102", DAY, _sod(8, 0))
    feed = tmp_path / "vehicles.json.gz"
    publish_feed(feed, _at(8, 9, 30), _ping(900, _at(8, 9, 6)))
    first = planner.search(path, "101", "102", DAY, _sod(8, 0), _at(8, 9, 40))
    assert first[0][0]["live"] is True
    assert planner.search(path, "101", "102", DAY, _sod(8, 0), _at(8, 9, 45)) == first
    assert len(plans) == 2
    assert plans[0] is scheduled
    assert plans[1] is not scheduled
    publish_feed(feed, _at(8, 10, 30), _ping(900, _at(8, 10, 6)))
    live.clear_cache()  # Advance the feed reader without waiting for its wall-clock TTL.
    second = planner.search(path, "101", "102", DAY, _sod(8, 0), _at(8, 10, 40))
    assert len(plans) == 3
    assert plans[2] is not plans[1]
    assert second != first
    assert planner.search(path, "101", "102", DAY, _sod(8, 0), _at(8, 30)) == baseline
    assert len(plans) == 3
