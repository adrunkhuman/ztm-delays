# ruff: noqa: PLR2004 - literal dates, trip keys and call counts are behavioral expectations
from __future__ import annotations

import logging
import os
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock, call
from weakref import WeakKeyDictionary

import duckdb
import pytest

from tests.test_journey import DAY, Stop, Trip, _write
from ztm_frontend import app as app_module
from ztm_frontend import db, journey, live, live_times, planner_warmup

if TYPE_CHECKING:
    from pathlib import Path

NOW = datetime(2026, 9, 22, 22, 30, tzinfo=UTC)  # September 23 in Warsaw, still September 22 in UTC.
DAYS = tuple(DAY + timedelta(days=offset) for offset in range(3))
_REAL_LIVE_VIEW = live_times.view


def _publish(
    path: Path,
    *,
    build_id: str = "old",
    first: date = DAY,
    last: date = DAY + timedelta(days=4),
    trip_base: int = 1,
) -> None:
    trips = tuple(
        Trip(trip_base + offset, (Stop("O:1", 100), Stop("D:1", 200)), first + timedelta(days=offset))
        for offset in range((last - first).days + 1)
    )
    with duckdb.connect(str(path)) as connection:
        _write(connection, trips, build_id=build_id)
        connection.execute("alter table planner_metadata add column first_date date")
        connection.execute("alter table planner_metadata add column last_date date")
        connection.execute("update planner_metadata set first_date = ?, last_date = ?", [first, last])


@pytest.fixture(autouse=True)
def _isolated_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(journey, "_cache", OrderedDict())
    monkeypatch.setattr(journey, "_pinned_days", set())
    monkeypatch.setattr(journey, "_prepared_networks", WeakKeyDictionary())


@pytest.fixture(autouse=True)
def live_view(monkeypatch: pytest.MonkeyPatch) -> Mock:
    view = Mock(return_value=None)
    monkeypatch.setattr(live_times, "view", view)
    return view


@pytest.fixture
def warmer(tmp_path: Path) -> planner_warmup.Warmer:
    path = tmp_path / "planner.duckdb"
    _publish(path)
    app = app_module.create_app()
    app.config["ZTM_PLANNER_PATH"] = path
    return planner_warmup.Warmer(app)


def test_first_check_warms_three_warsaw_days_sequentially_and_native_metadata(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    network = Mock(wraps=journey.network)
    prepare = Mock(wraps=journey.prepare)
    native = Mock(wraps=journey.PreparedNet)
    work = Mock()
    work.attach_mock(network, "network")
    work.attach_mock(prepare, "prepare")
    work.attach_mock(live_view, "live")
    monkeypatch.setattr(journey, "network", network)
    monkeypatch.setattr(journey, "prepare", prepare)
    monkeypatch.setattr(journey, "PreparedNet", native)

    warmer.check(NOW)

    nets = [args.args[0] for args in prepare.call_args_list]
    assert len(nets) == 3
    expected = []
    for day, net in zip(DAYS, nets, strict=True):
        expected.extend((call.network(path, day), call.prepare(net)))
    expected.append(call.live(path, nets[0], DAY, NOW))
    assert work.mock_calls == expected
    assert native.call_args_list == [call(net) for net in nets]
    for key, net in enumerate(nets, start=1):
        assert net.build_id == "old"
        assert set(net.trip_index) == {key}
        assert net.search("O", "D", 0)[0].legs[0].trip_key == key
        journey.prepare(net)
    assert native.call_count == 3  # Neither searches nor repeat preparation rebuild the native metadata.


def test_three_warm_days_stay_pinned_while_fourth_slot_rotates(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    native = Mock(wraps=journey.PreparedNet)
    monkeypatch.setattr(journey, "PreparedNet", native)
    warmer.check(NOW)
    warmed = [journey.network(path, day) for day in DAYS]
    assert native.call_count == 3

    fourth_day, fifth_day = DAY + timedelta(days=3), DAY + timedelta(days=4)
    first = journey.network(path, fourth_day)
    journey.prepare(first)
    assert journey.network(path, fourth_day) is first
    journey.prepare(first)
    assert native.call_count == 4
    second = journey.network(path, fifth_day)
    journey.prepare(second)
    assert len(journey._cache) == journey.CACHED_DAYS == 4  # noqa: SLF001 - assert bounded retention
    assert (str(path), "old", fourth_day) not in journey._cache  # noqa: SLF001
    assert [journey.network(path, day) for day in DAYS] == warmed
    for net in warmed:
        journey.prepare(net)
    assert native.call_count == 5
    assert journey.network(path, fifth_day) is second

    rebuilt = journey.network(path, fourth_day)
    assert rebuilt is not first
    journey.prepare(rebuilt)
    assert native.call_count == 6
    assert (str(path), "old", fifth_day) not in journey._cache  # noqa: SLF001
    assert [journey.network(path, day) for day in DAYS] == warmed


def test_pins_move_on_rollover_without_warming_a_fourth_day(warmer: planner_warmup.Warmer) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    warmer.check(NOW)
    yesterday = journey.network(path, DAY)
    warmer.check(NOW + timedelta(days=1))
    warmed = [journey.network(path, day + timedelta(days=1)) for day in DAYS]
    assert len(journey._cache) == 4  # noqa: SLF001 - yesterday initially occupies the rotating slot
    assert journey.network(path, DAY) is yesterday
    journey.network(path, DAY + timedelta(days=4))
    assert (str(path), "old", DAY) not in journey._cache  # noqa: SLF001
    assert [journey.network(path, day + timedelta(days=1)) for day in DAYS] == warmed


def test_new_publication_releases_old_pins_and_rotates_only_new_extra_day(
    warmer: planner_warmup.Warmer,
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    warmer.check(NOW)
    old = [journey.network(path, day) for day in DAYS]
    replacement = path.with_name("replacement.duckdb")
    _publish(replacement, build_id="new", trip_base=11)
    replacement.replace(path)
    warmer.check(NOW)
    warmed = [journey.network(path, day) for day in DAYS]
    assert all(net is not previous for net, previous in zip(warmed, old, strict=True))
    for offset in (3, 4):
        journey.network(path, DAY + timedelta(days=offset))
        assert [journey.network(path, day) for day in DAYS] == warmed
    assert len(journey._cache) == 4  # noqa: SLF001
    assert all(build_id == "new" for _, build_id, _ in journey._cache)  # noqa: SLF001


def test_unchanged_publication_does_no_database_network_or_feed_work(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    warmer.check(NOW)
    live_view.reset_mock()
    connect = Mock(side_effect=AssertionError("unchanged artifact opened DuckDB"))
    metadata = Mock(side_effect=AssertionError("unchanged artifact queried metadata"))
    network = Mock(side_effect=AssertionError("unchanged artifact requested a network"))
    prepare = Mock(side_effect=AssertionError("unchanged artifact prepared a network"))
    monkeypatch.setattr(db.duckdb, "connect", connect)
    monkeypatch.setattr(planner_warmup, "fetch_one", metadata)
    monkeypatch.setattr(journey, "network", network)
    monkeypatch.setattr(journey, "prepare", prepare)

    warmer.check(NOW + timedelta(minutes=1))

    for boundary in (connect, metadata, network, prepare, live_view):
        boundary.assert_not_called()


def test_atomic_replacement_is_detected_with_preserved_mtime_and_size(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    warmer.check(NOW)
    old_stat = path.stat()
    replacement = path.with_name("replacement.duckdb")
    _publish(replacement, build_id="new", trip_base=11)
    assert replacement.stat().st_size == old_stat.st_size
    os.utime(replacement, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
    replacement.replace(path)
    new_stat = path.stat()
    assert new_stat.st_ino != old_stat.st_ino
    assert (new_stat.st_mtime_ns, new_stat.st_size) == (old_stat.st_mtime_ns, old_stat.st_size)
    prepare = Mock(wraps=journey.prepare)
    monkeypatch.setattr(journey, "prepare", prepare)
    live_view.reset_mock()

    warmer.check(NOW)

    nets = [args.args[0] for args in prepare.call_args_list]
    assert [net.build_id for net in nets] == ["new"] * 3
    assert [set(net.trip_index) for net in nets] == [{11}, {12}, {13}]
    live_view.assert_called_once_with(path, nets[0], DAY, NOW)


def test_warsaw_date_rollover_warms_new_horizon_and_reuses_overlapping_days(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    warmer.check(NOW)
    overlapping = [journey.network(path, day) for day in DAYS[1:]]
    network = Mock(wraps=journey.network)
    prepare = Mock(wraps=journey.prepare)
    build = Mock(wraps=journey.Network)
    monkeypatch.setattr(journey, "network", network)
    monkeypatch.setattr(journey, "prepare", prepare)
    monkeypatch.setattr(journey, "Network", build)
    live_view.reset_mock()
    tomorrow = datetime(2026, 9, 23, 22, 0, tzinfo=UTC)
    assert NOW.date() != DAY
    assert tomorrow.date() == DAY  # Rollover is Warsaw midnight, not UTC midnight.

    warmer.check(tomorrow)

    assert network.call_args_list == [call(path, day + timedelta(days=1)) for day in DAYS]
    nets = [args.args[0] for args in prepare.call_args_list]
    assert nets[:2] == overlapping
    assert set(nets[2].trip_index) == {4}
    build.assert_called_once()
    live_view.assert_called_once_with(path, nets[0], DAY + timedelta(days=1), tomorrow)


@pytest.mark.parametrize(
    ("bounds", "offsets"),
    [((1, 4), (1, 2)), ((-1, 1), (0, 1)), ((1, 1), (1,)), ((-4, -1), ()), ((3, 4), ())],
)
def test_warm_days_intersect_artifact_date_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_view: Mock,
    bounds: tuple[int, int],
    offsets: tuple[int, ...],
) -> None:
    path = tmp_path / "planner.duckdb"
    first_offset, last_offset = bounds
    _publish(path, first=DAY + timedelta(days=first_offset), last=DAY + timedelta(days=last_offset))
    app = app_module.create_app()
    app.config["ZTM_PLANNER_PATH"] = path
    network = Mock(wraps=journey.network)
    prepare = Mock(wraps=journey.prepare)
    monkeypatch.setattr(journey, "network", network)
    monkeypatch.setattr(journey, "prepare", prepare)
    warmer = planner_warmup.Warmer(app)

    warmer.check(NOW)

    assert network.call_args_list == [call(path, DAY + timedelta(days=offset)) for offset in offsets]
    assert prepare.call_count == len(offsets)
    if 0 in offsets:
        live_view.assert_called_once_with(path, prepare.call_args_list[0].args[0], DAY, NOW)
    else:
        live_view.assert_not_called()
    network.reset_mock()
    warmer.check(NOW)
    network.assert_not_called()


def test_missing_artifact_clears_completion_and_is_retryable(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    warmer.check(NOW)
    unpublished = path.with_name("unpublished.duckdb")
    path.replace(unpublished)
    metadata = Mock(wraps=planner_warmup.fetch_one)
    network = Mock(wraps=journey.network)
    monkeypatch.setattr(planner_warmup, "fetch_one", metadata)
    monkeypatch.setattr(journey, "network", network)
    live_view.reset_mock()

    warmer.check(NOW)
    warmer.check(NOW)

    metadata.assert_not_called()
    network.assert_not_called()
    live_view.assert_not_called()
    unpublished.replace(path)  # Same inode and metadata as the previously completed publication.
    warmer.check(NOW)
    metadata.assert_called_once()
    assert network.call_args_list == [call(path, day) for day in DAYS]
    live_view.assert_called_once()


def test_empty_metadata_failure_remains_retryable(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    with duckdb.connect(str(path)) as connection:
        connection.execute("delete from planner_metadata")
    metadata = Mock(wraps=planner_warmup.fetch_one)
    network = Mock(wraps=journey.network)
    monkeypatch.setattr(planner_warmup, "fetch_one", metadata)
    monkeypatch.setattr(journey, "network", network)

    for _ in range(2):
        with pytest.raises(RuntimeError, match="planner artifact has no build metadata"):
            warmer.check(NOW)
    assert metadata.call_count == 2
    network.assert_not_called()
    live_view.assert_not_called()
    with duckdb.connect(str(path)) as connection:
        connection.execute("insert into planner_metadata values ('old', ?, ?)", [DAY, DAYS[-1]])
    warmer.check(NOW)
    assert metadata.call_count == 3
    assert network.call_args_list == [call(path, day) for day in DAYS]
    live_view.assert_called_once()


def test_partial_failure_reuses_completed_networks_and_native_metadata_on_retry(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    network = journey.network
    attempted = []
    failed = False

    def fail_second_day_once(path: Path, day: date) -> journey.Network:
        nonlocal failed
        attempted.append(day)
        if day == DAYS[1] and not failed:
            failed = True
            raise RuntimeError("temporary network failure")
        return network(path, day)

    build = Mock(wraps=journey.Network)
    native = Mock(wraps=journey.PreparedNet)
    prepare = Mock(wraps=journey.prepare)
    monkeypatch.setattr(journey, "network", fail_second_day_once)
    monkeypatch.setattr(journey, "Network", build)
    monkeypatch.setattr(journey, "PreparedNet", native)
    monkeypatch.setattr(journey, "prepare", prepare)

    with pytest.raises(RuntimeError, match="temporary network failure"):
        warmer.check(NOW)
    first = prepare.call_args.args[0]
    assert build.call_count == native.call_count == 1
    live_view.assert_not_called()

    warmer.check(NOW)

    assert attempted == [DAYS[0], DAYS[1], *DAYS]
    assert prepare.call_args_list[1] == call(first)
    assert build.call_count == native.call_count == 3
    live_view.assert_called_once_with(warmer.app.config["ZTM_PLANNER_PATH"], first, DAY, NOW)
    warmer.check(NOW)
    assert attempted == [DAYS[0], DAYS[1], *DAYS]


def test_optional_live_view_prepares_its_patched_network_separately(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, live_view: Mock
) -> None:
    patched = []

    def current_view(_path: Path, net: journey.Network, _day: date, _now: datetime) -> SimpleNamespace:
        current = net.patched({0: (150, 150, 160), 1: (200, 250, 260)})
        patched.append(current)
        return SimpleNamespace(net=current)

    live_view.side_effect = current_view
    prepare = Mock(wraps=journey.prepare)
    native = Mock(wraps=journey.PreparedNet)
    monkeypatch.setattr(journey, "prepare", prepare)
    monkeypatch.setattr(journey, "PreparedNet", native)

    warmer.check(NOW)

    nets = [args.args[0] for args in prepare.call_args_list]
    assert len(nets) == 4
    assert nets[-1] is patched[0]
    assert nets[-1] is not nets[0]
    assert native.call_args_list == [call(net) for net in nets]
    assert nets[0].search("O", "D", 0)[0].depart == 100
    assert nets[-1].search("O", "D", 0)[0].depart == 150
    assert native.call_count == 4


def test_unavailable_live_feed_keeps_timetable_native_networks_warm(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The autouse boundary mock is undone here; only external feed availability is mocked.
    monkeypatch.setattr(live_times, "view", _REAL_LIVE_VIEW)
    feed = Mock(return_value=None)
    monkeypatch.setattr(live, "current", feed)
    prepare = Mock(wraps=journey.prepare)
    native = Mock(wraps=journey.PreparedNet)
    monkeypatch.setattr(journey, "prepare", prepare)
    monkeypatch.setattr(journey, "PreparedNet", native)

    warmer.check(NOW)

    nets = [args.args[0] for args in prepare.call_args_list]
    assert len(nets) == native.call_count == 3
    feed.assert_called_once_with(warmer.app.config["ZTM_PLANNER_PATH"], nets[0], DAY, NOW)
    assert [net.search("O", "D", 0)[0].depart for net in nets] == [100] * 3
    warmer.check(NOW)
    assert feed.call_count == 1
    assert native.call_count == 3


@pytest.mark.skipif(os.name == "nt", reason="Windows cannot replace an open DuckDB file")
@pytest.mark.parametrize("fail_after_replacement", [False, True])
def test_mid_pass_replacement_keeps_one_snapshot_and_tears_down_connection(
    warmer: planner_warmup.Warmer,
    monkeypatch: pytest.MonkeyPatch,
    live_view: Mock,
    fail_after_replacement: bool,
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    replacement = path.with_name("replacement.duckdb")
    _publish(replacement, build_id="new", trip_base=11)
    fetch = planner_warmup.fetch_one
    held = []
    snapshots = []
    prepare = Mock(wraps=journey.prepare)
    monkeypatch.setattr(journey, "prepare", prepare)

    def replace_after_metadata(db_path: Path, sql: str) -> dict:
        metadata = fetch(db_path, sql)
        assert metadata is not None
        with db.read_connection(db_path) as connection:
            held.append(connection)
        if replacement.exists():
            replacement.replace(db_path)
            if fail_after_replacement:
                raise RuntimeError("failure after publication")
        return metadata

    def view_from_snapshot(db_path: Path, net: journey.Network, day: date, _now: datetime) -> None:
        with db.read_connection(db_path) as connection:
            assert connection is held[-1]
            snapshots.append((connection.execute("select build_id from planner_metadata").fetchone()[0], day))
        assert snapshots[-1][0] == net.build_id

    monkeypatch.setattr(planner_warmup, "fetch_one", replace_after_metadata)
    live_view.side_effect = view_from_snapshot

    if fail_after_replacement:
        with pytest.raises(RuntimeError, match="failure after publication"):
            warmer.check(NOW)
        prepare.assert_not_called()
        live_view.assert_not_called()
    else:
        warmer.check(NOW)
        nets = [args.args[0] for args in prepare.call_args_list]
        assert [net.build_id for net in nets] == ["old"] * 3
        assert [set(net.trip_index) for net in nets] == [{1}, {2}, {3}]
        assert snapshots == [("old", DAY)]
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        held[0].execute("select 1")
    prepare.reset_mock()
    live_view.reset_mock()

    warmer.check(NOW)

    nets = [args.args[0] for args in prepare.call_args_list]
    assert [net.build_id for net in nets] == ["new"] * 3
    assert [set(net.trip_index) for net in nets] == [{11}, {12}, {13}]
    assert held[1] is not held[0]
    assert snapshots[-1] == ("new", DAY)
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        held[1].execute("select 1")


@pytest.mark.skipif(os.name == "nt", reason="Windows cannot replace an open DuckDB file")
def test_publication_warms_new_build_while_request_keeps_old_instance_open(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = warmer.app.config["ZTM_PLANNER_PATH"]
    replacement = path.with_name("replacement.duckdb")
    _publish(replacement, build_id="new", trip_base=11)
    prepare = Mock(wraps=journey.prepare)
    monkeypatch.setattr(journey, "prepare", prepare)
    with warmer.app.test_request_context():
        assert db.fetch_one(path, "select build_id from planner_metadata") == {"build_id": "old"}
        replacement.replace(path)
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(warmer.check, NOW).result(timeout=10)
        assert db.fetch_one(path, "select build_id from planner_metadata") == {"build_id": "old"}
        # Old in-flight requests may rotate the extra slot, never the new build's pins.
        for day in DAYS:
            assert journey.network(path, day).build_id == "old"
    nets = [args.args[0] for args in prepare.call_args_list]
    assert [net.build_id for net in nets] == ["new"] * 3
    assert [set(net.trip_index) for net in nets] == [{11}, {12}, {13}]
    assert [journey.network(path, day) for day in DAYS] == nets
    prepare.reset_mock()
    warmer.check(NOW)
    prepare.assert_not_called()


def test_run_logs_failure_retries_on_interval_and_stops_without_sleeping(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stop = threading.Event()
    waits = []
    clock = Mock()
    clock.now.side_effect = [NOW, NOW + timedelta(seconds=planner_warmup.CHECK_SECONDS)]
    check = Mock(side_effect=[RuntimeError("temporary warmup failure"), None])
    monkeypatch.setattr(planner_warmup, "datetime", clock)
    monkeypatch.setattr(warmer, "check", check)

    def interval(seconds: float) -> bool:
        waits.append(seconds)
        if len(waits) == 2:
            stop.set()
        return stop.is_set()

    monkeypatch.setattr(stop, "wait", interval)
    with caplog.at_level(logging.ERROR, logger=planner_warmup.__name__):
        warmer.run(stop)

    assert check.call_args_list == [call(NOW), call(NOW + timedelta(seconds=planner_warmup.CHECK_SECONDS))]
    assert clock.now.call_args_list == [call(planner_warmup.WARSAW)] * 2
    assert waits == [planner_warmup.CHECK_SECONDS] * 2
    assert stop.is_set()
    records = [record for record in caplog.records if record.name == planner_warmup.__name__]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert "retrying in 30s" in records[0].getMessage()
    assert records[0].exc_info is not None
    assert str(records[0].exc_info[1]) == "temporary warmup failure"


def test_run_with_shutdown_already_requested_does_no_work(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop = threading.Event()
    stop.set()
    check = Mock()
    wait = Mock()
    monkeypatch.setattr(warmer, "check", check)
    monkeypatch.setattr(stop, "wait", wait)

    warmer.run(stop)

    check.assert_not_called()
    wait.assert_not_called()


def test_start_is_idempotent_and_launches_one_daemon_per_app(
    warmer: planner_warmup.Warmer, monkeypatch: pytest.MonkeyPatch
) -> None:
    thread = Mock()
    monkeypatch.setattr(planner_warmup.threading, "Thread", thread)

    planner_warmup.start(warmer.app)
    stop = warmer.app.extensions["planner_warmup"]
    planner_warmup.start(warmer.app)

    assert isinstance(stop, threading.Event)
    assert warmer.app.extensions["planner_warmup"] is stop
    thread.assert_called_once()
    kwargs = thread.call_args.kwargs
    assert kwargs["args"] == (stop,)
    assert kwargs["name"] == "planner-warmup"
    assert kwargs["daemon"] is True
    assert kwargs["target"].__func__ is planner_warmup.Warmer.run
    assert kwargs["target"].__self__.app is warmer.app
    thread.return_value.start.assert_called_once_with()
    other = app_module.create_app()
    planner_warmup.start(other)
    assert thread.call_count == 2
    assert other.extensions["planner_warmup"] is not stop


def test_serving_factory_starts_worker_but_plain_create_app_does_not(monkeypatch: pytest.MonkeyPatch) -> None:
    start = Mock()
    monkeypatch.setattr(planner_warmup, "start", start)
    connect = Mock(side_effect=AssertionError("app factory must not open DuckDB"))
    monkeypatch.setattr(db.duckdb, "connect", connect)

    plain = app_module.create_app()

    start.assert_not_called()
    assert "planner_warmup" not in plain.extensions
    serving = app_module.create_serving_app()
    start.assert_called_once_with(serving)
    assert serving is not plain
    connect.assert_not_called()
