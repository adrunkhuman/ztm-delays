from __future__ import annotations

from datetime import datetime, tzinfo
from typing import TYPE_CHECKING

import duckdb
import pytest

from tests.test_journey import Stop, Trip, _network
from tests.test_live import BACK, DAY, OUT, _at, _duty, _match, _ping, _shapes, _sod, publish_artifact, publish_feed
from ztm_frontend import app as app_module
from ztm_frontend import live, live_times, planner, queries
from ztm_frontend.journey import NO_BOARD

if TYPE_CHECKING:
    from pathlib import Path

    from ztm_frontend.journey import Network

# alpha 1; from 5 min beyond usual the error spreads wider.
ROWS = [
    (tram, minutes, band, 1.0, low, 0.0, 60.0)
    for tram in (False, True)
    for minutes in (10, 30)
    for band, low in ((-86400, -60.0), (300, -120.0))
]
TURNAROUND = [(False, -60.0, 120.0, 300.0), (True, -60.0, 120.0, 300.0)]
CALIBRATION = live_times.Calibration.from_rows(ROWS, TURNAROUND)


def _times(matcher: live.Matcher, now: int) -> dict[str, list[tuple[int, int, int]]]:
    """Patched (board_by, expected, late) of each trip's stops, by trip shape; unpatched stops as they were."""
    assert CALIBRATION is not None
    rows = live_times.row_times(matcher, matcher.fixes, CALIBRATION, now)
    net = matcher.net
    return {
        str(net.trip_meta[trip][4]): [
            rows.get(row, (net.board[row], net.expected[row], net.late_base[row])) for row in matcher.rows(trip)
        ]
        for trip in range(len(net.trip_keys))
    }


@pytest.fixture
def published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Network]:
    return publish_artifact(tmp_path, monkeypatch)


@pytest.fixture
def matcher() -> live.Matcher:
    return live.Matcher(_network(*_duty()), _shapes)


def test_calibration_picks_the_horizon_and_the_band_of_delay() -> None:
    assert CALIBRATION is not None
    assert CALIBRATION.ahead(False, 0, 0) == (1.0, (-60.0, 0.0, 60.0))
    assert CALIBRATION.ahead(False, 600, 299) == (1.0, (-60.0, 0.0, 60.0))
    assert CALIBRATION.ahead(False, 601, 300) == (1.0, (-120.0, 0.0, 60.0))
    assert CALIBRATION.ahead(False, 1801, 0) is None
    assert live_times.Calibration.from_rows([], TURNAROUND) is None


def test_running_vehicle_moves_the_stops_ahead_and_closes_the_ones_behind(matcher: live.Matcher) -> None:
    # 100 m before the middle stop (due 08:03:36 there), 5 min 30 s late against a usual 30 s: 5 min beyond usual.
    _match(matcher, _ping(900, _at(8, 9, 6)))
    out = _times(matcher, _sod(8, 9) + 30)[OUT]

    assert out[0][0] == NO_BOARD  # passed
    # At the middle stop, be there by 08:04:30 + 300 - 120 + 30; expected 08:09:30; late 60 s after.
    assert out[1] == (_sod(8, 8), _sod(8, 9) + 30, _sod(8, 10) + 30)
    assert out[2][1:] == (_sod(8, 13) + 30, _sod(8, 14) + 30)  # last stop: nobody boards there


def test_a_stop_the_vehicle_has_reached_is_not_boarded_later(matcher: live.Matcher) -> None:
    # At the middle stop itself, 5 min beyond usual: it may leave any moment.
    _match(matcher, _ping(1000, _at(8, 9, 30)))
    assert _times(matcher, _sod(8, 9) + 30)[OUT][1][0] == _sod(8, 4)


def test_boarding_moves_later_only_close_ahead(matcher: live.Matcher) -> None:
    _match(matcher, _ping(900, _at(8, 9, 6)))
    # Seen from 25 min earlier, the middle stop is too far ahead to tell people to come later...
    assert _times(matcher, _sod(7, 45))[OUT][1][0] == _sod(8, 4)
    # ...while an early vehicle moves it earlier whatever the distance.
    early = live.Matcher(matcher.net, _shapes)
    _match(early, _ping(1000, _at(8, 2)))
    assert _times(early, _sod(7, 30))[OUT][1][0] < _sod(8, 4)


def test_late_vehicle_delays_its_duty_next_trip_but_not_its_boarding(matcher: live.Matcher) -> None:
    # 10 min beyond usual: due at the terminus 08:18:30, then a 2 min turnaround: 5 min after BACK's usual start.
    assert set(_match(matcher, _ping(200, _at(8, 10)))) == {OUT}
    _match(matcher, _ping(1000, _at(8, 14, 30)))
    back = _times(matcher, _sod(8, 14) + 30)[BACK]

    assert [expected for _, expected, _ in back] == [_sod(8, 20) + 30, _sod(8, 24) + 30, _sod(8, 28) + 30]
    assert [board for board, _, _ in back[:2]] == [_sod(8, 15), _sod(8, 19)]


def test_waiting_vehicle_moves_its_trip_only_when_its_arrival_is_known(matcher: live.Matcher) -> None:
    usual = _times(live.Matcher(matcher.net, _shapes), 0)[BACK]
    _match(matcher, _ping(2000, _at(8, 20)))  # first seen at the terminus, 5 min past due
    assert _times(matcher, _sod(8, 20))[BACK] == usual
    known = live.Matcher(matcher.net, _shapes)
    assert set(_match(known, _ping(1500, _at(8, 9)))) == {OUT}
    _match(known, _ping(2000, _at(8, 14)))  # arrived 08:14: leaves 08:16, 30 s after its usual 08:15:30
    assert _times(known, _sod(8, 14))[BACK][0][1] == _sod(8, 16)


def test_patched_network_reorders_boarding_and_leaves_the_original_alone() -> None:
    first_due, second_due, delayed = 100, 160, 300
    net = _network(
        Trip(1, (Stop("A:1", first_due), Stop("B:1", first_due + 100))),
        Trip(2, (Stop("A:1", second_due), Stop("B:1", second_due + 100))),
    )
    patched = net.patched({net.trip_index[1][0]: (delayed, delayed + 30, delayed + 100)})  # trip 1 now leaves last

    _, _, times, trips = patched.incidence[net.stop_index["A:1"]][0]
    assert list(times) == [second_due, delayed]
    assert [patched.trip_keys[t] for t in trips] == [2, 1]
    assert patched.timing(1, 10, 20).board_by == delayed
    assert net.timing(1, 10, 20).board_by == first_due
    assert list(net.incidence[net.stop_index["A:1"]][0][2]) == [first_due, second_due]


def test_view_patches_today_once_per_feed_object(published: tuple[Path, Network], tmp_path: Path) -> None:
    path, net = published
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "create table planner_live_persistence (is_tram boolean, horizon_min integer, excess_s integer, "
            "alpha double, low_s double, mid_s double, high_s double)"
        )
        connection.executemany("insert into planner_live_persistence values (?, ?, ?, ?, ?, ?, ?)", ROWS)
        connection.execute(
            "create table planner_live_turnaround (is_tram boolean, low_s double, mid_s double, high_s double)"
        )
        connection.executemany("insert into planner_live_turnaround values (?, ?, ?, ?)", TURNAROUND)
    publish_feed(tmp_path / "vehicles.json.gz", _at(8, 9, 30), _ping(900, _at(8, 9, 6)))

    found = live_times.view(path, net, DAY, _at(8, 9, 40))
    assert found is not None
    assert found.net is not net
    assert found.net.timing(1, 20, 30).board_by == _sod(8, 8)  # the middle stop, boarded later
    again = live_times.view(path, net, DAY, _at(8, 9, 45))
    assert again is not None
    assert again.net is found.net


def test_view_needs_a_calibration(published: tuple[Path, Network], tmp_path: Path) -> None:
    path, net = published
    publish_feed(tmp_path / "vehicles.json.gz", _at(8, 9, 30), _ping(900, _at(8, 9, 6)))
    assert live_times.view(path, net, DAY, _at(8, 9, 40)) is None


def _calibrate(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "create table planner_live_persistence (is_tram boolean, horizon_min integer, excess_s integer, "
            "alpha double, low_s double, mid_s double, high_s double)"
        )
        connection.executemany("insert into planner_live_persistence values (?, ?, ?, ?, ?, ?, ?)", ROWS)
        connection.execute(
            "create table planner_live_turnaround (is_tram boolean, low_s double, mid_s double, high_s double)"
        )
        connection.executemany("insert into planner_live_turnaround values (?, ?, ?, ?)", TURNAROUND)
        connection.execute(
            "create table planner_stop_post as select distinct stop_id, 52.23 as lat, 21.01 as lon from planner_stop"
        )


def test_cards_show_where_the_vehicle_is(published: tuple[Path, Network], tmp_path: Path) -> None:
    path, _ = published
    _calibrate(path)
    publish_feed(tmp_path / "vehicles.json.gz", _at(8, 9, 30), _ping(900, _at(8, 9, 6)))

    cards, _ = planner.search(path, "101", "102", DAY, _sod(8, 0), _at(8, 9, 40))
    ride = next(item for item in cards[0]["timeline"] if item["kind"] == "ride")
    assert cards[0]["live"] is True
    assert (ride["live"]["status"], ride["live"]["late"]) == ("running", 6)  # 5 min 30 s behind the timetable
    shown = ride["live"]["map"]
    assert shown["stop"] == [21.01, 52.23]
    assert shown["path"][0] == pytest.approx(shown["vehicle"], abs=1e-4)  # the vehicle, placed on its shape
    # The stop list agrees with the card.
    stops = planner.trip_stops(
        path, ride["trip_key"], ride["board_sequence"], ride["alight_sequence"], DAY, _at(8, 9, 40)
    )
    assert planner.clock(stops[-1]["expected"]) == "08:14"
    # Without now, nothing is live.
    assert planner.search(path, "101", "102", DAY, _sod(8, 0))[0][0]["live"] is False


def test_planner_page_marks_live_rides_and_refreshes_only_then(
    published: tuple[Path, Network], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, _ = published
    _calibrate(path)
    publish_feed(tmp_path / "vehicles.json.gz", _at(8, 9, 30), _ping(900, _at(8, 9, 6)))
    with duckdb.connect(str(path)) as connection:  # what the page needs beyond the router's tables
        connection.execute(
            "create or replace table planner_metadata as select build_id, ?::date as first_date, "
            "?::date as last_date from planner_metadata",
            [DAY, DAY],
        )
        connection.execute(
            "create table planner_stop_group as select distinct stop_group_id, stop_group_id as name, "
            "stop_group_id as search_key, ['175'] as lines, 1 as visits from planner_stop"
        )
    monkeypatch.setenv("ZTM_PLANNER_PATH", str(path))
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})

    class Clock(datetime):
        moment = _at(8, 9, 40)

        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            return cls.moment.astimezone(tz)

    monkeypatch.setattr(app_module, "datetime", Clock)
    client = app_module.create_app().test_client()
    url = f"/planner?date={DAY}&time=08:00&from=101&to=102&lang=en"

    page = client.get(url).get_data(as_text=True)
    assert 'class="pl-card live"' in page
    assert ">6 min late</span>" in page
    assert 'class="pl-minimap"' in page
    assert 'hx-trigger="every[plannerShouldRefresh()] 60s"' in page
    Clock.moment = _at(8, 30)  # the feed is now stale: no live data, no polling
    page = client.get(url).get_data(as_text=True)
    assert "pl-minimap" not in page
    assert "every 60s" not in page
