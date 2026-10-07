from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import duckdb
import pytest

from tests.test_journey import DAY, Stop, Trip, _network, _write
from ztm_frontend import journey, live, live_status

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from ztm_frontend.journey import Network

LAT = 52.23
METRE_LON = 1 / live.M_PER_DEG_LON
OUT, BACK = 1, 2  # trip keys of one duty: out along the shape, then back


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return live_status.WARSAW.localize(datetime.combine(DAY, datetime.min.time()) + timedelta(
        hours=hour, minutes=minute, seconds=second))  # fmt: skip


def _sod(hour: int, minute: int) -> int:
    return hour * 3600 + minute * 60


def _duty() -> tuple[Trip, ...]:
    """Line 175, brigade 3: 2 km east 08:00-08:08, then back 08:15-08:23, each stop 1 km apart."""
    out = tuple(Stop(f"10{i}:1", _sod(8, 4 * i), shape_dist=1000 * i, usual=30) for i in range(3))
    back = tuple(Stop(f"10{2 - i}:2", _sod(8, 15 + 4 * i), shape_dist=1000 * i, usual=30) for i in range(3))
    return (
        Trip(OUT, out, line="175", brigade="3", duty="D1", shape="E"),
        Trip(BACK, back, line="175", brigade="3", duty="D1", shape="W"),
    )


def _shapes(ids: Iterable[str]) -> dict[str, live.Shape | None]:
    east = [21.01 + 100 * i * METRE_LON for i in range(21)]
    lines = {"E": east, "W": east[::-1]}
    return {i: live.Shape([LAT] * 21, lines[i], [100 * n for n in range(21)]) if i in lines else None for i in ids}


def _ping(
    metres_east: float, at: datetime, *, vehicle: str = "1001", north: float = 0, brigade: str = "3"
) -> live.Ping:
    lon = 21.01 + metres_east * METRE_LON
    return live.Ping("bus", "175", brigade, vehicle, LAT + north / live.M_PER_DEG_LAT, lon, int(at.timestamp()))


def _match(matcher: live.Matcher, *pings: live.Ping, now: datetime | None = None) -> dict[int, live.Fix]:
    matcher.update(now or datetime.fromtimestamp(max(p.time for p in pings), UTC), list(pings), DAY)
    return {matcher.net.trip_keys[trip]: fix for trip, fix in matcher.fixes.items()}


@pytest.fixture
def matcher() -> live.Matcher:
    return live.Matcher(_network(*_duty()), _shapes)


def test_running_vehicle_gets_its_delay_where_it_is(matcher: live.Matcher) -> None:
    fixes = _match(matcher, _ping(1500, _at(8, 8)))

    fix = fixes[OUT]
    assert (fix.waiting, round(fix.dist_m), fix.delay_s) == (False, 1500, 120)  # due at 1.5 km at 08:06


def test_vehicle_at_the_end_waits_for_the_duty_next_trip_and_keeps_its_arrival(matcher: live.Matcher) -> None:
    first = _match(matcher, _ping(2000, _at(8, 12)))[BACK]
    later = _match(matcher, _ping(2000, _at(8, 14)))[BACK]

    assert (first.waiting, first.delay_s, first.arrived) == (True, -180, _sod(8, 12))
    assert (later.delay_s, later.arrived) == (-60, _sod(8, 12))
    # Once it moves off, it runs the next trip.
    assert not _match(matcher, _ping(1500, _at(8, 18)))[BACK].waiting


def test_vehicle_off_the_route_or_far_off_schedule_gets_no_fix(matcher: live.Matcher) -> None:
    assert _match(matcher, _ping(1500, _at(8, 8), north=300)) == {}
    assert _match(matcher, _ping(1500, _at(9, 30))) == {}  # 84 min late at the halfway point
    assert _match(matcher, _ping(1500, _at(8, 8), brigade="4")) == {}


def test_stale_pings_are_ignored(matcher: live.Matcher) -> None:
    assert _match(matcher, _ping(1500, _at(8, 6)), now=_at(8, 8, 30)) == {}


def test_continuity_beats_a_closer_delay(matcher: live.Matcher) -> None:
    # At 08:20, 1 km east is mid-way on the way back (due 08:19); on the way out it is 16 min late. A vehicle
    # last seen on the outbound trip (5 min late at 08:07) stays on it.
    assert set(_match(matcher, _ping(500, _at(8, 7)))) == {OUT}
    assert _match(matcher, _ping(1000, _at(8, 20)))[OUT].delay_s == _sod(8, 20) - _sod(8, 4)
    fresh = live.Matcher(matcher.net, _shapes)
    assert set(_match(fresh, _ping(1000, _at(8, 20)))) == {BACK}


def test_one_vehicle_per_trip_prefers_the_one_in_service(matcher: live.Matcher) -> None:
    fixes = _match(matcher, _ping(0, _at(8, 0, 30), vehicle="1"), _ping(400, _at(8, 1), vehicle="2"))

    assert fixes[OUT].vehicle == "2"


def test_vehicle_long_past_due_at_the_terminus_is_dropped(matcher: live.Matcher) -> None:
    _match(matcher, _ping(2000, _at(8, 12)))
    assert _match(matcher, _ping(2000, _at(8, 20)))[BACK].delay_s == _sod(8, 20) - _sod(8, 15)
    # 11 min past due, 14 min after arriving: most such vehicles hand over their duty. It stays dropped.
    assert _match(matcher, _ping(2000, _at(8, 26))) == {}
    assert _match(matcher, _ping(2000, _at(8, 27))) == {}


def test_shape_projection_finds_each_pass() -> None:
    # Out 1 km east and back along the same street.
    lon = [21.01 + 100 * i * METRE_LON for i in [*range(11), *range(9, -1, -1)]]
    shape = live.Shape([LAT] * len(lon), lon, [None] * len(lon))
    x, y = live.to_metres(LAT + 20 / live.M_PER_DEG_LAT, 21.01 + 250 * METRE_LON)

    assert [(round(along), round(off)) for along, off in shape.project(x, y)] == [(250, 20), (1750, 20)]


def test_feed_rows_are_validated_one_by_one() -> None:
    payload = {
        "version": 1,
        "updated_at": "2026-09-23T06:00:00Z",
        "columns": list(live.COLUMNS),
        "vehicles": [
            ["bus", "175", "003", "1001", 52.2, 21.0, 1790000000],
            ["metro", "M1", "1", "1", 52.2, 21.0, 1790000000],
            ["tram", "17", "", "2", 52.2, 21.0, 1790000000],
            ["tram", "17", "1", "3", "52.2", 21.0, 1790000000],
            ["tram", "17", "1", "4", float("nan"), 21.0, 1790000000],
            ["tram", "17", "00", "5", 52.2, 21.0, 1790000000.5],
        ],
    }
    parsed = live.parse_feed(payload)

    assert parsed is not None
    assert [(p.vehicle, p.brigade) for p in parsed[1]] == [("1001", "3"), ("5", "0")]
    assert live.parse_feed({**payload, "version": 2}) is None
    assert live.parse_feed({**payload, "columns": ["mode"]}) is None


def test_feed_expanding_past_the_limit_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(live, "FEED_MAX_JSON_BYTES", 100)
    with pytest.raises(ValueError, match="limit"):
        live.gunzip(gzip.compress(b" " * 1000))


@pytest.fixture
def published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Network]:
    path = tmp_path / "planner.duckdb"
    east = [21.01 + 100 * i * METRE_LON for i in range(21)]
    with duckdb.connect(str(path)) as connection:
        _write(connection, _duty())
        connection.execute(
            "create table planner_shape as select * from (values ('E', ?::double[]), ('W', ?::double[])) t(shape_id, lon)",
            [east, east[::-1]],
        )
        connection.execute(
            "create or replace table planner_shape as select shape_id, list_transform(lon, x -> ?::double) as lat, "
            "lon, range(0, 2100, 100)::integer[] as dist_m from planner_shape",
            [LAT],
        )
    monkeypatch.setenv(live.FILE_ENV, str(tmp_path / "vehicles.json.gz"))
    live.clear_cache()
    return path, journey.network(path, DAY)


def _publish(path: Path, updated_at: datetime, *pings: live.Ping) -> None:
    rows = [[p.mode, p.line, p.brigade, p.vehicle, p.lat, p.lon, p.time] for p in pings]
    payload = {"version": 1, "updated_at": updated_at.astimezone(UTC).isoformat(), "columns": list(live.COLUMNS),
               "vehicles": rows}  # fmt: skip
    path.write_bytes(gzip.compress(json.dumps(payload).encode()))


def test_current_matches_the_published_feed_for_today_only(published: tuple[Path, Network], tmp_path: Path) -> None:
    path, net = published
    _publish(tmp_path / "vehicles.json.gz", _at(8, 8), _ping(1500, _at(8, 8)))

    now = _at(8, 8, 20)
    found = live.current(path, net, DAY, now)
    assert found is not None
    assert [net.trip_keys[t] for t in found.fixes] == [OUT]
    assert found.age_s(now) == (now - _at(8, 8)).total_seconds()
    assert live.current(path, net, DAY + timedelta(days=1), _at(8, 8, 20)) is None


def test_stale_or_missing_feed_means_no_live_data(
    published: tuple[Path, Network], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, net = published
    _publish(tmp_path / "vehicles.json.gz", _at(8, 0), _ping(0, _at(8, 0)))
    assert live.current(path, net, DAY, _at(8, 3)) is None
    live.clear_cache()
    (tmp_path / "vehicles.json.gz").unlink()
    assert live.current(path, net, DAY, _at(8, 0, 5)) is None
    monkeypatch.delenv(live.FILE_ENV)
    assert not live.enabled()
