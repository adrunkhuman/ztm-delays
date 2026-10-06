from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import pytest
from google.api_core.exceptions import Forbidden, NotFound

from ztm_frontend import live_status, queries
from ztm_frontend.app import create_app

if TYPE_CHECKING:
    from flask.testing import FlaskClient

START = datetime(2026, 10, 5, 15, 0, tzinfo=UTC)
NOW = START + timedelta(hours=12, minutes=30)  # index 750, inside the series
MINUTES = 1440
LOOKBACK_WEEKS = 4
BUS_USUAL = 1000
TWICE = 2


def _iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def history_payload(start: datetime = START, **overrides: object) -> dict[str, Any]:
    def mode(usual: float, fresh: float) -> dict[str, Any]:
        return {
            "status": "healthy",
            "baseline_samples": 4,
            "fresh": [fresh] * MINUTES,
            "usual": [usual] * (MINUTES + 180),
            "low": [usual * 0.9] * (MINUTES + 180),
            "high": [usual * 1.1] * (MINUTES + 180),
        }

    payload = {
        "version": 1,
        "evaluated_at": _iso(start + timedelta(hours=24, minutes=25)),
        "hour_start": _iso(start + timedelta(hours=23)),
        "series_start": _iso(start),
        "history_minutes": MINUTES,
        "rules": {"threshold": 0.5, "duration_minutes": 15, "minimum_fleet": 20, "lookback_days": 28},
        "vehicle_types": {"bus": mode(1000, 1000), "tram": mode(200, 200)},
        "incidents": [],
    }
    return payload | overrides


def live_payload(now: datetime = NOW, *, age: int = 20, bus: dict | None = None, tram: dict | None = None) -> dict:
    def mode(fresh: int, lines: int) -> dict[str, Any]:
        return {
            "last_attempt_at": _iso(now),
            "last_success_at": _iso(now),
            "consecutive_failures": 0,
            "fresh_vehicles": fresh,
            "fresh_lines": lines,
        }

    return {
        "version": 1,
        "updated_at": _iso(now - timedelta(seconds=age)),
        "poll_interval_seconds": 10.0,
        "heartbeat_interval_seconds": 60.0,
        "vehicle_types": {"bus": mode(1000, 200) | (bus or {}), "tram": mode(200, 20) | (tram or {})},
    }


def derive(live: dict | None, history: dict | None = None, now: datetime = NOW) -> dict[str, Any]:
    return live_status.build_live(
        live_status.parse_live(live) if live else None,
        live_status.parse_history(history) if history else None,
        now,
    )


class FakeBlob:
    def __init__(self, store: FakeStore, name: str) -> None:
        self.store, self.name = store, name

    def download_as_bytes(self, **kwargs: int) -> bytes:
        self.store.calls.append((self.name, kwargs))
        value = self.store.objects.get(self.name, NotFound("missing"))
        if isinstance(value, Exception):
            raise value
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        return data[: kwargs["end"] + 1]  # GCS ranges are inclusive


class FakeBucket:
    def __init__(self, store: FakeStore, name: str) -> None:
        self.store = store
        self.store.buckets.append(name)

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.store, name)


class FakeStore:
    """Stands in for google.cloud.storage.Client at the boundary the module uses."""

    def __init__(self, objects: dict[str, Any]) -> None:
        self.objects, self.calls, self.buckets = objects, [], []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self, name)


@pytest.fixture
def gcs(monkeypatch: pytest.MonkeyPatch) -> FakeStore:
    store = FakeStore({live_status.LIVE_OBJECT: live_payload(), live_status.HISTORY_OBJECT: history_payload()})
    monkeypatch.setenv(live_status.BUCKET_ENV, "status-bucket")
    monkeypatch.setattr(live_status, "_client", lambda: store)
    return store


def mode_of(view: dict[str, Any], mode: str = "bus") -> dict[str, Any]:
    return view["modes"][mode]


# --- derived state ------------------------------------------------------------------------------


def test_normal_state_and_online_pill() -> None:
    view = derive(live_payload(), history_payload())
    assert view["overall"] == ("online", "all systems normal")
    assert (mode_of(view)["state"], mode_of(view)["tone"]) == ("normal", "")
    assert (mode_of(view)["usual"], mode_of(view)["low"], mode_of(view)["high"]) == (1000, 900, 1100)
    assert mode_of(view)["meter"]["now"] is not None
    assert view["weeks"] == LOOKBACK_WEEKS


def test_thin_feed_is_a_problem() -> None:
    view = derive(live_payload(bus={"fresh_vehicles": 499}), history_payload())
    assert (mode_of(view)["state"], mode_of(view)["tone"]) == ("thin feed", "problem")
    assert mode_of(view, "tram")["state"] == "normal"
    assert view["overall"] == ("problems", "feed problems")


def test_exactly_half_of_usual_is_not_thin() -> None:
    assert mode_of(derive(live_payload(bus={"fresh_vehicles": 500}), history_payload()))["state"] == "normal"


def test_night_service_is_quiet_even_with_an_empty_feed() -> None:
    history = history_payload()
    history["vehicle_types"]["tram"].update(usual=[19.0] * (MINUTES + 180), low=[15.0] * (MINUTES + 180))
    view = derive(live_payload(tram={"fresh_vehicles": 0}), history)
    assert (mode_of(view, "tram")["state"], mode_of(view, "tram")["tone"]) == ("night service", "quiet")
    assert view["overall"][0] == "online"


def test_consecutive_failures_mean_api_not_answering() -> None:
    view = derive(live_payload(tram={"consecutive_failures": 3}), history_payload())
    assert (mode_of(view, "tram")["state"], mode_of(view, "tram")["tone"]) == ("API not answering", "problem")
    assert (
        mode_of(derive(live_payload(tram={"consecutive_failures": 2}), history_payload()), "tram")["state"] == "normal"
    )
    assert view["overall"][0] == "problems"


def test_failures_outrank_night_and_thin() -> None:
    view = derive(live_payload(bus={"consecutive_failures": 5, "fresh_vehicles": 1}), history_payload())
    assert mode_of(view)["state"] == "API not answering"


def test_stale_heartbeat_is_silent_and_offline() -> None:
    view = derive(live_payload(age=181), history_payload())
    assert view["silent"]
    assert view["overall"] == ("offline", "poller offline")
    assert {m["state"] for m in view["modes"].values()} == {"poller silent"}
    assert derive(live_payload(age=180), history_payload())["overall"][0] == "online"


def test_missing_heartbeat_is_offline() -> None:
    view = derive(None, history_payload())
    assert view["overall"] == ("offline", "poller offline")
    assert view["heartbeat_age"] is None


def test_no_baseline_is_answering_not_a_verdict() -> None:
    view = derive(live_payload(bus={"fresh_vehicles": 1}), None)
    assert (mode_of(view)["state"], mode_of(view)["tone"], mode_of(view)["meter"]) == ("answering", "", None)
    assert view["overall"][0] == "online"


def test_no_poll_yet_with_a_baseline() -> None:
    view = derive(live_payload(bus={"fresh_vehicles": None}), history_payload())
    assert (mode_of(view)["state"], mode_of(view)["fresh"]) == ("no data yet", None)


def test_ongoing_incident_raises_problems_even_when_the_live_feed_looks_normal() -> None:
    incident = {"mode": "bus", "start_at": _iso(NOW), "end_at": _iso(NOW + timedelta(minutes=30)), "ongoing": True}
    view = derive(live_payload(), history_payload(incidents=[incident | {"reason": "low_fleet"}]))
    assert view["overall"][0] == "problems"
    closed = incident | {"reason": "low_fleet", "ongoing": False}
    assert derive(live_payload(), history_payload(incidents=[closed]))["overall"][0] == "online"


def test_rules_come_from_the_history() -> None:
    history = history_payload(rules={"threshold": 0.8, "minimum_fleet": 1000})
    assert (
        mode_of(derive(live_payload(bus={"fresh_vehicles": 790}), history_payload(rules={"threshold": 0.8})))["state"]
        == "thin feed"
    )
    assert mode_of(derive(live_payload(), history), "tram")["state"] == "night service"


# --- usual now ----------------------------------------------------------------------------------


def test_usual_now_indexes_by_minute_from_series_start() -> None:
    history = history_payload()
    history["vehicle_types"]["bus"]["usual"][750] = 1234.0
    history["vehicle_types"]["bus"]["usual"][751] = 4321.0
    parsed = live_status.parse_history(history)
    now = START + timedelta(minutes=750, seconds=59)
    assert live_status.usual_now(parsed, "bus", now) == (1234.0, 900.0, 1100.0)
    assert live_status.usual_now(parsed, "bus", now + timedelta(seconds=1))[0] == 4321.0  # type: ignore[index]  # noqa: PLR2004


def test_usual_now_is_unknown_outside_the_array_or_when_null() -> None:
    history = history_payload()
    history["vehicle_types"]["bus"]["usual"][10] = None
    parsed = live_status.parse_history(history)
    assert live_status.usual_now(parsed, "bus", START - timedelta(minutes=1)) is None
    assert live_status.usual_now(parsed, "bus", START + timedelta(minutes=10)) is None
    assert live_status.usual_now(parsed, "bus", START + timedelta(minutes=MINUTES + 179)) is not None
    assert live_status.usual_now(parsed, "bus", START + timedelta(minutes=MINUTES + 180)) is None
    assert live_status.usual_now(None, "bus", NOW) is None


# --- validation ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"version": 2, "updated_at": "2026-10-06T10:00:00Z", "vehicle_types": {}},
        {"version": 1, "updated_at": "yesterday", "vehicle_types": {}},
        {"version": 1, "updated_at": "2026-10-06T10:00:00Z", "vehicle_types": []},
    ],
)
def test_malformed_live_is_rejected(payload: object) -> None:
    assert live_status.parse_live(payload) is None


def test_live_fields_are_coerced_defensively() -> None:
    payload = live_payload(bus={"fresh_vehicles": "many", "consecutive_failures": -2, "last_success_at": "nope"})
    payload["vehicle_types"]["tram"] = "broken"
    modes = live_status.parse_live(payload)["modes"]  # type: ignore[index]
    assert modes["bus"] | {} == {
        "last_success_at": None,
        "consecutive_failures": 0,
        "fresh_vehicles": None,
        "fresh_lines": 200.0,
    }
    assert modes["tram"]["fresh_vehicles"] is None


def test_history_rejects_inconsistent_series() -> None:
    short = history_payload()
    short["vehicle_types"]["bus"]["fresh"].pop()
    no_lookahead = history_payload()
    no_lookahead["vehicle_types"]["tram"]["usual"] = [1.0] * MINUTES + []
    no_lookahead["vehicle_types"]["tram"]["usual"].pop()
    uneven = history_payload()
    uneven["vehicle_types"]["bus"]["low"].pop()
    texty = history_payload()
    texty["vehicle_types"]["bus"]["fresh"][3] = "12"
    missing_mode = history_payload()
    del missing_mode["vehicle_types"]["tram"]
    for bad in (
        short,
        no_lookahead,
        uneven,
        texty,
        missing_mode,
        history_payload(version=3),
        history_payload(history_minutes=True),
    ):
        assert live_status.parse_history(bad) is None


def test_history_drops_only_bad_incidents() -> None:
    good = {
        "mode": "bus",
        "start_at": "2026-10-06T02:00:00Z",
        "end_at": "2026-10-06T03:00:00Z",
        "reason": "no_accepted",
    }
    bad = [
        {**good, "mode": "metro"},
        {**good, "reason": "weather"},
        {**good, "end_at": good["start_at"]},
        {**good, "start_at": None},
        "text",
    ]
    parsed = live_status.parse_history(history_payload(incidents=[*bad, good]))
    assert [i["reason"] for i in parsed["incidents"]] == ["no_accepted"]  # type: ignore[index]


# --- reader: bucket, cache, bounds, failures ----------------------------------------------------


def test_feature_is_off_without_a_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode() -> None:
        raise AssertionError("no client without a bucket")

    monkeypatch.setattr(live_status, "_client", explode)
    assert live_status.live_view() == {"available": False}
    assert live_status.history_view() == {"available": False}


def test_reads_both_objects_with_bounds_and_timeout(gcs: FakeStore) -> None:
    view = live_status.live_view(NOW)
    assert view["available"]
    assert gcs.buckets == ["status-bucket", "status-bucket"]
    calls = dict(gcs.calls)
    assert calls[live_status.LIVE_OBJECT]["end"] == live_status.LIVE_MAX_BYTES
    assert calls[live_status.HISTORY_OBJECT]["end"] == live_status.HISTORY_MAX_BYTES
    assert all(c["timeout"] == live_status.READ_TIMEOUT_SECONDS for c in calls.values())


def test_objects_are_cached_for_their_own_ttl(gcs: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [1000.0]
    monkeypatch.setattr(live_status.time, "monotonic", lambda: clock[0])

    def reads() -> list[str]:
        return [name for name, _ in gcs.calls]

    live_status.live_view(NOW)
    live_status.live_view(NOW)
    assert sorted(reads()) == [live_status.HISTORY_OBJECT, live_status.LIVE_OBJECT]
    clock[0] += live_status.LIVE_TTL_SECONDS
    live_status.live_view(NOW)
    assert reads().count(live_status.LIVE_OBJECT) == TWICE
    assert reads().count(live_status.HISTORY_OBJECT) == 1
    clock[0] += live_status.HISTORY_TTL_SECONDS
    live_status.history_view()
    assert reads().count(live_status.HISTORY_OBJECT) == TWICE


def test_client_is_created_once(monkeypatch: pytest.MonkeyPatch) -> None:
    created = []
    store = FakeStore({live_status.LIVE_OBJECT: live_payload(), live_status.HISTORY_OBJECT: history_payload()})
    monkeypatch.setenv(live_status.BUCKET_ENV, "status-bucket")
    monkeypatch.setattr(live_status.storage, "Client", lambda: created.append(1) or store)
    assert created == []  # lazy
    live_status.live_view(NOW)
    live_status._live.fetched_at = None  # noqa: SLF001
    live_status._history.fetched_at = None  # noqa: SLF001
    live_status.live_view(NOW)
    assert created == [1]


def test_oversized_object_degrades_before_decoding(gcs: FakeStore) -> None:
    gcs.objects[live_status.LIVE_OBJECT] = b" " * (live_status.LIVE_MAX_BYTES + 10)
    assert live_status.live_view(NOW) == {"available": False}


@pytest.mark.parametrize("raw", [b"not json", b"[]", b"", b'{"version": 1}'])
def test_garbage_live_object_is_unavailable_not_offline(gcs: FakeStore, raw: bytes) -> None:
    gcs.objects[live_status.LIVE_OBJECT] = raw
    assert live_status.live_view(NOW) == {"available": False}


def test_missing_live_object_means_poller_offline(gcs: FakeStore) -> None:
    del gcs.objects[live_status.LIVE_OBJECT]
    view = live_status.live_view(NOW)
    assert view["available"]
    assert view["overall"] == ("offline", "poller offline")


def test_bad_history_leaves_the_live_panel_working(gcs: FakeStore) -> None:
    gcs.objects[live_status.HISTORY_OBJECT] = b"{"
    view = live_status.live_view(NOW)
    assert mode_of(view)["state"] == "answering"
    assert live_status.history_view() == {"available": False}


def test_read_error_without_a_previous_value_is_unavailable(gcs: FakeStore) -> None:
    gcs.objects[live_status.LIVE_OBJECT] = Forbidden("no access")
    assert live_status.live_view(NOW) == {"available": False}


def test_transient_error_keeps_the_last_good_heartbeat_but_persistent_errors_are_unavailable(
    gcs: FakeStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [1000.0]
    monkeypatch.setattr(live_status.time, "monotonic", lambda: clock[0])
    assert live_status.live_view(NOW)["overall"][0] == "online"
    gcs.objects[live_status.LIVE_OBJECT] = Forbidden("blip")
    clock[0] += live_status.LIVE_TTL_SECONDS
    assert live_status.live_view(NOW + timedelta(seconds=30))["overall"][0] == "online"
    # Our own credentials failing for minutes must not read as a stopped poller.
    clock[0] += 2 * live_status.LIVE_TTL_SECONDS + live_status.SILENT_AFTER_SECONDS
    assert live_status.live_view(NOW + timedelta(minutes=5)) == {"available": False}


def test_poller_silence_with_working_reads_is_offline(gcs: FakeStore) -> None:
    gcs.objects[live_status.LIVE_OBJECT] = json.dumps(live_payload(NOW - timedelta(minutes=5))).encode()
    assert live_status.live_view(NOW)["overall"] == ("offline", "poller offline")


def test_stale_history_is_flagged_and_its_ongoing_incidents_are_not_trusted(gcs: FakeStore) -> None:
    end = START + timedelta(minutes=MINUTES)
    later = end + live_status.HISTORY_STALE_AFTER + timedelta(minutes=1)
    ongoing = {
        "mode": "bus",
        "start_at": _iso(end - timedelta(hours=1)),
        "end_at": _iso(end),
        "reason": "no_accepted",
        "ongoing": True,
    }
    gcs.objects[live_status.HISTORY_OBJECT] = json.dumps(history_payload(incidents=[ongoing])).encode()
    gcs.objects[live_status.LIVE_OBJECT] = json.dumps(live_payload(end + timedelta(hours=1))).encode()
    assert live_status.live_view(end + timedelta(hours=1))["overall"][0] == "problems"
    assert live_status.history_view(end + timedelta(hours=1))["stale_since"] is None
    gcs.objects[live_status.LIVE_OBJECT] = json.dumps(live_payload(later)).encode()
    live_status.clear_cache()
    assert live_status.live_view(later)["overall"][0] == "online"
    assert live_status.history_view(later)["stale_since"] == end.astimezone(live_status.WARSAW)


@pytest.mark.parametrize(
    ("rules", "expected"),
    [
        (
            {"threshold": 1e308, "minimum_fleet": 10**9, "duration_minutes": -5, "lookback_days": 9999},
            live_status.DEFAULT_RULES,
        ),
        (
            {"threshold": 0.4, "minimum_fleet": 30, "duration_minutes": 10, "lookback_days": 21},
            {"threshold": 0.4, "minimum_fleet": 30, "duration_minutes": 10, "lookback_days": 21},
        ),
    ],
)
def test_published_rules_are_kept_only_within_monitor_bounds(rules: dict[str, Any], expected: dict[str, Any]) -> None:
    parsed = live_status.parse_history(history_payload(rules=rules))
    assert parsed is not None
    assert parsed["rules"] == expected


def test_history_that_cannot_be_drawn_is_unavailable_not_an_error(gcs: FakeStore) -> None:
    gcs.objects[live_status.HISTORY_OBJECT] = json.dumps(history_payload(series_start="9999-12-31T23:00:00Z")).encode()
    assert live_status.history_view(NOW) == {"available": False}


# --- history view -------------------------------------------------------------------------------


def incident(mode: str, start: datetime, minutes: int, reason: str, *, ongoing: bool = False) -> dict[str, Any]:
    return {
        "mode": mode,
        "start_at": _iso(start),
        "end_at": _iso(start + timedelta(minutes=minutes)),
        "reason": reason,
        "ongoing": ongoing,
    }


def test_incident_reasons_in_plain_words() -> None:
    history = history_payload(start=START)
    low_start = START + timedelta(hours=2)
    bus = history["vehicle_types"]["bus"]
    bus["fresh"][120:180] = [300.0] * 60  # 30% of the usual 1000
    history["incidents"] = [
        incident("bus", low_start, 60, "low_fleet"),
        incident("tram", START + timedelta(hours=5), 30, "no_accepted"),
        incident("bus", START - timedelta(days=3), 40, "low_fleet"),  # before the series: no share
        incident("tram", START + timedelta(hours=8), 20, "api_failures"),
        incident("bus", START + timedelta(hours=8), 20, "api_failures"),
        incident("bus", START + timedelta(hours=20), 45, "no_accepted", ongoing=True),
    ]
    rows = live_status.build_history(live_status.parse_history(history), NOW)["incidents"]  # type: ignore[arg-type]
    by_what = {r["what"]: r for r in rows}
    assert set(by_what) == {
        "30% of usual bus fleet",
        "no trams in feed",
        "below half of usual bus fleet",
        "API not answering",
        "no buses in feed",
    }
    assert by_what["30% of usual bus fleet"]["fresh"] == 300.0  # noqa: PLR2004
    assert by_what["30% of usual bus fleet"]["usual"] == BUS_USUAL
    assert by_what["below half of usual bus fleet"]["fresh"] is None
    assert by_what["API not answering"]["mode"] == "all"  # both modes, one event
    assert by_what["API not answering"]["gap"]
    assert by_what["no buses in feed"]["ongoing"]
    assert rows[0]["start"] > rows[-1]["start"]  # newest first
    assert rows[0]["start"].tzname() in {"CEST", "CET"}


def test_a_nearly_empty_low_fleet_stretch_reads_as_an_empty_feed() -> None:
    history = history_payload()
    history["vehicle_types"]["tram"]["fresh"][60:120] = [4.0] * 60  # 2% of the usual 200
    history["incidents"] = [incident("tram", START + timedelta(hours=1), 60, "low_fleet")]
    rows = live_status.build_history(live_status.parse_history(history), NOW)["incidents"]  # type: ignore[arg-type]
    assert [r["what"] for r in rows] == ["no trams in feed"]


def test_charts_cover_the_whole_series_and_break_on_gaps() -> None:
    history = history_payload()
    history["vehicle_types"]["bus"]["fresh"][100:110] = [None] * 10
    history["incidents"] = [incident("bus", START + timedelta(hours=3), 60, "low_fleet")]
    view = live_status.build_history(live_status.parse_history(history), NOW)  # type: ignore[arg-type]
    bus = view["charts"]["bus"]
    assert len(bus["data"]) == MINUTES
    assert bus["actual"].count("M") == TWICE  # the gap starts a new subpath
    assert len(bus["shades"]) == 1
    assert len(view["charts"]["tram"]["shades"]) == 0
    assert bus["band"].startswith("M")
    assert bus["band"].endswith("Z")
    assert bus["ticks_x"][0]["label"] == "18:00"  # series starts 17:00 Warsaw time; ticks every third hour
    assert view["rules"] == {"ratio": 50, "run": 15, "floor": 20, "weeks": 4}


def test_chart_handles_empty_series() -> None:
    history = history_payload()
    for mode in history["vehicle_types"].values():
        mode["fresh"] = [None] * MINUTES
        mode["usual"] = mode["low"] = mode["high"] = [None] * (MINUTES + 180)
    chart = live_status.build_history(live_status.parse_history(history), NOW)["charts"]["bus"]  # type: ignore[arg-type]
    assert chart["actual"] == ""
    assert chart["band"] == ""


# --- routes -------------------------------------------------------------------------------------


@pytest.fixture
def status_page(monkeypatch: pytest.MonkeyPatch) -> FlaskClient:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _: False)
    monkeypatch.setattr(queries, "get_status", lambda _: {"metadata": {}, "status_summary": {}, "status_days": []})
    return create_app().test_client()


def test_status_renders_quietly_with_the_feature_disabled(status_page: FlaskClient) -> None:
    response = status_page.get("/status")
    assert response.status_code == HTTPStatus.OK
    html = response.get_data(as_text=True)
    assert "live status unavailable" in html
    assert "feed history unavailable" in html
    assert 'id="status-pill"' in html
    assert "hidden" in html.split('id="status-pill"')[1].split(">")[0]
    assert "recent days" in html
    fragment = status_page.get("/status/live")
    assert fragment.status_code == HTTPStatus.OK
    assert "live status unavailable" in fragment.get_data(as_text=True)


def fresh_fixture() -> dict[str, Any]:
    """Objects timed against the wall clock so 'now' lands inside the series."""
    now = datetime.now(UTC)
    start = (now - timedelta(hours=12)).replace(minute=0, second=0, microsecond=0)
    history = history_payload(start)
    history["incidents"] = [
        incident("bus", start + timedelta(hours=2), 90, "low_fleet"),
        incident("tram", start + timedelta(hours=4), 30, "no_accepted"),
    ]
    return {live_status.LIVE_OBJECT: live_payload(now, age=5), live_status.HISTORY_OBJECT: history}


def test_status_renders_live_panel_charts_and_incidents(gcs: FakeStore, status_page: FlaskClient) -> None:
    gcs.objects.update(fresh_fixture())
    html = status_page.get("/status").get_data(as_text=True)
    assert "all systems normal" in html
    assert 'class="status-pill online"' in html
    assert html.count('class="status-chart"') == TWICE
    assert "<svg" in html
    assert "Warsaw time" in html
    assert "no trams in feed" in html
    assert "of usual bus fleet" in html
    assert "poller offline" not in html
    assert "status.js" in html
    assert "<style" not in html.split("</head>")[1]
    fragment = status_page.get("/status/live")
    assert fragment.headers["Cache-Control"] == "no-store"
    assert 'data-overall="online"' in fragment.get_data(as_text=True)


def test_status_pill_reflects_a_thin_feed(gcs: FakeStore, status_page: FlaskClient) -> None:
    objects = fresh_fixture()
    objects[live_status.LIVE_OBJECT]["vehicle_types"]["bus"]["fresh_vehicles"] = 100
    gcs.objects.update(objects)
    html = status_page.get("/status/live").get_data(as_text=True)
    assert 'data-overall="problems"' in html
    assert "thin feed" in html


@pytest.mark.parametrize("bad", [b"{", b"[]", b"", Forbidden("denied")])
def test_status_survives_any_bad_object(gcs: FakeStore, status_page: FlaskClient, bad: object) -> None:
    gcs.objects.update({live_status.LIVE_OBJECT: bad, live_status.HISTORY_OBJECT: bad})
    for path in ("/status", "/status/live"):
        response = status_page.get(path)
        assert response.status_code == HTTPStatus.OK
        assert "live status unavailable" in response.get_data(as_text=True)
