# ruff: noqa: PLR2004, SLF001 - fixture times and request-local detail contracts
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from flask import Flask

from tests.test_planner import DAY, _write_artifact
from ztm_frontend import db, journey, planner

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


@pytest.mark.parametrize("destination", ["2002", "4004"])
def test_search_batches_ride_details_across_cards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, destination: str
) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    calls: list[str] = []
    original = planner.fetch_all

    def recorded(path: Path, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        calls.append(sql)
        return original(path, sql, params)

    monkeypatch.setattr(planner, "fetch_all", recorded)
    app = Flask(__name__)
    app.teardown_appcontext(db.close_request_connections)
    with app.test_request_context():
        cards, _ = planner.search(path, "1001", destination, DAY, 7 * 3600)
    assert len(cards) == (1 if destination == "4004" else 2)
    assert len(calls) == 1
    assert "with requested" in calls[0]
    assert all(card["timeline"][0]["name"] == "Łomianki" for card in cards)
    assert all(
        card["timeline"][-1]["name"] == ("Centrum" if destination == "4004" else "Metro Marymont") for card in cards
    )


def test_batched_details_distinguish_sequences_and_shift_previous_service_day(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    first = journey.Ride(7, 0, 1, 4200, 4200, 4750, 4830)
    second = journey.Ride(7, 1, 2, 4800, 4800, 5250, 5430)
    results = [journey.Journey(4200, 4830, (first,)), journey.Journey(4800, 5430, (second,))]
    details = planner._CardDetails(path, results, DAY)
    assert details.ride(first)["board_scheduled"] == 4200
    assert details.ride(first)["alight_scheduled"] == 4800
    assert details.ride(first)["board_name"] == "Łomianki"
    assert details.ride(second)["board_scheduled"] == 4800
    assert details.ride(second)["alight_scheduled"] == 5400
    assert details.ride(second)["board_name"] == "Marymont-Potok"
    assert details.ride(second)["alight_name"] == "Metro Marymont"


@pytest.mark.parametrize("missing", ["trip", "board_sequence", "alight_sequence", "depart", "arrive", "arrive_late"])
def test_missing_or_incomplete_ride_retains_error(tmp_path: Path, missing: str) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    values: dict[str, Any] = {
        "trip_key": 1,
        "board_sequence": 0,
        "alight_sequence": 1,
        "board_by": 27000,
        "depart": 27060,
        "arrive": 28560,
        "arrive_late": 28800,
    }
    values["trip_key" if missing == "trip" else missing] = (
        999 if missing in {"trip", "board_sequence", "alight_sequence"} else None
    )
    leg = journey.Ride(**values)
    details = planner._CardDetails(path, [journey.Journey(27000, 28800, (leg,))], DAY)
    with pytest.raises(ValueError, match="journey leg missing from the planner artifact"):
        details.ride(leg)


def test_walk_reuse_does_not_leak_point_labels_or_leg_distances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    details = planner._CardDetails(path, [], DAY)
    calls: list[str] = []
    original = planner.fetch_one

    def recorded(path: Path, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        calls.append(sql)
        return original(path, sql, params)

    monkeypatch.setattr(planner, "fetch_one", recorded)
    first = details.walk(journey.Walk(journey.ORIGIN_POINT, "100103", 60, 90), {journey.ORIGIN_POINT: "Home"})
    second = details.walk(journey.Walk(journey.ORIGIN_POINT, "100103", 120, 180), {journey.ORIGIN_POINT: "New home"})
    assert (first["from_name"], first["distance_m"]) == ("Home", 90)
    assert (second["from_name"], second["distance_m"]) == ("New home", 180)
    assert first["to_name"] == second["to_name"] == "Łomianki"
    assert len(calls) == 1
    with pytest.raises(ValueError, match="journey walk missing from the planner artifact"):
        details.walk(journey.Walk(journey.ORIGIN_POINT, "100103", 60), {})
    assert len(calls) == 1
    # A normal artifact walk still has its original distance after another leg overrides it.
    overridden = details.walk(journey.Walk("200201", "200203", 120, 240), {})
    plain = details.walk(journey.Walk("200201", "200203", 120), {})
    assert overridden["distance_m"] == 240
    assert plain["distance_m"] == 120
    assert len(calls) == 2


def test_post_lookup_reuses_missing_and_present_rows_only_within_one_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    details = planner._CardDetails(path, [], DAY)
    calls: list[str] = []
    original = planner.fetch_one

    def recorded(path: Path, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        calls.append(sql)
        return original(path, sql, params)

    monkeypatch.setattr(planner, "fetch_one", recorded)
    assert details.post("100103") == details.post("100103") == {"lon": 20.921, "lat": 52.331}
    assert details.post("missing") is details.post("missing") is None
    assert len(calls) == 2
    fresh = planner._CardDetails(path, [], DAY)
    assert fresh.post("100103") == details.post("100103")
    assert len(calls) == 3


def test_empty_results_do_not_query_ride_details(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(*_args: object, **_kwargs: object) -> list[dict[str, Any]]:
        pytest.fail("empty results must not query ride details")

    monkeypatch.setattr(planner, "fetch_all", unexpected)
    details = planner._CardDetails(tmp_path / "missing.duckdb", [], DAY)
    assert details.rides == {}
