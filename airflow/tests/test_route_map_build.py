from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import route_map_build as build


def test_month_dates_and_expected_service_dates() -> None:
    assert build.previous_month(date(2026, 1, 2)) == "2025-12"
    assert len(build.month_dates("2026-02")) == 28
    assert len(build.month_dates("2026-07")) == 31
    with pytest.raises(ValueError, match="YYYY-MM"):
        build.month_dates("2026-13")

    expected = build.expected_service_dates("2026-07", date(2026, 6, 27), [date(2026, 7, 5), date(2026, 7, 6)])
    assert len(expected) == 29
    assert date(2026, 7, 5) not in expected
    assert build.expected_service_dates("2026-06", date(2026, 6, 27), [])[0] == date(2026, 6, 27)


def _extract(dates: list[str], weekday_daytime: int = 3) -> dict[str, list[dict[str, Any]]]:
    coverage = [
        {"service_date": day, "period": "weekday", "daytime_usable_pairs": 0, "outside_usable_pairs": 0} for day in dates
    ]
    coverage[0]["daytime_usable_pairs"] = weekday_daytime
    coverage[0]["outside_usable_pairs"] = 1
    segments = [
        {"period": "weekday", "time_window": "daytime", "observation_count": 3},
        {"period": "weekday", "time_window": "outside", "observation_count": 1},
    ]
    return {"segments": segments, "shapes": [{"gtfs_snapshot_id": "s", "shape_id": "a"}], "coverage": coverage}


def test_parse_and_validate_extract_reconcile_traversals_with_coverage() -> None:
    rows = [("segment", '{"a": 1}'), ("shape", '{"b": 2}'), ("coverage", '{"c": 3}')]
    assert build.parse_extract(rows) == {"segments": [{"a": 1}], "shapes": [{"b": 2}], "coverage": [{"c": 3}]}
    with pytest.raises(ValueError, match="kind"):
        build.parse_extract([("other", "{}")])

    days = [date(2026, 9, 1), date(2026, 9, 2)]
    build.validate_extract(_extract(["2026-09-01", "2026-09-02"]), days)
    with pytest.raises(ValueError, match=r"missing \['2026-09-02'\]"):
        build.validate_extract(_extract(["2026-09-01"]), days)
    with pytest.raises(ValueError, match="weekday/daytime"):
        build.validate_extract(_extract(["2026-09-01", "2026-09-02"], weekday_daytime=4), days)


def test_check_mapped_share_rejects_unusual_geometry_loss() -> None:
    def report(mapped: int) -> dict[str, Any]:
        return {"time_windows": {"daytime": {"periods": {"weekday": {"input_traversals": 100, "mapped_traversals": mapped}}}}}

    build.check_mapped_share(report(95))
    with pytest.raises(ValueError, match="only 94 of 100"):
        build.check_mapped_share(report(94))


def _feature(feature_id: int, coords: list[list[float]], stats: dict[str, Any]) -> dict[str, Any]:
    return {"id": feature_id, "geometry": {"type": "LineString", "coordinates": coords}, "properties": {"daytime_stats": stats}}


def _stats(count: int, delta: float, lines: list[str]) -> dict[str, Any]:
    return {
        "observation_count": count,
        "mean_delta_seconds": delta,
        "gain_count": count,
        "recovery_count": 0,
        "lines": lines,
        "endpoints": ["Alpha 01 → Beta 02"],
    }


def test_site_files_keep_popup_fields_server_side_and_drop_sparse_corridors() -> None:
    geojson = {
        "features": [
            _feature(0, [[21.0, 52.2], [21.01, 52.21]], {"weekday": {"bus": _stats(150, 42.04, ["N01", "105"])}}),
            _feature(1, [[21.02, 52.22], [21.03, 52.23]], {"weekend": {"tram": _stats(120, -12.0, ["4"])}}),
            _feature(2, [[21.04, 52.24], [21.05, 52.25]], {"weekday": {"bus": _stats(99, 80.0, ["111"])}}),
        ]
    }
    files = build.site_files(geojson, {"issues": [1], "exclusions": {}}, "2026-09")

    assert set(files) == set(build.PUBLISHED_FILES)
    routes = json.loads(files["routes.geojson"])
    assert [feature["properties"] for feature in routes["features"]] == [{"wd_bus_d": 42.0}, {"we_tram_d": -12.0}]
    segments = json.loads(files["segments.json"])
    assert segments["segments"]["0"]["bus"] == ["105", "N01"]
    assert segments["segments"]["0"]["wd_bus_n"] == 150
    assert segments["meta"]["anchor"] == "2026-09-30"
    assert segments["meta"]["totals"]["weekend"]["tram"] == {"corridors": 1, "traversals": 120}
    assert segments["meta"]["tram_bounds"] == [[21.02, 52.22], [21.03, 52.23]]
    assert "issues" not in json.loads(files["report.json"])
    assert files["mini-bus.svg"].startswith("<svg")
    assert "#d84a45" in files["mini-bus.svg"]


def test_publish_replaces_month_atomically(tmp_path: Path) -> None:
    files = {name: name for name in build.PUBLISHED_FILES}
    target = build.publish(tmp_path, "2026-09", files)
    assert (target / "segments.json").read_text() == "segments.json"
    assert oct(target.stat().st_mode & 0o777) == "0o755"

    build.publish(tmp_path, "2026-09", {**files, "segments.json": "new"})
    assert (target / "segments.json").read_text() == "new"
    assert [path.name for path in tmp_path.iterdir()] == ["2026-09"]

    with pytest.raises(ValueError, match="Refusing"):
        build.publish(tmp_path, "2026-09", {"segments.json": "partial"})
    with pytest.raises(ValueError, match="Refusing"):
        build.publish(tmp_path, "../x", files)
