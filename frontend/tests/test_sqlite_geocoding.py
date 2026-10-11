"""Behavior checks using tiny maintained v1 SQLite artifacts."""

from __future__ import annotations

import json
import sqlite3
import unicodedata
from typing import TYPE_CHECKING, Any

import pytest

from ztm_frontend import sqlite_geocoding as geocoder
from ztm_frontend.sqlite_geocoding import GeocodingError

if TYPE_CHECKING:
    from pathlib import Path

_SCHEMA = """
CREATE TABLE places (
    id INTEGER PRIMARY KEY, kind TEXT, street TEXT, house TEXT, city TEXT,
    lat REAL, lon REAL, osm_id TEXT, geometry TEXT
);
CREATE VIRTUAL TABLE places_fts USING fts5(text, tokenize='unicode61', prefix='2 3 4');
CREATE VIRTUAL TABLE places_rtree USING rtree(id, min_lon, max_lon, min_lat, max_lat);
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
"""
_LAT, _LON = 52.2297, 21.0122


def _row(  # noqa: PLR0913 - explicit fields keep fixture addresses readable
    street: str = "Marszałkowska",
    house: str = "10",
    city: str = "Warszawa",
    *,
    lat: float = _LAT,
    lon: float = _LON,
    kind: str = "address",
    geometry: list[list[float]] | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "street": street,
        "house": house,
        "city": city,
        "lat": lat,
        "lon": lon,
        "osm_id": "node/1" if kind == "address" else "way/1",
        "geometry": json.dumps(geometry) if geometry is not None else None,
    }


def _build(path: Path, rows: list[dict[str, Any]], metadata: dict[str, str] | None = None) -> Path:
    with sqlite3.connect(path) as connection:
        connection.executescript(_SCHEMA)
        for index, row in enumerate(rows, start=1):
            connection.execute(
                "INSERT INTO places VALUES (:id, :kind, :street, :house, :city, :lat, :lon, :osm_id, :geometry)",
                {"id": index, **row},
            )
            text = " ".join(row[key] for key in ("street", "house", "city"))
            text = unicodedata.normalize("NFKD", text.casefold().replace("ł", "l"))
            text = "".join(char for char in text if not unicodedata.combining(char))
            connection.execute("INSERT INTO places_fts (rowid, text) VALUES (?, ?)", (index, text))
            geometry = json.loads(row["geometry"]) if row["geometry"] else [[row["lon"], row["lat"]]]
            lons, lats = [point[0] for point in geometry], [point[1] for point in geometry]
            connection.execute(
                "INSERT INTO places_rtree VALUES (?, ?, ?, ?, ?)",
                (index, min(lons), max(lons), min(lats), max(lats)),
            )
        addresses = sum(row["kind"] == "address" for row in rows)
        streets = sum(row["kind"] == "street" for row in rows)
        values = {
            "schema_version": "1",
            "status": "complete",
            "bbox": "[20.3, 51.8, 21.8, 52.7]",
            "source": str(path.resolve().parent / "source.osm"),
            "timestamp": "2026-09-23T00:00:00+00:00",
            "counts": json.dumps(
                {
                    "address": addresses,
                    "street": streets,
                    "address_nodes": addresses,
                    "address_ways": 0,
                    "street_ways": streets,
                    "invalid_address_nodes": 0,
                    "invalid_relevant_ways": 0,
                    "invalid_address_rings": 0,
                }
            ),
            "osm_attribution": "OpenStreetMap contributors, ODbL 1.0, https://www.openstreetmap.org/copyright",
            **(metadata or {}),
        }
        connection.executemany("INSERT INTO metadata VALUES (?, ?)", values.items())
    connection.close()
    return path


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return _build(tmp_path / "addresses.sqlite", [_row()])


@pytest.mark.parametrize("query", ["Marszałkowska 10", "  ul. MARSZALKOWSKA   nr 10 ", "marszałkowska 10 warsz"])
def test_normalization_and_label(database: Path, query: str) -> None:
    assert geocoder.search(database, query) == [{"name": "Marszałkowska 10, Warszawa", "lat": _LAT, "lon": _LON}]


def test_diacritics_and_prefix_search(tmp_path: Path) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row("Żółkiewskiego", "", "Łomianki", kind="street")])
    assert geocoder.search(path, "ZOLK")[0]["name"] == "Żółkiewskiego, Łomianki"
    assert geocoder.search(path, "żółkiewskiego lomi")[0]["name"] == "Żółkiewskiego, Łomianki"


@pytest.mark.parametrize("query_length", [3, 160])
def test_valid_query_length_boundaries(tmp_path: Path, query_length: int) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row("x" * 160, "", kind="street")])
    assert geocoder.search(path, "x" * query_length)[0]["name"] == "x" * 160 + ", Warszawa"


@pytest.mark.parametrize("house", ["1", "10", "10A", "12/14", "12/14B", "12-14a"])
def test_complete_house_numbers(tmp_path: Path, house: str) -> None:
    rows = [
        _row(house=number, lon=_LON + index * 0.002)
        for index, number in enumerate(["1", "10", "10A", "12/14", "12/14B", "12-14a", "100"])
    ]
    rows.append(_row(house="", kind="street"))
    path = _build(tmp_path / "addresses.sqlite", rows)
    results = geocoder.search(path, f"marszalkowska {house.casefold()}")
    assert [result["name"] for result in results] == [f"Marszałkowska {house}, Warszawa"]
    assert geocoder.search(path, f"marszalkowska {house} warszawa") == results


@pytest.mark.parametrize("number", ["12A/7;9", "12a/7", "9"])
def test_semicolon_house_number_alternatives(tmp_path: Path, number: str) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row("Testowa", "12A/7;9")])
    assert [result["name"] for result in geocoder.search(path, f"Testowa {number}")] == ["Testowa 12A/7;9, Warszawa"]
    assert geocoder.search(path, "Testowa 12") == []
    assert geocoder.search(path, "Testowa 7") == []


@pytest.mark.parametrize("street", ["3 Maja", "11 Listopada", "Aleja 1 Sierpnia", "Dywizjonu 303"])
def test_street_numbers_are_not_house_numbers(tmp_path: Path, street: str) -> None:
    rows = [
        _row(street, "", kind="street"),
        _row(street, "7", lon=_LON + 0.002),
        _row("Maja", "3"),
    ]
    path = _build(tmp_path / "addresses.sqlite", rows)
    assert geocoder.search(path, street)[0]["name"] == f"{street}, Warszawa"
    assert geocoder.search(path, f"{street} 7")[0]["name"] == f"{street} 7, Warszawa"
    if street == "3 Maja":
        assert all(result["name"] != "Maja 3, Warszawa" for result in geocoder.search(path, street))


def test_house_number_can_repeat_street_day_number(tmp_path: Path) -> None:
    path = _build(
        tmp_path / "addresses.sqlite", [_row("3 Maja", "", kind="street"), _row("3 Maja", "3"), _row("3 Maja", "30")]
    )
    assert [result["name"] for result in geocoder.search(path, "3 Maja 3")] == ["3 Maja 3, Warszawa"]


def test_town_terms_and_center_bias(tmp_path: Path) -> None:
    rows = [
        _row(city="Piaseczno", lat=52.1),
        _row(city="Łomianki", lat=52.3),
        _row(city="Warszawa"),
        _row(city="", lon=_LON + 0.02),
    ]
    path = _build(tmp_path / "addresses.sqlite", rows)
    assert geocoder.search(path, "Marszałkowska 10")[0]["name"] == "Marszałkowska 10, Warszawa"
    assert [result["name"] for result in geocoder.search(path, "Marszalkowska 10 piase")] == [
        "Marszałkowska 10, Piaseczno"
    ]
    assert [result["name"] for result in geocoder.search(path, "lomianki marszalkowska 10")] == [
        "Marszałkowska 10, Łomianki"
    ]
    assert geocoder.search(path, "marszalkowska 10 nieistniejace") == []


def test_near_duplicates_prefer_real_city_without_inventing_one(tmp_path: Path) -> None:
    rows = [_row(city=""), _row(city="Warszawa", lon=_LON + 0.0001), _row(city="Warszawa", lon=_LON + 0.0002)]
    rows.append(_row("Puławska", "2", "", lon=_LON + 0.01))
    path = _build(tmp_path / "addresses.sqlite", rows)
    assert geocoder.search(path, "Marszalkowska 10") == [
        {"name": "Marszałkowska 10, Warszawa", "lat": _LAT, "lon": _LON + 0.0001}
    ]
    assert geocoder.search(path, "pulawska 2")[0]["name"] == "Puławska 2"
    result = geocoder.reverse(path, _LAT, _LON)
    assert result is not None
    assert result["name"] == "Marszałkowska 10, Warszawa"
    assert result["distance_m"] > 0


def test_distinct_city_labels_are_not_deduplicated(tmp_path: Path) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row(city="Warszawa"), _row(city="Ząbki")])
    assert {result["name"] for result in geocoder.search(path, "marszalkowska 10")} == {
        "Marszałkowska 10, Warszawa",
        "Marszałkowska 10, Ząbki",
    }


def test_search_caps_reranking_and_results(tmp_path: Path) -> None:
    rows = [_row("Testowa", str(index), lon=_LON + index * 0.002) for index in range(300)]
    path = _build(tmp_path / "addresses.sqlite", rows)
    assert len(geocoder.search(path, "testowa")) == geocoder.MAX_RESULTS
    # The closer entry is outside the first 250 FTS matches; work is deliberately bounded.
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE places SET lon = ? WHERE id = 300", (_LON,))
    assert "Testowa 299, Warszawa" not in [result["name"] for result in geocoder.search(path, "testowa")]


@pytest.mark.parametrize("query", ["", "ab", "x" * 161, " " * 5, "!!!", "ul. nr", "abc\x00", "a\u200bbc", None, 12, []])
def test_invalid_queries_do_not_open_database(tmp_path: Path, query: Any, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ANN401 - invalid caller inputs
    def forbidden(*_args: Any, **_kwargs: Any) -> None:  # noqa: ANN401 - generic connect spy
        pytest.fail("invalid query opened SQLite")

    monkeypatch.setattr(geocoder.sqlite3, "connect", forbidden)
    assert geocoder.search(tmp_path / "missing.sqlite", query) == []


def test_fts_operator_text_is_safe(database: Path) -> None:
    for query in ('" OR * ()', 'marszalkowska" OR "other', "marszalkowska NOT 10", "***"):
        assert geocoder.search(database, query) == []


@pytest.mark.parametrize(
    ("lat", "lon"),
    [
        (float("nan"), _LON),
        (_LAT, float("inf")),
        (91, _LON),
        (_LAT, -181),
        (True, _LON),
        ("52.2", _LON),
        (None, _LON),
        (10**400, _LON),
    ],
)
def test_invalid_coordinates_do_not_open_database(
    tmp_path: Path,
    lat: Any,  # noqa: ANN401 - deliberately invalid caller inputs
    lon: Any,  # noqa: ANN401
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: Any, **_kwargs: Any) -> None:  # noqa: ANN401
        pytest.fail("invalid coordinates opened SQLite")

    monkeypatch.setattr(geocoder.sqlite3, "connect", forbidden)
    assert geocoder.reverse(tmp_path / "missing.sqlite", lat, lon) is None


@pytest.mark.parametrize(("lat", "lon"), [(90, 180), (-90, -180), (51.79, _LON), (_LAT, 21.81)])
def test_outside_import_bbox(database: Path, lat: float, lon: float) -> None:
    assert geocoder.reverse(database, lat, lon) is None


def test_bbox_rejects_even_retained_road_crossing_boundary(tmp_path: Path) -> None:
    road = _row("Graniczna", "", kind="street", lat=51.799, geometry=[[_LON - 0.01, 51.799], [_LON + 0.01, 51.799]])
    path = _build(tmp_path / "addresses.sqlite", [road])
    assert geocoder.reverse(path, 51.799, _LON) is None
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM metadata WHERE key = 'bbox'")
    with pytest.raises(GeocodingError):
        geocoder.reverse(path, 51.799, _LON)


def _road(*, offset_m: float = 0) -> dict[str, Any]:
    latitude = _LAT + offset_m / geocoder.METERS_PER_DEGREE
    return _row(
        "Długa",
        "",
        kind="street",
        lat=latitude,
        lon=_LON + 0.05,
        geometry=[[_LON - 0.01, latitude], [_LON + 0.11, latitude]],
    )


def test_reverse_projects_road_but_returns_label_metadata(tmp_path: Path) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_road(offset_m=40)])
    result = geocoder.reverse(path, _LAT, _LON)
    assert result is not None
    assert result["name"] == "Długa, Warszawa"
    assert result["lat"] == _LAT + 40 / geocoder.METERS_PER_DEGREE
    assert result["lon"] == _LON + 0.05
    assert result["distance_m"] == pytest.approx(40, abs=0.1)


def test_reverse_prefers_numbered_address_over_closer_road(tmp_path: Path) -> None:
    address = _row(lat=_LAT + 70 / geocoder.METERS_PER_DEGREE)
    path = _build(tmp_path / "addresses.sqlite", [_road(), address, _row(house="", lat=_LAT)])
    result = geocoder.reverse(path, _LAT, _LON)
    assert result is not None
    assert result["name"] == "Marszałkowska 10, Warszawa"
    assert result["distance_m"] == pytest.approx(70, abs=0.1)
    assert result["lat"] == address["lat"]


@pytest.mark.parametrize(
    ("address_m", "road_m", "expected"),
    [(79, 119, "Marszałkowska 10, Warszawa"), (81, 119, "Długa, Warszawa"), (81, 121, None)],
)
def test_reverse_thresholds(tmp_path: Path, address_m: float, road_m: float, expected: str | None) -> None:
    path = _build(
        tmp_path / "addresses.sqlite", [_road(offset_m=road_m), _row(lat=_LAT + address_m / geocoder.METERS_PER_DEGREE)]
    )
    result = geocoder.reverse(path, _LAT, _LON)
    assert (result["name"] if result else None) == expected


def test_reverse_uses_nearest_segment_not_infinite_line(tmp_path: Path) -> None:
    geometry = [[_LON + 0.01, _LAT], [_LON + 0.02, _LAT], [_LON + 0.02, _LAT + 0.01]]
    road = _row("Zakole", "", kind="street", geometry=geometry)
    path = _build(tmp_path / "addresses.sqlite", [road])
    assert geocoder.reverse(path, _LAT, _LON) is None
    result = geocoder.reverse(path, _LAT + 0.005, _LON + 0.02)
    assert result is not None
    assert result["distance_m"] == pytest.approx(0, abs=0.01)


def test_reverse_exact_distance_not_bbox_distance(tmp_path: Path) -> None:
    address = _row(lat=_LAT + 0.001, lon=_LON + 0.001)
    path = _build(tmp_path / "addresses.sqlite", [address])
    assert geocoder.reverse(path, _LAT, _LON) is None


def test_reverse_chooses_closest_valid_address(tmp_path: Path) -> None:
    path = _build(
        tmp_path / "addresses.sqlite", [_row(house="10", lat=_LAT + 0.0005), _row(house="20", lat=_LAT + 0.0002)]
    )
    result = geocoder.reverse(path, _LAT, _LON)
    assert result is not None
    assert result["name"] == "Marszałkowska 20, Warszawa"


def test_reverse_candidate_overflow_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row(), _row(house="20")])
    monkeypatch.setattr(geocoder, "REVERSE_CANDIDATES", 1)
    assert geocoder.reverse(path, _LAT, _LON) is None


def test_maintained_version_one(database: Path) -> None:
    assert geocoder.search(database, "marszalkowska 10")
    assert geocoder.reverse(database, _LAT, _LON)


@pytest.mark.parametrize(
    "key", ["schema_version", "status", "bbox", "source", "timestamp", "counts", "osm_attribution"]
)
def test_missing_required_metadata_is_rejected(database: Path, key: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM metadata WHERE key = ?", (key,))
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(database, _LAT, _LON)


def test_unversioned_prototype_is_rejected(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM metadata WHERE key NOT IN ('status', 'bbox')")
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(database, _LAT, _LON)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("schema_version", "0"),
        ("schema_version", "2"),
        ("schema_version", ""),
        ("status", "building"),
        ("bbox", "garbage"),
        ("bbox", "[21,52]"),
        ("bbox", "[22,53,21,52]"),
        ("bbox", "[20,NaN,22,53]"),
        ("bbox", "[true,52,22,53]"),
        ("bbox", "[20,52,20,53]"),
        ("bbox", "[20,52,22,52]"),
        ("bbox", "[20,52,181,53]"),
        ("bbox", '{"west": 20, "south": 52, "east": 22, "north": 53}'),
        ("source", ""),
        ("timestamp", " "),
        ("counts", ""),
        ("osm_attribution", ""),
    ],
)
def test_bad_metadata_raises_geocoding_error(database: Path, key: str, value: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", (key, value))
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(database, _LAT, _LON)


@pytest.mark.parametrize("table", ["places", "places_fts", "places_rtree", "metadata"])
def test_missing_schema_raises_geocoding_error(database: Path, table: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(f"DROP TABLE {table}")  # fixed test-only identifiers
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(database, _LAT, _LON)


@pytest.mark.parametrize("table", ["places_fts", "places_rtree"])
def test_nonvirtual_indexes_are_schema_errors(database: Path, table: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(f"DROP TABLE {table}")  # fixed test-only identifiers
        if table == "places_fts":
            connection.execute("CREATE TABLE places_fts (text TEXT)")
        else:
            connection.execute("CREATE TABLE places_rtree (id, min_lon, max_lon, min_lat, max_lat)")
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(database, _LAT, _LON)


@pytest.mark.parametrize("column", ["geometry", "osm_id"])
def test_missing_required_column_is_schema_error(database: Path, column: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(f"ALTER TABLE places DROP COLUMN {column}")  # fixed test-only identifiers
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(database, _LAT, _LON)


@pytest.mark.parametrize("content", [None, b"", b"not a SQLite database"])
def test_missing_or_malformed_database(tmp_path: Path, content: bytes | None) -> None:
    path = tmp_path / "missing.sqlite"
    if content is not None:
        path.write_bytes(content)
    with pytest.raises(GeocodingError):
        geocoder.search(path, "marszalkowska 10")
    with pytest.raises(GeocodingError):
        geocoder.reverse(path, _LAT, _LON)
    if content is None:
        assert not path.exists()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("lat", None),
        ("lat", "wrong"),
        ("lat", float("inf")),
        ("lon", 181),
        ("kind", "town"),
        ("street", None),
        ("street", ""),
        ("street", b"binary"),
        ("house", None),
        ("city", b"binary"),
        ("city", "x" * 513),
    ],
)
def test_malformed_rows_are_skipped(database: Path, column: str, value: object) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(f"UPDATE places SET {column} = ?", (value,))  # noqa: S608 - fixed test-only identifiers
    assert geocoder.search(database, "marszalkowska 10") == []
    assert geocoder.reverse(database, _LAT, _LON) is None


@pytest.mark.parametrize(
    "geometry",
    [
        None,
        "",
        "junk",
        "{}",
        "[]",
        "[[21,52]]",
        "[[21,52],[21]]",
        '[[21,52],["21",52]]',
        "[[21,52],[NaN,52]]",
        "[[21,52],[21,91]]",
        "[" * 1100,
        " " * 65537,
    ],
)
def test_bad_road_geometry_never_uses_midpoint(tmp_path: Path, geometry: str | None) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_road()])
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE places SET geometry = ?, lat = ?, lon = ?", (geometry, _LAT, _LON))
    assert geocoder.reverse(path, _LAT, _LON) is None


def test_database_lock_does_not_wait(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        with pytest.raises(GeocodingError):
            geocoder.search(database, "marszalkowska 10")


def test_readonly_uri_pragmas_and_connection_lifetime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _build(tmp_path / "żółć ?#%& addresses.sqlite", [_row()])
    original = sqlite3.connect
    connections: list[sqlite3.Connection] = []
    statements: list[str] = []
    uris: list[str] = []

    def connect(database: str, **kwargs: Any) -> sqlite3.Connection:  # noqa: ANN401 - SQLite connect wrapper
        assert kwargs["uri"] is True
        assert kwargs["timeout"] == 0
        uris.append(database)
        connection = original(database, **kwargs)
        connection.set_trace_callback(statements.append)
        connections.append(connection)
        return connection

    monkeypatch.setattr(geocoder.sqlite3, "connect", connect)
    before = path.read_bytes()
    assert geocoder.search(path, "marszalkowska 10")
    assert geocoder.reverse(path, _LAT, _LON)
    assert len(connections) == 2  # noqa: PLR2004
    assert all(uri == path.resolve().as_uri() + "?mode=ro" for uri in uris)
    assert "PRAGMA query_only=ON" in statements
    assert "PRAGMA cache_size=-4096" in statements
    assert "BEGIN" in statements
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_error_closes_connection(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM metadata WHERE key = 'status'")
    original = sqlite3.connect
    connections = []

    def connect(path: str, **kwargs: Any) -> sqlite3.Connection:  # noqa: ANN401
        connection = original(path, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(geocoder.sqlite3, "connect", connect)
    with pytest.raises(GeocodingError):
        geocoder.search(database, "marszalkowska 10")
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")


def test_atomic_replace_keeps_snapshot_and_next_call_sees_new_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _build(tmp_path / "addresses.sqlite", [_row()])
    replacement = _build(tmp_path / "replacement.sqlite", [_row("Puławska", "20")])
    original = sqlite3.connect

    def replace_on_query(sql: str) -> None:
        if sql.startswith("SELECT p.*") and replacement.exists():
            replacement.replace(path)

    def connect(database: str, **kwargs: Any) -> sqlite3.Connection:  # noqa: ANN401
        connection = original(database, **kwargs)
        connection.set_trace_callback(replace_on_query)
        return connection

    monkeypatch.setattr(geocoder.sqlite3, "connect", connect)
    assert geocoder.search(path, "marszalkowska 10")[0]["name"] == "Marszałkowska 10, Warszawa"
    assert geocoder.search(path, "marszalkowska 10") == []
    assert geocoder.search(path, "pulawska 20")[0]["name"] == "Puławska 20, Warszawa"
    result = geocoder.reverse(path, _LAT, _LON)
    assert result is not None
    assert result["name"] == "Puławska 20, Warszawa"
