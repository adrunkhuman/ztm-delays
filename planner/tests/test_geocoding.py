from __future__ import annotations

import json
import math
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from ztm_planner import geocoding
from ztm_planner.cli import main

CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "geocoding_artifact_v1.json"


@pytest.fixture
def osm_xml(tmp_path: Path) -> Path:
    source = tmp_path / "tiny.osm"
    source.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="test">
  <node id="1" lon="21.0" lat="52.2">
    <tag k="addr:street" v="ŁÓDZKA Straße"/><tag k="addr:housenumber" v="12A/7;9"/>
    <tag k="addr:city" v="Ząbki"/>
  </node>
  <node id="2" lon="21.01" lat="52.21"><tag k="addr:housenumber" v="3B"/></node>
  <node id="3" lon="20.0" lat="52.2"><tag k="addr:street" v="Outside"/></node>
  <node id="4" lon="20.3" lat="51.8"><tag k="addr:street" v="Boundary"/></node>
  <node id="5"><tag k="addr:housenumber" v="Invalid"/></node>
  <node id="10" lon="21.10" lat="52.21"/>
  <node id="11" lon="21.12" lat="52.21"/>
  <node id="12" lon="21.12" lat="52.23"/>
  <node id="13" lon="21.10" lat="52.23"/>
  <node id="20" lon="20.0" lat="52.2"/>
  <node id="21" lon="22.0" lat="52.2"/>
  <node id="22" lon="20.0" lat="52.6"/>
  <node id="23" lon="20.5" lat="53.0"/>
  <node id="24" lon="21.0" lat="52.1"/>
  <node id="25" lon="21.3" lat="52.1"/>
  <way id="100">
    <nd ref="10"/><nd ref="11"/><nd ref="12"/><nd ref="13"/><nd ref="10"/>
    <tag k="building" v="yes"/><tag k="addr:street" v="Żółta"/><tag k="addr:housenumber" v="8/10"/>
  </way>
  <way id="101"><nd ref="10"/><nd ref="11"/>
    <tag k="building" v="yes"/><tag k="addr:housenumber" v="Open ring"/>
  </way>
  <way id="102"><nd ref="20"/><nd ref="21"/>
    <tag k="highway" v="primary"/><tag k="name" v="Crossing"/>
  </way>
  <way id="103"><nd ref="24"/><nd ref="99999"/><nd ref="25"/>
    <tag k="highway" v="residential"/><tag k="name" v="Missing reference"/>
  </way>
  <way id="104"><nd ref="10"/><nd ref="11"/><nd ref="10"/>
    <tag k="building" v="yes"/><tag k="addr:housenumber" v="Degenerate"/>
  </way>
  <way id="105"><nd ref="24"/><nd ref="25"/><tag k="highway" v="residential"/></way>
  <way id="106"><nd ref="10"/><nd ref="11"/>
    <tag k="addr:street" v="Not a building"/>
  </way>
  <way id="107"><nd ref="24"/>
    <tag k="highway" v="residential"/><tag k="name" v="One point"/>
  </way>
  <way id="108"><nd ref="24"/><nd ref="24"/>
    <tag k="highway" v="residential"/><tag k="name" v="Zero length"/>
  </way>
  <way id="109"><nd ref="10"/><nd ref="99999"/><nd ref="11"/><nd ref="10"/>
    <tag k="building" v="yes"/><tag k="addr:housenumber" v="Missing building reference"/>
  </way>
  <way id="110"><nd ref="22"/><nd ref="23"/>
    <tag k="highway" v="residential"/><tag k="name" v="Bounds overlap but no intersection"/>
  </way>
  <way id="111"><nd ref="24"/><nd ref="25"/>
    <tag k="highway" v="residential"/><tag k="name" v="Łąkowa"/><tag k="addr:city" v="Warszawa"/>
  </way>
  <relation id="200"><member type="way" ref="100" role="outer"/>
    <tag k="type" v="multipolygon"/><tag k="building" v="yes"/><tag k="addr:housenumber" v="Relations ignored"/>
  </relation>
</osm>
""",
        encoding="utf-8",
    )
    return source


def test_normalization_preserves_whitespace() -> None:
    assert geocoding.normalize("  ŁÓDŹ\tStraße  É\n") == "  lodz\tstrasse  e\n"
    assert geocoding.normalize("Z\u0307o\u0301łta") == "zolta"


def test_schema_search_geometry_and_metadata(osm_xml: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Exercise multiple flushes without making a large fixture.
    monkeypatch.setattr(geocoding, "BATCH_SIZE", 2)
    output = tmp_path / "artifact" / "addresses.sqlite"
    result = geocoding.build(osm_xml, output)
    assert result["records"] == 6
    assert result["addresses"] == 4
    assert result["streets"] == 2
    assert result["db_bytes"] == output.stat().st_size
    assert output.stat().st_mode & 0o777 == 0o644
    assert list(output.parent.iterdir()) == [output]
    contract = json.loads(CONTRACT.read_text())
    with sqlite3.connect(output) as connection:
        for table, columns in contract["tables"].items():
            actual = connection.execute(f"PRAGMA table_info({table})").fetchall()
            assert [c[1] for c in actual] == [c["name"] for c in columns]
            if table in {"places", "metadata"}:
                assert [c[2] for c in actual] == [c["sqlite_type"] for c in columns]
        assert connection.execute("PRAGMA table_info(places)").fetchone()[5] == 1
        index_sql = connection.execute("SELECT sql FROM sqlite_master WHERE name='places_fts'").fetchone()[0]
        assert "unicode61" in index_sql and "prefix='2 3 4'" in index_sql
        places = {
            r[0]: r[1:]
            for r in connection.execute("SELECT osm_id, kind, street, house, city, lon, lat, geometry FROM places")
        }
        assert set(places) == {"node/1", "node/2", "node/4", "way/100", "way/102", "way/111"}
        assert places["node/1"] == ("address", "ŁÓDZKA Straße", "12A/7;9", "Ząbki", 21.0, 52.2, None)
        assert places["node/2"][:4] == ("address", "", "3B", "")
        assert places["way/100"][:4] == ("address", "Żółta", "8/10", "")
        assert places["way/100"][4:6] == pytest.approx((21.11, 52.22))
        assert places["way/100"][6] is None
        assert json.loads(places["way/102"][6]) == [[20.0, 52.2], [22.0, 52.2]]
        assert places["way/102"][4:6] == pytest.approx((21.0, 52.2))
        assert places["way/111"][4:6] == pytest.approx((21.15, 52.1))
        assert connection.execute("""
            SELECT p.osm_id FROM places_fts f JOIN places p ON p.id=f.rowid
            WHERE places_fts MATCH 'lod* strasse zabki'
        """).fetchall() == [("node/1",)]
        assert connection.execute("""
            SELECT p.osm_id FROM places_rtree r JOIN places p ON p.id=r.id
            WHERE min_lon <= 20.1 AND max_lon >= 20.1 AND min_lat <= 52.2 AND max_lat >= 52.2
        """).fetchall() == [("way/102",)]
        for osm_id, lon, lat, lo, hi, south, north in connection.execute("""
            SELECT p.osm_id, lon, lat, min_lon, max_lon, min_lat, max_lat
            FROM places p JOIN places_rtree r ON p.id=r.id
        """):
            assert lo <= lon <= hi and south <= lat <= north, osm_id
        metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        assert set(contract["metadata"]["required_keys"]) <= metadata.keys()
        assert metadata["schema_version"] == "1" and metadata["status"] == "complete"
        assert json.loads(metadata["bbox"]) == list(geocoding.DEFAULT_BBOX)
        assert metadata["source"] == str(osm_xml.resolve())
        assert datetime.fromisoformat(metadata["timestamp"]).utcoffset() is not None
        assert "OpenStreetMap" in metadata["osm_attribution"] and "ODbL" in metadata["osm_attribution"]
        counts = json.loads(metadata["counts"])
        assert counts == {
            "address": 4,
            "street": 2,
            "address_nodes": 3,
            "address_ways": 1,
            "street_ways": 2,
            "invalid_address_nodes": 1,
            "invalid_relevant_ways": 4,
            "invalid_address_rings": 2,
        }
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)


def test_custom_bbox_and_atomic_replacement(osm_xml: Path, tmp_path: Path) -> None:
    output = tmp_path / "addresses.sqlite"
    output.write_bytes(b"previous artifact")
    geocoding.build(osm_xml, output, (20.9, 52.19, 21.005, 52.205))
    with sqlite3.connect(output) as connection:
        assert connection.execute("SELECT osm_id FROM places ORDER BY id").fetchall() == [("node/1",), ("way/102",)]
        assert json.loads(dict(connection.execute("SELECT * FROM metadata"))["bbox"]) == [20.9, 52.19, 21.005, 52.205]


@pytest.mark.parametrize("stage", ["validate", "replace"])
def test_failed_rebuild_keeps_previous_and_cleans_only_own_temp(
    osm_xml: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    output = tmp_path / "addresses.sqlite"
    geocoding.build(osm_xml, output)
    previous = output.read_bytes()
    other_build = tmp_path / ".addresses.sqlite.other.tmp"
    other_build.write_bytes(b"another build")

    def fail_validation(connection: sqlite3.Connection, counts: object) -> None:
        path = Path(connection.execute("PRAGMA database_list").fetchone()[2])
        assert path != output and path.parent == output.parent
        assert output.read_bytes() == previous
        raise ValueError("injected validation failure")

    def fail_replace(source: Path, destination: Path) -> None:
        assert source != output and destination == output
        assert source.stat().st_mode & 0o777 == 0o644
        assert output.read_bytes() == previous
        raise OSError("injected publication failure")

    if stage == "validate":
        monkeypatch.setattr(geocoding, "_validate", fail_validation)
    else:
        monkeypatch.setattr(geocoding.os, "replace", fail_replace)
    with pytest.raises((ValueError, OSError), match="injected"):
        geocoding.build(osm_xml, output)
    assert output.read_bytes() == previous
    assert other_build.read_bytes() == b"another build"
    assert set(tmp_path.iterdir()) == {osm_xml, output, other_build}


@pytest.mark.parametrize("content", ["", "<osm version='0.6'><broken>", "<osm version='0.6'></osm>"])
def test_invalid_or_empty_input_preserves_output(tmp_path: Path, content: str) -> None:
    source, output = tmp_path / "bad.osm", tmp_path / "addresses.sqlite"
    source.write_text(content)
    output.write_bytes(b"previous artifact")
    with pytest.raises((ValueError, RuntimeError)):
        geocoding.build(source, output)
    assert output.read_bytes() == b"previous artifact"
    assert set(tmp_path.iterdir()) == {source, output}


@pytest.mark.parametrize(
    "bbox",
    [
        (21.0, 52.0, 20.0, 53.0),
        (20.0, 53.0, 21.0, 52.0),
        (20.0, 52.0, 20.0, 53.0),
        (-181.0, 52.0, 21.0, 53.0),
        (20.0, -91.0, 21.0, 53.0),
        (20.0, 52.0, math.inf, 53.0),
        (20.0, math.nan, 21.0, 53.0),
    ],
)
def test_bad_bbox_rejected_before_writing(osm_xml: Path, tmp_path: Path, bbox: geocoding.BBox) -> None:
    output = tmp_path / "absent" / "addresses.sqlite"
    with pytest.raises(ValueError, match="bbox"):
        geocoding.build(osm_xml, output, bbox)
    assert not output.parent.exists()


def test_source_cannot_be_output_or_alias(osm_xml: Path, tmp_path: Path) -> None:
    previous = osm_xml.read_bytes()
    with pytest.raises(ValueError, match="different files"):
        geocoding.build(osm_xml, osm_xml)
    alias = tmp_path / "alias.sqlite"
    os.link(osm_xml, alias)
    with pytest.raises(ValueError, match="different files"):
        geocoding.build(osm_xml, alias)
    assert osm_xml.read_bytes() == previous


def test_geometry_helpers() -> None:
    ring = [(21.0, 52.0), (21.1, 52.0), (21.1, 52.1), (21.0, 52.1), (21.0, 52.0)]
    assert geocoding._centroid(ring) == pytest.approx((21.05, 52.05))
    assert geocoding._centroid(list(reversed(ring))) == pytest.approx((21.05, 52.05))
    assert geocoding._centroid(ring[:-1]) is None
    assert geocoding._midpoint([(21.0, 52.0), (21.0, 52.0), (21.01, 52.0), (21.1, 52.0)]) == pytest.approx(
        (21.05, 52.0)
    )
    assert geocoding._intersects((20.0, 52.0), (22.0, 52.0), geocoding.DEFAULT_BBOX)
    assert geocoding._intersects((20.3, 51.0), (20.3, 53.0), geocoding.DEFAULT_BBOX)
    assert not geocoding._intersects((20.0, 52.6), (20.5, 53.0), geocoding.DEFAULT_BBOX)


def test_validation_detects_missing_index_row(osm_xml: Path, tmp_path: Path) -> None:
    output = tmp_path / "addresses.sqlite"
    geocoding.build(osm_xml, output)
    with sqlite3.connect(output) as connection:
        counts = geocoding.Counter(json.loads(dict(connection.execute("SELECT * FROM metadata"))["counts"]))
        connection.execute("DELETE FROM places_rtree WHERE id=1")
        with pytest.raises(ValueError, match="Incomplete places_rtree"):
            geocoding._validate(connection, counts)


@pytest.mark.parametrize("custom_bbox", [False, True])
def test_cli_dispatch_builds_xml_and_prints_json(
    osm_xml: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    custom_bbox: bool,
) -> None:
    output = tmp_path / "addresses.sqlite"
    args = ["--workdir", str(tmp_path), "--nice", "0", "geocoding", "--osm-pbf", str(osm_xml), "--output", str(output)]
    if custom_bbox:
        args += ["--bbox", "20.9", "52.19", "21.005", "52.205"]
    main(args)
    result = json.loads(capsys.readouterr().out)
    assert result["output"] == str(output)
    assert result["records"] == (2 if custom_bbox else 6)


def test_cli_failure_reports_error_without_replacing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = tmp_path / "addresses.sqlite"
    output.write_bytes(b"previous artifact")
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "--workdir",
                str(tmp_path),
                "--nice",
                "0",
                "geocoding",
                "--osm-pbf",
                str(tmp_path / "missing.pbf"),
                "--output",
                str(output),
            ]
        )
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert "Geocoding build failed:" in captured.err
    assert output.read_bytes() == b"previous artifact"
