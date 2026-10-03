"""Rebuild the mini-map street backgrounds in dags/route_map_assets/ from OpenStreetMap.

Streets change slowly and the mini-map frames are fixed, so this runs by hand, not monthly:

    python airflow/scripts/build_route_map_backgrounds.py

Data © OpenStreetMap contributors (ODbL), credited inside each background.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from route_map_build import ASSETS_DIR, MINI_FRAMES, mini_frame, path_data

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
MARGIN_DEGREES = 0.04
ROAD_STYLE = {  # OSM highway class: (colour, width in viewBox units)
    "motorway": ("#333333", 1.4),
    "trunk": ("#333333", 1.4),
    "primary": ("#2c2c2c", 1.1),
    "secondary": ("#262626", 0.8),
    "tertiary": ("#202020", 0.6),
}
WATER = "#050505"
VISTULA_METRES = 400
ATTRIBUTION = "© OpenStreetMap contributors"


def fetch_streets() -> dict[str, list[list[object]]]:
    """Major roads and rivers covering both mini-map frames."""
    west = min(frame[0][0] for frame in MINI_FRAMES.values()) - MARGIN_DEGREES
    south = min(frame[0][1] for frame in MINI_FRAMES.values()) - MARGIN_DEGREES
    east = max(frame[1][0] for frame in MINI_FRAMES.values()) + MARGIN_DEGREES
    north = max(frame[1][1] for frame in MINI_FRAMES.values()) + MARGIN_DEGREES
    query = f"""
[out:json][timeout:180];
(
  way["highway"~"^({"|".join(ROAD_STYLE)})$"]({south},{west},{north},{east});
  way["waterway"="river"]({south},{west},{north},{east});
);
out geom;
"""
    request = urllib.request.Request(  # noqa: S310
        OVERPASS_URL,
        data=urllib.parse.urlencode({"data": query}).encode(),
        headers={"User-Agent": "ztm-delays route map backgrounds"},
    )
    with urllib.request.urlopen(request, timeout=240) as response:  # noqa: S310
        elements = json.load(response)["elements"]
    streets: dict[str, list[list[object]]] = {"roads": [], "rivers": []}
    for element in elements:
        coords = [[round(p["lon"], 5), round(p["lat"], 5)] for p in element.get("geometry", [])]
        if len(coords) < 2:  # noqa: PLR2004
            continue
        tags = element.get("tags", {})
        if "highway" in tags:
            streets["roads"].append([tags["highway"], coords])
        else:
            streets["rivers"].append([tags.get("name", ""), coords])
    return streets


def chain(lines: list[list[list[float]]]) -> list[list[tuple[float, float]]]:
    """Join polylines that meet end-to-end (OSM splits roads at every junction)."""
    tuples = [[(x, y) for x, y in line] for line in lines]
    starts: dict[tuple[float, float], list[int]] = {}
    for i, line in enumerate(tuples):
        starts.setdefault(line[0], []).append(i)
    used: set[int] = set()
    chains = []
    for i, line in enumerate(tuples):
        if i in used:
            continue
        used.add(i)
        current = list(line)
        while nexts := [j for j in starts.get(current[-1], []) if j not in used]:
            used.add(nexts[0])
            current += tuples[nexts[0]][1:]
        chains.append(current)
    return chains


def background(streets: dict[str, list[list[object]]], mode: str) -> str:
    """SVG elements for one mode's frame: rivers, roads, and the OSM credit."""
    frame = mini_frame(mode)
    metres_per_unit = 110540 / frame.scale

    # Background only: ~1 unit (under a pixel) of error is invisible and halves the file.
    def data(lines: list[list[list[float]]]) -> str:
        return path_data(chain(lines), frame, 1.0, 0)

    river_names = sorted({str(name) for name, _ in streets["rivers"]})
    rivers = "".join(
        f'<path stroke="{WATER}" stroke-width="{(VISTULA_METRES / metres_per_unit if name == "Wisła" else 1.2):.1f}" d="{d}"/>'
        for name in river_names
        if (d := data([c for n, c in streets["rivers"] if n == name]))  # type: ignore[misc]
    )
    roads = "".join(
        f'<path stroke="{colour}" stroke-width="{stroke}" d="{d}"/>'
        for road_class, (colour, stroke) in reversed(ROAD_STYLE.items())
        if (d := data([c for k, c in streets["roads"] if k == road_class]))  # type: ignore[misc]
    )
    credit = (
        f'<text x="{frame.width - 6}" y="{frame.height - 6}" text-anchor="end" fill="#555555" stroke="none" '
        f'font-family="sans-serif" font-size="9">{ATTRIBUTION}</text>'
    )
    return rivers + roads + credit


def main() -> None:
    """Fetch (or load) streets and write one background per mini-map mode."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--streets", type=Path, help="reuse a previously fetched streets JSON instead of Overpass")
    args = parser.parse_args()
    streets = json.loads(args.streets.read_text()) if args.streets else fetch_streets()
    for mode in MINI_FRAMES:
        path = ASSETS_DIR / f"mini-background-{mode}.svg"
        path.write_text(background(streets, mode) + "\n", encoding="utf-8")
        print(path, f"{path.stat().st_size / 1e3:.0f} kB")


if __name__ == "__main__":
    main()
