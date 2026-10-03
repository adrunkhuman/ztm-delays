"""Rebuild static/map-style.json: OpenFreeMap Positron restyled dark, baked once so the browser doesn't restyle it.

Run by hand after changing the colours below: python frontend/scripts/build_map_style.py
"""

import json
import urllib.request
from pathlib import Path

BASE_STYLE = "https://tiles.openfreemap.org/styles/positron"
OUT = Path(__file__).parents[1] / "ztm_frontend/static/map-style.json"
C = {
    "bg": "#111111",
    "land": "#141414",
    "water": "#050505",
    "road": "#2c2c2c",
    "rail": "#3a3a3a",
    "label": "#7a7a7a",
    "district": "#a8a8a8",
}
DISTRICTS = [
    ("Bemowo", 20.912561, 52.24211),
    ("Białołęka", 21.020622, 52.331563),
    ("Bielany", 20.947194, 52.277611),
    ("Mokotów", 21.04444, 52.193666),
    ("Ochota", 20.972643, 52.212235),
    ("Praga-Południe", 21.071262, 52.237393),
    ("Praga-Północ", 21.02736, 52.264874),
    ("Rembertów", 21.162801, 52.261407),
    ("Śródmieście", 21.019077, 52.23282),
    ("Targówek", 21.058087, 52.275195),
    ("Ursus", 20.882907, 52.196086),
    ("Ursynów", 21.032338, 52.141048),
    ("Wawer", 21.137094, 52.22036),
    ("Wesoła", 21.229277, 52.251793),
    ("Wilanów", 21.110444, 52.153083),
    ("Włochy", 20.948439, 52.186116),
    ("Wola", 20.95479, 52.236237),
    ("Żoliborz", 20.979681, 52.267606),
]


def dark_style(base: dict) -> dict:
    """Keep only the Positron layers the map needs, recoloured to the site palette, plus district labels."""

    def pick(ids: list[str]) -> list[dict]:
        return [layer for layer in base["layers"] if layer["id"] in ids]

    land = [
        {
            **layer,
            "paint": {"background-color": C["bg"]} if layer["type"] == "background" else {"fill-color": C["land"]},
        }
        for layer in pick(["background", "park", "landuse_residential", "landcover_wood"])
    ]
    water = [
        {
            **layer,
            "paint": {"fill-color": C["water"]}
            if layer["type"] == "fill"
            else {"line-color": C["water"], "line-width": 1.2},
        }
        for layer in pick(["water", "waterway"])
    ]
    lines = []
    for layer in pick(["highway_minor", "highway_major_inner", "highway_motorway_inner", "railway"]):
        rail = layer["id"] == "railway"
        paint = {
            "line-color": C["rail"] if rail else C["road"],
            "line-width": ["interpolate", ["linear"], ["zoom"], 9, 0.4, 13, 1, 17, 1 if rail else 3],
        }
        if rail:
            paint["line-dasharray"] = [3, 3]
        lines.append({**layer, **({"minzoom": 11} if rail else {}), "paint": paint})
    labels = [
        {
            **layer,
            "layout": {
                **layer.get("layout", {}),
                "text-field": ["coalesce", ["get", "name"], ["get", "name_en"]],
                "text-font": ["Noto Sans Regular"],
                "text-size": 10 if layer["id"] == "highway-name-major" else 11,
            },
            "paint": {"text-color": C["label"], "text-halo-color": C["bg"], "text-halo-width": 1.4},
        }
        for layer in pick(["label_village", "label_town", "label_city", "highway-name-major"])
    ]
    districts = {
        "id": "district-labels",
        "type": "symbol",
        "source": "district-labels",
        "minzoom": 9,
        "layout": {
            "text-field": ["get", "name"],
            "text-font": ["Noto Sans Bold"],
            "text-size": ["interpolate", ["linear"], ["zoom"], 9, 10, 13, 13],
            "text-transform": "uppercase",
            "text-letter-spacing": 0.12,
            "text-padding": 6,
        },
        "paint": {"text-color": C["district"], "text-halo-color": C["bg"], "text-halo-width": 2},
    }
    anchors = [
        {"type": "Feature", "properties": {"name": name}, "geometry": {"type": "Point", "coordinates": [lng, lat]}}
        for name, lng, lat in DISTRICTS
    ]
    return {
        "version": 8,
        "glyphs": base["glyphs"],
        "sources": {
            "openmaptiles": base["sources"]["openmaptiles"],
            "district-labels": {"type": "geojson", "data": {"type": "FeatureCollection", "features": anchors}},
        },
        "layers": [*land, *water, *lines, *labels, districts],
    }


def main() -> None:
    """Fetch Positron and write the dark style."""
    # OpenFreeMap rejects urllib's default User-Agent.
    request = urllib.request.Request(BASE_STYLE, headers={"User-Agent": "ztm-delays map style builder"})  # noqa: S310
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        base = json.load(response)
    OUT.write_text(json.dumps(dark_style(base), separators=(",", ":"), ensure_ascii=False) + "\n")
    print(OUT, f"{OUT.stat().st_size / 1e3:.1f} kB")


if __name__ == "__main__":
    main()
