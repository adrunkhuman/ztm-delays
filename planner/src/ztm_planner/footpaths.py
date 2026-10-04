"""Walking distances between nearby stop posts along OpenStreetMap paths.

Weekly input to scoring. Reads a regional ``.osm.pbf`` and the stops of a GTFS ZIP and writes
``from_stop_id, to_stop_id, distance_m`` for every ordered pair of posts at most WALK_MAX_M apart on foot.
Every post that snapped to the path network also gets a zero-length row to itself: scoring treats such a post
as covered (no row to a neighbour means none is in walking distance) and estimates walks only for the rest.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from array import array
from pathlib import Path

import numpy as np
import osmium
import osmium.filter
import osmium.osm
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import KDTree

from ztm_planner.settings import WALK_MAX_M

log = logging.getLogger(__name__)
WALKABLE = frozenset({
    "footway", "path", "pedestrian", "steps", "living_street", "residential", "service", "unclassified", "road",
    "tertiary", "tertiary_link", "secondary", "secondary_link", "primary", "primary_link", "track", "cycleway",
    "bridleway", "corridor", "platform",
})  # fmt: skip
FOOT_ALLOWED = frozenset({"yes", "designated", "permissive"})
SNAP_MAX_M = 150  # a post farther than this from any path is left to the straight-line estimate
MIN_COMPONENT_NODES = 500  # snap only to the connected network, not to isolated fragments (platforms, yards)
BBOX_MARGIN_DEG = 0.02
SOURCE_BATCH = 8  # dijkstra returns a dense row per source over all nodes; keeps memory near 150 MB
EARTH_M = 6_371_000


def walkable(tags: osmium.osm.TagList) -> bool:
    """Whether a way with these tags can be walked along."""
    highway, foot = tags.get("highway"), tags.get("foot")
    if foot in {"no", "private"}:
        return False
    if highway in WALKABLE:
        return not (tags.get("access") in {"no", "private"} and foot not in FOOT_ALLOWED)
    # trunk roads usually have their sidewalks mapped separately
    return highway in {"trunk", "trunk_link"} and foot in FOOT_ALLOWED


def read_stops(gtfs_zip: Path) -> tuple[list[str], np.ndarray]:
    """Boarding posts (location_type empty or 0) and their (lat, lon)."""
    with zipfile.ZipFile(gtfs_zip) as archive, archive.open("stops.txt") as raw:
        rows = [
            r for r in csv.DictReader(io.TextIOWrapper(raw, "utf-8-sig")) if r.get("location_type", "") in {"", "0"}
        ]
    return [r["stop_id"] for r in rows], np.array([(float(r["stop_lat"]), float(r["stop_lon"])) for r in rows])


def build(pbf: Path, gtfs_zip: Path, output: Path, min_component_nodes: int = MIN_COMPONENT_NODES) -> dict:
    """Write the footpath table for the stops of ``gtfs_zip`` to ``output`` (parquet)."""
    stop_ids, stop_ll = read_stops(gtfs_zip)
    lo, hi = stop_ll.min(axis=0) - BBOX_MARGIN_DEG, stop_ll.max(axis=0) + BBOX_MARGIN_DEG
    node_ll, edges, lengths = _path_graph(pbf, (lo[0], lo[1], hi[0], hi[1]))
    n = len(node_ll)
    graph = csr_matrix((lengths, (edges[:, 0], edges[:, 1])), shape=(n, n))
    _, labels = connected_components(graph, directed=False)
    sizes = np.bincount(labels)
    eligible = np.flatnonzero(sizes[labels] >= min_component_nodes)

    ref_lat = float(stop_ll[:, 0].mean())
    stop_xy, node_xy = _project(stop_ll, ref_lat), _project(node_ll[eligible], ref_lat)
    snap_m, nearest = KDTree(node_xy).query(stop_xy, distance_upper_bound=SNAP_MAX_M)
    snapped = np.isfinite(snap_m)
    stop_node = np.full(len(stop_ids), -1)
    stop_node[snapped] = eligible[nearest[snapped]]

    # candidate pairs by straight line (a lower bound on the walk), then measured on the network
    pairs = KDTree(stop_xy).query_pairs(WALK_MAX_M, output_type="ndarray")
    pairs = np.vstack([pairs, pairs[:, ::-1]])
    pairs = pairs[snapped[pairs[:, 0]] & snapped[pairs[:, 1]]]
    crow = np.linalg.norm(stop_xy[pairs[:, 0]] - stop_xy[pairs[:, 1]], axis=1)
    src_node, dst_node = stop_node[pairs[:, 0]], stop_node[pairs[:, 1]]
    order = np.argsort(src_node, kind="stable")
    src_sorted = src_node[order]
    sources = np.unique(src_node)
    network = np.full(len(pairs), np.inf)
    for start in range(0, len(sources), SOURCE_BATCH):
        batch = sources[start : start + SOURCE_BATCH]
        dist = dijkstra(graph, directed=False, indices=batch, limit=WALK_MAX_M)
        sel = order[np.searchsorted(src_sorted, batch[0]) : np.searchsorted(src_sorted, batch[-1], side="right")]
        network[sel] = dist[np.searchsorted(batch, src_node[sel]), dst_node[sel]]
    distance = np.maximum(crow, snap_m[pairs[:, 0]] + network + snap_m[pairs[:, 1]])
    keep = distance <= WALK_MAX_M
    pairs, distance = pairs[keep], distance[keep]
    covered = np.flatnonzero(snapped)
    ids = np.array(stop_ids, dtype=object)
    table = pa.table({
        "from_stop_id": np.concatenate([ids[pairs[:, 0]], ids[covered]]).tolist(),
        "to_stop_id": np.concatenate([ids[pairs[:, 1]], ids[covered]]).tolist(),
        "distance_m": np.concatenate([np.round(distance), np.zeros(len(covered))]).astype(np.int32),
    })  # fmt: skip
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output)
    result = {"stops": len(stop_ids), "snapped": int(snapped.sum()), "pairs": len(pairs), "graph_nodes": n}
    log.info("footpaths %s: %s", output, result)
    return result


def _path_graph(pbf: Path, bbox: tuple[float, float, float, float]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Walkable ways inside ``bbox`` as (node lat/lon, undirected edge index pairs, edge lengths in metres)."""
    min_lat, min_lon, max_lat, max_lon = bbox
    refs, lats, lons, starts = array("q"), array("d"), array("d"), array("b")
    processor = (
        osmium.FileProcessor(str(pbf))
        .with_locations()
        .with_filter(osmium.filter.EntityFilter(osmium.osm.WAY))
        .with_filter(osmium.filter.KeyFilter("highway"))
    )
    for way in processor:
        if not isinstance(way, osmium.osm.Way) or not walkable(way.tags):
            continue
        run = False  # a node outside the box (or without a location) breaks the way into separate runs
        for node in way.nodes:
            loc = node.location
            if not loc.valid() or not (min_lat <= loc.lat <= max_lat and min_lon <= loc.lon <= max_lon):
                run = False
                continue
            refs.append(node.ref)
            lats.append(loc.lat)
            lons.append(loc.lon)
            starts.append(0 if run else 1)
            run = True
    ref_a = np.frombuffer(refs, dtype=np.int64)
    lat_a, lon_a = np.frombuffer(lats), np.frombuffer(lons)
    linked = np.flatnonzero(np.frombuffer(starts, dtype=np.int8) == 0)
    nodes, index = np.unique(ref_a, return_inverse=True)
    node_ll = np.empty((len(nodes), 2))
    node_ll[index] = np.column_stack([lat_a, lon_a])
    a, b = index[linked - 1], index[linked]
    length = _haversine(lat_a[linked - 1], lon_a[linked - 1], lat_a[linked], lon_a[linked])
    # both directions, duplicates collapsed to the shortest; csgraph reads explicit zeros as missing edges
    a, b, length = np.concatenate([a, b]), np.concatenate([b, a]), np.maximum(np.concatenate([length, length]), 0.01)
    key = a.astype(np.int64) * len(nodes) + b
    order = np.lexsort((length, key))
    first = order[np.concatenate([[True], key[order][1:] != key[order][:-1]])]
    return node_ll, np.column_stack([a[first], b[first]]), length[first]


def _project(ll: np.ndarray, ref_lat: float) -> np.ndarray:
    """Equirectangular metres; within a city the error is far below GPS precision."""
    return np.column_stack(
        [np.radians(ll[:, 1]) * EARTH_M * np.cos(np.radians(ref_lat)), np.radians(ll[:, 0]) * EARTH_M]
    )


def _haversine(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    p1, p2 = np.radians(lat1), np.radians(lat2)
    h = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * EARTH_M * np.arcsin(np.sqrt(h))
