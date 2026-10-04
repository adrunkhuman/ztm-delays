"""``ztm-planner``: local-file entry points used by the Airflow DAGs (which handle all cloud I/O)."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date
from pathlib import Path

from ztm_planner import footpaths, score, train
from ztm_planner.settings import HORIZON_DAYS, Resources


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and run ``train`` or ``score``; prints a JSON summary."""
    parser = argparse.ArgumentParser(prog="ztm-planner")
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--threads", type=int, help="default: CPU cores minus two")
    parser.add_argument("--memory-limit", default=None, help="DuckDB memory limit, default 1500MB")
    parser.add_argument("--nice", type=int, default=10, help="lower CPU priority so other jobs keep running")
    sub = parser.add_subparsers(dest="command", required=True)

    t = sub.add_parser("train", help="observed segments + stop tables -> model bundle")
    t.add_argument("--segments", required=True, help="parquet path or glob")
    t.add_argument("--weather-json", type=Path, required=True)
    t.add_argument("--stop-slots", type=Path, required=True)
    t.add_argument("--stop-eps", type=Path, required=True)
    t.add_argument("--start", type=date.fromisoformat, required=True)
    t.add_argument("--end", type=date.fromisoformat, required=True)
    t.add_argument("--version", required=True)
    t.add_argument("--bundle-out", type=Path, required=True)

    s = sub.add_parser("score", help="timetable + model bundle -> planner artifact")
    s.add_argument("--bundle", type=Path, required=True)
    s.add_argument("--gtfs-zip", type=Path, required=True)
    s.add_argument("--previous-gtfs-zip", type=Path, help="last snapshot before today, for yesterday's night trips")
    s.add_argument("--recent-daily", type=Path, required=True)
    s.add_argument("--weather-json", type=Path, required=True)
    s.add_argument("--start", type=date.fromisoformat, required=True)
    s.add_argument("--days", type=int, default=HORIZON_DAYS)
    s.add_argument("--output", type=Path, required=True)
    s.add_argument("--build-id", default=None)
    s.add_argument("--footpaths", type=Path, help="weekly OSM footpaths parquet; walks are estimated without it")

    f = sub.add_parser("footpaths", help="OSM extract + GTFS stops -> walking distances between nearby posts")
    f.add_argument("--osm-pbf", type=Path, required=True)
    f.add_argument("--gtfs-zip", type=Path, required=True)
    f.add_argument("--output", type=Path, required=True)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.nice:
        os.nice(args.nice)
    defaults = Resources.default(str(args.workdir / "spill"))
    resources = Resources(
        threads=args.threads or defaults.threads,
        memory_limit=args.memory_limit or defaults.memory_limit,
        temp_dir=defaults.temp_dir,
    )
    if args.command == "train":
        inputs = train.TrainInputs(
            args.segments, args.weather_json, args.stop_slots, args.stop_eps, args.start, args.end
        )
        result = train.train(inputs, args.workdir, args.bundle_out, args.version, resources)
    elif args.command == "footpaths":
        result = footpaths.build(args.osm_pbf, args.gtfs_zip, args.output)
    else:
        result = score.score(
            args.bundle, args.gtfs_zip, args.recent_daily, args.weather_json, args.start, args.days,
            args.workdir, args.output, args.build_id or score.build_id_now(), resources, args.previous_gtfs_zip,
            args.footpaths,
        )  # fmt: skip
    print(json.dumps(result, sort_keys=True))
