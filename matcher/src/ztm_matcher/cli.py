"""Matcher command-line interface."""

import argparse
import json
import sys
from pathlib import Path

from ztm_matcher.config import RunConfig, parse_date
from ztm_matcher.errors import MatcherError, fail
from ztm_matcher.runtime import ReconstructionRun


def main(argv: list[str] | None = None) -> int:
    """Prepare one run, returning stable JSON errors and nonzero exits."""
    parser = argparse.ArgumentParser(prog="ztm-matcher")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    for name in ("processing-date", "snapshot-id", "gps-root", "gtfs-zip", "output-dir"):
        prepare.add_argument(f"--{name}", required=True)
    prepare.add_argument("--metrics-json")
    prepare.add_argument("--threads", type=int, default=2)
    prepare.add_argument("--memory-limit", default="384MB")
    prepare.add_argument("--temp-limit", default="20GB")
    prepare.add_argument("--max-vehicle-rows", type=int, default=1_000_000)
    prepare.add_argument(
        "--alignment-workers",
        type=int,
        default=1,
        help="Concurrent vehicle chunks for stop alignment; each worker uses one DuckDB thread.",
    )
    prepare.add_argument(
        "--line",
        dest="diagnostic_line",
        help="Retain duties containing one line or a comma-separated line list.",
    )
    prepare.add_argument(
        "--vehicle-number", dest="diagnostic_vehicle_number", help="Retain GPS from this vehicle only."
    )
    prepare.add_argument("--trip-id", dest="diagnostic_trip_id", help="Retain the full duty containing this trip.")
    args = parser.parse_args(argv)
    try:
        if args.threads < 1 or args.max_vehicle_rows < 1 or args.alignment_workers < 1:
            raise fail("invalid_configuration", "threads, max_vehicle_rows, and alignment_workers must be positive", 2)
        output = Path(args.output_dir)
        config = RunConfig(
            parse_date(args.processing_date),
            args.snapshot_id,
            Path(args.gps_root),
            Path(args.gtfs_zip),
            output,
            Path(args.metrics_json) if args.metrics_json else output / "metrics.json",
            args.threads,
            args.memory_limit,
            args.temp_limit,
            args.max_vehicle_rows,
            False,
            args.alignment_workers,
            args.diagnostic_line,
            args.diagnostic_vehicle_number,
            args.diagnostic_trip_id,
        )
        with ReconstructionRun(config) as run:
            result = run.prepare()
        print(json.dumps(result["metrics"], sort_keys=True))
        return 0
    except MatcherError as exc:
        print(json.dumps({"error": {"code": exc.code, "message": exc.message}}, sort_keys=True), file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
