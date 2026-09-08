"""Compile schedule SQL with real dbt, anonymous credentials, and all sockets blocked.

Run with: uv run --with dbt-core --with dbt-bigquery python dbt/tools/compile_schedule.py --vars-file plan.json
The JSON file contains dbt vars, including a compile-only schedule_ledger_plan.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from dbt.cli.main import dbtRunner
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

DEFAULT_MODELS = [
    "int_schedule_fingerprint_daily",
    "int_schedule_version",
    "dim_schedule_version",
    "int_gtfs_processing_snapshot",
    "int_gtfs_trip_schedule_history",
]


def main() -> None:
    """Compile reviewed selectors without credentials or network access."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vars-file", type=Path, required=True)
    parser.add_argument("--select", nargs="+", default=DEFAULT_MODELS)
    args = parser.parse_args()
    variables = json.loads(args.vars_file.read_text())
    if "schedule_ledger_plan" not in variables:
        parser.error("vars file must contain schedule_ledger_plan (an empty list is an explicit no-op)")
    project_dir = Path(__file__).resolve().parents[1]
    project = os.environ.get("GCP_PROJECT", "ztm-data")
    location = os.environ.get("BIGQUERY_LOCATION", "europe-north1")
    with TemporaryDirectory(prefix="ztm-dbt-offline-") as temporary:
        # JSON is valid YAML; avoid templating untrusted environment values into YAML.
        Path(temporary, "profiles.yml").write_text(
            json.dumps(
                {
                    "ztm_pipeline": {
                        "target": "dev",
                        "outputs": {
                            "dev": {
                                "type": "bigquery",
                                "method": "oauth",
                                "project": project,
                                "dataset": "ztm_stg",
                                "location": location,
                                "threads": 1,
                            }
                        },
                    },
                }
            )
        )
        client = bigquery.Client(project=project, credentials=AnonymousCredentials())
        with (
            patch("dbt.adapters.bigquery.connections.create_bigquery_client", return_value=client),
            patch("socket.socket.connect", side_effect=RuntimeError("Offline compile forbids network access")),
            patch("socket.socket.connect_ex", side_effect=RuntimeError("Offline compile forbids network access")),
        ):
            result = dbtRunner().invoke(
                [
                    "--no-send-anonymous-usage-stats",
                    "compile",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    temporary,
                    "--no-introspect",
                    "--no-populate-cache",
                    "--select",
                    *args.select,
                    "--vars",
                    json.dumps(variables),
                ]
            )
        if not result.success:
            raise SystemExit(f"Offline compilation failed: {result.exception}")


if __name__ == "__main__":
    main()
