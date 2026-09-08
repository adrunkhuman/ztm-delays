"""Dry-run compiled schedule expansion and replacement; never execute warehouse SQL.

Reads a dbt manifest produced by compile_schedule.py. Temp-table scripts cannot be
reliably dry-run: report the CTAS SELECT separately and an inline-source MERGE
proxy. The latter deliberately rereads raw input instead of pretending that the
future temp table is free. It is not an exact bill or a guaranteed upper bound.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import bigquery

# Imports stay lazy so the no-Google-SDK Airflow boundary suite can test dry-run safety.
# SQL inputs are trusted local dbt artifacts, never request parameters; no query executes.
# ruff: noqa: PLC0415, S608

COLUMNS = (
    "processing_date",
    "gtfs_snapshot_id",
    "line",
    "direction_id",
    "schedule_day_type",
    "timetable_fingerprint",
    "scheduled_trip_count",
    "is_date_marker",
)


def replacement_query(source_sql: str, relation: str) -> str:
    """Include the entire future MERGE source, not just the existing target scan."""
    columns = ", ".join(f"`{name}`" for name in COLUMNS)
    marker = re.search(r"^-- schedule_ledger_plan: (.+)$", source_sql, re.MULTILINE)
    if marker is None:
        raise ValueError("Compiled ledger has no pinned plan marker")
    plan = json.loads(marker[1])
    days = [date.fromisoformat(item["processing_date"]).isoformat() for item in plan]
    partitions = ", ".join("date('" + day + "')" for day in days) or "cast(null as date)"
    return f"""merge into {relation} as dest
using ({source_sql}) as src on false
when not matched by source and dest.processing_date in ({partitions}) then delete
when not matched then insert ({columns}) values ({", ".join("src.`" + name + "`" for name in COLUMNS)})"""


def dry_run(client: bigquery.Client, sql: str, location: str) -> dict:
    """No result(), destination, or execution option exists at this boundary."""
    from google.cloud import bigquery

    job = client.query(
        sql,
        location=location,
        job_config=bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        ),
    )
    if job.total_bytes_processed is None:
        raise RuntimeError("BigQuery returned no byte estimate; do not treat this as zero")
    return {
        "bytes_processed": job.total_bytes_processed,
        "gib_processed": job.total_bytes_processed / 2**30,
        "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
    }


def estimate(client: bigquery.Client, manifest: dict, location: str, output: Path) -> dict:
    """Produce reproducible SQL and partial estimates without creating any tables."""
    from google.api_core.exceptions import NotFound

    nodes = manifest["nodes"]
    ledger = nodes["model.ztm_pipeline.int_schedule_fingerprint_daily"]
    config = ledger["config"]
    if (config["materialized"], config["incremental_strategy"], config["on_schema_change"]) != (
        "incremental",
        "insert_overwrite",
        "ignore",
    ) or config.get("partitions"):
        raise ValueError("Estimator requires the reviewed dynamic insert_overwrite ledger configuration")
    sql = ledger["compiled_code"].strip().rstrip(";")
    relation = ledger["relation_name"]
    queries = {"expansion_select": sql}
    missing = []
    try:
        table = client.get_table(relation.replace("`", ""))
    except NotFound:
        table = None
        missing.append(
            "Ledger does not exist: MERGE, planner, completeness, and version-read estimates await bootstrap."
        )
    if table is not None:
        if table.table_type != "TABLE" or getattr(table.time_partitioning, "field", None) != "processing_date":
            raise ValueError("Existing ledger is not a processing-date-partitioned table")
        queries["replacement_inline_source_proxy"] = replacement_query(sql, relation)
        queries["versions_from_existing_ledger"] = nodes["model.ztm_pipeline.int_schedule_version"]["compiled_code"]
        # Count planner/readiness scans independently. This deliberately scans all
        # small ledger columns rather than omitting the pre-hook and planning job.
        mapping = nodes["model.ztm_pipeline.int_gtfs_processing_snapshot"]["relation_name"]
        queries["planner_and_readiness_read_proxy"] = (
            f"select to_json_string(t) from {relation} as t union all select to_json_string(t) from {mapping} as t"
        )
    queries["mapping_rebuild"] = nodes["model.ztm_pipeline.int_gtfs_processing_snapshot"]["compiled_code"]
    output.mkdir(parents=True, exist_ok=True)
    estimates = {}
    for name, query in queries.items():
        (output / f"{name}.sql").write_text(query + "\n")
        estimates[name] = dry_run(client, query, location)
    # The planning and readiness query each read the small relations once.
    if "planner_and_readiness_read_proxy" in estimates:
        estimates["planner_and_readiness_read_proxy"]["executions"] = 2
    total = sum(item["bytes_processed"] * item.get("executions", 1) for item in estimates.values())
    return {
        "dry_run_only": True,
        "complete_cost_estimate": False,
        "estimates": estimates,
        "sum_of_estimates_bytes": total,
        "sum_at_6_25_usd_per_tib": total / 2**40 * 6.25,
        "ledger_existing_logical_bytes": None if table is None else table.num_bytes,
        "missing": missing,
        "limitations": [
            "Expansion is charged once in production. The MERGE proxy includes it again instead of reading a future temp table; this is a conservative planning proxy, not a proven upper bound.",
            "The MERGE proxy uses the pinned date partitions from compiled SQL. Production infers the same partitions from the temp markers. Estimate-only SQL must not be executed.",
            "Newly written ledger bytes, temp partition discovery, billing minimums/rounding, migration equivalence tests, and retries are not exactly estimated.",
            "Mapping rebuild applies to snapshot tasks, not nightly no-change tasks. Version estimate reads the current ledger, not future partitions.",
            "This excludes unrelated dbt models, matcher/fact recovery, storage, and exports. Sum every approved bootstrap batch and validation query before approval.",
        ],
    }


def main() -> None:
    """Write SQL and a deliberately partial cost report using dry runs only."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("dbt/target/manifest.json"))
    parser.add_argument("--location", default="europe-north1")
    parser.add_argument("--output", type=Path, default=Path("dbt/target/schedule_estimate"))
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    ledger = manifest["nodes"]["model.ztm_pipeline.int_schedule_fingerprint_daily"]
    from google.cloud import bigquery

    client = bigquery.Client(project=ledger["database"])
    report = estimate(client, manifest, args.location, args.output)
    text = json.dumps(report, indent=2)
    (args.output / "estimate.json").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
