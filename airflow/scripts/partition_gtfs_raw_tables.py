"""One-off migration: rebuild raw_gtfs_trips and raw_gtfs_stop_times partitioned by gtfs_snapshot_date.

Clustering alone stopped bounding BigQuery's MERGE estimates once stop_times passed the 100 GB
maximum_bytes_billed cap. Run with the GTFS and daily DAGs paused, before deploying the code that
writes and filters the new column, using credentials that can write ztm_raw:

    python airflow/scripts/partition_gtfs_raw_tables.py            # plan and dry-run cost only
    python airflow/scripts/partition_gtfs_raw_tables.py --execute  # copy, verify, swap

Each original is kept as <table>__unpartitioned_backup for recovery; drop it once the new layout
has run cleanly. The script is idempotent per table: already-partitioned tables are skipped.
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery

PROJECT = "ztm-data"
DATASET = "ztm_raw"
LOCATION = "europe-north1"
TABLES = ("raw_gtfs_trips", "raw_gtfs_stop_times")
DATE_FIELD = "gtfs_snapshot_date"
# Must match dag_gtfs_load._gtfs_snapshot_date: the UTC date prefix of the snapshot ID.
SNAPSHOT_DATE_SQL = "cast(substr(gtfs_snapshot_id, 1, 10) as date)"


def _migrate(client: bigquery.Client, dataset: str, table: str, *, execute: bool) -> None:
    source_id = f"{PROJECT}.{dataset}.{table}"
    staged_id = f"{source_id}__partitioned"
    source = client.get_table(source_id)
    if source.time_partitioning is not None and source.time_partitioning.field == DATE_FIELD:
        print(f"{table}: already partitioned by {DATE_FIELD}, skipping")
        return

    columns = ", ".join(f"`{field.name}`" for field in source.schema)
    select_sql = f"select {columns}, {SNAPSHOT_DATE_SQL} from `{source_id}`"  # noqa: S608 - identifiers from table metadata
    estimate = client.query(
        select_sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False), location=LOCATION
    ).total_bytes_processed
    print(f"{table}: {source.num_rows} rows, copy reads {estimate / 1e9:.1f} GB")
    if not execute:
        return

    staged = bigquery.Table(
        staged_id, schema=[*source.schema, bigquery.SchemaField(DATE_FIELD, "DATE", mode="REQUIRED")]
    )
    staged.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=DATE_FIELD)
    staged.clustering_fields = ["gtfs_snapshot_id"]
    staged.require_partition_filter = True
    # Fails on a leftover staged table from an interrupted run; inspect and drop it by hand.
    client.create_table(staged)
    client.query(
        f"insert into `{staged_id}` ({columns}, {DATE_FIELD}) {select_sql}",
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=int(estimate * 1.1)),
        location=LOCATION,
    ).result()

    staged = client.get_table(staged_id)
    current = client.get_table(source_id)
    if current.modified != source.modified:
        raise RuntimeError(f"{table} changed during the copy; pause GTFS loads, drop {staged_id}, and rerun")
    if staged.num_rows != source.num_rows:
        raise RuntimeError(f"{table}: copied {staged.num_rows} rows, expected {source.num_rows}")

    for statement in (
        f"alter table `{source_id}` rename to `{table}__unpartitioned_backup`",
        f"alter table `{staged_id}` rename to `{table}`",
    ):
        client.query(statement, location=LOCATION).result()
    print(f"{table}: swapped; original kept as {table}__unpartitioned_backup")


def main() -> None:
    """Plan, or with --execute run, the migration for each table."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--execute", action="store_true", help="copy and swap instead of only planning")
    parser.add_argument("--dataset", default=DATASET, help="rehearse against copies in another dataset")
    args = parser.parse_args()
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    for table in TABLES:
        _migrate(client, args.dataset, table, execute=args.execute)


if __name__ == "__main__":
    main()
