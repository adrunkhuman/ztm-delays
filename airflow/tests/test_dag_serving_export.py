from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
import types
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest


def test_mart_table_list_exports_frontend_source_tables() -> None:
    dag = _load_dag_module()

    assert set(dag.MART_TABLES) == {
        "dim_serving_date",
        "dim_serving_window_date",
        "dim_schedule_version",
        "dim_stop_group_current",
        "dim_stop_post_current",
        "fct_expected_stop_event",
        "mart_entity_daily_summary",
        "mart_entity_window_daily_summary",
        "mart_entity_rankings",
        "mart_entity_timeline_daily",
        "mart_hour_window_summary",
        "mart_line_course_stop_window",
        "mart_line_course_window",
        "mart_line_reliability_daily",
        "mart_line_trip_group_daily",
        "mart_line_window_summary",
        "mart_mode_window_summary",
        "mart_pipeline_status",
        "mart_pipeline_status_recent_summary",
        "mart_stop_group_line_group_window",
        "mart_stop_group_window_summary",
        "mart_stop_line_window_summary",
        "mart_stop_post_line_group_window",
        "mart_stop_post_window_summary",
        "mart_trip_daily",
        "mart_trip_line_daily",
        "mart_trip_mode_daily_summary",
        "mart_worst_delay_event",
    }


def test_serving_table_groups_partition_export_contract() -> None:
    dag = _load_dag_module()

    assert set(dag.GLOBAL_EXPORT_TABLES) == {
        "dim_schedule_version",
        "dim_serving_date",
        "dim_stop_group_current",
        "dim_stop_post_current",
        "mart_pipeline_status_recent_summary",
    }
    assert set(dag.SHARDED_EXPORT_TABLES) == set(dag.MART_TABLES) - set(dag.GLOBAL_EXPORT_TABLES)
    assert set(dag.SHARDED_EXPORT_TABLES.values()) == {"service_date", "source_end_date"}


def test_export_config_uses_safe_defaults() -> None:
    dag = _load_dag_module()

    config = dag._export_config({"dag_run": FakeDagRun({})}, datetime(2026, 7, 2, 12, 30, tzinfo=UTC))

    assert config.export_id == "20260702T123000Z"
    assert config.output_dir == Path("/opt/airflow/serving")
    assert config.output_filename == "ztm.duckdb"
    assert config.gcs_bucket == "ztm-analytics-bucket"
    assert config.gcs_prefix == "serving/duckdb/staging"
    assert config.max_source_bytes == 20 * 1024 * 1024 * 1024
    assert config.max_duckdb_bytes == 20 * 1024 * 1024 * 1024
    assert config.cleanup_gcs_staging is True
    assert config.changed_partition_dates == ()
    assert config.validation_timeout_seconds == 600
    assert config.validation_memory_limit_mb == 4096
    assert config.validation_temp_limit_mb == 6144
    assert config.validation_threads == 1
    assert config.staging_retention_days == 3


def test_export_config_uses_gps_models_asset_changed_partition_dates() -> None:
    dag = _load_dag_module()

    config = dag._export_config(
        {
            "dag_run": FakeDagRun({}),
            "triggering_asset_events": {
                dag.GPS_MODELS_DATE_ASSET: [
                    FakeAssetEvent(
                        {
                            "processing_date": "2026-07-08",
                            "changed_partition_dates": ["2026-07-07", "2026-07-08"],
                        }
                    )
                ]
            },
        },
        datetime(2026, 7, 9, 5, 0, tzinfo=UTC),
    )

    assert config.changed_partition_dates == ("2026-07-07", "2026-07-08")


def test_export_config_falls_back_to_asset_processing_date() -> None:
    dag = _load_dag_module()

    config = dag._export_config(
        {
            "dag_run": FakeDagRun({}),
            "triggering_asset_events": {dag.GPS_MODELS_DATE_ASSET: [FakeAssetEvent({"processing_date": "2026-07-08"})]},
        }
    )

    assert config.changed_partition_dates == ("2026-07-08",)


def test_export_config_combines_coalesced_asset_events() -> None:
    dag = _load_dag_module()

    config = dag._export_config(
        {
            "dag_run": FakeDagRun({}),
            "triggering_asset_events": {
                dag.GPS_MODELS_DATE_ASSET: [
                    FakeAssetEvent({"processing_date": "2026-07-07"}),
                    FakeAssetEvent(
                        {
                            "processing_date": "2026-07-08",
                            "changed_partition_dates": ["2026-07-07", "2026-07-08"],
                        }
                    ),
                ]
            },
        }
    )

    assert config.changed_partition_dates == ("2026-07-07", "2026-07-08")


def test_export_config_accepts_legacy_changed_partition_date() -> None:
    dag = _load_dag_module()

    config = dag._export_config({"dag_run": FakeDagRun({"changed_partition_date": "2026-07-08"})})

    assert config.changed_partition_dates == ("2026-07-08",)


@pytest.mark.parametrize(
    "conf",
    [
        {"changed_partition_dates": "2026-07-08"},
        {"changed_partition_dates": ["not-a-date"]},
        {"changed_partition_dates": ["2026-07-07"], "changed_partition_date": "2026-07-08"},
    ],
)
def test_export_config_rejects_invalid_changed_partition_dates(conf: dict[str, object]) -> None:
    dag = _load_dag_module()

    with pytest.raises((TypeError, ValueError)):
        dag._export_config({"dag_run": FakeDagRun(conf)})


def test_export_config_uses_shared_max_bytes_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SERVING_EXPORT_MAX_BYTES", "777")
    dag = _load_dag_module()

    config = dag._export_config({"dag_run": FakeDagRun({})})

    assert config.max_source_bytes == 777
    assert config.max_duckdb_bytes == 777


def test_export_config_accepts_manual_overrides(tmp_path: Path) -> None:
    dag = _load_dag_module()

    config = dag._export_config(
        {
            "dag_run": FakeDagRun(
                {
                    "export_id": "manual-1",
                    "output_dir": str(tmp_path),
                    "output_filename": "alpha.duckdb",
                    "gcs_bucket": "bucket",
                    "gcs_prefix": "/custom/prefix/",
                    "max_source_bytes": 123,
                    "max_duckdb_bytes": "456",
                    "cleanup_gcs_staging": False,
                    "changed_partition_dates": ["2026-07-06", "2026-07-07", "2026-07-07"],
                    "validation_timeout_seconds": 30,
                    "validation_memory_limit_mb": 512,
                    "validation_temp_limit_mb": 768,
                    "validation_threads": 2,
                    "staging_retention_days": 5,
                }
            )
        }
    )

    assert config.export_id == "manual-1"
    assert config.output_dir == tmp_path
    assert config.output_filename == "alpha.duckdb"
    assert config.gcs_bucket == "bucket"
    assert config.gcs_prefix == "custom/prefix"
    assert config.max_source_bytes == 123
    assert config.max_duckdb_bytes == 456
    assert config.cleanup_gcs_staging is False
    assert config.changed_partition_dates == ("2026-07-06", "2026-07-07")
    assert config.validation_timeout_seconds == 30
    assert config.validation_memory_limit_mb == 512
    assert config.validation_temp_limit_mb == 768
    assert config.validation_threads == 2
    assert config.staging_retention_days == 5


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("export_id", "bad;id", ValueError),
        ("output_filename", "nested/ztm.duckdb", ValueError),
        ("max_source_bytes", 0, ValueError),
        ("validation_timeout_seconds", 0, ValueError),
        ("staging_retention_days", 0, ValueError),
        ("staging_retention_days", True, ValueError),
        ("gcs_prefix", "/", ValueError),
        ("cleanup_gcs_staging", "yes", TypeError),
    ],
)
def test_export_config_rejects_unsafe_manual_overrides(key: str, value: object, error: type[Exception]) -> None:
    dag = _load_dag_module()

    with pytest.raises(error):
        dag._export_config({"dag_run": FakeDagRun({key: value})})


def test_validate_source_stats_requires_every_mart() -> None:
    dag = _load_dag_module()
    stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=1) for table_name in dag.MART_TABLES[:-1]]

    with pytest.raises(RuntimeError, match="Missing mart tables"):
        dag._validate_source_stats(stats, 1000)


def test_validate_source_stats_enforces_size_guardrail() -> None:
    dag = _load_dag_module()
    stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=10) for table_name in dag.MART_TABLES]

    with pytest.raises(RuntimeError, match="exceeds configured limit"):
        dag._validate_source_stats(stats, 50)


def test_source_table_stats_merges_date_ranges() -> None:
    dag = _load_dag_module()
    client = FakeStatsBigQueryClient(dag)

    stats = dag._source_table_stats(client)

    by_table = {stat.table_name: stat for stat in stats}
    assert len(stats) == len(dag.MART_TABLES)
    assert by_table["mart_trip_daily"].row_count == 10
    assert by_table["mart_trip_daily"].size_bytes == 100
    assert by_table["mart_trip_daily"].min_date == "2026-06-27"
    assert by_table["mart_trip_daily"].max_date == "2026-07-02"
    assert by_table["mart_trip_daily"].date_count == 6
    assert by_table["dim_serving_date"].min_date is None
    assert by_table["dim_serving_date"].max_date is None
    assert by_table["dim_serving_date"].date_count is None
    assert client.table_refs == [f"ztm-data.ztm_marts.{table_name}" for table_name in dag.MART_TABLES]
    assert client.queries == []
    assert client.partition_table_refs == [
        f"ztm-data.ztm_marts.{table_name}"
        for table_name in dag.DATE_RANGE_SQL_BY_TABLE
        if table_name in dag.MART_TABLES and table_name != "dim_serving_date"
    ]


def test_table_partition_dates_ignores_non_date_partitions() -> None:
    dag = _load_dag_module()
    client = FakeStatsBigQueryClient(dag)

    assert dag._table_partition_dates(client, "mart_trip_daily") == [
        dag.date(2026, 6, 27),
        dag.date(2026, 6, 28),
        dag.date(2026, 6, 29),
        dag.date(2026, 6, 30),
        dag.date(2026, 7, 1),
        dag.date(2026, 7, 2),
    ]


def test_extract_mart_to_gcs_uses_parquet_extract_contract() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    dag._extract_mart_to_gcs(client, config, "mart_trip_daily")

    assert client.extract_call is not None
    assert client.extract_call.source_table == "ztm-data.ztm_marts.mart_trip_daily"
    assert client.extract_call.destination_uri == "gs://bucket/prefix/export_id=export-1/mart_trip_daily/part-*.parquet"
    assert client.extract_call.job_id == "serving_export_export_1_mart_trip_daily"
    assert client.extract_call.location == dag.BIGQUERY_LOCATION
    assert client.extract_call.job_config.destination_format == dag.bigquery.DestinationFormat.PARQUET
    assert client.extract_call.job.result_called is True


def test_extract_mart_to_gcs_rejects_existing_job_after_conflict() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(raise_conflict=True)
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    with pytest.raises(RuntimeError, match="job already exists"):
        dag._extract_mart_to_gcs(client, config, "mart_trip_daily")

    assert client.existing_job.result_called is False


def test_sync_partition_cache_updates_changed_partitions() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(partition_ids=["20260701", "20260702"])
    storage_client = FakeStorageClient(
        [
            FakeBlob(
                "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-01/part-000.parquet"
            ),
            FakeBlob("prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-02/old.parquet"),
            FakeBlob(
                "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-02/part-000.parquet"
            ),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
        changed_partition_dates=("2026-07-01", "2026-07-02"),
    )

    dag._sync_partition_cache(client, storage_client, config, "fct_expected_stop_event")

    assert client.query_call is None
    assert [call.source_table for call in client.extract_calls] == [
        "ztm-data.ztm_marts.fct_expected_stop_event$20260701",
        "ztm-data.ztm_marts.fct_expected_stop_event$20260702",
    ]
    assert client.extract_calls[-1].destination_uri == (
        "gs://bucket/prefix/partition_staging/export_id=export-1/fct_expected_stop_event/"
        "service_date=2026-07-02/part-*.parquet"
    )
    assert client.extract_calls[-1].job_config.destination_format == dag.bigquery.DestinationFormat.PARQUET
    assert client.extract_calls[-1].job_id == "serving_export_partition_export_1_fct_expected_stop_event_2026_07_02"
    assert client.extract_calls[-1].location == dag.BIGQUERY_LOCATION
    assert all(call.job.result_called for call in client.extract_calls)
    assert storage_client.deleted_blob_names == [
        "prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-02/old.parquet",
    ]
    assert storage_client.copied_blob_names == [
        (
            "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-01/part-000.parquet",
            "prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-01/generation=export-1/part-000.parquet",
        ),
        (
            "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-02/part-000.parquet",
            "prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-02/generation=export-1/part-000.parquet",
        ),
    ]


def test_sync_partition_cache_extracts_missing_partitions() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(partition_ids=["20260701", "20260702", "20260703"])
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-01/part-000.parquet"),
            FakeBlob(
                "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-02/part-000.parquet"
            ),
            FakeBlob(
                "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-03/part-000.parquet"
            ),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
        changed_partition_dates=("2026-07-03",),
    )

    dag._sync_partition_cache(client, storage_client, config, "fct_expected_stop_event")

    assert client.partition_table_refs == ["ztm-data.ztm_marts.fct_expected_stop_event"]
    assert [call.source_table for call in client.extract_calls] == [
        "ztm-data.ztm_marts.fct_expected_stop_event$20260703",
        "ztm-data.ztm_marts.fct_expected_stop_event$20260702",
    ]
    assert storage_client.copied_blob_names == [
        (
            "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-03/part-000.parquet",
            "prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-03/generation=export-1/part-000.parquet",
        ),
        (
            "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-02/part-000.parquet",
            "prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-02/generation=export-1/part-000.parquet",
        ),
    ]


def test_sync_partition_cache_skips_changed_dates_absent_from_source() -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(partition_ids=["20260701"])
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/partition_cache/fct_expected_stop_event/service_date=2026-06-30/stale.parquet"),
            FakeBlob("prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-01/part-000.parquet"),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
        changed_partition_dates=("2026-06-30",),
    )

    dag._sync_partition_cache(client, storage_client, config, "fct_expected_stop_event")

    assert client.extract_calls == []
    assert storage_client.deleted_blob_names == [
        "prefix/partition_cache/fct_expected_stop_event/service_date=2026-06-30/stale.parquet"
    ]


def test_partition_cache_copy_failure_keeps_previous_generation_active(tmp_path: Path) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(partition_ids=["20260701"])
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/partition_cache/fct_expected_stop_event/service_date=2026-07-01/old.parquet"),
            FakeBlob(
                "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-01/part-000.parquet"
            ),
            FakeBlob(
                "prefix/partition_staging/export_id=export-1/fct_expected_stop_event/service_date=2026-07-01/part-001.parquet",
                fail_copy=True,
            ),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=Path("export"),
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
        changed_partition_dates=("2026-07-01",),
    )

    with pytest.raises(RuntimeError, match="copy failed"):
        dag._sync_partition_cache(client, storage_client, config, "fct_expected_stop_event")

    assert storage_client.deleted_blob_names == []
    assert dag._cached_partition_dates(storage_client, config, "fct_expected_stop_event", "service_date") == {
        "2026-07-01"
    }
    paths = dag._download_partitioned_mart_parquet(storage_client, config, "fct_expected_stop_event", tmp_path)
    assert [path.name for path in paths] == ["old.parquet"]


def test_download_mart_parquet_downloads_only_parquet_files(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/export_id=export-1/mart_trip_daily/part-000.parquet"),
            FakeBlob("prefix/export_id=export-1/mart_trip_daily_extra/part-999.parquet"),
            FakeBlob("prefix/export_id=export-1/mart_trip_daily/_SUCCESS"),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    paths = dag._download_mart_parquet(storage_client, config, "mart_trip_daily", tmp_path)

    assert [path.name for path in paths] == ["part-000.parquet"]
    assert paths[0].read_text(encoding="utf-8") == "downloaded"
    assert storage_client.list_prefixes == ["prefix/export_id=export-1/mart_trip_daily/"]
    assert storage_client.blob_names == ["prefix/export_id=export-1/mart_trip_daily/part-000.parquet"]


def test_download_mart_parquet_resolves_listed_blobs_by_name(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [FakeBlob("prefix/export_id=export-1/mart_trip_daily/part-000.parquet", fail_download=True)]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    paths = dag._download_mart_parquet(storage_client, config, "mart_trip_daily", tmp_path)

    assert [path.name for path in paths] == ["part-000.parquet"]
    assert paths[0].read_text(encoding="utf-8") == "downloaded"
    assert storage_client.blob_names == ["prefix/export_id=export-1/mart_trip_daily/part-000.parquet"]


def test_cleanup_gcs_staging_resolves_listed_blobs_by_name(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/export_id=export-1/mart_trip_daily/part-000.parquet", fail_delete=True),
            FakeBlob("prefix/export_id=export-1/mart_trip_daily/_SUCCESS", fail_delete=True),
        ]
    )
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )

    dag._cleanup_gcs_staging(storage_client, config)

    assert storage_client.list_prefixes == [
        "prefix/export_id=export-1/",
        "prefix/partition_staging/export_id=export-1/",
    ]
    assert storage_client.blob_names == [
        "prefix/export_id=export-1/mart_trip_daily/part-000.parquet",
        "prefix/export_id=export-1/mart_trip_daily/_SUCCESS",
    ]
    assert storage_client.deleted_blob_names == [
        "prefix/export_id=export-1/mart_trip_daily/part-000.parquet",
        "prefix/export_id=export-1/mart_trip_daily/_SUCCESS",
    ]


def test_cleanup_stale_gcs_staging_deletes_only_expired_temporary_objects(tmp_path: Path) -> None:
    dag = _load_dag_module()
    old = datetime(2026, 7, 1, tzinfo=UTC)
    cutoff = datetime(2026, 7, 2, tzinfo=UTC)
    recent = datetime(2026, 7, 4, tzinfo=UTC)
    storage_client = FakeStorageClient(
        [
            FakeBlob("prefix/export_id=old/mart/part.parquet", updated=old, size=100),
            FakeBlob("prefix/partition_staging/export_id=old/mart/date=2026-07-01/part.parquet", updated=old, size=200),
            FakeBlob("prefix/export_id=cutoff/mart/part.parquet", updated=cutoff, size=300),
            FakeBlob("prefix/export_id=recent/mart/part.parquet", updated=recent, size=400),
            FakeBlob("prefix/export_id=current/mart/part.parquet", updated=old, size=500),
            FakeBlob("prefix/export_id=unknown/mart/part.parquet", size=600),
            FakeBlob("prefix/export_id=mixed/mart/old.parquet", updated=old, size=700),
            FakeBlob("prefix/export_id=mixed/mart/recent.parquet", updated=recent, size=800),
            FakeBlob("prefix/partition_staging/export_id=mixed/mart/old.parquet", updated=old, size=900),
            FakeBlob("prefix/partition_cache/mart/date=2026-07-01/part.parquet", updated=old, fail_delete=True),
        ]
    )
    config = dag.ExportConfig(
        export_id="current",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=True,
        staging_retention_days=3,
    )

    result = dag._cleanup_stale_gcs_staging(
        storage_client,
        config,
        datetime(2026, 7, 5, tzinfo=UTC),
    )

    assert result == dag.StagingCleanupResult(deleted_object_count=2, deleted_bytes=300)
    assert storage_client.list_prefixes == [
        "prefix/export_id=",
        "prefix/partition_staging/export_id=",
    ]
    assert storage_client.deleted_blob_names == [
        "prefix/export_id=old/mart/part.parquet",
        "prefix/partition_staging/export_id=old/mart/date=2026-07-01/part.parquet",
    ]


def test_cleanup_stale_gcs_staging_is_best_effort_after_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dag = _load_dag_module()
    config = dag.ExportConfig(
        export_id="current",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=True,
    )

    def fail_cleanup(*_args: object) -> None:
        raise RuntimeError("cleanup unavailable")

    monkeypatch.setattr(dag, "_cleanup_stale_gcs_staging", fail_cleanup)

    dag._cleanup_stale_gcs_staging_best_effort(
        FakeStorageClient([]),
        config,
        datetime(2026, 7, 5, tzinfo=UTC),
    )

    assert "Failed to clean stale serving export GCS staging" in caplog.text


def test_run_serving_export_keeps_published_result_when_current_cleanup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dag = _load_dag_module()
    config = dag.ExportConfig(
        export_id="current",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=True,
    )
    published = dag.ExportResult(
        export_id="current",
        duckdb_path=str(tmp_path / "ztm.duckdb"),
        metadata_path=str(tmp_path / "ztm.duckdb.meta.json"),
        duckdb_size_bytes=100,
        source_size_bytes=200,
        source_row_count=3,
        exported_table_count=len(dag.MART_TABLES),
    )
    stale_cleanup_called = []

    monkeypatch.setattr(dag.bigquery, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(dag.storage, "Client", lambda **_kwargs: object())
    monkeypatch.setattr(dag, "_poller_status", lambda *_args: None)
    monkeypatch.setattr(dag, "_source_table_stats", lambda *_args: [])
    monkeypatch.setattr(dag, "_validate_source_stats", lambda *_args: None)
    monkeypatch.setattr(dag, "_extract_and_download_marts", lambda *_args: {})
    monkeypatch.setattr(dag, "_publish_duckdb", lambda *_args: published)

    def fail_current_cleanup(*_args: object) -> None:
        raise RuntimeError("cleanup unavailable")

    monkeypatch.setattr(dag, "_cleanup_gcs_staging", fail_current_cleanup)
    monkeypatch.setattr(
        dag,
        "_cleanup_stale_gcs_staging_best_effort",
        lambda *_args: stale_cleanup_called.append(True),
    )

    result = dag._run_serving_export(config)

    assert result is published
    assert stale_cleanup_called == [True]
    assert "Failed to clean current serving export GCS staging" in caplog.text


def test_cleanup_stale_gcs_staging_uses_listed_generation_precondition(tmp_path: Path) -> None:
    dag = _load_dag_module()
    storage_client = FakeStorageClient(
        [
            FakeBlob(
                "prefix/export_id=old/mart/part.parquet",
                updated=datetime(2026, 7, 1, tzinfo=UTC),
                generation=1,
                current_generation=2,
            )
        ]
    )
    config = dag.ExportConfig(
        export_id="current",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=True,
    )

    with pytest.raises(RuntimeError, match="stale generation was deleted"):
        dag._cleanup_stale_gcs_staging(storage_client, config, datetime(2026, 7, 5, tzinfo=UTC))

    assert storage_client.deleted_blob_names == []


def test_configure_duckdb_build_connection_sets_resource_limits(tmp_path: Path) -> None:
    dag = _load_dag_module()
    connection = RecordingDuckdbConnection()
    temp_directory = tmp_path / "duckdb temp's"

    dag._configure_duckdb_build_connection(connection, temp_directory)

    escaped_temp_directory = temp_directory.as_posix().replace("'", "''")
    assert connection.queries == [
        f"set temp_directory = '{escaped_temp_directory}'",
        "set max_temp_directory_size = '2GB'",
        "set memory_limit = '1GB'",
        "set threads = 2",
        "set preserve_insertion_order = false",
    ]


def test_configure_duckdb_build_connection_applies_real_duckdb_settings(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    temp_directory = tmp_path / "duckdb-temp"
    temp_directory.mkdir()

    with duckdb.connect() as connection:
        dag._configure_duckdb_build_connection(connection, temp_directory)
        settings = connection.execute(
            """
            select
                current_setting('memory_limit'),
                current_setting('max_temp_directory_size'),
                current_setting('threads'),
                current_setting('preserve_insertion_order'),
                current_setting('temp_directory')
            """
        ).fetchone()

    assert settings[0]
    assert settings[1]
    assert settings[2] == 2
    assert settings[3] is False
    assert Path(settings[4]) == temp_directory


def test_publish_duckdb_removes_temp_file_after_build_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=1000,
        cleanup_gcs_staging=False,
    )
    source_stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=1) for table_name in dag.MART_TABLES]
    parquet_paths_by_table = {table_name: [tmp_path / f"{table_name}.parquet"] for table_name in dag.MART_TABLES}
    temp_wal_path = tmp_path / ".ztm.duckdb.export-1.tmp.wal"
    temp_wal_path.write_text("stale wal", encoding="utf-8")
    stale_temp_dir = tmp_path / ".duckdb-tmp-export-1"
    stale_temp_dir.mkdir()
    (stale_temp_dir / "stale.tmp").write_text("stale", encoding="utf-8")
    monkeypatch.setattr(dag, "_duckdb_module", FailingDuckdbModule)

    with pytest.raises(RuntimeError, match="build failed"):
        dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []
    assert not temp_wal_path.exists()
    assert list(tmp_path.glob(".duckdb-tmp-*")) == []


def test_build_partitioned_catalog_materializes_globals_and_exposes_shard_views(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=10) for table_name in dag.MART_TABLES]
    config = dag.ExportConfig(
        export_id="partitioned-1",
        output_dir=tmp_path,
        output_filename="catalog.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )
    temp_directory = tmp_path / "catalog-temp"
    temp_directory.mkdir()
    catalog_path = tmp_path / "catalog.duckdb"

    dag._build_partitioned_catalog_file(
        duckdb,
        catalog_path,
        dag.DuckdbBuildInput(
            parquet_paths_by_table=parquet_paths_by_table,
            source_stats=source_stats,
            config=config,
            exported_at=datetime(2026, 7, 2, tzinfo=UTC),
            temp_directory=temp_directory,
        ),
    )

    with duckdb.connect(catalog_path, read_only=True) as connection:
        table_types = dict(
            connection.execute(
                "select table_name, table_type from information_schema.tables where table_schema = 'main'"
            ).fetchall()
        )
        assert table_types["dim_serving_date"] == "BASE TABLE"
        assert table_types["mart_trip_daily"] == "VIEW"
        assert connection.execute("select count(*) from mart_trip_daily").fetchone()[0] == 1
        assert connection.execute("select export_id from export_metadata").fetchone()[0] == "partitioned-1"


def test_publish_duckdb_builds_queryable_file_with_metadata(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=2 if table_name == "mart_pipeline_status" else 1,
            size_bytes=10,
            min_date="2026-06-27" if table_name == "mart_trip_daily" else None,
            max_date="2026-07-02" if table_name == "mart_trip_daily" else None,
            date_count=6 if table_name == "mart_trip_daily" else None,
        )
        for table_name in dag.MART_TABLES
    ]
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )

    result = dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert Path(result.duckdb_path).exists()
    assert Path(result.metadata_path).exists()
    assert list(tmp_path.glob(".duckdb-tmp-*")) == []
    with duckdb.connect(result.duckdb_path, read_only=True) as connection:
        assert connection.execute("select count(*) from mart_trip_daily").fetchone()[0] == 1
        assert connection.execute("select export_id from export_metadata").fetchone()[0] == "export-1"
        assert connection.execute("select exported_table_count from export_metadata").fetchone()[0] == len(
            dag.MART_TABLES
        )
        assert connection.execute("select semantic_validation_status from export_metadata").fetchone()[0] == "pass"
        assert connection.execute("select count(*) from export_table_stats").fetchone()[0] == len(dag.MART_TABLES)
        assert connection.execute("select count(*) from mart_mode_window_summary").fetchone()[0] == 1
        assert connection.execute("select count(*) from mart_hour_window_summary").fetchone()[0] == 1
        assert connection.execute("select count(*) from mart_worst_delay_event").fetchone()[0] == 1
        assert connection.execute(
            "select stop_code, effective_zone_id, town_name from dim_stop_post_current"
        ).fetchone() == (
            "01",
            "1",
            "Zabki",
        )
        assert connection.execute(
            "select effective_zone_ids, stop_name_stems, town_names from dim_stop_group_current"
        ).fetchone() == (
            "1",
            "Boundary Stop",
            "Zabki",
        )
    sidecar = json.loads(Path(result.metadata_path).read_text(encoding="utf-8"))
    assert sidecar["semantic_validation"] == {
        "checked_dates": ["2026-07-02"],
        "status": "pass",
        "warning_count": 0,
        "warnings_truncated": False,
        "warnings": [],
    }


def test_export_metadata_includes_last_export_and_poller_status(tmp_path: Path) -> None:
    dag = _load_dag_module()
    exported_at = datetime(2026, 7, 6, 12, tzinfo=UTC)
    metadata = dag._export_metadata(
        dag.ExportConfig(
            export_id="export-1",
            output_dir=tmp_path,
            output_filename="ztm.duckdb",
            gcs_bucket="bucket",
            gcs_prefix="prefix",
            max_source_bytes=1000,
            max_duckdb_bytes=1000,
            cleanup_gcs_staging=False,
        ),
        [dag.TableStats(table_name="mart_trip_daily", row_count=1, size_bytes=10)],
        exported_at,
        duckdb_size_bytes=100,
        poller_status={"status": "healthy"},
    )

    assert metadata["last_export_at"] == exported_at.isoformat()
    assert metadata["poller_status"] == {"status": "healthy"}


def test_poller_status_sanitizes_private_heartbeat() -> None:
    dag = _load_dag_module()
    heartbeat = {
        "updated_at": "2026-07-06T12:00:00+00:00",
        "status": "ok",
        "poller_hostname": "private-hostname",
        "vehicle_types": {
            "bus": {
                "last_success_at": "2026-07-06T11:59:50+00:00",
                "last_accepted_rows": 12,
                "consecutive_failures": 0,
                "last_error_type": None,
                "private_extra": "secret",
            },
            "tram": {
                "last_success_at": "2026-07-06T11:59:40+00:00",
                "last_accepted_rows": 3,
                "consecutive_failures": 1,
                "last_error_type": "request_error",
            },
        },
    }

    status = dag._poller_status(
        FakeStorageClient([FakeBlob("health/poller/latest.json", data=json.dumps(heartbeat).encode())]),
        datetime(2026, 7, 6, 12, 1, tzinfo=UTC),
        "ztm-analytics-bucket",
    )

    assert status == {
        "status": "ok",
        "updated_at": "2026-07-06T12:00:00+00:00",
        "last_success_at": "2026-07-06T11:59:50+00:00",
        "stale_after_seconds": 180,
        "vehicle_types": {
            "bus": {
                "last_success_at": "2026-07-06T11:59:50+00:00",
                "last_accepted_rows": 12,
                "consecutive_failures": 0,
                "last_error_type": None,
            },
            "tram": {
                "last_success_at": "2026-07-06T11:59:40+00:00",
                "last_accepted_rows": 3,
                "consecutive_failures": 1,
                "last_error_type": "request_error",
            },
        },
    }


def test_poller_status_marks_old_heartbeat_stale() -> None:
    dag = _load_dag_module()
    heartbeat = {"updated_at": "2026-07-06T12:00:00+00:00", "status": "ok", "vehicle_types": {}}

    status = dag._poller_status(
        FakeStorageClient([FakeBlob("health/poller/latest.json", data=json.dumps(heartbeat).encode())]),
        datetime(2026, 7, 6, 12, 3, 1, tzinfo=UTC),
        "ztm-analytics-bucket",
    )

    assert status["status"] == "stale"


def test_poller_status_returns_unknown_when_heartbeat_missing() -> None:
    dag = _load_dag_module()

    status = dag._poller_status(FakeStorageClient([]), datetime(2026, 7, 6, 12, tzinfo=UTC), "ztm-analytics-bucket")

    assert status["status"] == "unknown"
    assert status["error_type"] == "RuntimeError"


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        json.dumps(["bad"]).encode(),
        json.dumps({"status": "ok"}).encode(),
        json.dumps({"updated_at": "not-a-time", "status": "ok"}).encode(),
    ],
)
def test_poller_status_returns_unknown_for_malformed_heartbeat(payload: bytes) -> None:
    dag = _load_dag_module()

    status = dag._poller_status(
        FakeStorageClient([FakeBlob("health/poller/latest.json", data=payload)]),
        datetime(2026, 7, 6, 12, tzinfo=UTC),
        "ztm-analytics-bucket",
    )

    assert status["status"] == "unknown"


def test_publish_duckdb_keeps_previous_file_when_validation_fails(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    final_path = tmp_path / "ztm.duckdb"
    metadata_path = tmp_path / "ztm.duckdb.meta.json"
    final_path.write_text("old duckdb", encoding="utf-8")
    metadata_path.write_text("old metadata", encoding="utf-8")
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [dag.TableStats(table_name=table_name, row_count=2, size_bytes=10) for table_name in dag.MART_TABLES]
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )

    with pytest.raises(RuntimeError, match="row count mismatch"):
        dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert final_path.read_text(encoding="utf-8") == "old duckdb"
    assert metadata_path.read_text(encoding="utf-8") == "old metadata"
    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []
    assert list(tmp_path.glob(".duckdb-tmp-*")) == []


def test_publish_duckdb_blocks_semantic_failure_and_keeps_previous_artifact(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    final_path = tmp_path / "ztm.duckdb"
    metadata_path = tmp_path / "ztm.duckdb.meta.json"
    final_path.write_text("old duckdb", encoding="utf-8")
    metadata_path.write_text("old metadata", encoding="utf-8")
    parquet_paths_by_table = _write_minimal_parquet_files(
        tmp_path,
        dag.MART_TABLES,
        duckdb,
        duplicate_trip=True,
    )
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=2 if table_name in {"mart_trip_daily", "mart_pipeline_status"} else 1,
            size_bytes=10,
        )
        for table_name in dag.MART_TABLES
    ]
    config = _test_export_config(dag, tmp_path)

    with pytest.raises(RuntimeError, match="mart_trip_daily_unique_key"):
        dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert final_path.read_text(encoding="utf-8") == "old duckdb"
    assert metadata_path.read_text(encoding="utf-8") == "old metadata"
    assert list(tmp_path.glob(".validation-tmp-*")) == []


def test_publish_duckdb_blocks_invalid_pipeline_status(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(
        tmp_path,
        dag.MART_TABLES,
        duckdb,
        invalid_status=True,
    )
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=2 if table_name == "mart_pipeline_status" else 1,
            size_bytes=10,
        )
        for table_name in dag.MART_TABLES
    ]

    with pytest.raises(RuntimeError, match="mart_pipeline_status_contract"):
        dag._publish_duckdb(
            _test_export_config(dag, tmp_path),
            parquet_paths_by_table,
            source_stats,
            datetime(2026, 7, 2, tzinfo=UTC),
        )


def test_publish_duckdb_checks_corrupt_changed_date_before_clean_latest_date(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(
        tmp_path,
        dag.MART_TABLES,
        duckdb,
        historical_duplicate_trip=True,
    )
    row_counts = {"dim_serving_date": 2, "mart_mode_window_summary": 2, "mart_pipeline_status": 2, "mart_trip_daily": 3}
    source_stats = [
        dag.TableStats(table_name=table_name, row_count=row_counts.get(table_name, 1), size_bytes=10)
        for table_name in dag.MART_TABLES
    ]
    config = replace(_test_export_config(dag, tmp_path), changed_partition_dates=("2026-07-01",))

    with pytest.raises(RuntimeError, match="mart_trip_daily_unique_key"):
        dag._publish_duckdb(
            config,
            parquet_paths_by_table,
            source_stats,
            datetime(2026, 7, 2, tzinfo=UTC),
        )


def test_publish_duckdb_records_warning_only_degradation(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(
        tmp_path,
        dag.MART_TABLES,
        duckdb,
        incomplete_status=True,
    )
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=2 if table_name == "mart_pipeline_status" else 1,
            size_bytes=10,
        )
        for table_name in dag.MART_TABLES
    ]

    result = dag._publish_duckdb(
        _test_export_config(dag, tmp_path),
        parquet_paths_by_table,
        source_stats,
        datetime(2026, 7, 2, tzinfo=UTC),
    )

    sidecar = json.loads(Path(result.metadata_path).read_text(encoding="utf-8"))
    assert sidecar["semantic_validation"]["status"] == "warning"
    assert sidecar["semantic_validation"]["warning_count"] == 1
    assert sidecar["semantic_validation"]["warnings_truncated"] is False
    assert sidecar["semantic_validation"]["warnings"] == [
        {
            "code": "incomplete_gps_day",
            "completeness_ratio": 23 / 24,
            "mode": "bus",
            "service_date": "2026-07-02",
        }
    ]


def test_semantic_validation_timeout_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    config = _test_export_config(dag, tmp_path)

    class TimeoutProcess:
        pid = 999_999
        returncode = None

        def communicate(self, timeout: int | None = None) -> tuple[str, str]:
            if timeout is not None:
                raise dag.subprocess.TimeoutExpired(["validator"], timeout)
            return "", ""

        def kill(self) -> None:
            return None

    monkeypatch.setattr(dag.subprocess, "Popen", lambda *_args, **_kwargs: TimeoutProcess())

    with pytest.raises(RuntimeError, match="timed out after 600 seconds"):
        dag._run_semantic_validation(config, tmp_path / "candidate.duckdb", tmp_path / "validation-temp")


@pytest.mark.skipif(os.name != "posix", reason="process-group termination is a Linux runtime contract")
def test_semantic_validation_terminates_timed_out_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dag = _load_dag_module()
    survived_path = tmp_path / "survived"
    child_code = (
        "import pathlib, time; time.sleep(1.5); "
        f"pathlib.Path({str(survived_path)!r}).write_text('bad', encoding='utf-8')"
    )
    (tmp_path / "serving_export_validator.py").write_text(
        f"import subprocess, sys, time\nsubprocess.Popen([sys.executable, '-c', {child_code!r}])\ntime.sleep(30)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dag, "__file__", str(tmp_path / "dag_serving_export.py"))
    config = replace(_test_export_config(dag, tmp_path), validation_timeout_seconds=1)

    with pytest.raises(RuntimeError, match="timed out after 1 seconds"):
        dag._run_semantic_validation(config, tmp_path / "candidate.duckdb", tmp_path / "validation-temp")

    time.sleep(1)
    assert not survived_path.exists()


@pytest.mark.parametrize(
    "report",
    [
        {"status": "pass", "checked_dates": [], "warning_count": 1, "warnings_truncated": False, "warnings": []},
        {
            "status": "warning",
            "checked_dates": ["not-a-date"],
            "warning_count": 1,
            "warnings_truncated": False,
            "warnings": [{"code": "serving_date_absent", "service_date": "not-a-date"}],
        },
        {
            "status": "warning",
            "checked_dates": ["2026-07-02"],
            "warning_count": 2,
            "warnings_truncated": False,
            "warnings": [{"code": "serving_date_absent", "service_date": "2026-07-02"}],
        },
        {
            "status": "warning",
            "checked_dates": ["2026-07-02"],
            "warning_count": 1,
            "warnings_truncated": False,
            "warnings": [{"code": "unexpected", "service_date": "2026-07-02"}],
        },
    ],
)
def test_semantic_validation_rejects_malformed_child_reports(report: dict[str, object]) -> None:
    dag = _load_dag_module()

    with pytest.raises(TypeError, match="invalid report"):
        dag._validate_semantic_report(report)


def test_semantic_validation_report_exposes_warning_truncation() -> None:
    validator = _load_validator_module()
    warnings = [
        {"code": "serving_date_absent", "service_date": "2026-01-01"} for _ in range(validator.MAX_WARNINGS + 1)
    ]

    report = validator._validation_report(("2026-01-01",), warnings)

    assert report["warning_count"] == validator.MAX_WARNINGS + 1
    assert report["warnings_truncated"] is True
    assert len(report["warnings"]) == validator.MAX_WARNINGS


@pytest.mark.skipif(os.name != "posix", reason="RLIMIT_AS is a Linux runtime contract")
def test_validator_applies_process_memory_limit_in_child() -> None:
    validator_path = Path(__file__).parents[1] / "dags" / "serving_export_validator.py"
    code = (
        "import importlib.util, resource; "
        f"spec=importlib.util.spec_from_file_location('validator', {str(validator_path)!r}); "
        "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
        "module._apply_process_memory_limit(256); print(resource.getrlimit(resource.RLIMIT_AS))"
    )

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and repository module path.
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == str((256 * 1024 * 1024, 256 * 1024 * 1024))


def test_publish_duckdb_cleans_temp_files_when_metadata_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    final_path = tmp_path / "ztm.duckdb"
    metadata_path = tmp_path / "ztm.duckdb.meta.json"
    final_path.write_text("old duckdb", encoding="utf-8")
    metadata_path.write_text("old metadata", encoding="utf-8")
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=2 if table_name == "mart_pipeline_status" else 1,
            size_bytes=10,
        )
        for table_name in dag.MART_TABLES
    ]
    config = dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )

    def fail_metadata_write(_path: Path, _metadata: dict[str, object]) -> None:
        raise RuntimeError("metadata write failed")

    monkeypatch.setattr(dag, "_write_metadata_file", fail_metadata_write)

    with pytest.raises(RuntimeError, match="metadata write failed"):
        dag._publish_duckdb(config, parquet_paths_by_table, source_stats, datetime(2026, 7, 2, tzinfo=UTC))

    assert list(tmp_path.glob("*.tmp")) == []
    assert list(tmp_path.glob(".*.tmp")) == []
    assert list(tmp_path.glob(".duckdb-tmp-*")) == []
    assert final_path.read_text(encoding="utf-8") == "old duckdb"
    assert metadata_path.read_text(encoding="utf-8") == "old metadata"
    assert list(tmp_path.glob(".validation-tmp-*")) == []


def test_publish_duckdb_restores_sidecar_when_database_swap_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    final_path = tmp_path / "ztm.duckdb"
    metadata_path = tmp_path / "ztm.duckdb.meta.json"
    final_path.write_text("old duckdb", encoding="utf-8")
    metadata_path.write_text("old metadata", encoding="utf-8")
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=2 if table_name == "mart_pipeline_status" else 1,
            size_bytes=10,
        )
        for table_name in dag.MART_TABLES
    ]
    original_replace = Path.replace

    def fail_database_swap(path: Path, target: Path) -> Path:
        if path.name == ".ztm.duckdb.export-1.tmp":
            raise OSError("database swap failed")
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_database_swap)

    with pytest.raises(OSError, match="database swap failed"):
        dag._publish_duckdb(
            _test_export_config(dag, tmp_path),
            parquet_paths_by_table,
            source_stats,
            datetime(2026, 7, 2, tzinfo=UTC),
        )

    assert final_path.read_text(encoding="utf-8") == "old duckdb"
    assert metadata_path.read_text(encoding="utf-8") == "old metadata"


def test_dag_is_asset_scheduled_and_exposes_single_export_task() -> None:
    dag = _load_dag_module()

    assert dag.dag.kwargs["dag_display_name"] == "Serving DuckDB export"
    assert isinstance(dag.dag.kwargs["schedule"], FakePartitionedAssetTimetable)
    assert dag.dag.kwargs["schedule"].assets is dag.GPS_MODELS_DATE_ASSET
    assert dag.dag.kwargs["max_active_runs"] == 1
    assert dag.dag.kwargs["on_failure_callback"] is dag.airflow_failure_alert
    assert dag.export_serving_duckdb.kwargs == {"retries": 0, "on_failure_callback": dag.airflow_failure_alert}


def _load_dag_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()
    sys.modules.pop("ztm_airflow_common", None)

    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    module_path = dag_dir / "dag_serving_export.py"
    module_name = "dag_serving_export_under_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load DAG module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_validator_module() -> types.ModuleType:
    module_path = Path(__file__).parents[1] / "dags" / "serving_export_validator.py"
    spec = importlib.util.spec_from_file_location("serving_export_validator_under_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load serving validator module spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _install_airflow_stubs() -> None:
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.task = FakeTaskDecorator()
    airflow_sdk_module.get_current_context = lambda: {"dag_run": FakeDagRun({})}
    airflow_sdk_module.Asset = FakeAsset
    airflow_sdk_module.PartitionedAssetTimetable = FakePartitionedAssetTimetable

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_api_core_module = types.ModuleType("google.api_core")
    google_api_core_exceptions_module = types.ModuleType("google.api_core.exceptions")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    storage_module = types.ModuleType("google.cloud.storage")

    google_api_core_exceptions_module.Conflict = Conflict
    google_api_core_exceptions_module.NotFound = NotFound
    bigquery_module.Client = lambda project: FakeBigQueryClient()
    bigquery_module.ExtractJobConfig = FakeExtractJobConfig
    bigquery_module.DestinationFormat = types.SimpleNamespace(PARQUET="PARQUET")
    storage_module.Client = lambda project: FakeStorageClient([])
    google_cloud_module.bigquery = bigquery_module
    google_cloud_module.storage = storage_module

    sys.modules["google"] = google_module
    sys.modules["google.api_core"] = google_api_core_module
    sys.modules["google.api_core.exceptions"] = google_api_core_exceptions_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module
    sys.modules["google.cloud.storage"] = storage_module


class FakeTaskDecorator:
    def __call__(self, function: Any | None = None, **kwargs: Any) -> Any:
        if function is None:
            return lambda decorated: FakeTask(decorated, kwargs)
        return FakeTask(function, kwargs)


class FakeTask:
    def __init__(self, function: Any, kwargs: dict[str, Any] | None = None) -> None:
        self.function = function
        self.kwargs = kwargs or {}

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self


class FakeAsset:
    def __init__(self, uri: str, *, name: str | None = None) -> None:
        self.uri = uri
        self.name = name


class FakePartitionedAssetTimetable:
    def __init__(self, *, assets: FakeAsset) -> None:
        self.assets = assets


class FakeDAG:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


@dataclass(frozen=True)
class FakeDagRun:
    conf: dict[str, object]


@dataclass(frozen=True)
class FakeAssetEvent:
    extra: dict[str, object]


@dataclass(frozen=True)
class ExtractCall:
    source_table: str
    destination_uri: str
    job_config: Any
    job_id: str
    location: str
    job: FakeJob


class FakeBigQueryClient:
    def __init__(self, *, raise_conflict: bool = False, partition_ids: list[str] | None = None) -> None:
        self.raise_conflict = raise_conflict
        self.extract_call: ExtractCall | None = None
        self.extract_calls: list[ExtractCall] = []
        self.query_call: QueryCall | None = None
        self.existing_job = FakeJob()
        self.get_job_call: tuple[str, str, str] | None = None
        self.partition_ids = partition_ids or []
        self.partition_table_refs: list[str] = []

    def extract_table(
        self,
        source_table: str,
        destination_uri: str,
        *,
        job_config: Any,
        job_id: str,
        location: str,
    ) -> FakeJob:
        if self.raise_conflict:
            raise Conflict("job exists")
        job = FakeJob()
        self.extract_call = ExtractCall(source_table, destination_uri, job_config, job_id, location, job)
        self.extract_calls.append(self.extract_call)
        return job

    def list_partitions(self, table_ref: str) -> list[str]:
        self.partition_table_refs.append(table_ref)
        return self.partition_ids

    def get_job(self, job_id: str, *, project: str, location: str) -> FakeJob:
        self.get_job_call = (job_id, project, location)
        return self.existing_job

    def query(self, query: str, *, job_id: str, location: str) -> FakeJob:
        if self.raise_conflict:
            raise Conflict("job exists")
        job = FakeJob()
        self.query_call = QueryCall(query, job_id, location, job)
        return job


@dataclass(frozen=True)
class QueryCall:
    query: str
    job_id: str
    location: str
    job: FakeJob


class FakeStatsBigQueryClient:
    def __init__(self, dag: types.ModuleType) -> None:
        self.dag = dag
        self.queries: list[str] = []
        self.table_refs: list[str] = []
        self.partition_table_refs: list[str] = []

    def get_table(self, table_ref: str) -> FakeTableMetadata:
        self.table_refs.append(table_ref)
        table_name = table_ref.rsplit(".", 1)[-1]
        return FakeTableMetadata(
            num_rows=10, num_bytes=100, time_partitioning=None if table_name == "dim_serving_date" else object()
        )

    def query(self, query: str) -> FakeQueryJob:
        self.queries.append(query)
        raise RuntimeError(f"unexpected query: {query}")

    def list_partitions(self, table_ref: str) -> list[str]:
        self.partition_table_refs.append(table_ref)
        return ["20260627", "20260628", "20260629", "20260630", "20260701", "20260702", "__NULL__"]


class FakeQueryJob:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def result(self) -> list[object]:
        return self.rows


@dataclass(frozen=True)
class FakeTableMetadata:
    num_rows: int
    num_bytes: int
    time_partitioning: object | None = object()


class FakeExtractJobConfig:
    def __init__(self, *, destination_format: str) -> None:
        self.destination_format = destination_format


class FakeJob:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeStorageClient:
    def __init__(self, blobs: list[FakeBlob]) -> None:
        self.blobs = blobs
        self.list_prefixes: list[str] = []
        self.blob_names: list[str] = []
        self.deleted_blob_names: list[str] = []
        self.copied_blob_names: list[tuple[str, str]] = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(bucket_name, self)


class FakeBucket:
    def __init__(self, bucket_name: str, storage_client: FakeStorageClient) -> None:
        self.bucket_name = bucket_name
        self.blobs = storage_client.blobs
        self.list_prefixes = storage_client.list_prefixes
        self.blob_names = storage_client.blob_names
        self.deleted_blob_names = storage_client.deleted_blob_names
        self.copied_blob_names = storage_client.copied_blob_names

    def list_blobs(self, *, prefix: str) -> list[FakeBlob]:
        self.list_prefixes.append(prefix)
        return [
            blob for blob in self.blobs if blob.name.startswith(prefix) and blob.name not in self.deleted_blob_names
        ]

    def blob(self, blob_name: str) -> FakeBlob:
        matching_blob = next((blob for blob in self.blobs if blob.name == blob_name), None)
        if matching_blob is None:
            if not blob_name.endswith("/_MANIFEST.json"):
                raise RuntimeError(f"unexpected blob lookup: {blob_name}")
            matching_blob = FakeBlob(blob_name, deleted_blob_names=self.deleted_blob_names)
            self.blobs.append(matching_blob)
        self.blob_names.append(blob_name)
        if matching_blob.data:
            return matching_blob
        return FakeBlob(
            blob_name,
            deleted_blob_names=self.deleted_blob_names,
            generation=matching_blob.current_generation or matching_blob.generation,
        )

    def copy_blob(self, blob: FakeBlob, _destination_bucket: FakeBucket, new_name: str) -> FakeBlob:
        if blob.fail_copy:
            raise RuntimeError("copy failed")
        self.copied_blob_names.append((blob.name, new_name))
        copied_blob = FakeBlob(new_name)
        self.blobs.append(copied_blob)
        return copied_blob


@dataclass
class FakeBlob:
    name: str
    fail_download: bool = False
    fail_delete: bool = False
    fail_copy: bool = False
    data: bytes = b""
    deleted_blob_names: list[str] | None = None
    updated: datetime | None = None
    size: int | None = None
    generation: int | None = 1
    current_generation: int | None = None

    def download_to_filename(self, filename: str) -> None:
        if self.fail_download:
            raise RuntimeError("stale listed blob was downloaded")
        Path(filename).write_text("downloaded", encoding="utf-8")

    def delete(self, *, if_generation_match: int | None = None) -> None:
        if self.fail_delete:
            raise RuntimeError("stale listed blob was deleted")
        if if_generation_match is not None and if_generation_match != self.generation:
            raise RuntimeError("stale generation was deleted")
        if self.deleted_blob_names is not None:
            self.deleted_blob_names.append(self.name)

    def download_as_bytes(self) -> bytes:
        return self.data

    def upload_from_string(self, data: str, *, content_type: str) -> None:
        assert content_type == "application/json"
        self.data = data.encode()


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass


class RecordingDuckdbConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str) -> None:
        self.queries.append(query)


def _write_minimal_parquet_files(  # noqa: C901, PLR0913
    tmp_path: Path,
    table_names: tuple[str, ...],
    duckdb: Any,
    *,
    duplicate_trip: bool = False,
    incomplete_status: bool = False,
    invalid_status: bool = False,
    historical_duplicate_trip: bool = False,
) -> dict[str, list[Path]]:
    paths_by_table = {}
    with duckdb.connect() as connection:
        for table_name in table_names:
            table_dir = tmp_path / "parquet" / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            path = table_dir / "part-000.parquet"
            escaped_path = path.as_posix().replace("'", "''")
            if table_name == "mart_trip_daily":
                connection.execute(_copy_minimal_trip_sql(escaped_path))
                if duplicate_trip:
                    duplicate_path = table_dir / "duplicate.parquet"
                    connection.execute("create temp table duplicate_trip as select * from read_parquet(?)", [str(path)])
                    duplicate_relation = connection.table("duplicate_trip")
                    duplicate_relation.union(duplicate_relation).write_parquet(str(duplicate_path))
                    connection.execute("drop table duplicate_trip")
                    duplicate_path.replace(path)
            elif table_name == "fct_expected_stop_event":
                connection.execute(_copy_minimal_expected_stop_event_sql(escaped_path))
            elif table_name == "dim_serving_date":
                connection.execute(_copy_minimal_serving_date_sql(escaped_path))
            elif table_name == "mart_mode_window_summary":
                connection.execute(_copy_minimal_mode_window_sql(escaped_path))
            elif table_name == "mart_pipeline_status":
                connection.execute(_copy_minimal_pipeline_status_sql(escaped_path, incomplete_status, invalid_status))
            elif table_name == "dim_stop_post_current":
                connection.execute(_copy_minimal_stop_post_current_sql(escaped_path))
            elif table_name == "dim_stop_group_current":
                connection.execute(_copy_minimal_stop_group_current_sql(escaped_path))
            else:
                connection.execute(
                    f"copy (select 1 as id, ? as table_name) to '{escaped_path}' (format parquet)", [table_name]
                )
            paths_by_table[table_name] = [path]
        if historical_duplicate_trip:
            _add_historical_duplicate_trip(connection, paths_by_table)
    return paths_by_table


def _add_historical_duplicate_trip(connection: Any, paths_by_table: dict[str, list[Path]]) -> None:
    additions = {
        "dim_serving_date": connection.sql(
            "select date '2026-07-01', '2026-07-01', null::date, date '2026-07-02', false, 2"
        ),
        "mart_mode_window_summary": connection.sql(
            "select 'bus', 'day', '2026-07-01', date '2026-07-01', date '2026-07-01', 1"
        ),
    }
    historical_trip = connection.sql(
        """
        select
            'gtfs-old', date '2026-07-01', 'trip-old', '1002', '190', '190', 'bus', 0,
            'Old', timestamp '2026-07-01 06:00:00', 'complete'
        """
    )
    additions["mart_trip_daily"] = historical_trip.union(historical_trip)
    for table_name, addition in additions.items():
        path = paths_by_table[table_name][0]
        replacement = path.with_name("historical.parquet")
        connection.read_parquet(str(path)).union(addition).write_parquet(str(replacement))
        replacement.replace(path)


def _test_export_config(dag: Any, tmp_path: Path) -> Any:
    return dag.ExportConfig(
        export_id="export-1",
        output_dir=tmp_path,
        output_filename="ztm.duckdb",
        gcs_bucket="bucket",
        gcs_prefix="prefix",
        max_source_bytes=1000,
        max_duckdb_bytes=10_000_000,
        cleanup_gcs_staging=False,
    )


def _copy_minimal_serving_date_sql(escaped_path: str) -> str:
    return f"""
        copy (
            select
                date '2026-07-02' as service_date,
                '2026-07-02' as service_date_key,
                null::date as previous_service_date,
                null::date as next_service_date,
                true as is_latest,
                1 as service_date_rank_desc
        ) to '{escaped_path}' (format parquet)
    """


def _copy_minimal_mode_window_sql(escaped_path: str) -> str:
    return f"""
        copy (
            select
                'bus' as mode,
                'day' as window_type,
                '2026-07-02' as window_key,
                date '2026-07-02' as source_start_date,
                date '2026-07-02' as source_end_date,
                1 as source_day_count
        ) to '{escaped_path}' (format parquet)
    """


def _copy_minimal_pipeline_status_sql(escaped_path: str, incomplete_status: bool, invalid_status: bool) -> str:
    present_hours = 25 if invalid_status else (23 if incomplete_status else 24)
    missing_hours = "[23]" if incomplete_status else "[]"
    completeness_ratio = 1.0 if invalid_status else present_hours / 24
    return f"""
        copy (
            select
                date '2026-07-02' as service_date,
                1 as vehicle_type,
                'bus' as mode,
                24 as expected_hours,
                {present_hours} as present_hours,
                {missing_hours}::integer[] as missing_hours,
                timestamp '2026-07-02 00:00:00' as first_observed_time,
                timestamp '2026-07-02 23:59:00' as last_observed_time,
                {completeness_ratio}::double as completeness_ratio,
                {str(not incomplete_status).lower()} as is_complete_day,
                100 as gps_row_count,
                10 as max_vehicle_count,
                1.0::double as mean_hourly_coverage_ratio,
                1.0::double as min_hourly_coverage_ratio,
                10 as max_gap_seconds,
                100 as pings_total,
                1 as trips_observed,
                1 as trips_complete,
                0 as trips_partial,
                0 as trips_broken,
                0.0::double as broken_rate,
                1 as expected_trips,
                1 as observed_trips,
                1.0::double as service_coverage_ratio,
                20.0::double as expected_service_minutes,
                20.0::double as observed_service_minutes,
                1 as stop_arrivals_count,
                'gtfs-1' as latest_gtfs_snapshot_id,
                timestamp '2026-07-02 00:00:00' as latest_gtfs_snapshot_at,
                1 as gtfs_snapshot_age_hours,
                1 as schedule_versions_active,
                1.0::double as health_ratio,
                'good' as health_label,
                null::timestamp as last_export_at,
                timestamp '2026-07-02 23:59:00' as status_generated_at
            union all
            select
                date '2026-07-02', 2, 'tram', 24, 24, []::integer[],
                timestamp '2026-07-02 00:00:00', timestamp '2026-07-02 23:59:00', 1.0::double, true,
                100, 10, 1.0::double, 1.0::double, 10, 100, 1, 1, 0, 0, 0.0::double,
                1, 1, 1.0::double, 20.0::double, 20.0::double, 1, 'gtfs-1',
                timestamp '2026-07-02 00:00:00', 1, 1, 1.0::double, 'good', null::timestamp,
                timestamp '2026-07-02 23:59:00'
        ) to '{escaped_path}' (format parquet)
    """


def _copy_minimal_expected_stop_event_sql(escaped_path: str) -> str:
    return f"""
        copy (
            select
                'gtfs-1' as gtfs_snapshot_id,
                date '2026-07-02' as service_date,
                'trip-1' as trip_id,
                '1001' as vehicle_number,
                0 as stop_sequence,
                '700201' as stop_id,
                '7002' as stop_group_id,
                '01' as stop_post_code,
                'Boundary Stop 01' as stop_name,
                timestamp '2026-07-02 06:00:00' as scheduled_arrival_time,
                timestamp '2026-07-02 06:01:00' as actual_arrival_time,
                60 as delay_seconds,
                'observed' as observation_status
        ) to '{escaped_path}' (format parquet)
    """


def _copy_minimal_stop_post_current_sql(escaped_path: str) -> str:
    return f"""
        copy (
            select
                '700201' as stop_id,
                '7002' as stop_group_id,
                '01' as stop_post_code,
                'Boundary Stop 01' as stop_name,
                '01' as stop_code,
                '1+2' as zone_id,
                '1' as effective_zone_id,
                'Boundary Stop' as stop_name_stem,
                'Zabki' as town_name,
                52.1 as stop_lat,
                21.1 as stop_lon,
                '190' as lines_served,
                'bus' as modes_served,
                '0' as directions_served,
                'gtfs-1' as gtfs_snapshot_id
        ) to '{escaped_path}' (format parquet)
    """


def _copy_minimal_stop_group_current_sql(escaped_path: str) -> str:
    return f"""
        copy (
            select
                '7002' as stop_group_id,
                'Boundary Stop' as stop_group_name,
                'Boundary Stop 01' as stop_group_names,
                '1+2' as zone_ids,
                '1' as effective_zone_ids,
                'Boundary Stop' as stop_name_stems,
                'Zabki' as town_names,
                52.1 as centroid_lat,
                21.1 as centroid_lon,
                1 as stop_post_count,
                '190' as lines_served,
                'bus' as modes_served,
                '0' as directions_served,
                'gtfs-1' as gtfs_snapshot_id
        ) to '{escaped_path}' (format parquet)
    """


def _copy_minimal_trip_sql(escaped_path: str) -> str:
    return f"""
        copy (
            select
                'gtfs-1' as gtfs_snapshot_id,
                date '2026-07-02' as service_date,
                'trip-1' as trip_id,
                '1001' as vehicle_number,
                '190' as line,
                '190' as route_short_name,
                'bus' as mode,
                0 as direction_id,
                'Boundary Stop' as trip_headsign,
                timestamp '2026-07-02 06:00:00' as scheduled_start_time,
                'complete' as trip_quality
        ) to '{escaped_path}' (format parquet)
    """


class FailingDuckdbModule:
    def connect(self, path: str, *, read_only: bool = False) -> FailingDuckdbConnection:
        return FailingDuckdbConnection(path, read_only)


class FailingDuckdbConnection:
    def __init__(self, path: str, read_only: bool) -> None:
        self.path = Path(path)
        self.read_only = read_only

    def __enter__(self) -> FailingDuckdbConnection:
        self.path.write_bytes(b"partial duckdb")
        raise RuntimeError("build failed")

    def __exit__(self, *_args: object) -> None:
        return None
