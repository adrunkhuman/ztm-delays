from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest


def test_mart_table_list_exports_frontend_source_tables() -> None:
    dag = _load_dag_module()

    assert set(dag.MART_TABLES) == {
        "dim_serving_date",
        "dim_stop_group_current",
        "dim_stop_post_current",
        "fct_expected_stop_event",
        "mart_entity_daily_summary",
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


def test_derived_table_list_exports_frontend_serving_tables() -> None:
    dag = _load_dag_module()

    assert dag.DERIVED_TABLES == ()
    assert dag.EXPORTED_TABLES == dag.MART_TABLES + dag.DERIVED_TABLES


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


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("export_id", "bad;id", ValueError),
        ("output_filename", "nested/ztm.duckdb", ValueError),
        ("max_source_bytes", 0, ValueError),
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


def test_publish_duckdb_builds_queryable_file_with_metadata(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [
        dag.TableStats(
            table_name=table_name,
            row_count=1,
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
            dag.EXPORTED_TABLES
        )
        assert connection.execute("select count(*) from export_table_stats").fetchone()[0] == len(dag.EXPORTED_TABLES)
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


def test_publish_duckdb_cleans_temp_files_when_metadata_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    parquet_paths_by_table = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    source_stats = [dag.TableStats(table_name=table_name, row_count=1, size_bytes=10) for table_name in dag.MART_TABLES]
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


def test_dag_is_asset_scheduled_and_exposes_single_export_task() -> None:
    dag = _load_dag_module()

    assert dag.dag.kwargs["dag_display_name"] == "Serving DuckDB export"
    assert dag.dag.kwargs["schedule"] == [dag.GPS_MODELS_DATE_ASSET]
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


def _install_airflow_stubs() -> None:
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")

    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.task = FakeTaskDecorator()
    airflow_sdk_module.get_current_context = lambda: {"dag_run": FakeDagRun({})}
    airflow_sdk_module.Asset = FakeAsset

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
        return FakeBlob(blob_name, deleted_blob_names=self.deleted_blob_names)

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

    def download_to_filename(self, filename: str) -> None:
        if self.fail_download:
            raise RuntimeError("stale listed blob was downloaded")
        Path(filename).write_text("downloaded", encoding="utf-8")

    def delete(self) -> None:
        if self.fail_delete:
            raise RuntimeError("stale listed blob was deleted")
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


def _write_minimal_parquet_files(tmp_path: Path, table_names: tuple[str, ...], duckdb: Any) -> dict[str, list[Path]]:
    paths_by_table = {}
    with duckdb.connect() as connection:
        for table_name in table_names:
            table_dir = tmp_path / "parquet" / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            path = table_dir / "part-000.parquet"
            escaped_path = path.as_posix().replace("'", "''")
            if table_name == "mart_trip_daily":
                connection.execute(_copy_minimal_trip_sql(escaped_path))
            elif table_name == "dim_stop_post_current":
                connection.execute(_copy_minimal_stop_post_current_sql(escaped_path))
            elif table_name == "dim_stop_group_current":
                connection.execute(_copy_minimal_stop_group_current_sql(escaped_path))
            else:
                connection.execute(
                    f"copy (select 1 as id, ? as table_name) to '{escaped_path}' (format parquet)", [table_name]
                )
            paths_by_table[table_name] = [path]
    return paths_by_table


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
