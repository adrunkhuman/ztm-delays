from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


def test_available_gps_part_uris_lists_existing_bus_and_tram_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    blobs = [
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/not-a-part.txt",
        "raw/gps/vehicle_type=tram/date=2026-06-25/hour=08/part-b.parquet",
    ]
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))

    uris = dag._available_gps_part_uris("2026-06-25")

    assert uris == [
        "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=2026-06-25/hour=08/part-b.parquet",
    ]


def test_gps_prefix_uses_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAW_GPS_PREFIX", "dev/raw/gps")
    dag = _load_dag_module()

    assert dag._gps_date_prefixes("2026-06-25") == [
        "dev/raw/gps/vehicle_type=bus/date=2026-06-25/",
        "dev/raw/gps/vehicle_type=tram/date=2026-06-25/",
    ]


def test_load_raw_gps_pings_returns_when_no_parts_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient([]))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert client.load_calls == []


def test_load_raw_gps_pings_uses_expected_bigquery_load_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient()
    blobs = [
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "raw/gps/vehicle_type=tram/date=2026-06-25/hour=07/part-b.parquet",
    ]
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert len(client.load_calls) == 2
    assert (
        client.load_calls[0].uri
        == "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"
    )
    assert client.load_calls[0].destination == "ztm-data.ztm_raw.raw_gps_pings"
    assert client.load_calls[0].job_id == dag._load_job_id(client.load_calls[0].uri)
    assert client.load_calls[0].location == dag.BIGQUERY_LOCATION
    assert client.load_calls[0].job_config.source_format == dag.bigquery.SourceFormat.PARQUET
    assert client.load_calls[0].job_config.create_disposition == dag.bigquery.CreateDisposition.CREATE_IF_NEEDED
    assert client.load_calls[0].job_config.write_disposition == dag.bigquery.WriteDisposition.WRITE_APPEND
    assert client.load_calls[0].job_config.time_partitioning.field == "Time"
    assert client.load_calls[0].job_config.time_partitioning.require_partition_filter is True
    assert client.load_calls[0].job_config.clustering_fields == ["Lines"]
    contract_path = Path(__file__).resolve().parents[2] / "contracts" / "raw_gps_v1.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    assert [
        (field.name, field.field_type, field.mode == "NULLABLE") for field in client.load_calls[0].job_config.schema
    ] == [(field["name"], field["bigquery_type"], field["nullable"]) for field in contract["fields"]]
    assert all(load_call.job.result_called for load_call in client.load_calls)


def test_load_raw_gps_pings_waits_on_existing_job_after_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    blobs = ["raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"]
    uri = "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"
    client = FakeBigQueryClient(conflict_job_ids={dag._load_job_id(uri)})
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert client.get_job_call == (dag._load_job_id(uri), "ztm-data", dag.BIGQUERY_LOCATION)
    assert client.existing_job.result_called is True


def test_load_raw_gps_pings_continues_after_one_existing_job(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    existing_uri = "gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet"
    new_uri = "gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=2026-06-25/hour=07/part-b.parquet"
    client = FakeBigQueryClient(conflict_job_ids={dag._load_job_id(existing_uri)})
    blobs = [
        "raw/gps/vehicle_type=bus/date=2026-06-25/hour=07/part-a.parquet",
        "raw/gps/vehicle_type=tram/date=2026-06-25/hour=07/part-b.parquet",
    ]
    monkeypatch.setattr(dag.storage, "Client", lambda project: FakeStorageClient(blobs))
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    dag._load_raw_gps_pings("2026-06-25")

    assert client.get_job_calls == [(dag._load_job_id(existing_uri), "ztm-data", dag.BIGQUERY_LOCATION)]
    assert [load_call.uri for load_call in client.load_calls] == [new_uri]
    assert client.load_calls[0].job.result_called is True
    assert client.existing_job.result_called is True


def test_selected_gtfs_snapshot_id_returns_processing_date_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(snapshot_rows=[FakeRow(gtfs_snapshot_id="snapshot-1")])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    assert dag._selected_gtfs_snapshot_id("2026-07-08") == "snapshot-1"

    assert client.query_call is not None
    assert "int_gtfs_processing_snapshot`" in client.query_call.query
    assert "where processing_date = @processing_date" in client.query_call.query
    assert client.query_call.job_config is not None
    assert client.query_call.job_config.query_parameters == [
        dag.bigquery.ScalarQueryParameter("processing_date", "DATE", "2026-07-08")
    ]


def test_selected_gtfs_snapshot_id_rejects_missing_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(snapshot_rows=[])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    with pytest.raises(dag.AirflowException, match="No governing GTFS snapshot mapping exists"):
        dag._selected_gtfs_snapshot_id("2026-07-08")


def test_selected_gtfs_snapshot_id_validates_optional_manual_expectation(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    client = FakeBigQueryClient(snapshot_rows=[FakeRow(gtfs_snapshot_id="snapshot-1")])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    assert dag._selected_expected_gtfs_snapshot_id("2026-07-08", "snapshot-1") == "snapshot-1"
    with pytest.raises(dag.AirflowException, match="does not match manual expectation"):
        dag._selected_expected_gtfs_snapshot_id("2026-07-08", "snapshot-other")


def test_selected_gtfs_snapshot_task_reads_optional_expected_snapshot_conf(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    monkeypatch.setattr(
        dag,
        "get_current_context",
        lambda: {"dag_run": types.SimpleNamespace(conf={"expected_gtfs_snapshot_id": "snapshot-1"})},
    )
    monkeypatch.setattr(dag, "_selected_expected_gtfs_snapshot_id", lambda date, expected: f"{date}:{expected}")

    assert dag.selected_gtfs_snapshot_id.function("2026-07-08") == "2026-07-08:snapshot-1"


def test_expected_input_inventory_digest_conf_is_optional_but_nonempty(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    monkeypatch.setattr(dag, "get_current_context", lambda: {"dag_run": types.SimpleNamespace(conf={})})
    assert dag._configured_expected_input_inventory_digest() is None
    monkeypatch.setattr(
        dag,
        "get_current_context",
        lambda: {"dag_run": types.SimpleNamespace(conf={"expected_input_inventory_digest": "digest"})},
    )
    assert dag._configured_expected_input_inventory_digest() == "digest"
    monkeypatch.setattr(
        dag,
        "get_current_context",
        lambda: {"dag_run": types.SimpleNamespace(conf={"expected_input_inventory_digest": " "})},
    )
    with pytest.raises(dag.AirflowException, match="non-empty"):
        dag._configured_expected_input_inventory_digest()


def test_guard_excluded_historical_processing_date_blocks_only_excluded_dates() -> None:
    dag = _load_dag_module()

    with pytest.raises(dag.AirflowException, match="exclusion_reason=incomplete_raw_gps_archive"):
        dag._guard_excluded_historical_processing_date("2026-06-26")
    with pytest.raises(dag.AirflowException, match="exclusion_reason=degraded_raw_gps_archive"):
        dag._guard_excluded_historical_processing_date("2026-07-05")

    assert dag._guard_excluded_historical_processing_date("2026-07-12") is None


def test_guard_prior_publication_requires_skip_only_for_excluded_prior_date() -> None:
    dag = _load_dag_module()

    with pytest.raises(dag.AirflowException, match="skip_prior_publication=true is required"):
        dag._guard_prior_publication("2026-06-27", False)
    assert dag._guard_prior_publication("2026-06-27", True) is True
    assert dag._guard_prior_publication("2026-07-09", False) is False
    with pytest.raises(dag.AirflowException, match="only allowed"):
        dag._guard_prior_publication("2026-07-09", True)


def test_matcher_input_policy_explicitly_excludes_prior_gps_at_outage_boundary() -> None:
    dag = _load_dag_module()

    assert dag._matcher_input_policy("2026-07-09", False) == {
        "include_prior_gps": True,
        "input_dates": ["2026-07-08", "2026-07-09"],
    }
    assert dag._matcher_input_policy("2026-07-08", True) == {
        "include_prior_gps": False,
        "input_dates": ["2026-07-08"],
    }


def test_bigquery_dbt_job_cost_summary_queries_jobs_by_user(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    started_at = dag.datetime(2026, 7, 5, 4, 0, tzinfo=dag.UTC)
    cost_row = FakeCostRow(
        job_count=2,
        total_bytes_processed=123,
        total_bytes_billed=100,
        top_jobs=[{"job_id": "job-1", "total_bytes_billed": 100}],
    )
    client = FakeBigQueryClient(cost_rows=[cost_row])
    monkeypatch.setattr(dag.bigquery, "Client", lambda project: client)

    summary = dag._bigquery_dbt_job_cost_summary(started_at)

    assert summary == {
        "job_count": 2,
        "total_bytes_processed": 123,
        "total_bytes_billed": 100,
        "top_jobs": [{"job_id": "job-1", "total_bytes_billed": 100}],
    }
    assert client.query_call is not None
    assert "INFORMATION_SCHEMA.JOBS_BY_USER" in client.query_call.query
    assert 'starts_with(query, \'/* {"app": "dbt"\')' in client.query_call.query
    assert client.query_call.job_config.query_parameters == [
        dag.bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at)
    ]


def test_log_bigquery_dbt_job_costs_returns_error_when_metadata_query_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _load_dag_module()

    def raise_metadata_error(_started_at: object) -> dict[str, object]:
        raise RuntimeError("metadata unavailable")

    monkeypatch.setattr(dag, "_bigquery_dbt_job_cost_summary", raise_metadata_error)

    result = dag.log_bigquery_dbt_job_costs.function()

    assert result["error"] == "metadata unavailable"


def test_log_bigquery_dbt_job_costs_warns_when_billed_bytes_cross_threshold(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dag = _load_dag_module()
    summary = {
        "job_count": 1,
        "total_bytes_processed": 200,
        "total_bytes_billed": dag.BIGQUERY_DBT_BYTES_BILLED_WARN_THRESHOLD + 1,
        "top_jobs": [],
    }
    monkeypatch.setattr(dag, "_bigquery_dbt_job_cost_summary", lambda _started_at: summary)
    caplog.set_level("WARNING")

    assert dag.log_bigquery_dbt_job_costs.function()["total_bytes_billed"] == summary["total_bytes_billed"]
    assert "exceeded warning threshold" in caplog.text


def test_log_bigquery_dbt_job_costs_uses_dag_run_start_date(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag_module()
    started_at = dag.datetime(2026, 7, 5, 4, 0, tzinfo=dag.UTC)
    captured_started_at = None

    def summarize_costs(received_started_at: object) -> dict[str, object]:
        nonlocal captured_started_at
        captured_started_at = received_started_at
        return {"job_count": 0, "total_bytes_processed": 0, "total_bytes_billed": 0, "top_jobs": []}

    monkeypatch.setattr(dag, "get_current_context", lambda: {"dag_run": types.SimpleNamespace(start_date=started_at)})
    monkeypatch.setattr(dag, "_bigquery_dbt_job_cost_summary", summarize_costs)

    result = dag.log_bigquery_dbt_job_costs.function()

    assert captured_started_at == started_at
    assert result["started_at"] == started_at.isoformat()


def _dbt_selected_models(task: Any) -> set[str]:
    command = task.kwargs["bash_command"]
    selector = command.split(" --select ", 1)[1].split(" --vars ", 1)[0]
    return set(selector.split())


def test_dag_schedules_partitioned_ingest_and_warehouse_runs() -> None:
    dag = _load_dag_module()

    assert isinstance(dag.raw_gps_dag.kwargs["schedule"], FakeCronPartitionTimetable)
    assert dag.raw_gps_dag.kwargs["schedule"].cron == dag.GPS_RAW_LOAD_CRON
    assert dag.raw_gps_dag.kwargs["schedule"].timezone == "Europe/Warsaw"
    assert dag.raw_gps_dag.kwargs["default_args"] == dag.AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS
    assert dag.raw_gps_dag.kwargs["on_failure_callback"] is dag.airflow_failure_alert
    assert "dag_run.partition_key" in dag.RAW_GPS_PROCESSING_DATE
    assert "data_interval_start" not in dag.RAW_GPS_PROCESSING_DATE
    assert dag.load_raw_gps_pings.kwargs == {"outlets": [dag.RAW_GPS_DATE_ASSET]}
    assert isinstance(dag.dag.kwargs["schedule"], FakeCronPartitionTimetable)
    assert dag.dag.kwargs["schedule"].cron == dag.GPS_WAREHOUSE_CRON
    assert dag.dag.kwargs["schedule"].timezone == "Europe/Warsaw"
    assert dag.dag.kwargs["schedule"].run_offset == -1
    assert dag.dag.kwargs["schedule"].key_format == "%Y-%m-%d"
    assert dag.dag.kwargs["default_args"] == dag.AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS
    assert dag.dag.kwargs["on_failure_callback"] is dag.airflow_failure_alert
    assert "dag_run.conf.get('processing_date') or dag_run.partition_key" in dag.PROCESSING_DATE
    assert "dag_run.conf.get('processing_date') or dag_run.partition_key" in dag.PRIOR_SERVICE_DATE


def test_dag_dbt_tasks_keep_bounded_model_and_test_selection() -> None:
    dag = _load_dag_module()

    assert _dbt_selected_models(dag.dbt_run_matcher_fact_dependencies) == {
        "stg_gtfs__trips",
        "stg_gtfs__stop_times",
        "stg_gtfs__stops",
        "stg_gtfs__routes",
        "stg_gtfs__calendar_dates",
        "int_gtfs_trip_schedule_history",
        "int_gtfs_trip_schedule",
        "int_gtfs_duty_chain",
        "int_schedule_version",
        "dim_schedule_version",
    }
    assert _dbt_selected_models(dag.dbt_run_fct_expected_stop_event_current) == {"fct_expected_stop_event"}
    assert _dbt_selected_models(dag.dbt_run_pipeline_status) == {"mart_pipeline_status"}
    assert _dbt_selected_models(dag.dbt_run_serving_marts) == {
        "int_serving_trip_execution",
        "int_serving_stop_arrival",
        "int_serving_observed_date",
        "int_serving_entity_window_summary",
        "dim_serving_window_date",
        "dim_serving_date",
        "mart_mode_window_summary",
        "mart_entity_daily_summary",
        "mart_entity_window_daily_summary",
        "mart_line_window_summary",
        "mart_stop_group_window_summary",
        "mart_stop_post_window_summary",
        "mart_hour_window_summary",
        "mart_entity_rankings",
        "mart_entity_timeline_daily",
        "mart_worst_delay_event",
        "mart_line_reliability_daily",
        "mart_trip_daily",
        "mart_trip_mode_daily_summary",
        "mart_trip_line_daily",
        "mart_line_trip_group_daily",
        "mart_line_course_window",
        "mart_line_course_stop_window",
        "mart_stop_line_window_summary",
        "mart_stop_post_line_group_window",
        "mart_stop_group_line_group_window",
        "mart_pipeline_status_recent_summary",
    }
    assert "--exclude test_type:unit" in dag.dbt_test_fct_stop_arrival_current.kwargs["bash_command"]
    assert "--exclude test_type:unit" in dag.dbt_test_fct_expected_stop_event_current.kwargs["bash_command"]
    assert '"publish_service_date": "' + dag.PROCESSING_DATE in dag.dbt_run_fct_trip_current.kwargs["bash_command"]
    assert '"publish_service_date": "' + dag.PRIOR_SERVICE_DATE in dag.dbt_run_fct_trip_prior.kwargs["bash_command"]
    assert "--exclude tag:audit" in dag.dbt_test_stg_gps_pings.kwargs["bash_command"]
    assert "--exclude test_type:generic" in dag.dbt_test_stg_gps_pings.kwargs["bash_command"]
    assert "--exclude test_type:generic" in dag.dbt_test_int_gps_hourly_completeness.kwargs["bash_command"]
    assert "--exclude test_type:generic" in dag.dbt_test_serving_universe_prior.kwargs["bash_command"]
    assert "--exclude test_type:generic" in dag.dbt_test_serving_universe.kwargs["bash_command"]
    assert "--exclude test_type:generic" in dag.dbt_test_serving_marts_prior.kwargs["bash_command"]
    assert "--exclude test_type:generic" in dag.dbt_test_serving_marts.kwargs["bash_command"]
    assert "--exclude tag:audit" in dag.dbt_test_serving_marts.kwargs["bash_command"]
    assert dag.emit_gps_models_date_asset.kwargs == {"outlets": [dag.GPS_MODELS_DATE_ASSET]}


def test_dag_runs_trip_facts_before_serving_publication() -> None:
    dag = _load_dag_module()

    expected_edges = [
        (dag.dbt_run_stg_gps_pings, dag.dbt_test_stg_gps_pings),
        (dag.historical_date_guard, dag.prior_publication_guard),
        (dag.prior_publication_guard, dag.selected_gtfs_snapshot),
        (dag.prior_publication_guard, dag.selected_prior_gtfs_snapshot),
        (dag.selected_gtfs_snapshot, dag.dbt_run_stg_gps_pings),
        (dag.selected_gtfs_snapshot, dag.matcher_load),
        (dag.dbt_test_stg_gps_pings, dag.matcher_load),
        (dag.matcher_load, dag.matcher_publish),
        (dag.selected_gtfs_snapshot, dag.dbt_run_matcher_fact_dependencies),
        (dag.dbt_test_stg_gps_pings, dag.dbt_run_int_gps_hourly_completeness),
        (dag.dbt_run_int_gps_hourly_completeness, dag.dbt_test_int_gps_hourly_completeness),
        (dag.matcher_publish, dag.dbt_run_fct_trip_current),
        (dag.matcher_publish, dag.dbt_run_fct_trip_prior),
        (dag.dbt_run_matcher_fact_dependencies, dag.dbt_run_fct_trip_current),
        (dag.dbt_run_matcher_fact_dependencies, dag.dbt_run_fct_trip_prior),
        (dag.dbt_test_fct_trip_current, dag.dbt_run_fct_stop_arrival_current),
        (dag.dbt_test_fct_trip_prior, dag.dbt_run_fct_stop_arrival_prior),
        (dag.dbt_test_fct_stop_arrival_current, dag.dbt_run_fct_expected_stop_event_current),
        (dag.dbt_run_fct_expected_stop_event_current, dag.dbt_test_fct_expected_stop_event_current),
        (dag.dbt_test_fct_stop_arrival_prior, dag.dbt_run_fct_expected_stop_event_prior),
        (dag.dbt_run_fct_expected_stop_event_prior, dag.dbt_test_fct_expected_stop_event_prior),
        (dag.dbt_test_fct_expected_stop_event_current, dag.dbt_run_completeness_and_coverage),
        (dag.dbt_test_fct_expected_stop_event_prior, dag.dbt_run_completeness_and_coverage),
        (dag.dbt_test_int_gps_hourly_completeness, dag.dbt_run_completeness_and_coverage),
        (dag.selected_prior_gtfs_snapshot, dag.dbt_run_prior_coverage_schedule),
    ]
    for upstream_task, downstream_task in expected_edges:
        assert downstream_task in upstream_task.downstream

    assert dag.dbt_run_pipeline_status in dag.dbt_test_completeness_and_coverage.downstream
    assert dag.dbt_run_prior_coverage_schedule in dag.dbt_test_pipeline_status.downstream
    assert dag.dbt_run_completeness_and_coverage_prior in dag.dbt_run_prior_coverage_schedule.downstream
    assert dag.dbt_run_pipeline_status_prior in dag.dbt_test_completeness_and_coverage_prior.downstream
    assert dag.dbt_run_serving_universe_prior in dag.dbt_test_pipeline_status_prior.downstream
    assert dag.dbt_test_serving_universe_prior in dag.dbt_run_serving_universe_prior.downstream
    assert dag.dbt_restore_current_coverage_schedule in dag.dbt_test_serving_universe_prior.downstream
    assert dag.dbt_run_serving_universe in dag.dbt_restore_current_coverage_schedule.downstream
    assert dag.dbt_restore_current_coverage_schedule.kwargs["trigger_rule"] == dag.TriggerRule.ALL_DONE
    assert dag.dbt_test_serving_universe in dag.dbt_run_serving_universe.downstream
    assert dag.dbt_run_serving_marts_prior in dag.dbt_test_serving_universe_prior.downstream
    assert dag.dbt_run_serving_marts_prior in dag.dbt_test_serving_universe.downstream
    assert dag.dbt_test_serving_marts_prior in dag.dbt_run_serving_marts_prior.downstream
    assert dag.dbt_run_serving_marts in dag.dbt_test_serving_marts_prior.downstream
    assert dag.dbt_test_serving_marts in dag.dbt_run_serving_marts.downstream
    assert dag.LOG_BIGQUERY_DBT_JOB_COSTS is False
    assert dag.log_bigquery_dbt_job_costs not in dag.dbt_test_serving_marts.downstream
    assert dag.emit_gps_models_date_asset in dag.dbt_test_serving_marts.downstream
    assert dag.matcher_load.kwargs["execution_timeout"] == dag.timedelta(minutes=60)
    assert dag.matcher_publish.kwargs["execution_timeout"] == dag.timedelta(minutes=60)
    assert dag.dbt_run_fct_trip_current in dag.matcher_publish.downstream
    assert dag.log_bigquery_dbt_job_costs.kwargs == {"do_xcom_push": False}
    assert dag.watcher in dag.dbt_test_serving_marts.downstream
    assert dag.fail_on_any_task_failure.kwargs["retries"] == 0


def test_dag_can_enable_bigquery_dbt_job_cost_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_BIGQUERY_DBT_JOB_COSTS", "true")
    dag = _load_dag_module()

    assert dag.LOG_BIGQUERY_DBT_JOB_COSTS is True
    assert dag.log_bigquery_dbt_job_costs in dag.dbt_test_serving_marts.downstream
    assert dag.emit_gps_models_date_asset not in dag.log_bigquery_dbt_job_costs.downstream


def test_dag_uses_bounded_mart_windows() -> None:
    dag = _load_dag_module()
    expected_aggregation_vars = [
        (dag.dbt_run_completeness_and_coverage, dag.PROCESSING_DATE),
        (dag.dbt_test_completeness_and_coverage, dag.PROCESSING_DATE),
        (dag.dbt_run_pipeline_status, dag.PROCESSING_DATE),
        (dag.dbt_test_pipeline_status, dag.PROCESSING_DATE),
        (dag.dbt_run_completeness_and_coverage_prior, dag.PRIOR_SERVICE_DATE),
        (dag.dbt_test_completeness_and_coverage_prior, dag.PRIOR_SERVICE_DATE),
        (dag.dbt_run_pipeline_status_prior, dag.PRIOR_SERVICE_DATE),
        (dag.dbt_test_pipeline_status_prior, dag.PRIOR_SERVICE_DATE),
    ]

    for task, expected_start_date in expected_aggregation_vars:
        assert '"aggregation_start_date": "' + expected_start_date in task.kwargs["bash_command"]
    assert "aggregation_start_date" not in dag.dbt_run_serving_marts.kwargs["bash_command"]
    assert "aggregation_start_date" not in dag.dbt_test_serving_marts.kwargs["bash_command"]
    assert '"processing_date": "' + dag.PRIOR_SERVICE_DATE in dag.dbt_run_serving_marts_prior.kwargs["bash_command"]
    assert '"processing_date": "' + dag.PRIOR_SERVICE_DATE in dag.dbt_test_serving_marts_prior.kwargs["bash_command"]
    assert '"max_gps_date": "' + dag.PROCESSING_DATE in dag.dbt_run_serving_marts_prior.kwargs["bash_command"]
    assert '"processing_date": "' + dag.PRIOR_SERVICE_DATE in dag.dbt_run_serving_universe_prior.kwargs["bash_command"]
    assert dag.SELECTED_PRIOR_GTFS_SNAPSHOT_ID in dag.dbt_run_serving_universe_prior.kwargs["bash_command"]
    assert dag.SERVING_UNIVERSE_MODELS in dag.dbt_run_serving_universe.kwargs["bash_command"]
    assert dag.COVERAGE_SCHEDULE_MODELS in dag.dbt_run_prior_coverage_schedule.kwargs["bash_command"]
    assert dag.SELECTED_PRIOR_GTFS_SNAPSHOT_ID in dag.dbt_run_prior_coverage_schedule.kwargs["bash_command"]
    assert dag.SELECTED_GTFS_SNAPSHOT_ID in dag.dbt_restore_current_coverage_schedule.kwargs["bash_command"]
    assert dag.SERVING_MODELS in dag.dbt_run_serving_marts.kwargs["bash_command"]
    assert dag.PRIOR_SERVING_MODELS in dag.dbt_run_serving_marts_prior.kwargs["bash_command"]


def test_gps_models_asset_reports_current_and_prior_changed_partitions() -> None:
    dag = _load_dag_module()

    metadata = list(dag.emit_gps_models_date_asset.function("2026-07-08", False))

    assert len(metadata) == 1
    assert metadata[0].asset == dag.GPS_MODELS_DATE_ASSET
    assert metadata[0].extra == {
        "processing_date": "2026-07-08",
        "changed_partition_dates": ["2026-07-07", "2026-07-08"],
    }


def test_gps_models_asset_omits_excluded_prior_changed_partition() -> None:
    dag = _load_dag_module()

    metadata = list(dag.emit_gps_models_date_asset.function("2026-07-08", True))

    assert metadata[0].extra == {
        "processing_date": "2026-07-08",
        "changed_partition_dates": ["2026-07-08"],
    }


def test_prior_publication_dbt_tasks_noop_when_requested() -> None:
    dag = _load_dag_module()

    for task in [
        dag.dbt_run_fct_trip_prior,
        dag.dbt_test_fct_trip_prior,
        dag.dbt_run_fct_stop_arrival_prior,
        dag.dbt_test_fct_stop_arrival_prior,
        dag.dbt_run_fct_expected_stop_event_prior,
        dag.dbt_test_fct_expected_stop_event_prior,
        dag.dbt_run_prior_coverage_schedule,
        dag.dbt_run_completeness_and_coverage_prior,
        dag.dbt_test_completeness_and_coverage_prior,
        dag.dbt_run_pipeline_status_prior,
        dag.dbt_test_pipeline_status_prior,
        dag.dbt_run_serving_universe_prior,
        dag.dbt_test_serving_universe_prior,
        dag.dbt_run_serving_marts_prior,
        dag.dbt_test_serving_marts_prior,
    ]:
        assert "skip_prior_publication" in task.kwargs["bash_command"]
        assert "Skipping prior publication task" in task.kwargs["bash_command"]


@dataclass
class FakeBlob:
    name: str


class FakeBucket:
    def __init__(self, blob_names: list[str]) -> None:
        self.blob_names = blob_names

    def list_blobs(self, *, prefix: str) -> list[FakeBlob]:
        return [FakeBlob(blob_name) for blob_name in self.blob_names if blob_name.startswith(prefix)]


class FakeStorageClient:
    def __init__(self, blob_names: list[str]) -> None:
        self.blob_names = blob_names

    def bucket(self, bucket_name: str) -> FakeBucket:
        assert bucket_name == "ztm-analytics-bucket"
        return FakeBucket(self.blob_names)


@dataclass
class LoadCall:
    uri: str
    destination: str
    job_config: Any
    job_id: str
    location: str
    job: FakeJob


@dataclass(frozen=True)
class QueryCall:
    query: str
    job_config: Any


@dataclass(frozen=True)
class FakeRow:
    gtfs_snapshot_id: str | None = None
    source_start_date: str | None = None
    partition_dates: str | None = None


@dataclass(frozen=True)
class FakeCostRow:
    job_count: int
    total_bytes_processed: int
    total_bytes_billed: int
    top_jobs: list[dict[str, object]]


class FakeJob:
    def __init__(self) -> None:
        self.result_called = False

    def result(self) -> None:
        self.result_called = True


class FakeQueryJob:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def result(self) -> list[Any]:
        return self.rows


class FakeBigQueryClient:
    def __init__(
        self,
        *,
        conflict_job_ids: set[str] | None = None,
        snapshot_rows: list[FakeRow] | None = None,
        period_window_rows: list[FakeRow] | None = None,
        cost_rows: list[FakeCostRow] | None = None,
    ) -> None:
        self.conflict_job_ids = conflict_job_ids or set()
        self.snapshot_rows = snapshot_rows or []
        self.period_window_rows = period_window_rows or []
        self.cost_rows = cost_rows or []
        self.load_calls: list[LoadCall] = []
        self.existing_job = FakeJob()
        self.get_job_call: tuple[str, str, str] | None = None
        self.get_job_calls: list[tuple[str, str, str]] = []
        self.query_call: QueryCall | None = None

    def load_table_from_uri(
        self,
        uri: str,
        destination: str,
        *,
        job_config: Any,
        job_id: str,
        location: str,
    ) -> FakeJob:
        if job_id in self.conflict_job_ids:
            raise Conflict("job already exists")
        job = FakeJob()
        self.load_calls.append(LoadCall(uri, destination, job_config, job_id, location, job))
        return job

    def get_job(self, job_id: str, *, project: str, location: str) -> FakeJob:
        self.get_job_call = (job_id, project, location)
        self.get_job_calls.append((job_id, project, location))
        return self.existing_job

    def query(self, query: str, *, job_config: Any = None) -> FakeQueryJob:
        self.query_call = QueryCall(query, job_config)
        if "INFORMATION_SCHEMA.JOBS_BY_USER" in query:
            return FakeQueryJob(self.cost_rows)
        if "dim_schedule_version" in query:
            return FakeQueryJob(self.period_window_rows)
        return FakeQueryJob(self.snapshot_rows)


def _load_dag_module() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()
    sys.modules.pop("ztm_airflow_common", None)

    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    module_path = dag_dir / "dag_daily_gps.py"
    module_name = "dag_daily_gps_under_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load DAG module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _install_airflow_stubs() -> None:
    airflow_module = types.ModuleType("airflow")
    airflow_exceptions_module = types.ModuleType("airflow.exceptions")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    bash_module = types.ModuleType("airflow.providers.standard.operators.bash")

    airflow_exceptions_module.AirflowException = type("AirflowException", (Exception,), {})
    airflow_sdk_module.DAG = FakeDAG
    airflow_sdk_module.Asset = FakeAsset
    airflow_sdk_module.CronPartitionTimetable = FakeCronPartitionTimetable
    airflow_sdk_module.Metadata = FakeMetadata
    airflow_sdk_module.PartitionedAssetTimetable = FakePartitionedAssetTimetable
    airflow_sdk_module.StartOfDayMapper = FakeStartOfDayMapper
    airflow_sdk_module.TaskGroup = FakeTaskGroup
    airflow_sdk_module.TriggerRule = types.SimpleNamespace(ALL_DONE="all_done", ONE_FAILED="one_failed")
    airflow_sdk_module.get_current_context = lambda: {"dag_run": FakeDagRun()}
    airflow_sdk_module.task = FakeTaskDecorator()
    bash_module.BashOperator = FakeOperator

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.exceptions"] = airflow_exceptions_module
    sys.modules["airflow.sdk"] = airflow_sdk_module
    sys.modules["airflow.providers"] = types.ModuleType("airflow.providers")
    sys.modules["airflow.providers.standard"] = types.ModuleType("airflow.providers.standard")
    sys.modules["airflow.providers.standard.operators"] = types.ModuleType("airflow.providers.standard.operators")
    sys.modules["airflow.providers.standard.operators.bash"] = bash_module


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    google_api_core_module = types.ModuleType("google.api_core")
    google_api_core_exceptions_module = types.ModuleType("google.api_core.exceptions")
    google_cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    storage_module = types.ModuleType("google.cloud.storage")

    google_api_core_exceptions_module.Conflict = Conflict
    google_api_core_exceptions_module.NotFound = NotFound
    google_api_core_exceptions_module.PreconditionFailed = PreconditionFailed
    bigquery_module.Client = lambda project: FakeBigQueryClient()
    bigquery_module.SourceFormat = types.SimpleNamespace(PARQUET="PARQUET")
    bigquery_module.CreateDisposition = types.SimpleNamespace(CREATE_IF_NEEDED="CREATE_IF_NEEDED")
    bigquery_module.WriteDisposition = types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND")
    bigquery_module.TimePartitioningType = types.SimpleNamespace(DAY="DAY")
    bigquery_module.TimePartitioning = FakeTimePartitioning
    bigquery_module.Dataset = FakeDataset
    bigquery_module.Table = FakeTable
    bigquery_module.SchemaField = FakeSchemaField
    bigquery_module.LoadJobConfig = FakeLoadJobConfig
    bigquery_module.QueryJobConfig = FakeQueryJobConfig
    bigquery_module.ScalarQueryParameter = FakeScalarQueryParameter
    storage_module.Client = lambda project: FakeStorageClient([])
    google_cloud_module.bigquery = bigquery_module
    google_cloud_module.storage = storage_module

    sys.modules["google"] = google_module
    sys.modules["google.api_core"] = google_api_core_module
    sys.modules["google.api_core.exceptions"] = google_api_core_exceptions_module
    sys.modules["google.cloud"] = google_cloud_module
    sys.modules["google.cloud.bigquery"] = bigquery_module
    sys.modules["google.cloud.storage"] = storage_module


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass


class PreconditionFailed(Exception):
    pass


class FakeDAG:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> FakeDAG:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeTaskGroup:
    def __init__(self, group_id: str | None, **kwargs: Any) -> None:
        self.kwargs = {"group_id": group_id} | kwargs

    def __enter__(self) -> FakeTaskGroup:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeDagRun:
    start_date = None


class FakeOperator:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.downstream: list[FakeOperator] = []

    def __rshift__(self, _other: FakeOperator) -> FakeOperator:
        self.downstream.append(_other)
        return _other

    def __rrshift__(self, upstream: list[object]) -> FakeOperator:
        for task in upstream:
            if hasattr(task, "downstream"):
                task.downstream.append(self)
        return self


class FakeTaskDecorator:
    def __call__(self, function: Any | None = None, **kwargs: Any) -> Any:
        if function is None:
            return lambda decorated: FakeTask(decorated, kwargs)
        return FakeTask(function, kwargs)


class FakeTask:
    def __init__(self, function: Any, kwargs: dict[str, Any] | None = None) -> None:
        self.function = function
        self.kwargs = kwargs or {}
        self.downstream: list[object] = []

    def __call__(self, *_args: object, **_kwargs: object) -> FakeTask:
        return self

    def __rshift__(self, downstream: object) -> object:
        self.downstream.append(downstream)
        return downstream

    def __rrshift__(self, upstream: list[object]) -> FakeTask:
        for task in upstream:
            if hasattr(task, "downstream"):
                task.downstream.append(self)
        return self


class FakeAsset:
    def __init__(self, uri: str, *, name: str | None = None) -> None:
        self.uri = uri
        self.name = name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeAsset) and self.uri == other.uri

    def __hash__(self) -> int:
        return hash(self.uri)


class FakeCronPartitionTimetable:
    def __init__(
        self,
        cron: str,
        *,
        timezone: str,
        run_offset: int = 0,
        key_format: str = "%Y-%m-%dT%H:%M:%S",
    ) -> None:
        self.cron = cron
        self.timezone = timezone
        self.run_offset = run_offset
        self.key_format = key_format


class FakeStartOfDayMapper:
    pass


class FakePartitionedAssetTimetable:
    def __init__(self, *, assets: FakeAsset, default_partition_mapper: FakeStartOfDayMapper) -> None:
        self.assets = assets
        self.default_partition_mapper = default_partition_mapper


class FakeMetadata:
    def __init__(self, asset: FakeAsset, extra: dict[str, Any]) -> None:
        self.asset = asset
        self.extra = extra


class FakeTimePartitioning:
    def __init__(self, *, type_: str, field: str, require_partition_filter: bool = False) -> None:
        self.type_ = type_
        self.field = field
        self.require_partition_filter = require_partition_filter


class FakeDataset:
    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id
        self.location: str | None = None


class FakeSchemaField:
    def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode


class FakeTable:
    def __init__(self, table_id: str, schema: list[FakeSchemaField]) -> None:
        self.table_id = table_id
        self.schema = schema
        self.time_partitioning: FakeTimePartitioning | None = None
        self.labels: dict[str, str] = {}


class FakeLoadJobConfig:
    def __init__(  # noqa: PLR0913
        self,
        *,
        schema: list[FakeSchemaField],
        source_format: str,
        create_disposition: str,
        write_disposition: str,
        time_partitioning: FakeTimePartitioning,
        clustering_fields: list[str],
    ) -> None:
        self.schema = schema
        self.source_format = source_format
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.time_partitioning = time_partitioning
        self.clustering_fields = clustering_fields


@dataclass(frozen=True)
class FakeScalarQueryParameter:
    name: str
    type_: str
    value: str


class FakeQueryJobConfig:
    def __init__(self, *, query_parameters: list[FakeScalarQueryParameter]) -> None:
        self.query_parameters = query_parameters
