from __future__ import annotations

import importlib.util
import re
import sys
import types
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from .test_dag_daily_gps import PreconditionFailed, _install_airflow_stubs, _install_google_stubs


def _query_config(**kwargs: object) -> types.SimpleNamespace:
    return types.SimpleNamespace(**kwargs)


def _query_parameter(*args: object) -> tuple[object, ...]:
    return args


def test_historical_plan_requires_explicit_bounded_non_degraded_dates() -> None:
    planner = _load_planner()

    with pytest.raises(ValueError, match="explicit"):
        planner.build_historical_correction_plan(plan_id="plan-1")
    with pytest.raises(ValueError, match="no eligible"):
        planner.build_historical_correction_plan(
            "2026-07-05", "2026-07-05", plan_id="plan-1", refresh_through_date="2026-07-05"
        )
    with pytest.raises(ValueError, match="allowed bounded"):
        planner.build_historical_correction_plan(
            "2026-06-26", "2026-06-27", plan_id="plan-1", refresh_through_date="2026-06-27"
        )
    with pytest.raises(ValueError, match="plan_id"):
        planner.build_historical_correction_plan(
            "2026-07-09", "2026-07-09", plan_id="bad plan", refresh_through_date="2026-07-09"
        )
    with pytest.raises(SystemExit):
        planner.main(["plan", "--start-date", "2026-07-09", "--end-date", "2026-07-09"])


def test_historical_plan_inventories_exact_mapping_without_mutation() -> None:
    planner = _load_planner()
    planner.bigquery.QueryJobConfig = _query_config
    planner.bigquery.ScalarQueryParameter = _query_parameter
    bq_client = FakeBigQueryClient(
        [
            types.SimpleNamespace(
                processing_date="2026-07-08", gtfs_snapshot_id="snapshot-8", gcs_path="gs://gtfs/snapshot.zip"
            ),
            types.SimpleNamespace(
                processing_date="2026-07-09", gtfs_snapshot_id="snapshot-9", gcs_path="gs://gtfs/snapshot.zip"
            ),
        ]
    )
    storage_client = FakeStorageClient()

    plan = planner.build_historical_correction_plan(
        "2026-07-09",
        "2026-07-09",
        plan_id="plan-9",
        refresh_through_date="2026-07-09",
        bq_client=bq_client,
        storage_client=storage_client,
    )

    assert plan["read_only"] is True
    assert plan["plan_version"] == "matcher-historical-correction-v6"
    assert plan["plan_id"] == "plan-9"
    assert plan["days"][0]["gtfs_snapshot_id"] == "snapshot-9"
    assert plan["days"][0]["gps_inventory_by_mode"]["bus"]["count"] == 2
    assert plan["days"][0]["gps_inventory_by_mode"]["tram"]["bytes"] == 40
    assert plan["days"][0]["input_dates"] == ["2026-07-08", "2026-07-09"]
    assert plan["days"][0]["gps_inventory_by_input_date"]["2026-07-08"]["bus"]["count"] == 1
    assert plan["estimated_input_totals"]["gps_objects"] == 4
    assert bq_client.query_calls == 1
    assert bq_client.mutation_calls == []
    assert storage_client.mutation_calls == []
    assert "--dry_run" in plan["sequential_commands"][0]
    assert "on mapping.gtfs_snapshot_id = snapshots.snapshot_id" in bq_client.queries[0]
    assert "using (gtfs_snapshot_id)" not in bq_client.queries[0]


def test_historical_plan_emits_ascending_executable_date_specific_commands() -> None:
    planner = _load_planner()
    planner.bigquery.QueryJobConfig = _query_config
    planner.bigquery.ScalarQueryParameter = _query_parameter
    plan = planner.build_historical_correction_plan(
        "2026-07-03",
        "2026-07-09",
        bq_client=FakeBigQueryClient(
            [
                types.SimpleNamespace(
                    processing_date="2026-07-02", gtfs_snapshot_id="snapshot-2", gcs_path="gs://gtfs/snapshot.zip"
                ),
                types.SimpleNamespace(
                    processing_date="2026-07-03", gtfs_snapshot_id="snapshot-3", gcs_path="gs://gtfs/snapshot.zip"
                ),
                types.SimpleNamespace(
                    processing_date="2026-07-04", gtfs_snapshot_id="snapshot-4", gcs_path="gs://gtfs/snapshot.zip"
                ),
                types.SimpleNamespace(
                    processing_date="2026-07-08", gtfs_snapshot_id="snapshot-8", gcs_path="gs://gtfs/snapshot.zip"
                ),
                types.SimpleNamespace(
                    processing_date="2026-07-09", gtfs_snapshot_id="snapshot-9", gcs_path="gs://gtfs/snapshot.zip"
                ),
            ]
        ),
        plan_id="approved-plan-7",
        refresh_through_date="2026-07-09",
        storage_client=FakeStorageClient(),
    )

    commands = plan["sequential_commands"]
    command_dates = [match.group(0) for command in commands for match in re.finditer(r"2026-07-0[3489]", command)]

    assert command_dates.count("2026-07-03") >= 4
    assert all("2026-07-03" in command for command in commands[:4])
    assert all("2026-07-04" in command for command in commands[4:8])
    assert all("2026-07-08" in command for command in commands[8:12])
    assert all("2026-07-09" in command for command in commands[12:16])
    assert commands[0].startswith("bq query --use_legacy_sql=false --dry_run")
    assert commands[1].startswith("gcloud storage ls ")
    assert "expected_input_inventory_digest" in commands[2]
    assert "wait-for-dag-run" in commands[3]
    assert "--timeout-seconds 7200" in commands[3]
    assert "expected_input_inventory_digest" in commands[10]
    assert '"skip_prior_publication": true' in commands[10]
    assert "matcher-historical-correction__approved-plan-7__2026-07-09" in commands[15]
    assert '"historical_correction": true' in commands[2]
    assert commands[16].startswith("airflow dags trigger dag_historical_serving_refresh")
    assert "matcher-historical-serving-refresh__approved-plan-7" in commands[16]
    assert "dag_historical_serving_refresh" in commands[17]
    assert plan["serving_refresh"]["affected_service_dates"] == [
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
        "2026-07-08",
        "2026-07-09",
    ]
    assert all("<bounded correction query>" not in command for command in commands)
    assert plan["eligible_processing_segments"] == [
        {"start_date": "2026-07-03", "end_date": "2026-07-04", "days": 2},
        {"start_date": "2026-07-08", "end_date": "2026-07-09", "days": 2},
    ]
    assert plan["days"][2]["prior_publication_boundary"] == {
        "processing_date": "2026-07-08",
        "reason": "prior_service_date_excluded",
        "prior_service_date": "2026-07-07",
        "prior_raw_exclusion_reason": "degraded_raw_gps_archive",
    }
    assert plan["days"][2]["include_prior_gps"] is False
    assert plan["days"][2]["input_dates"] == ["2026-07-08"]
    assert plan["days"][2]["publication_mode"] == "current_only"
    assert plan["days"][2]["affected_partitions"] == {"current_service_date": "2026-07-08"}
    assert plan["days"][3]["include_prior_gps"] is True
    assert plan["days"][3]["input_dates"] == ["2026-07-08", "2026-07-09"]


def test_eligible_processing_dates_preserve_raw_exclusions_and_keep_prior_boundaries() -> None:
    planner = _load_planner()

    june_dates, june_skips = planner._eligible_processing_dates(date(2026, 6, 27), date(2026, 6, 28))
    july_dates, july_skips = planner._eligible_processing_dates(date(2026, 7, 5), date(2026, 7, 9))

    assert june_dates == [date(2026, 6, 27), date(2026, 6, 28)]
    assert june_skips == []
    assert planner._prior_publication_boundary(date(2026, 6, 27)) == {
        "processing_date": "2026-06-27",
        "reason": "prior_service_date_excluded",
        "prior_service_date": "2026-06-26",
        "prior_raw_exclusion_reason": "incomplete_raw_gps_archive",
    }
    assert july_dates == [date(2026, 7, 8), date(2026, 7, 9)]
    assert [item["processing_date"] for item in july_skips] == ["2026-07-05", "2026-07-06", "2026-07-07"]
    assert [item["reason"] for item in july_skips] == [
        "raw_processing_date_excluded",
        "raw_processing_date_excluded",
        "raw_processing_date_excluded",
    ]


def test_wait_for_dag_run_returns_on_success_and_exits_on_failure_or_timeout() -> None:
    planner = _load_planner()
    states = iter(["queued", "running", "success"])

    planner.wait_for_dag_run(
        "dag_daily_gps",
        "run-1",
        timeout_seconds=10,
        poll_interval_seconds=1,
        get_state=lambda _dag_id, _run_id: next(states),
        sleep_for=lambda _seconds: None,
    )
    with pytest.raises(RuntimeError, match="DAG run failed"):
        planner.wait_for_dag_run(
            "dag_daily_gps",
            "run-2",
            timeout_seconds=10,
            poll_interval_seconds=1,
            get_state=lambda _dag_id, _run_id: "failed",
        )


def test_execute_plan_resumes_successful_dates_and_refreshes_only_after_corrections() -> None:
    planner = _load_planner()
    plan = {
        "plan_version": "matcher-historical-correction-v6",
        "read_only": True,
        "plan_id": "resume-plan",
        "bounds": {"run_timeout_seconds": 10, "run_poll_interval_seconds": 1},
        "days": [
            {"processing_date": "2026-07-08"},
            {"processing_date": "2026-07-09"},
        ],
        "serving_refresh": {"run_id": "matcher-historical-serving-refresh__resume-plan"},
        "sequential_commands": [
            "preflight-8",
            "inventory-8",
            "trigger-8",
            "wait-8",
            "preflight-9",
            "inventory-9",
            "trigger-9",
            "wait-9",
            "trigger-refresh",
            "wait-refresh",
        ],
    }
    states = {
        ("dag_daily_gps", "matcher-historical-correction__resume-plan__2026-07-08"): "success",
    }
    commands: list[str] = []
    waits: list[tuple[str, str]] = []

    planner.execute_historical_correction_plan(
        plan,
        get_state=lambda dag_id, run_id: states.get((dag_id, run_id)),
        run_command=commands.append,
        wait_for_run=lambda dag_id, run_id: waits.append((dag_id, run_id)),
    )

    assert commands == ["trigger-9", "trigger-refresh"]
    assert waits == [
        ("dag_daily_gps", "matcher-historical-correction__resume-plan__2026-07-09"),
        ("dag_historical_serving_refresh", "matcher-historical-serving-refresh__resume-plan"),
    ]

    states[("dag_daily_gps", "matcher-historical-correction__resume-plan__2026-07-09")] = "failed"
    with pytest.raises(RuntimeError, match="must be cleared"):
        planner.execute_historical_correction_plan(
            plan,
            get_state=lambda dag_id, run_id: states.get((dag_id, run_id)),
            run_command=commands.append,
            wait_for_run=lambda _dag_id, _run_id: None,
        )
    clock_values = iter([0.0, 1.0])
    with pytest.raises(TimeoutError, match="Timed out"):
        planner.wait_for_dag_run(
            "dag_daily_gps",
            "run-3",
            timeout_seconds=1,
            poll_interval_seconds=1,
            get_state=lambda _dag_id, _run_id: "running",
            clock=lambda: next(clock_values),
        )


def test_historical_plan_allows_july_12() -> None:
    planner = _load_planner()
    planner.bigquery.QueryJobConfig = _query_config
    planner.bigquery.ScalarQueryParameter = _query_parameter

    plan = planner.build_historical_correction_plan(
        "2026-07-12",
        "2026-07-12",
        plan_id="plan-12",
        refresh_through_date="2026-07-12",
        bq_client=FakeBigQueryClient(
            [
                types.SimpleNamespace(
                    processing_date="2026-07-11", gtfs_snapshot_id="snapshot-11", gcs_path="gs://gtfs/snapshot.zip"
                ),
                types.SimpleNamespace(
                    processing_date="2026-07-12", gtfs_snapshot_id="snapshot-12", gcs_path="gs://gtfs/snapshot.zip"
                ),
            ]
        ),
        storage_client=FakeStorageClient(),
    )

    assert plan["date_range"]["start_date"] == "2026-07-12"


def test_historical_plan_refuses_missing_mapping_or_input() -> None:
    planner = _load_planner()
    planner.bigquery.QueryJobConfig = _query_config
    planner.bigquery.ScalarQueryParameter = _query_parameter
    with pytest.raises(RuntimeError, match="exact GTFS snapshot mapping"):
        planner.build_historical_correction_plan(
            "2026-07-09",
            "2026-07-09",
            plan_id="plan-9",
            refresh_through_date="2026-07-09",
            bq_client=FakeBigQueryClient([]),
            storage_client=FakeStorageClient(),
        )
    bq_client = FakeBigQueryClient(
        [
            types.SimpleNamespace(
                processing_date="2026-07-09", gtfs_snapshot_id="snapshot-9", gcs_path="gs://gtfs/snapshot.zip"
            )
        ]
    )
    with pytest.raises(RuntimeError, match="missing bus objects"):
        planner.build_historical_correction_plan(
            "2026-07-09",
            "2026-07-09",
            plan_id="plan-9",
            refresh_through_date="2026-07-09",
            bq_client=bq_client,
            storage_client=FakeStorageClient(no_gps=True),
        )
    with pytest.raises(RuntimeError, match="missing tram objects"):
        planner.build_historical_correction_plan(
            "2026-07-09",
            "2026-07-09",
            plan_id="plan-9",
            refresh_through_date="2026-07-09",
            bq_client=bq_client,
            storage_client=FakeStorageClient(missing_mode="tram"),
        )


def test_plan_report_upload_is_create_only() -> None:
    planner = _load_planner()
    client = FakeUploadClient()

    planner.upload_plan_report({"plan": "one"}, client, "gs://reports/plan.json")

    assert client.report_blob.if_generation_match == 0
    client.report_blob.conflict = True
    with pytest.raises(RuntimeError, match="already exists"):
        planner.upload_plan_report({"plan": "two"}, client, "gs://reports/plan.json")


class FakeBigQueryClient:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows
        self.query_calls = 0
        self.queries: list[str] = []
        self.mutation_calls: list[str] = []

    def query(self, query: str, **_kwargs: Any) -> types.SimpleNamespace:
        self.query_calls += 1
        self.queries.append(query)
        return types.SimpleNamespace(result=lambda: self.rows)

    def __getattr__(self, name: str) -> object:
        if name.startswith(("load_", "insert_", "delete_", "update_")):
            self.mutation_calls.append(name)
            raise AssertionError(f"unexpected mutation API: {name}")
        raise AttributeError(name)


class FakeBlob:
    def __init__(self, name: str, size: int) -> None:
        self.name = name
        self.generation = "123"
        self.size = size
        self.md5_hash = "hash"
        self.crc32c = None


class FakeBucket:
    def __init__(self, name: str, storage_client: FakeStorageClient) -> None:
        self.name = name
        self.storage_client = storage_client

    def get_blob(self, name: str) -> FakeBlob | None:
        return FakeBlob(name, 10) if self.name == "gtfs" and name == "snapshot.zip" else None

    def list_blobs(self, *, prefix: str) -> list[FakeBlob]:
        if self.storage_client.no_gps:
            return []
        mode = "bus" if "vehicle_type=bus" in prefix else "tram"
        if self.storage_client.missing_mode == mode:
            return []
        return [FakeBlob(f"{prefix}hour=01/part-{mode}.parquet", 20)]

    def __getattr__(self, name: str) -> object:
        if name in {"blob", "upload_from_string", "upload_from_filename"}:
            self.storage_client.mutation_calls.append(name)
            raise AssertionError(f"unexpected mutation API: {name}")
        raise AttributeError(name)


class FakeStorageClient:
    def __init__(self, *, no_gps: bool = False, missing_mode: str | None = None) -> None:
        self.no_gps = no_gps
        self.missing_mode = missing_mode
        self.mutation_calls: list[str] = []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(name, self)


class FakeUploadBlob:
    def __init__(self) -> None:
        self.conflict = False
        self.if_generation_match: int | None = None

    def upload_from_string(self, _payload: str, **kwargs: object) -> None:
        value = kwargs.get("if_generation_match")
        self.if_generation_match = value if isinstance(value, int) else None
        if self.conflict:
            raise PreconditionFailed("exists")


class FakeUploadBucket:
    def __init__(self, report_blob: FakeUploadBlob) -> None:
        self.report_blob = report_blob

    def blob(self, _name: str) -> FakeUploadBlob:
        return self.report_blob


class FakeUploadClient:
    def __init__(self) -> None:
        self.report_blob = FakeUploadBlob()

    def bucket(self, _name: str) -> FakeUploadBucket:
        return FakeUploadBucket(self.report_blob)


def _load_planner() -> types.ModuleType:
    _install_airflow_stubs()
    _install_google_stubs()
    sys.modules.pop("ztm_airflow_common", None)
    sys.modules.pop("matcher_historical_correction", None)
    dag_dir = Path(__file__).parents[1] / "dags"
    if str(dag_dir) not in sys.path:
        sys.path.insert(0, str(dag_dir))
    spec = importlib.util.spec_from_file_location(
        "matcher_historical_correction", dag_dir / "matcher_historical_correction.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load historical correction planner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["matcher_historical_correction"] = module
    spec.loader.exec_module(module)
    return module
