from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from .test_dag_daily_gps import _install_airflow_stubs, _install_google_stubs


def _query_config(**kwargs: object) -> types.SimpleNamespace:
    return types.SimpleNamespace(**kwargs)


def _query_parameter(*args: object) -> tuple[object, ...]:
    return args


def test_historical_plan_requires_explicit_bounded_non_degraded_dates() -> None:
    planner = _load_planner()

    with pytest.raises(ValueError, match="explicit"):
        planner.build_historical_correction_plan()
    with pytest.raises(ValueError, match="degraded"):
        planner.build_historical_correction_plan("2026-07-05", "2026-07-05")
    with pytest.raises(ValueError, match="allowed bounded"):
        planner.build_historical_correction_plan("2026-06-24", "2026-06-25")


def test_historical_plan_inventories_exact_mapping_without_mutation() -> None:
    planner = _load_planner()
    planner.bigquery.QueryJobConfig = _query_config
    planner.bigquery.ScalarQueryParameter = _query_parameter
    bq_client = FakeBigQueryClient(
        [
            types.SimpleNamespace(
                processing_date="2026-07-09", gtfs_snapshot_id="snapshot-9", gcs_path="gs://gtfs/snapshot.zip"
            )
        ]
    )
    storage_client = FakeStorageClient()

    plan = planner.build_historical_correction_plan(
        "2026-07-09", "2026-07-09", bq_client=bq_client, storage_client=storage_client
    )

    assert plan["read_only"] is True
    assert plan["days"][0]["gtfs_snapshot_id"] == "snapshot-9"
    assert plan["estimated_input_totals"]["gps_objects"] == 2
    assert bq_client.query_calls == 1
    assert bq_client.mutation_calls == []
    assert storage_client.mutation_calls == []
    assert "--dry_run" in plan["sequential_commands"][0]


def test_historical_plan_refuses_missing_mapping_or_input() -> None:
    planner = _load_planner()
    planner.bigquery.QueryJobConfig = _query_config
    planner.bigquery.ScalarQueryParameter = _query_parameter
    with pytest.raises(RuntimeError, match="exact GTFS snapshot mapping"):
        planner.build_historical_correction_plan(
            "2026-07-09", "2026-07-09", bq_client=FakeBigQueryClient([]), storage_client=FakeStorageClient()
        )
    bq_client = FakeBigQueryClient(
        [
            types.SimpleNamespace(
                processing_date="2026-07-09", gtfs_snapshot_id="snapshot-9", gcs_path="gs://gtfs/snapshot.zip"
            )
        ]
    )
    with pytest.raises(RuntimeError, match="GPS input inventory is missing"):
        planner.build_historical_correction_plan(
            "2026-07-09", "2026-07-09", bq_client=bq_client, storage_client=FakeStorageClient(no_gps=True)
        )


class FakeBigQueryClient:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows
        self.query_calls = 0
        self.mutation_calls: list[str] = []

    def query(self, _query: str, **_kwargs: Any) -> types.SimpleNamespace:
        self.query_calls += 1
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
        return [FakeBlob(f"{prefix}hour=01/part-{mode}.parquet", 20)]

    def __getattr__(self, name: str) -> object:
        if name in {"blob", "upload_from_string", "upload_from_filename"}:
            self.storage_client.mutation_calls.append(name)
            raise AssertionError(f"unexpected mutation API: {name}")
        raise AttributeError(name)


class FakeStorageClient:
    def __init__(self, *, no_gps: bool = False) -> None:
        self.no_gps = no_gps
        self.mutation_calls: list[str] = []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(name, self)


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
