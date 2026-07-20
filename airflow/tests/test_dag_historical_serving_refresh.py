from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from jinja2 import Template

from .test_dag_daily_gps import _load_dag_module as _load_daily_dag

if TYPE_CHECKING:
    import types


def _load_refresh_dag() -> types.ModuleType:
    _load_daily_dag()
    module_path = Path(__file__).parents[1] / "dags" / "dag_historical_serving_refresh.py"
    spec = importlib.util.spec_from_file_location("dag_historical_serving_refresh", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["dag_historical_serving_refresh"] = module
    spec.loader.exec_module(module)
    return module


def test_refresh_config_requires_bounded_ascending_days() -> None:
    refresh = _load_refresh_dag()
    valid = {
        "days": [
            {"processing_date": "2026-07-08", "gtfs_snapshot_id": "snapshot-8"},
            {"processing_date": "2026-07-09", "gtfs_snapshot_id": "snapshot-9"},
        ],
        "restore_day": {"processing_date": "2026-07-09", "gtfs_snapshot_id": "snapshot-9"},
        "maximum_bytes_billed": 5 * 1024**3,
        "historical_plan_id": "plan-9",
    }

    assert refresh._validated_refresh_config(valid) == valid
    with pytest.raises(refresh.AirflowException, match="unique and ascending"):
        refresh._validated_refresh_config({"days": list(reversed(valid["days"])), "restore_day": valid["restore_day"]})
    with pytest.raises(refresh.AirflowException, match="invalid snapshot"):
        refresh._validated_refresh_config(
            {
                "days": [{"processing_date": "2026-07-08", "gtfs_snapshot_id": "bad snapshot"}],
                "restore_day": {"processing_date": "2026-07-08", "gtfs_snapshot_id": "bad snapshot"},
            }
        )


def test_refresh_runs_partition_models_once_per_day_then_emits_one_asset() -> None:
    refresh = _load_refresh_dag()

    assert refresh.dag.kwargs["schedule"] is None
    assert refresh.dag.kwargs["max_active_runs"] == 1
    command = refresh.refresh_serving.kwargs["bash_command"]
    assert "{% for day in dag_run.conf['days'] %}" in command
    assert refresh.SERVING_UNIVERSE_MODELS in command
    assert refresh.SERVING_PARTITION_MODELS in command
    assert command.count(refresh.SERVING_FULL_MODELS) == 2
    assert "DBT_BIGQUERY_MAXIMUM_BYTES_BILLED" in command
    rendered = Template(command).render(
        dag_run=SimpleNamespace(
            conf={
                "days": [
                    {"processing_date": "2026-07-08", "gtfs_snapshot_id": "snapshot-8"},
                    {"processing_date": "2026-07-09", "gtfs_snapshot_id": "snapshot-9"},
                ],
                "restore_day": {"processing_date": "2026-07-09", "gtfs_snapshot_id": "snapshot-9"},
                "maximum_bytes_billed": 5 * 1024**3,
                "historical_plan_id": "plan-9",
            }
        )
    )
    assert "{{" not in rendered
    assert rendered.count("dbt run --select int_gtfs_trip_schedule int_gtfs_duty_chain") == 2
    assert '"processing_date":"2026-07-09"' in rendered
    assert refresh.refresh_serving in refresh.validate_refresh_config.downstream
    assert refresh.restore_current_schedule in refresh.refresh_serving.downstream
    assert refresh.restore_current_schedule.kwargs["trigger_rule"] == refresh.TriggerRule.ALL_DONE
    invalid_restore = Template(refresh.RESTORE_COMMAND).render(ti=SimpleNamespace(xcom_pull=lambda **_kwargs: None))
    assert "refusing schedule restoration" in invalid_restore
    valid_restore = Template(refresh.RESTORE_COMMAND).render(
        ti=SimpleNamespace(
            xcom_pull=lambda **_kwargs: refresh._validated_refresh_config(
                {
                    "days": [{"processing_date": "2026-07-09", "gtfs_snapshot_id": "snapshot-9"}],
                    "restore_day": {"processing_date": "2026-07-09", "gtfs_snapshot_id": "snapshot-9"},
                    "maximum_bytes_billed": 5 * 1024**3,
                    "historical_plan_id": "plan-9",
                }
            )
        )
    )
    assert "{{" not in valid_restore
    assert '"processing_date":"2026-07-09"' in valid_restore
