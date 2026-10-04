"""Model bundle: everything scoring needs, written by training as plain files (parquet, LightGBM text, JSON).

Stop tables come from BigQuery (airflow/dags/planner_sql); they are validated and copied in unchanged.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import lightgbm as lgb

from ztm_planner import lookup
from ztm_planner.settings import STOP_EPS_GRID

MODEL_FILE, META_FILE = "gbm.txt", "meta.json"
STOP_SLOT_COLUMNS = (
    "level", "line", "direction_id", "stop_id", "daytype", "hr", "hb", "is_tram", "is_origin", "rel_b", "n",
    "dq10", "dq50", "dq90", *(f"e{i}" for i in range(len(STOP_EPS_GRID))),
)  # fmt: skip
STOP_LEVELS = ("line_stop_hour", "line_stop_band", "line_stop", "generic")
STOP_EPS_COLUMNS = ("is_tram", "band", "eps_index")


def write(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    booster: lgb.Booster,
    meta: dict,
    stop_slots: Path,
    stop_eps: Path,
) -> None:
    """Write a complete bundle; the directory only appears once every file is in place."""
    tmp = out_dir.with_name(out_dir.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    lookup.save(con, "final_", tmp)
    con.execute(f"copy line_map to '{tmp / 'line_map.parquet'}' (format parquet)")
    con.execute(f"copy ride_range to '{tmp / 'ride_range.parquet'}' (format parquet)")
    _check_columns(con, stop_slots, STOP_SLOT_COLUMNS)
    _check_columns(con, stop_eps, STOP_EPS_COLUMNS)
    shutil.copyfile(stop_slots, tmp / "stop_slots.parquet")
    shutil.copyfile(stop_eps, tmp / "stop_eps.parquet")
    booster.save_model(str(tmp / MODEL_FILE))
    (tmp / META_FILE).write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    shutil.rmtree(out_dir, ignore_errors=True)
    tmp.rename(out_dir)


def load(con: duckdb.DuckDBPyConnection, directory: Path) -> tuple[lgb.Booster, dict]:
    """Load bundle tables into ``con`` (lookup under prefix ``m_``) and return the booster and metadata."""
    lookup.load(con, directory, "m_")
    for name in ("line_map", "ride_range", "stop_slots", "stop_eps"):
        con.execute(f"create or replace table {name} as select * from read_parquet('{directory / f'{name}.parquet'}')")
    meta = json.loads((directory / META_FILE).read_text(encoding="utf-8"))
    return lgb.Booster(model_file=str(directory / MODEL_FILE)), meta


def _check_columns(con: duckdb.DuckDBPyConnection, path: Path, expected: tuple[str, ...]) -> None:
    actual = [row[0] for row in con.execute(f"describe select * from read_parquet('{path}')").fetchall()]
    missing = [c for c in expected if c not in actual]
    if missing:
        raise ValueError(f"{path.name} lacks columns {missing}")
