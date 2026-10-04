"""LightGBM correction on top of the lookup: predicts the lookup's residual from richer features."""

from __future__ import annotations

from typing import TYPE_CHECKING

import lightgbm as lgb
import numpy as np

from ztm_planner.db import one
from ztm_planner.lookup import LEVELS, TOP
from ztm_planner.settings import EARLY_STOPPING_ROUNDS, GBM_PARAMS
from ztm_planner.weather import WEATHER_FEATURES

if TYPE_CHECKING:
    import duckdb

_K = len(LEVELS)
FEATURES = (
    *(f"p{k}" for k in range(_K + 1)), *(f"n{k}" for k in range(1, _K + 1)),
    "sched_s", "dist_m", "sod_h", "dow", "daytype", "is_holiday", "is_tram", "a_request", "b_request", "pos",
    "direction_id", "line_id", "shift_seg", "shift_seghb", "recent_n_seg", "recent_n_seghb", *WEATHER_FEATURES,
)  # fmt: skip
CATEGORICAL = ("line_id",)
MONOTONE_UP = ("precip", "precip_3h", "snow_24h", "freeze_risk")  # more rain or snow never speeds a ride up


def matrix(con: duckdb.DuckDBPyConnection, table: str) -> np.ndarray:
    """Feature matrix as float32, filled column by column to keep the memory peak near the result size."""
    n = one(con, f"select count(*) from {table}")[0]
    out = np.empty((n, len(FEATURES)), dtype=np.float32)
    for i, name in enumerate(FEATURES):
        column = con.execute(f"select {name}::double from {table} order by row_id").fetchnumpy()
        values = next(iter(column.values())).astype(np.float32, copy=False)
        # Plain ndarray assignment discards DuckDB's NULL mask; LightGBM needs explicit missing values.
        out[:, i] = np.ma.filled(values, np.nan)
    return out


def target(con: duckdb.DuckDBPyConnection, table: str) -> np.ndarray:
    """Residual of the top lookup level: what LightGBM learns."""
    result = con.execute(f"select (actual_s - {TOP})::double from {table} order by row_id").fetchnumpy()
    return next(iter(result.values()))


def train(
    x: np.ndarray, y: np.ndarray, threads: int, rounds: int, valid: tuple[np.ndarray, np.ndarray] | None = None
) -> lgb.Booster:
    """Train, with early stopping when a validation set is given."""
    params = {
        **GBM_PARAMS,
        "num_threads": threads,
        "monotone_constraints": [int(f in MONOTONE_UP) for f in FEATURES],
    }
    categorical = [FEATURES.index(c) for c in CATEGORICAL]
    data = lgb.Dataset(x, y, categorical_feature=categorical, free_raw_data=True)
    if valid is None:
        return lgb.train(params, data, num_boost_round=rounds)
    vdata = lgb.Dataset(valid[0], valid[1], reference=data, categorical_feature=categorical)
    return lgb.train(
        params, data, num_boost_round=rounds, valid_sets=[vdata],
        callbacks=[lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False)],
    )  # fmt: skip


def predict(booster: lgb.Booster, con: duckdb.DuckDBPyConnection, table: str, threads: int) -> np.ndarray:
    """Predicted segment seconds (top lookup level + correction), ordered by row_id."""
    top = con.execute(f"select {TOP}::double from {table} order by row_id").fetchnumpy()
    return next(iter(top.values())) + booster.predict(matrix(con, table), num_threads=threads)
