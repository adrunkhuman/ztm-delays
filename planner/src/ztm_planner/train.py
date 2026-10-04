"""Weekly training: observed segments of a rolling window -> model bundle.

Stage A fits on the window minus its last CAL_DAYS, early-stops LightGBM on those days and calibrates ride
ranges from predictions there (out of sample). Stage B refits lookup and LightGBM on the whole window.
Everything heavy goes through an on-disk DuckDB database with a memory cap; LightGBM sees a row sample.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa

from ztm_planner import assemble, bundle, calibrate, features, lookup, model, weather
from ztm_planner.db import connect, one, yielding_memory
from ztm_planner.settings import MAX_ROUNDS, OOF_FOLDS, ROUNDS_MARGIN, SAMPLE_ROWS, Resources

log = logging.getLogger(__name__)
CAL_DAYS = 7


@dataclass(frozen=True)
class TrainInputs:
    """Local inputs prepared by the DAG."""

    segments: str  # parquet glob: observed segments with SEGMENT_COLUMNS + actual_s
    weather_json: Path  # Open-Meteo archive response covering the window
    stop_slots: Path  # parquet from planner_sql/stop_slots.sql
    stop_eps: Path  # parquet from planner_sql/stop_eps.sql
    start: date
    end: date


def train(inputs: TrainInputs, workdir: Path, out_dir: Path, version: str, resources: Resources) -> dict:
    """Train and write a bundle to ``out_dir``; returns the bundle metadata."""
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "train.duckdb"
    db_path.unlink(missing_ok=True)
    con = connect(resources, str(db_path))
    cal_start = inputs.end - timedelta(days=CAL_DAYS - 1)
    asof_cal = cal_start - timedelta(days=1)
    features.register_holidays(con, inputs.start, inputs.end)
    con.execute(
        f"create or replace view raw as select * from read_parquet('{inputs.segments}') "
        f"where service_date between date '{inputs.start}' and date '{inputs.end}' and actual_s > 0"
    )
    features.add_features(con, "raw", "seg_all")
    con.execute(f"create or replace view seg_fit as select * from seg_all where service_date < date '{cal_start}'")
    con.execute(f"create or replace view seg_cal as select * from seg_all where service_date >= date '{cal_start}'")
    weather.load(con, inputs.weather_json)
    con.execute(
        "create or replace table line_map as select line, (row_number() over (order by line) - 1)::integer as line_id "
        "from (select distinct line from seg_all)"
    )
    total = one(con, "select count(*) from seg_all")[0]
    per_mille = max(1, min(1000, round(1000 * SAMPLE_ROWS / max(total, 1))))
    sample = f"hash(trip_key) % 1000 < {per_mille}"
    log.info("%s segments; LightGBM samples %s per mille of trips", total, per_mille)

    # Stage A: out-of-sample early stopping and range calibration on the last CAL_DAYS.
    features.build_daily(con, "seg_fit", "daily_fit")
    x, y = _oof_matrix(con, "seg_fit", sample, "daily_fit")
    lookup.fit(con, "seg_fit", "a_")
    con.execute(f"create or replace table cal_rows as select * from seg_cal where {sample}")
    assemble.model_rows(con, "cal_rows", "cal_model", "a_", "daily_fit", asof_cal)
    valid = (model.matrix(con, "cal_model"), model.target(con, "cal_model"))
    with yielding_memory(con, resources):
        booster_a = model.train(x, y, resources.threads, MAX_ROUNDS, valid)
    del x, y, valid
    rounds = max(1, int(booster_a.best_iteration * ROUNDS_MARGIN))
    log.info("stage A best iteration %s", booster_a.best_iteration)

    assemble.model_rows(con, "seg_cal", "cal_all", "a_", "daily_fit", asof_cal)
    con.register("cal_pred_src", _predict_by_day(con, booster_a, "cal_all", resources.threads))
    con.execute(
        "create or replace table cal_pred as select c.trip_key, c.b_seq, c.service_date, c.is_tram, c.a_sched_sod, "
        "c.actual_s, c.sched_s, c.p3 as lookup_pred, p.pred from cal_all c join cal_pred_src p using (row_id)"
    )
    con.unregister("cal_pred_src")
    pairs = calibrate.pairs(con, "cal_pred")
    calibrate.ride_ranges(con, pairs)
    metrics = _metrics(con, pairs)
    log.info("stage A held-out metrics: %s", metrics)

    # Stage B: refit everything on the whole window.
    features.build_daily(con, "seg_all", "daily_all")
    x, y = _oof_matrix(con, "seg_all", sample, "daily_all")
    with yielding_memory(con, resources):
        booster = model.train(x, y, resources.threads, rounds)
    del x, y
    lookup.fit(con, "seg_all", "final_")
    meta = {
        "version": version,
        "window_start": inputs.start.isoformat(),
        "window_end": inputs.end.isoformat(),
        "calibration_start": cal_start.isoformat(),
        "segments": total,
        "lightgbm_rows_per_mille": per_mille,
        "rounds": rounds,
        "metrics": metrics,
    }
    bundle.write(con, out_dir, booster, meta, inputs.stop_slots, inputs.stop_eps)
    con.close()
    db_path.unlink(missing_ok=True)
    return meta


def _predict_by_day(con: duckdb.DuckDBPyConnection, booster, table: str, threads: int) -> pa.Table:
    """Predictions keyed by row_id, one service date at a time to keep the feature matrix small."""
    parts = []
    for (day,) in con.execute(f"select distinct service_date from {table} order by 1").fetchall():
        con.execute(f"create or replace table predict_chunk as select * from {table} where service_date = date '{day}'")
        row_ids = con.execute("select row_id from predict_chunk order by row_id").fetchnumpy()["row_id"]
        parts.append(pa.table({"row_id": row_ids, "pred": model.predict(booster, con, "predict_chunk", threads)}))
    con.execute("drop table if exists predict_chunk")
    return pa.concat_tables(parts)


def _oof_matrix(con: duckdb.DuckDBPyConnection, source: str, sample: str, daily: str) -> tuple[np.ndarray, np.ndarray]:
    """LightGBM rows sampled from ``source`` with lookup features fitted without the row's own week."""
    parts = []
    for fold in range(OOF_FOLDS):
        lookup.fit(con, f"(select * from {source} where fold <> {fold})", f"oof{fold}_")
        con.execute(
            f"create or replace table oof_src_{fold} as select * from {source} where fold = {fold} and {sample}"
        )
        lookup.apply(con, f"oof_src_{fold}", f"oof_lk_{fold}", f"oof{fold}_")
        parts.append(f"select * from oof_lk_{fold}")
    con.execute(f"create or replace table oof_lk as {' union all by name '.join(parts)}")
    assemble.finish(con, "oof_lk", "oof_model", daily, None)
    x, y = model.matrix(con, "oof_model"), model.target(con, "oof_model")
    for fold in range(OOF_FOLDS):
        for name in (f"oof_src_{fold}", f"oof_lk_{fold}", *(f"oof{fold}_{t}" for t in lookup.TABLES)):
            con.execute(f"drop table if exists {name}")
    con.execute("drop table oof_lk")
    con.execute("drop table oof_model")
    return x, y


def _metrics(con: duckdb.DuckDBPyConnection, pairs: pa.Table) -> dict:
    """Held-out errors (timetable vs lookup vs model) and ride-range coverage on the calibration days."""
    seg = one(
        con,
        "select avg(abs(actual_s - sched_s)), avg(abs(actual_s - lookup_pred)), avg(abs(actual_s - pred)), count(*) "
        "from cal_pred",
    )
    con.register("pairs_eval", pairs)
    pair = one(
        con,
        "select avg(abs(p.actual - p.pred)), "
        "avg((p.actual between p.pred * r.low_ratio and p.pred * r.high_ratio)::int) "
        "from pairs_eval p join ride_range r on r.is_tram = p.is_tram and r.weekday = p.weekday "
        "and r.hour = p.hour and p.pred > r.min_ride_s and p.pred <= r.max_ride_s",
    )
    con.unregister("pairs_eval")
    return {
        "calibration_segments": int(seg[3]),
        "segment_mae_s": {"timetable": _round(seg[0]), "lookup": _round(seg[1]), "model": _round(seg[2])},
        "pair_mae_s": _round(pair[0]),
        "range_coverage_calibration_days": _round(pair[1], 4),  # fitted on these days: optimistic
    }


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(float(value), digits)
