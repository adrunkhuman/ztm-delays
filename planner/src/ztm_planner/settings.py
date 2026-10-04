"""Model and runtime constants shared by training and scoring.

Values come from the scratch experiment (scratch/travel-time) evaluated on 22 Sep - 2 Oct 2026.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

SHRINK = 20  # pseudo-observations pulling a lookup level toward its parent
RECENT_DAYS = 7  # recent-conditions window
RECENT_GAP_DAYS = 5  # training rows see the window ending this many days earlier (~ mid scoring horizon)
OOF_FOLDS = 4  # out-of-fold lookup features for LightGBM, folds by week
SAMPLE_ROWS = 3_500_000  # LightGBM training rows (~0.45 GB as float32); keeps training under ~4 GB
EARLY_STOPPING_ROUNDS = 30
MAX_ROUNDS = 2000
ROUNDS_MARGIN = 1.1  # refit on the full window uses best_iteration * this
MIN_RANGE_PAIRS = 150  # calibration pairs before an hour gets its own ride range, else its time band
RIDE_BUCKETS = (180, 360, 600, 900, 1500, 2400, 3600)  # predicted ride seconds; ranges calibrated per bucket
RANGE_QUANTILES = (0.10, 0.90)
STOP_TOLERANCE_S = 30  # a vehicle more than this before the announced time strands the rider
STOP_MISS_TARGET = 0.01
STOP_EPS_GRID = (0.001, 0.002, 0.003, 0.005, 0.007, 0.01, 0.015, 0.02)
STOP_MIN_ARRIVALS = 150  # per slot before a finer stop slot is trusted
HORIZON_DAYS = 7

# Journey planning: metro and SKM keep their timetable (no observations); walks between nearby posts.
WALK_MAX_M = 700  # longest walk offered between two posts
WALK_SPEED_MPS = 1.2
WALK_DETOUR = 1.3  # straight line -> walking distance, for posts the OSM paths don't cover
WALK_MIN_S = 30  # even between posts a few metres apart
STATION_ACCESS_S = 60  # stairs and corridors to a metro or rail platform, on top of the walk
RAIL_LATE_S = 60  # SKM "late" arrival margin over its timetable; metro runs to its timetable

GBM_PARAMS = {
    "objective": "regression",
    "learning_rate": 0.08,
    "num_leaves": 255,
    "min_data_in_leaf": 200,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 10,
    "max_bin": 255,
    "verbose": -1,
    "seed": 7,
}


@dataclass(frozen=True)
class Resources:
    """Bounds for a small shared VPS: slower is fine, memory is not."""

    threads: int
    memory_limit: str
    temp_dir: str

    @classmethod
    def default(cls, temp_dir: str) -> Resources:
        cores = os.cpu_count() or 2
        return cls(threads=max(1, cores - 2), memory_limit="1500MB", temp_dir=temp_dir)
