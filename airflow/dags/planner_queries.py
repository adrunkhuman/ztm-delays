"""BigQuery queries feeding the planner (planner/ component): training segments, recent conditions, stop tables.

Slot keys must match the planner's DuckDB code: hours count from service-date midnight (night trips exceed 23),
day type is weekday / Saturday / Sunday-or-holiday, hour bands split at 6, 10, 14 and 19, and time bands
(night, weekday peak, other) use the hour of the day.
"""

from __future__ import annotations

# Same grid as planner/src/ztm_planner/settings.py STOP_EPS_GRID, as per-mille offsets into APPROX_QUANTILES(x, 1000).
STOP_EPS_PER_MILLE = (1, 2, 3, 5, 7, 10, 15, 20)
STOP_MIN_ARRIVALS = 150
STOP_TOLERANCE_S = 30
STOP_MISS_TARGET = 0.01
# Live adjustments: minutes ahead of the vehicle's position, upper bounds of each horizon bucket.
LIVE_HORIZONS_MIN = (5, 10, 20, 30, 45, 60, 90)
# Per-mille quantiles of the live residual: low keeps boarding times as safe as the stop tables (STOP_MISS_TARGET),
# high matches the late delay (dq90).
LIVE_LOW_PER_MILLE, LIVE_HIGH_PER_MILLE = 10, 900
LIVE_TURN_SLACK_S = 120  # the planned break counts as used up when less than this remains
# Lower bounds of the excess-delay bands (-86400: no bound). A very late vehicle is less predictable, so its
# error quantiles are its band's; a band with fewer than LIVE_MIN_PAIRS pairs takes the pooled ones.
LIVE_EXCESS_FROM_S = (-86400, -120, 120, 300, 600)
LIVE_MIN_PAIRS = 1000

_DAYTYPE = """case when is_holiday or extract(dayofweek from service_date) = 1 then 2
             when extract(dayofweek from service_date) = 7 then 1 else 0 end"""


def _hour_band(hour: str) -> str:
    return f"case when {hour} < 6 then 0 when {hour} < 10 then 1 when {hour} < 14 then 2 when {hour} < 19 then 3 else 4 end"


def _time_band(alias: str) -> str:
    hour = f"MOD({alias}.hr, 24)"
    weekday = f"EXTRACT(DAYOFWEEK FROM {alias}.service_date) BETWEEN 2 AND 6"
    return f"""CASE WHEN {hour} >= 23 OR {hour} < 5 THEN 'night'
    WHEN {weekday} AND {hour} IN (7, 8, 15, 16, 17) THEN 'peak' ELSE 'other' END"""


def training_segments(marts_dataset: str) -> str:
    """Consecutive observed stops of complete trips in [@start, @end] (planner SEGMENT_COLUMNS + actual_s)."""
    return f"""
WITH a AS (
  SELECT service_date, trip_id, vehicle_number, mode, line, direction_id, stop_id, stop_sequence, pickup_type,
    stop_lat, stop_lon, scheduled_arrival_time AS s, actual_arrival_time AS t
  FROM `{marts_dataset}.fct_stop_arrival`
  WHERE service_date BETWEEN @start AND @end AND trip_quality = 'complete'
), w AS (
  SELECT *, LAG(stop_id) OVER p AS a_stop, LAG(stop_sequence) OVER p AS a_seq, LAG(t) OVER p AS a_t,
    LAG(s) OVER p AS a_s, LAG(pickup_type) OVER p AS a_pickup, LAG(stop_lat) OVER p AS a_lat,
    LAG(stop_lon) OVER p AS a_lon,
    MIN(stop_sequence) OVER (PARTITION BY service_date, trip_id, vehicle_number) AS first_seq
  FROM a WINDOW p AS (PARTITION BY service_date, trip_id, vehicle_number ORDER BY stop_sequence)
)
SELECT service_date,
  FARM_FINGERPRINT(CONCAT(CAST(service_date AS STRING), trip_id, vehicle_number)) AS trip_key,
  mode, line, direction_id, a_stop, stop_id AS b_stop, CAST(stop_sequence AS INT64) AS b_seq,
  CAST(stop_sequence - first_seq AS INT64) AS pos, a_pickup = 3 AS a_request, pickup_type = 3 AS b_request,
  ST_DISTANCE(ST_GEOGPOINT(a_lon, a_lat), ST_GEOGPOINT(stop_lon, stop_lat)) AS dist_m,
  TIMESTAMP_DIFF(a_s, TIMESTAMP(service_date, 'Europe/Warsaw'), SECOND) AS a_sched_sod,
  TIMESTAMP_DIFF(s, a_s, SECOND) AS sched_s,
  TIMESTAMP_DIFF(t, a_t, SECOND) AS actual_s
FROM w
WHERE a_stop IS NOT NULL AND stop_sequence = a_seq + 1 AND TIMESTAMP_DIFF(t, a_t, SECOND) > 0
"""


def recent_daily(marts_dataset: str) -> str:
    """Daily observed sums per segment x hour band in [@start, @end] for the recent-conditions features."""
    return f"""
WITH seg AS ({training_segments(marts_dataset)})
SELECT CONCAT(a_stop, '>', b_stop) AS seg, {_hour_band("DIV(a_sched_sod, 3600)")} AS hb, service_date,
  CAST(SUM(actual_s) AS FLOAT64) AS s, COUNT(*) AS n
FROM seg GROUP BY 1, 2, 3
"""


def _arrivals(marts_dataset: str) -> str:
    return f"""
arrivals AS (
  SELECT service_date, trip_id, vehicle_number, stop_sequence, mode = 'tram' AS is_tram, line, direction_id, stop_id,
    delay_seconds AS delay,
    TIMESTAMP_DIFF(scheduled_arrival_time, TIMESTAMP(service_date, 'Europe/Warsaw'), SECOND) AS sod,
    DIV(TIMESTAMP_DIFF(scheduled_arrival_time, TIMESTAMP(service_date, 'Europe/Warsaw'), SECOND), 3600) AS hr,
    stop_sequence = MIN(stop_sequence) OVER t AS is_origin,
    stop_sequence = MAX(stop_sequence) OVER t AS is_last,
    LEAST(9, CAST(FLOOR(10 * SAFE_DIVIDE(stop_sequence - MIN(stop_sequence) OVER t,
      MAX(stop_sequence) OVER t - MIN(stop_sequence) OVER t)) AS INT64)) AS rel_b,
    {_DAYTYPE} AS daytype
  FROM `{marts_dataset}.fct_stop_arrival`
  WHERE service_date BETWEEN @start AND @end AND trip_quality = 'complete'
  WINDOW t AS (PARTITION BY service_date, trip_id, vehicle_number)
),
keyed AS (SELECT *, {_hour_band("hr")} AS hb, COALESCE(rel_b, 0) AS rel_b0 FROM arrivals)"""


# level -> grouping keys; finer levels need STOP_MIN_ARRIVALS, the generic level always applies.
_LEVELS = {
    "line_stop_hour": ("line", "direction_id", "stop_id", "daytype", "hr"),
    "line_stop_band": ("line", "direction_id", "stop_id", "daytype", "hb"),
    "line_stop": ("line", "direction_id", "stop_id"),
    "generic": ("is_tram", "is_origin", "rel_b", "daytype", "hb"),
}
_ALL_KEYS = ("line", "direction_id", "stop_id", "daytype", "hr", "hb", "is_tram", "is_origin", "rel_b")


def _key_expr(key: str) -> str:
    return "rel_b0" if key == "rel_b" else key


def _slot_tables(source: str) -> str:
    """One UNION ALL of per-level quantiles from ``source`` (a CTE of keyed arrivals)."""
    parts = []
    for level, keys in _LEVELS.items():
        cols = ", ".join(
            f"{_key_expr(k)} AS {k}" if k in keys else f"CAST(NULL AS {_TYPES[k]}) AS {k}" for k in _ALL_KEYS
        )
        having = "" if level == "generic" else f"HAVING COUNT(*) >= {STOP_MIN_ARRIVALS}"
        parts.append(
            f"""SELECT '{level}' AS level, {cols}, COUNT(*) AS n,
  APPROX_QUANTILES(delay, 1000) AS q_all, APPROX_QUANTILES(IF(is_last, NULL, delay), 1000) AS q_board
FROM {source} GROUP BY {", ".join(_key_expr(k) for k in keys)} {having}"""
        )
    return "\nUNION ALL\n".join(parts)


_TYPES = {
    "line": "STRING", "direction_id": "INT64", "stop_id": "STRING", "daytype": "INT64", "hr": "INT64",
    "hb": "INT64", "is_tram": "BOOL", "is_origin": "BOOL", "rel_b": "INT64",
}  # fmt: skip


def stop_slots(marts_dataset: str) -> str:
    """Per-slot delay quantiles over [@start, @end]: dq10/50/90 of all arrivals, e0..e7 of boarding arrivals."""
    eps = ", ".join(f"q_board[SAFE_OFFSET({pm})] AS e{i}" for i, pm in enumerate(STOP_EPS_PER_MILLE))
    return f"""
WITH {_arrivals(marts_dataset)},
slots AS ({_slot_tables("keyed")})
SELECT level, {", ".join(_ALL_KEYS)}, n,
  q_all[SAFE_OFFSET(100)] AS dq10, q_all[SAFE_OFFSET(500)] AS dq50, q_all[SAFE_OFFSET(900)] AS dq90, {eps}
FROM slots
"""


def _usual_joins(alias: str) -> tuple[str, str]:
    """LEFT JOINs of fit_slots per level onto ``alias``, and the most specific level's quantiles."""
    joins, picks = [], []
    for i, (level, keys) in enumerate(_LEVELS.items()):
        on = " AND ".join(f"l{i}.{k} = {alias}.{_key_expr(k)}" for k in keys)
        joins.append(f"LEFT JOIN (SELECT * FROM fit_slots WHERE level = '{level}') l{i} ON {on}")
        picks.append(f"l{i}.q_all")
    return "\n  ".join(joins), f"COALESCE({', '.join(picks)})"


def live_persistence(marts_dataset: str) -> str:
    """How a trip's delay beyond its usual one carries down the route, per mode x horizon, on [@cal_start, @end].

    Usual delays are the stop tables' medians fitted on [@start, @cal_start), as in the artifact. For each pair of
    stops of a trip, x is the earlier stop's delay beyond usual and y the later one's; alpha is the least-squares
    slope of y on x per mode x horizon, and low/mid/high_s quantiles of y - alpha * x per band of x as well.
    """
    joins, usual = _usual_joins("c")
    horizon = " ".join(f"WHEN j.sod - i.sod <= {m * 60} THEN {m}" for m in LIVE_HORIZONS_MIN)
    band = " ".join(f"WHEN x >= {b} THEN {b}" for b in reversed(LIVE_EXCESS_FROM_S[1:]))
    return f"""
WITH {_arrivals(marts_dataset)},
fit_slots AS ({_slot_tables("(SELECT * FROM keyed WHERE service_date < @cal_start)")}),
usual AS (
  SELECT c.*, c.delay - {usual}[SAFE_OFFSET(500)] AS excess
  FROM (SELECT * FROM keyed WHERE service_date >= @cal_start) c
  {joins}
),
pairs AS (
  SELECT i.is_tram, CASE {horizon} END AS horizon_min, i.excess AS x, j.excess AS y
  FROM usual i JOIN usual j ON i.service_date = j.service_date AND i.trip_id = j.trip_id
    AND i.vehicle_number = j.vehicle_number AND j.stop_sequence > i.stop_sequence
  WHERE j.sod - i.sod BETWEEN 1 AND {LIVE_HORIZONS_MIN[-1] * 60} AND i.excess IS NOT NULL AND j.excess IS NOT NULL
),
slopes AS (SELECT is_tram, horizon_min, COVAR_POP(x, y) / NULLIF(VAR_POP(x), 0) AS alpha FROM pairs GROUP BY 1, 2),
residuals AS (
  SELECT is_tram, horizon_min, alpha, CASE {band} ELSE {LIVE_EXCESS_FROM_S[0]} END AS excess_s, y - alpha * x AS r
  FROM pairs JOIN slopes USING (is_tram, horizon_min) WHERE alpha IS NOT NULL
),
pooled AS (
  SELECT is_tram, horizon_min, ANY_VALUE(alpha) AS alpha, APPROX_QUANTILES(r, 1000) AS q FROM residuals GROUP BY 1, 2
),
banded AS (
  SELECT is_tram, horizon_min, excess_s, COUNT(*) AS n, APPROX_QUANTILES(r, 1000) AS q FROM residuals GROUP BY 1, 2, 3
),
bands AS (SELECT excess_s FROM UNNEST({list(LIVE_EXCESS_FROM_S)}) AS excess_s),
fitted AS (
  SELECT p.is_tram, p.horizon_min, b.excess_s, COALESCE(d.n, 0) AS n, p.alpha,
    IF(COALESCE(d.n, 0) >= {LIVE_MIN_PAIRS}, d.q, p.q) AS q
  FROM pooled p CROSS JOIN bands b
  LEFT JOIN banded d ON d.is_tram = p.is_tram AND d.horizon_min = p.horizon_min AND d.excess_s = b.excess_s
)
SELECT is_tram, horizon_min, excess_s, n, alpha, q[SAFE_OFFSET({LIVE_LOW_PER_MILLE})] AS low_s,
  q[SAFE_OFFSET(500)] AS mid_s, q[SAFE_OFFSET({LIVE_HIGH_PER_MILLE})] AS high_s
FROM fitted ORDER BY 1, 2, 3
"""


def live_turnaround(marts_dataset: str) -> str:
    """Seconds from a vehicle's arrival at a terminus to its next departure when the planned break is used up.

    Consecutive trips of one vehicle on [@cal_start, @end], both terminals observed; quantiles per mode.
    """
    return f"""
WITH trips AS (
  SELECT service_date, vehicle_number, mode = 'tram' AS is_tram, scheduled_start_time, scheduled_end_time,
    start_delay_seconds, end_delay_seconds, is_first_stop_observed, is_last_stop_observed
  FROM `{marts_dataset}.fct_trip`
  WHERE service_date BETWEEN @cal_start AND @end AND trip_quality IN ('complete', 'partial')
),
pairs AS (
  SELECT *, LEAD(scheduled_start_time) OVER w AS b_start, LEAD(start_delay_seconds) OVER w AS b_delay,
    LEAD(is_first_stop_observed) OVER w AS b_observed
  FROM trips WINDOW w AS (PARTITION BY service_date, vehicle_number ORDER BY scheduled_start_time)
),
turns AS (
  SELECT is_tram, TIMESTAMP_DIFF(b_start, scheduled_end_time, SECOND) AS layover, end_delay_seconds AS a_delay,
    b_delay
  FROM pairs
  WHERE is_last_stop_observed AND b_observed AND TIMESTAMP_DIFF(b_start, scheduled_end_time, SECOND) BETWEEN 0 AND 3600
),
fitted AS (
  SELECT is_tram, COUNT(*) AS n, APPROX_QUANTILES(b_delay + layover - a_delay, 1000) AS turn
  FROM turns WHERE layover - a_delay < {LIVE_TURN_SLACK_S} GROUP BY 1
)
SELECT is_tram, n, turn[SAFE_OFFSET({LIVE_LOW_PER_MILLE})] AS low_s, turn[SAFE_OFFSET(500)] AS mid_s,
  turn[SAFE_OFFSET({LIVE_HIGH_PER_MILLE})] AS high_s
FROM fitted ORDER BY 1
"""


def stop_eps(marts_dataset: str) -> str:
    """Loosest margin quantile per mode x time band keeping misses <= target on [@cal_start, @end].

    Slots are fitted on [@start, @cal_start); a miss is a vehicle more than the tolerance before the
    announced time (scheduled + min(quantile + tolerance, 0)).
    """
    level_joins, picks = [], []
    for i, (level, keys) in enumerate(_LEVELS.items()):
        on = " AND ".join(f"l{i}.{k} = c.{_key_expr(k)}" for k in keys)
        level_joins.append(f"LEFT JOIN (SELECT * FROM fit_slots WHERE level = '{level}') l{i} ON {on}")
        picks.append(f"l{i}.q_board")
    misses = ",\n  ".join(
        f"AVG(IF(delay < LEAST(q[SAFE_OFFSET({pm})] + {STOP_TOLERANCE_S}, 0) - {STOP_TOLERANCE_S}, 1, 0)) AS m{i}"
        for i, pm in enumerate(STOP_EPS_PER_MILLE)
    )
    choose = " ".join(f"WHEN m{i} <= {STOP_MISS_TARGET} THEN {i}" for i in reversed(range(len(STOP_EPS_PER_MILLE))))
    return f"""
WITH {_arrivals(marts_dataset)},
fit_slots AS ({_slot_tables("(SELECT * FROM keyed WHERE service_date < @cal_start)")}),
cal AS (
  SELECT c.*, COALESCE({", ".join(picks)}) AS q, {_time_band("c")} AS band
  FROM (SELECT * FROM keyed WHERE service_date >= @cal_start AND NOT is_last) c
  {chr(10).join("  " + j for j in level_joins)}
),
rates AS (
  SELECT is_tram, band,
  {misses}
  FROM cal GROUP BY is_tram, band
)
SELECT is_tram, band, CASE {choose} ELSE 0 END AS eps_index FROM rates
"""
