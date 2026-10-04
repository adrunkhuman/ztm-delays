# Planner

Travel-time models and the nightly artifact behind the frontend's planner tab: for every scheduled bus and tram trip of the coming week, when to be at each stop, the usual and late delay there, and the predicted ride time from the trip's first stop.

`ztm-planner train` turns a rolling window of observed segments into a model bundle. `ztm-planner score` turns the current timetable and that bundle into `planner.duckdb` ([contract](../contracts/planner_artifact_v1.json)). Both read and write local files only. The [Airflow DAGs](../airflow/dags/dag_planner.py) handle BigQuery, GCS, weather downloads and publication.

## Model

A segment is the ride between two consecutive scheduled stops of one trip. Any A→B ride on a trip is a sum of segments, so one model covers every stop pair. Lines share a segment's statistics; the stop posts give direction.

| Part | What it does |
| --- | --- |
| Lookup | Mean segment time by mode × day type × hour × scheduled length, then residual adjustments per stop-group pair, segment, and segment × day type × hour, each shrunk toward its parent by 20 pseudo-observations. A new stop post inherits its stop group's behaviour. |
| LightGBM | Learns the lookup's residual from lookup levels (out of fold by week), schedule, distance, time, calendar, line, position, request stops, the last 7 days' shift against the long-run mean, and hourly weather. Precipitation, snow and freezing risk are monotone: they never make a ride faster. |
| Ride ranges | 10th and 90th percentiles of actual/predicted A→B rides per mode × hour × predicted length, from the held-out week; the time band (night, weekday peak, other) where an hour has fewer than 150 pairs. |
| Stop tables | Computed in BigQuery: median and 90th-percentile delay per line × direction × stop × day type × hour, falling back to coarser slots. The "be at the stop" margin is the quantile that kept at most 1% of vehicles more than 30 s early on the held-out week, chosen per mode × time band, and is never later than the timetable. |

Day types are weekday, Saturday, and Sunday or holiday, with holidays as in `dim_date`. Hours count from service-date midnight, so night trips have hours above 23.

## Training and scoring

Training holds out the window's last 7 days. On them it early-stops LightGBM, calibrates ride ranges and measures errors, then refits everything on the full window. The DAG promotes a bundle only if, on that week, the model beats the lookup and the lookup beats the timetable.

Scoring expands the latest GTFS snapshot for 7 days, plus yesterday's service date from the previous snapshot for night trips after midnight. It predicts segments one service date at a time and writes the artifact atomically, readable by other users.

On 10 weeks to 2 Oct 2026, the held-out mean absolute segment error was 27.3 s for the timetable, 18.2 s for the lookup and 17.6 s for LightGBM. A→B rides were off by 84 s on average.

## Resources

DuckDB works in an on-disk database capped at 1.5 GB and spills to `--workdir`, which must be on disk: on tmpfs the spill counts as memory. LightGBM trains on a sample of about 3.5M segments and predicts one day at a time. Measured on 43M segments with 4 threads, training peaked at 2.9 GB (maximum RSS) and completed in 139 s inside the Airflow image under a 4 GB limit. Scoring 7 days peaked at 1.8 GB and took 71 s on 2 threads. Threads default to CPU cores minus two, and the process lowers its priority (`--nice`).

## Checks

```sh
uv sync --locked
uv run ruff check .
uv run ty check
uv run pytest
```

Tests build a small synthetic network with a known peak slowdown, train on it, score a week, and check the artifact against the contract.
