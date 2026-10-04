# Planner

Travel-time models and the nightly artifact for planning journeys in the coming week. Bus and tram trips carry predicted ride times, usual and late delays, and when to be at each stop. Metro and SKM use their timetable; footpaths connect nearby stop posts for journeys with changes.

`ztm-planner train` turns a rolling window of observed segments into a model bundle. `ztm-planner score` turns the current timetable and that bundle into `planner.duckdb` ([contract](../contracts/planner_artifact_v1.json)). `ztm-planner footpaths` builds walking distances from an OSM extract and GTFS stops. All three commands read and write local files only. The [Airflow DAGs](../airflow/dags/dag_planner.py) handle BigQuery, GCS, weather and OSM downloads, and publication.

## Model

A segment is the ride between two consecutive scheduled stops of one trip. Any A→B ride on a trip is a sum of segments, so one model covers every stop pair. Lines share a segment's statistics; the stop posts give direction.

| Part | What it does |
| --- | --- |
| Lookup | Mean segment time by mode × day type × hour × scheduled length, then residual adjustments per stop-group pair, segment, and segment × day type × hour, each shrunk toward its parent by 20 pseudo-observations. A new stop post inherits its stop group's behaviour. |
| LightGBM | Learns the lookup's residual from lookup levels (out of fold by week), schedule, distance, time, calendar, line, position, request stops, the last 7 days' shift against the long-run mean, and hourly weather. Precipitation, snow and freezing risk are monotone: they never make a ride faster. |
| Ride ranges | 10th and 90th percentiles of actual/predicted A→B rides per mode × hour × predicted length, from the held-out week; the time band (night, weekday peak, other) where an hour has fewer than 150 pairs. |
| Stop tables | Computed in BigQuery: median and 90th-percentile delay per line × direction × stop × day type × hour, falling back to coarser slots. The "be at the stop" margin is the quantile that kept at most 1% of vehicles more than 30 s early on the held-out week, chosen per mode × time band, and is never later than the timetable. |

Day types are weekday, Saturday, and Sunday or holiday, with holidays as in `dim_date`. Hours count from service-date midnight, so night trips have hours above 23.

Missing features, including unseen lines and unavailable numerical or weather values, reach LightGBM as `NaN`, not zero. Bundles trained before this missing-feature fix must be retrained: scoring alone cannot correct what the model learned from lost NULL masks.

## Training and scoring

Training holds out the window's last 7 days. On them it early-stops LightGBM, calibrates ride ranges and measures errors, then refits everything on the full window. The DAG promotes a bundle only if, on that week, the model beats the lookup and the lookup beats the timetable.

Scoring expands the latest GTFS snapshot for 7 days, plus yesterday's service date from the previous snapshot for night trips after midnight. It predicts bus and tram segments one service date at a time. Metro runs are expanded at the headways in `frequencies.txt`; metro and SKM ride times and boarding deadlines follow the timetable, with a fixed 60 s late-arrival margin for SKM and none for metro. The artifact is written atomically, readable by other users.

GTFS pickup-only stops (`drop_off_type = 1`) stay in every mode's timetable and artifact: passengers may board or ride through, but must not alight there. The final `planner_stop` column, non-null `can_alight`, is false at these stops and true otherwise. Boarding is controlled separately by `leave_by_offset_s`; stops with both pickup and drop-off forbidden are skipped. Types 2 and 3 remain supported under the current coordination/request-stop policy.

On 10 weeks to 2 Oct 2026, the held-out mean absolute segment error was 27.3 s for the timetable, 18.2 s for the lookup and 17.6 s for LightGBM. A→B rides were off by 84 s on average.

## Journeys and walks

The frontend's [journey router](../frontend/ztm_frontend/journey.py) is a bounded round-based earliest-arrival search inspired by RAPTOR. Each round adds one vehicle, up to five (four changes). It does not assume FIFO: trips can overtake, so each improved boarding post has its own boarding-deadline ordering and all feasible trips are considered, subject to arrival-bound pruning.

Ride times depend on where the passenger boards, not the alighting stop's delay model. Let `duration` be the non-negative difference between predicted cumulative ride times at alighting and boarding, and `board_by` the boarding timetable time plus its non-positive boarding margin.

| Timing | Calculation |
| --- | --- |
| Expected departure | Boarding timetable time + usual delay, clamped to at least `board_by`. |
| Expected arrival | Expected departure + `duration`. |
| Conservative arrival | Boarding timetable time + late delay, clamped to at least `board_by` and expected departure, plus a monotone calibrated upper ride-duration envelope; rounded up to a whole second. |

The envelope uses calibrated upper ride ratios floored at 1, with 1.1 for missing cells or duration gaps. It retains earlier upper bounds across downward ratio boundaries, so riding farther never improves the conservative bound. Metro and SKM keep timetable ride durations, without a learned envelope; SKM retains its fixed 60 s late margin.

Every change requires `conservative arrival + walk <= next board_by`. A change at the same post has zero walk. Walks may precede or follow rides, but cannot chain without an intervening ride. Per-ride bounds do **not** imply 90% end-to-end reliability.

Each search finds earliest conservative arrivals by vehicle count. The departure profile is a profile search (rRAPTOR) over windows from the requested time, up to 8 h: it finds every alternative leaving in those windows not beaten on later departure, earlier conservative arrival and fewer vehicles. A search includes the selected service day's trips and previous-day trips still running after midnight. It does not include the next service day's daytime trips in that same search; cross-service-day routing is deferred.

Weekly footpaths use walkable OSM paths from Geofabrik's Mazowieckie extract, with distances up to 700 m. Nightly scoring puts distances and walking times in `planner_footpath`. If either post is not covered, it estimates distance as straight line × 1.3; if both are covered but no OSM path is in range, it does not invent a connection. Without the weekly file, all walks are estimated. Walking time uses 1.2 m/s, at least 30 s, plus 60 s when either end is a metro or rail platform. See [operations](../docs/operations.md#trip-planner) for publication and failure handling.

## Resources

DuckDB works in an on-disk database capped at 1.5 GB and spills to `--workdir`, which must be on disk: on tmpfs the spill counts as memory. LightGBM trains on a sample of about 3.5M segments and predicts one day at a time. Measured on 43M segments with 4 threads, training peaked at 2.9 GB (maximum RSS) and completed in 139 s inside the Airflow image under a 4 GB limit. Scoring 7 days peaked at 1.8 GB and took 71 s on 2 threads. Threads default to CPU cores minus two, and the process lowers its priority (`--nice`).

## Checks

```sh
uv sync --locked
uv run ruff check .
uv run ty check
uv run pytest
```

Tests build a small synthetic network with a known peak slowdown, train on it, score a week, and check the artifact against the contract. They also cover metro frequencies, timetable-only rail, OSM walking distances and estimated walks.
