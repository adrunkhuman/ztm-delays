# Architecture

The pipeline separates collection, reconstruction, warehouse modelling, and serving. GCS retains the raw evidence; BigQuery holds the historical warehouse; a local DuckDB export serves the web app without cloud queries.

## Collection and reconstruction

The poller collects buses and trams from the [Warsaw vehicle-location API](../poller/poller.py). It buffers accepted positions in a durable spool and writes append-safe Parquet parts, partitioned by mode and Warsaw-local date/hour. GPS timestamps are stored in UTC. The shared [raw GPS schema](../contracts/raw_gps_v1.json) is checked across collection and ingestion.

Airflow polls the [mkuran GTFS feed](https://mkuran.pl/gtfs/warsaw.zip) hourly and stores changed ZIPs with timestamp-and-hash snapshot IDs. GTFS is the timetable: routes, stops, scheduled trips, service dates, and vehicle duties. The feed is a rolling window, so historical schedules are known only from collected snapshots onward.

The Python matcher reads raw GPS and one pinned GTFS ZIP locally. It assigns vehicles to ordered duty courses, then aligns GPS segments to scheduled stop occurrences. A duty is a sequence of trips assigned to a vehicle; GTFS `block_id` provides its identity. Line/brigade fallback is weaker evidence and cannot produce high-confidence execution facts.

Only confidently assigned executions enter the trip facts. Stop alignment preserves repeated stops and rejects time regression or reuse of a GPS segment. Unsettled passenger-service boundaries do not produce confident passenger delays. The matcher records uncertainty rather than filling gaps with inferred arrivals.

## Dates and history

The persisted `int_gtfs_processing_snapshot` mapping selects the timetable for each processing date. Reruns use that mapping, not the newest available ZIP. Historical facts carry their own labels and snapshot IDs; `_current` dimensions are lookup tables, not a way to relabel the archive.

| Field | Meaning |
| --- | --- |
| `processing_date` | Warsaw GPS date being rebuilt, not the wall-clock date of execution. |
| `service_date` | GTFS service day; a trip can continue past midnight. |
| `gps_date` | In published matcher facts, the artifact's processing-date partition. |
| `source_gps_date` | Actual raw GPS date of a direct stop observation. |
| `publish_service_date` | Warehouse fact partition being replaced. |

A normal run for date `D` reads GPS from `D-1` and `D` into one matcher artifact, then publishes both service dates. Trips extending beyond `D` are completed by the next run. Rebuilding an older processing date preserves newer overnight observations already published into its service-date partition. Known archive boundaries use an explicit current-date-only policy.

Scheduled times use Warsaw wall-clock time, including GTFS times beyond 24:00. The matcher and dbt share the same daylight-saving policy: normalize spring-forward gaps and use the first fall-back occurrence.

## Warehouse

| Layer | Contents |
| --- | --- |
| `ztm_raw` | Reloadable GPS and GTFS data from GCS. |
| `ztm_stg` | Typed, cleaned, snapshot-aware inputs. |
| `ztm_matcher_input` | Validated matcher outputs, replaced atomically by processing date. |
| `ztm_int` | Schedule lineage, duty chains, canonical serving inputs. |
| `ztm_marts` | Historical dimensions, service-date facts, coverage, and display aggregates. |

`fct_trip` describes an observed vehicle trip. `fct_stop_arrival` describes a detected scheduled stop occurrence. `fct_expected_stop_event` retains scheduled stops for matched trips even when no usable arrival was observed.

Schedule versions distinguish timetable changes from feed republication. A daily fingerprint ledger hashes ordered stop/time content, excluding display labels and unstable GTFS identifiers. Version windows are rebuilt from that small ledger rather than repeatedly expanding raw schedule history. A timetable that disappears and later returns starts a new version period.

Large models overwrite bounded date partitions and require partition filters. Normal runs check the affected data; a separate weekly audit runs broader contracts. Structural failures block publication, while low coverage and suspicious observations remain explicit quality signals.

## Serving

Airflow exports a fixed [table allowlist](../airflow/dags/dag_serving_export.py) from BigQuery through GCS Parquet. It builds a candidate DuckDB artifact, validates it, and replaces the active catalog only after success. This is a frontend-specific export, not a warehouse mirror.

The exporter supports a materialized DuckDB file or a partitioned store: small reference tables in DuckDB, date-bearing tables as views over retained local Parquet. Partitioned runs refresh changed dates rather than rewriting the archive. Total local storage still grows with history.

Route delay maps are a separate monthly artifact. Airflow aggregates delay change between consecutive scheduled stops of complete trips, aligns each stop pair onto its GTFS shape, and publishes one directory per month under `maps/` in the same serving storage. Stop pairs that cannot be placed unambiguously on the shape are dropped and counted; they are never drawn as straight lines.

The Flask app holds a read-only DuckDB connection for each request. Warehouse marts supply entity summaries and exact display-grain quantiles; frontend queries also filter, aggregate, and rank exported trip rows for browsing. New requests see a newly published catalog without an app restart.

## Trip planner

The planner artifact supplies trips and walks for journeys between two stop groups in the coming week, including journeys with changes. It lives at `planner/planner.duckdb`, published beside the export like the route maps. The [planner component](../planner/README.md) builds it outside the warehouse, from a model bundle, the current timetable and weekly footpaths.

A weekly DAG extracts ten weeks of observed bus and tram segments and stop arrivals into GCS Parquet with BigQuery extract jobs, trains a lookup plus LightGBM model, and keeps the bundle in GCS. It promotes the bundle only if the model beats the lookup and the lookup beats the timetable on the held-out week. After training finishes, even if it fails (`all_done`), a separate task builds walking distances from Geofabrik's Mazowieckie OSM extract and the latest GTFS stops, then publishes `planner/footpaths/footpaths.parquet` in GCS. The tasks run sequentially to limit peak memory.

A nightly DAG scores the latest GTFS snapshot for seven days with the promoted bundle, the last week's observed conditions, and an Open-Meteo forecast, then replaces the artifact atomically. The previous snapshot supplies yesterday's overnight trips. Metro and SKM have no learned predictions: they use timetable times, with metro templates expanded at GTFS frequency headways and a fixed 60 s late-arrival margin for SKM. The [contract](../contracts/planner_artifact_v1.json) includes `planner_footpath` for walks between nearby posts. Scoring uses OSM distances where both posts are covered and straight-line estimates otherwise; a missing weekly file makes all walks estimates.

The frontend's [journey router](../frontend/ztm_frontend/journey.py) is a bounded round-based earliest-arrival search inspired by RAPTOR, with up to five vehicles. It assumes no FIFO ordering: boarding-dependent predictions allow overtaking, so it examines feasible trips using boarding-deadline indexes at each post rather than choosing one trip per pattern.

Expected bus/tram arrival is the boarding timetable time plus its usual delay and the predicted cumulative ride-time difference. Conservative arrival uses the boarding late delay plus a monotone calibrated upper ride-duration envelope, with ratios floored at 1 and a 1.1 fallback. Both departure bases are clamped to `board_by`, and the conservative base is at least the expected departure. Metro and SKM keep timetable ride durations. Each change requires `conservative arrival + walk <= next board_by`; same-post walks are zero. Per-ride bounds do not imply 90% end-to-end reliability. [Planner timing details](../planner/README.md#journeys-and-walks) define the envelope and rounding.

Each search finds earliest conservative arrivals by vehicle count; a profile search (rRAPTOR, pruned by a no-waiting lower bound to the destination) lists every nondominated departure/arrival/vehicle-count alternative leaving in its windows, up to 8 h after the requested time. The network includes the selected service day and previous-day trips still running after midnight, not the next service day's daytime trips; cross-service-day routing is deferred. The planner page shows per-leg cards, walks and change deadlines, with lazily loaded stop lists for each ride.

Predictions describe usual conditions, not live positions. A disruption on the day, such as a detour, crash or event, is invisible to them.

## Interpreting the results

Delay is actual minus scheduled arrival time: positive is late. Delay summaries use `complete` trips; `partial` trips remain useful for drill-down, while `broken` trips are diagnostic. These classifications describe evidence quality, not whether a service ran successfully.

Ingestion completeness measures raw GPS presence. Service coverage compares scheduled service with observed complete/partial trips. A missing coverage row means no scheduled service for that slice; zero coverage means scheduled service was not observed. Neither ingestion gaps nor unmatched trips establish cancellations.

The frontend's [metric and period definitions](../frontend/README.md#reading-the-archive) further constrain comparisons. Poller status is a heartbeat captured at export time, not live monitoring.
