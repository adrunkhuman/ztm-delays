# Architecture

The pipeline separates collection, reconstruction, warehouse modelling, and serving. GCS retains the raw evidence; BigQuery holds the historical warehouse; a local DuckDB export serves the web app without cloud queries.

## Collection and reconstruction

The poller collects buses and trams from the [Warsaw vehicle-location API](../poller/poller.py). It buffers accepted positions in a durable spool and writes append-safe Parquet parts, partitioned by mode and Warsaw-local date/hour. GPS timestamps are stored in UTC. The shared [raw GPS schema](../contracts/raw_gps_v1.json) is checked across collection and ingestion.

An independent hourly monitor compares each minute's fresh fleet with the usual fleet for that weekday and minute. It publishes per-mode health reports. BigQuery stores those rows in `ztm_raw.raw_poller_hourly_health`; separate `mart_poller_hourly_health` and `mart_poller_daily_health` views expose UTC hours with Warsaw-local date/hour and daily summaries. This health feed does not modify `mart_pipeline_status`, the serving shard schema, or nightly model selections. The collector and the monitor also write small public objects under `health/poller/public/`, which the status page reads live with a read-only service account.

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

The planner finds journeys between two stops in the coming week, with changes and walks. It reads its own artifact, `planner/planner.duckdb`, published beside the export like the route maps. The [planner component](../planner/README.md) builds it outside the warehouse.

A weekly DAG extracts ten weeks of observed bus and tram segments and stop arrivals from BigQuery to GCS Parquet and trains a lookup plus LightGBM model. It promotes the bundle only if, on the held-out last week, the model beats the lookup and the lookup beats the timetable. A second task then builds walking distances between nearby stop posts from Geofabrik's Mazowieckie OSM extract.

A nightly DAG predicts the latest GTFS snapshot for seven days from the promoted bundle, the last week's observed conditions and an Open-Meteo forecast, then replaces the artifact atomically. The previous snapshot supplies yesterday's trips still running after midnight. Metro and SKM follow the timetable. The [contract](../contracts/planner_artifact_v1.json) defines the artifact's tables.

The frontend's [journey router](../frontend/README.md#planner) searches that artifact per request. A change must still work when the arriving vehicle runs late: its conservative arrival plus the walk must not pass the next vehicle's boarding deadline. [Planner timing](../planner/README.md#journeys-and-walks) defines these bounds; they are per-ride planning margins, not a 90% guarantee for the whole journey.

Predictions describe usual conditions, not live positions. A disruption on the day, such as a detour, crash or event, is invisible to them.

## Interpreting the results

Delay is actual minus scheduled arrival time: positive is late. Delay summaries use `complete` trips; `partial` trips remain useful for drill-down, while `broken` trips are diagnostic. These classifications describe evidence quality, not whether a service ran successfully.

Ingestion completeness measures raw GPS presence. Service coverage compares scheduled service with observed complete/partial trips. A missing coverage row means no scheduled service for that slice; zero coverage means scheduled service was not observed. Neither ingestion gaps nor unmatched trips establish cancellations.

The frontend's [metric and period definitions](../frontend/README.md#reading-the-archive) further constrain comparisons. The status page shows the poller and GPS feed live; coverage and trip quality come from the nightly export.
