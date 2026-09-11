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

The Flask app holds a read-only DuckDB connection for each request. Warehouse marts supply entity summaries and exact display-grain quantiles; frontend queries also filter, aggregate, and rank exported trip rows for browsing. New requests see a newly published catalog without an app restart.

## Interpreting the results

Delay is actual minus scheduled arrival time: positive is late. Delay summaries use `complete` trips; `partial` trips remain useful for drill-down, while `broken` trips are diagnostic. These classifications describe evidence quality, not whether a service ran successfully.

Ingestion completeness measures raw GPS presence. Service coverage compares scheduled service with observed complete/partial trips. A missing coverage row means no scheduled service for that slice; zero coverage means scheduled service was not observed. Neither ingestion gaps nor unmatched trips establish cancellations.

The frontend's [metric and period definitions](../frontend/README.md#reading-the-archive) further constrain comparisons. Poller status is a heartbeat captured at export time, not live monitoring.
