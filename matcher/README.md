# ZTM Matcher

Standalone, local-first preparation for one Warsaw GPS processing date and one
pinned GTFS ZIP. It uses bounded DuckDB/Arrow execution, not Airflow, dbt, or
BigQuery.

```shell
uv run --project matcher ztm-matcher prepare --processing-date 2026-07-09 \
  --snapshot-id 2026-07-08T00:00:00Z_37117bdef8d6 --gps-root cache/raw/gps \
  --gtfs-zip cache/gtfs/snapshot.zip --output-dir work/2026-07-09 \
  --alignment-workers 8
```

For development diagnostics, retain full duty context while reducing schedule
and GPS work with `--line`, `--trip-id`, and optionally `--vehicle-number`:

```shell
uv run --project matcher ztm-matcher prepare --processing-date 2026-07-09 \
  --snapshot-id 2026-07-08T00:00:00Z_37117bdef8d6 --gps-root cache/raw/gps \
  --gtfs-zip cache/gtfs/snapshot.zip --output-dir work/z26 \
  --line Z26 --vehicle-number 9812
```

Line selectors accept one line or a comma-separated list. Line and trip
selectors retain every course in each matching duty and every GPS line used by
those duties. Vehicle selection then narrows GPS observations.
Diagnostic outputs are investigation artifacts, not complete daily publication
partitions. Omitting all selectors preserves the production full-day behavior.

GPS partitions are `vehicle_type={bus,tram}/date=YYYY-MM-DD/hour=HH/*.parquet`.
The runtime validates `raw-gps-v1`, reports missing hours, applies the dbt
Warsaw-day/numeric-coordinate/dedup contract, and retains same-coordinate
pings. GTFS requires six UTF-8 (BOM accepted) files: `trips`, `stop_times`,
`stops`, `shapes`, `routes`, and `calendar_dates`; only exception type `1` is
active. Prior and current service dates are both required, and GTFS times may
exceed 24:00.

Missing hourly partitions are recorded in the manifest but do not block
reconstruction. No-service hours are normal; completeness and coverage outputs
distinguish expected absence from ingestion gaps downstream.

Success atomically publishes normalized GPS, duty schedule, stop semantics,
`duty_execution.parquet`, `operational_stop_crossings.parquet`,
`passenger_stop_arrivals.parquet`, `reconstruction_trip_facts.parquet`,
`reconstruction_stop_arrivals.parquet`, `reconstruction_expected_stop_events.parquet`,
manifest, and metrics. `duty_execution` has exactly
one outcome per scheduled course: `executed`, `missed`, `skipped`,
`short_turned`, `vehicle_change_signal`, or `uncertain`. A vehicle-change
signal is competing GPS evidence, not a claim that a physical vehicle swap
occurred. It retains source observation bounds, candidate counts, reasons, and
evidence; ownership intervals are emitted only for confident executed courses.
Failed `.incomplete-*` work directories are preserved.
Errors use stable codes: `missing_input`, `schema_drift`, `invalid_data`,
`snapshot_mismatch`, `resource_limit`, and `invalid_output`.

DuckDB defaults to two threads, 384 MB of managed memory, and 20 GB of
temporary disk. The loader additionally rejects GTFS archives above 640 MB
uncompressed or 1,800,000 selected stop rows. It streams the rolling feed and
retains only services active on the required current/prior dates so Python
schedule preparation remains bounded outside DuckDB; stop semantics are written
in 10,000-row Parquet batches. Metrics include wall and CPU time,
peak RSS and swap where the OS exposes them, temporary and artifact disk use,
and vehicle-group sizes plus execution-status counts. Terminal visits use a
250 m radius and start a new episode after a GPS gap over 180 seconds. The
runtime processes one normalized vehicle Arrow stream and only its matching
schedule subset at a time; final allocation reads one duty at a time. Allocation
does not reject late journeys by a fixed lateness cutoff: it evolves observed
delay through ordered courses, has an explicit skipped-course state, and never
reuses a terminal traversal. Unknown passenger boundaries do not invalidate a
chosen vehicle traversal: it remains high-confidence execution ownership with
`passenger_boundaries_unknown` evidence, while passenger semantics stay
suppressed. `line_brigade` duty fallback cannot receive high confidence.

The July 9 VPS proof with duty alignment measured about 1.63 GiB peak process
RSS with this limit and zero matcher swap. The lower DuckDB allowance trades
bounded temporary I/O for enough memory headroom to add stop alignment.

## Stop Alignment

The runtime reads one high-confidence executed course and its bounded vehicle
GPS interval at a time. It generates only 1--180 second consecutive segments,
then selects one bounded dynamic-programming path through stop *occurrences*
ordered by `stop_sequence`; a repeated `stop_id` is not collapsed. Segment reuse,
time regression, and crossings outside ownership/source bounds are hard rejects.
Distance, scheduled residual, and use of an expanded radius are soft costs.

`operational_stop_crossings-v1` retains direct technical and passenger movement
with segment diagnostics for every high-confidence owned execution.
`passenger_stop_arrivals-v1` is its settled-passenger subset and is the explicit
Parquet adapter toward `fct_stop_arrival`. Unknown passenger boundaries cannot
enter that subset or produce a confident passenger delay. Missing stops produce
no inferred arrival: #102 remains responsible for any separately qualified
interpolation.

Stop alignment defaults to `--alignment-workers 1`, which is the VPS-safe
setting. For local development, `--alignment-workers 8` splits the sorted active
vehicle IDs into contiguous balanced chunks. Each worker has an isolated DuckDB
connection with one thread and the configured memory and temporary-disk caps;
only compact counts return to the parent. Workers write private Parquet shards,
which the parent merges in chunk order, so logical artifacts do not depend on
worker count and each fixed worker-count run has deterministic output bytes.

## Reconstruction Facts

`reconstruction-trip-facts-v2`, `reconstruction-stop-arrivals-v2`, and
`reconstruction-expected-stop-events-v2` are local Parquet adapters, not
warehouse tables. Their published grains are respectively
`snapshot/service_date/trip/vehicle`, that trip plus `stop_sequence`, and the
same expected passenger-stop occurrence. They are ordered by those grains and
their Arrow schemas are validated before publication.

Only `executed` duty outcomes with `high` confidence and a concrete ownership
interval enter the facts. `line_brigade` fallback, competing ownership, and
ambiguous execution outcomes therefore cannot manufacture observed trips or
arrivals. Fact construction uses a DuckDB interval join over normalized GPS to
calculate each accepted ownership interval's max ping gap and speed; it never
loads a GPS day into Python. Trip quality is then streamed one trip record at a
time. Duplicate accepted trip or direct-arrival grains reject the run.

The parity source for quality and service-observation policy is
`dbt/models/intermediate/int_trip_summary.sql`. The Python port retains its
thresholds: complete coverage `.80`, broken coverage `.30`,
ping gap `900` seconds, zero-based-safe terminal tolerance `2`, stale lag
`120` seconds, sequence gap `4`, speed `50 m/s`, and extreme delay `3600`
seconds. It emits the same trip-quality, quality-flag, service-observation
flag, and service-observation-class policy. Regular settled passenger stops
alone determine coverage; request stops remain explicit optional expected
events and their detections do not change regular coverage. An owned execution
with unsettled passenger boundaries still emits an operational trip fact, but is
conservatively `broken`/`matching_failure` with
`unsettled_passenger_boundaries` in both flag arrays; it has no passenger
arrival or expected-event adapter rows. Full stop semantics remain available for
a canonical expected-event adapter to publish unknown passenger status later.

Scheduled timestamps intentionally use Python's Warsaw wall-clock policy as
the authoritative internal semantics. This differs from legacy dbt elapsed-UTC
behavior on DST transition dates: spring-forward gaps normalize and fall-back
times use the first occurrence.

Expected events contain one row for every settled passenger stop occurrence of
an accepted trip. A high-confidence direct crossing is `observed`; a medium or
ambiguous direct crossing is `uncertain`; if trip assignment failed, absent
regular and request stops are also `uncertain`; otherwise absent request stops
are `skipped_optional` and regular stops are `missed`. Technical stops are
excluded. `interpolated` is deliberately not emitted before #102. Prior service
dates remain intact through `processing_date`, `gps_date`, and, for direct
arrival facts, `source_gps_date`.

`trip-universe-v1` is a compact schedule-derived artifact. It preserves raw and
effective stop zones (`1+2` normalizes to `1`; absent zones remain unknown and
therefore fail the zone-1 check), ordered scheduled stop occurrences, terminal
pair frequency/rank, and the persisted
`is_zone1_public_ranking_trip` classification. It requires a settled,
non-depot, non-technical, non-malformed passenger segment, all scheduled stops
in effective zone 1, and no contained lower-ranked short-turn pattern. The
local classifier groups terminal patterns by snapshot, line, direction, and
actual `service_date`; warehouse `int_serving_trip_universe` groups by
`schedule_day_type`, which is not available in the local artifacts. This is a
documented conservative approximation, not a claim of byte-for-byte dbt
parity. Eligibility is copied into trip and direct-arrival facts only through
this persisted artifact.

## Overnight Evidence Gate

Run the local proof against the three published reconstruction artifacts and
`trip_universe.parquet`. It does not query BigQuery:

```shell
uv run --project matcher ztm-matcher overnight-proof \
  --input-dir work/2026-07-09 \
  --report-json work/2026-07-09/overnight-proof.json
```

The command uses DuckDB aggregates over Parquet rather than loading artifacts
into Python. It writes deterministic JSON with SHA-256 artifact identities,
processing and snapshot IDs, prior-service quality, N-line complete
ranking-universe arrival counts, the unchanged line-ranking floor of 20,
eligibility, and every contract violation count. It verifies fact eligibility
against the persisted schedule artifact, requires every artifact `gps_date` to
equal its `processing_date`, and rejects any non-observed expected event with
actual, delay, or source-GPS data. It returns nonzero unless the artifacts have
no violations, include prior-service evidence, and include at least one healthy
N line at the floor. It does not require every N line to meet the floor.

Processing dates `2026-07-05` through `2026-07-07` are rejected as degraded
evidence. The expected real proof is the July 9, 2026 local matcher output:
after-midnight GPS for the July 8 service date must retain July 9 processing and
source GPS lineage while supplying a healthy prior-service N line.
