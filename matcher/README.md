# Matcher

Reconstructs vehicle trips and stop arrivals from local GPS Parquet and one pinned GTFS ZIP. Runs in Python with DuckDB and Arrow; it needs neither Airflow nor cloud credentials.

## Run

From the repository root, with downloaded inputs and their actual snapshot ID:

```sh
uv run --locked --project matcher ztm-matcher prepare \
  --processing-date "$PROCESSING_DATE" --snapshot-id "$SNAPSHOT_ID" \
  --gps-root cache/raw/gps --gtfs-zip cache/gtfs/snapshot.zip \
  --output-dir work/reconstruction
```

GPS paths follow `vehicle_type={bus,tram}/date=YYYY-MM-DD/hour=HH/*.parquet`. Normal runs read the processing date and its preceding GPS date. `--no-include-prior-gps` is the explicit outage-boundary exception, not a performance shortcut. See [date semantics](../docs/architecture.md#dates-and-history).

GTFS requires `trips`, `stop_times`, `stops`, `shapes`, `routes`, and `calendar_dates`. Active service comes from `calendar_dates` exception type `1`; times may exceed 24:00. Missing GPS hours are recorded, not treated as proof of no service.

For diagnosis, `--line`, `--trip-id`, and `--vehicle-number` narrow the run. Line/trip filters retain full duty context. These outputs are incomplete daily partitions and must not be published as production results.

## Reconstruction

Duty alignment assigns ordered scheduled courses to vehicle evidence, preserving layovers, line changes, and possible skipped courses. Stop alignment then selects a bounded dynamic-programming path through stop occurrences in schedule order. It cannot reuse a segment or reverse time; repeated stop IDs remain distinct occurrences.

Confident vehicle ownership is separate from passenger-service certainty. An execution can be identified while its passenger boundaries remain unknown. Such evidence is retained operationally, but cannot manufacture a confident passenger arrival or delay. Request stops are optional and do not reduce regular-stop coverage.

## Outputs

| Artifact | Contents |
| --- | --- |
| `duty_execution.parquet` | One outcome per scheduled course, with confidence and evidence. |
| `operational_stop_crossings.parquet` | Direct crossings, including technical stops. |
| `passenger_stop_arrivals.parquet` | Crossings with settled passenger semantics. |
| `reconstruction_*.parquet` | Trip, arrival, and expected-stop adapters for warehouse publication. |
| Manifest and metrics | Input identity, schema versions, missing hours, counts, and resource use. |

The output also includes normalised GPS, schedules, stop semantics, and ranking eligibility. A successful run publishes its output directory atomically; failed `.incomplete-*` directories remain for diagnosis. Airflow separately validates and promotes the warehouse adapters.

## Bounds and checks

The default run uses two DuckDB threads, 384 MB managed memory, a 20 GB spill allowance, and one alignment worker. These are not a total process-memory cap. GPS processing is vehicle-bounded; schedules and artifacts have explicit size guards. Increasing `--alignment-workers` creates independent worker memory/spill allowances.

From `matcher/`:

```sh
uv sync --locked
uv run pytest
uv run ruff check .
uv run ty check
```

[CLI options](src/ztm_matcher/cli.py), [quality policy](src/ztm_matcher/policy.py), and [artifact schemas](src/ztm_matcher/schemas.py) define the detailed runtime contract.
