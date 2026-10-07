# GPS poller

Collects Warsaw bus and tram positions every 10 seconds. Accepted rows are buffered, snapshotted locally, and uploaded as Parquet parts to GCS. Run one instance for both modes.

## Run

From `poller/`, with a Warsaw API token in `ZTM_API_TOKEN`:

```sh
uv sync --locked
uv run python poller.py --once --no-upload
```

This contacts the live API but does not initialise GCS or upload data. Omit both flags for continuous collection; uploads require Google Cloud credentials and the target bucket configuration.

| Setting | Purpose |
| --- | --- |
| `ZTM_API_TOKEN` | City API authorization token. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Mounted service-account key for GCS access. |
| `GCS_BUCKET`, `GCS_PREFIX` | Destination bucket and prefix; prefix defaults to `raw/gps`. |
| `POLLER_SPOOL_DIR` | Durable GPS and independent health checkpoint directory; default `/var/lib/ztm-poller-spool`. |
| `POLLER_HEALTH_GCS_PREFIX` | Private cumulative UTC hourly summaries; default `health/poller/hourly`. |
| `POLLER_HEALTH_MAX_BYTES` | Health checkpoint/pending-counter cap, separate from the GPS cap; default 8 MiB. Reserves 1 KiB before each API attempt. |
| `POLLER_HEARTBEAT_INTERVAL_SECONDS` | Heartbeat and closed-hour diagnostic upload/retry tick; default 60 seconds. |
| `POLLER_PUBLIC_STATUS_GCS_PATH` | Sanitized public heartbeat object; default `health/poller/public/live.json`. |
| `POLLER_LIVE_VEHICLES_GCS_PATH` | Live positions object for the planner; default `health/poller/public/vehicles.json.gz`. |
| `ZTM_API_PROXY` | Optional SOCKS proxy for city API traffic. |

Timing, freshness, and buffer limits are defined in [poller.py](poller.py).

## Storage and failure behaviour

```text
raw/gps/vehicle_type=bus/date=YYYY-MM-DD/hour=HH/part-<row-digest>.parquet
raw/gps/vehicle_type=tram/date=YYYY-MM-DD/hour=HH/part-<row-digest>.parquet
```

Multiple parts per hour are expected. A batch's row digest gives retries the same object name; different batches remain separate parts. The [raw schema](../contracts/raw_gps_v1.json) stores timestamps in UTC, with date/hour partitions based on Warsaw time.

Stale and far-future pings are rejected before buffering. By default, flushing runs every 15 minutes and holds recent rows for five minutes. Graceful shutdown flushes the remaining buffer. Failed uploads stay buffered for retry; persisted spool state survives container replacement. The default 100 MiB spool cap fails loudly rather than allowing unbounded disk growth.

A private GCS heartbeat records per-mode attempts, successes, and failures, plus the parsed, accepted and stale/future-dropped row counts of the last successful poll. The poller issues no freshness verdict: the city API keeps serving parked vehicles' last positions overnight, so a high stale share is normal and says nothing about feed health. Baseline-dependent detection of fleet collapse belongs to the downstream monitor. The private heartbeat is read by the monitor and the serving export, never by the frontend.

Each heartbeat tick also uploads a sanitized public copy, `health/poller/public/live.json` (override with `POLLER_PUBLIC_STATUS_GCS_PATH`), with `Cache-Control: no-cache`. It is built from an explicit allowlist: `version`, `updated_at`, the poll and heartbeat intervals, and per mode `last_attempt_at`, `last_success_at`, `consecutive_failures`, `fresh_vehicles` and `fresh_lines`. Timestamps are UTC with a `Z` suffix, or `null`; the fresh counts come from the last successful poll and are `null` before the first one. Hostnames, paths and error text stay private. A failed public upload is logged and does not affect the private heartbeat, and the reverse.

## Live positions

After every poll the poller replaces `health/poller/public/vehicles.json.gz` with the latest fresh ping of each vehicle: mode, line, brigade, vehicle number, position and GPS time ([contract](../contracts/live_vehicles_v1.json)). The frontend planner reads it to adjust trips running now. A mode whose polls fail keeps its last rows until they are older than `MAX_PING_AGE_SECONDS`. The upload has a short timeout and no retry, since the next poll replaces it; a failure is logged and never delays polling or the GPS archive. At one write per poll this is about 260,000 GCS writes a month, roughly $1.30 on a regional Standard bucket. `--no-upload` skips it.

## Durable feed diagnostics

```text
health/poller/hourly/YYYY-MM-DD/HH.json
```

The [v1 shared contract](../contracts/poller_health_v1.json) defines exact fields. Each mode has only **observed** UTC minutes: attempt/success counts; parsed, accepted, stale-dropped and future-dropped rows; and sums of distinct fresh vehicles/lines **per poll**, not hourly unique fleets. Attempts use request-start timestamps, not GPS timestamps. Empty successful responses count as successes; failed attempts contribute no row counts. Missing minutes/hours mean unmonitored, not zero. Deploying mid-hour does not invent earlier minutes; `collection_started_at` is the first retained attempt and survives restart. The private heartbeat also carries this optional root marker, so the monitor can distinguish stopped collection in the first monitored hour from an older collector with no monitoring marker. Legacy heartbeat callers omit it.

`poller-health-v1.json` is a separate versioned checkpoint under the durable spool directory; `buffers.json` is unchanged. Each attempt is atomically checkpointed with file/directory fsync before the next API call, including when all GPS rows were discarded. No raw rejected rows or historical vehicle-ID sets are retained. A crash during an in-flight request or before its checkpoint completes can still leave that attempt unrecorded.

Closed hours upload at heartbeat ticks (normally **one additional object per hour**, covering both modes). Startup retries prior closed hours before polling; graceful shutdown also uploads the partial hour. Failed GCS writes are logged and retained locally without stopping ingestion, then retried at the next tick/startup. Shutdown returns nonzero if uploads remain pending. Writes replace cumulative snapshots at the same key: retries do not add counts. Partial-hour state stays local even after upload, so a same-hour restart extends rather than replaces earlier counts. Successful closed hours are retired locally; collection start remains.

Run **one writer per spool and health prefix**, preserve its volume, and stop the old instance before replacement. Lost/deleted checkpoints cannot reconstruct earlier counts. Do not run multiple writers or move the clock backward into a finalized hour. Closed-hour objects have no invented completeness flag; downstream readers infer coverage from observed minutes.

Pending counters are bounded by `POLLER_HEALTH_MAX_BYTES`. The poller stops loudly **before another API call** if it cannot reserve space; it never evicts pending hours to keep collecting. Disk/checkpoint errors also fail loudly, retaining the previous checkpoint and attempting shutdown uploads. Check logs, restore disk/GCS access, or explicitly increase the cap before restarting; do not delete the checkpoint to clear an outage. Atomic replacement temporarily needs disk space for both old and new checkpoints (up to roughly twice the cap). `--no-upload` neither restores nor writes health checkpoints and makes no GCS calls.

## Container

The [image](Dockerfile) runs Tailscale in userspace mode. City API traffic uses its SOCKS proxy; GCS traffic stays direct. Configure a Polish `TS_EXIT_NODE` and enable `POLLER_REQUIRE_POLISH_EGRESS=true` to check the route at startup. This is not continuous egress monitoring.

Persist `/var/lib/tailscale` and the spool directory. `TS_AUTHKEY` is needed for initial registration when persisted identity is absent. Never share that identity directory between live containers: stop the old instance before replacing it. Keep auth keys in runtime configuration, not build arguments.

The Docker healthcheck checks local processes and Tailscale readiness, not the city API. Use heartbeat freshness and logs to diagnose upstream failures.

## Checks

From `poller/`, run offline checks with:

```sh
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked ty check poller.py poller_health_counters.py measure_raw_gps_volume.py
```

The targeted typecheck covers maintained sources; known existing test-only type diagnostics are separate. To size storage from GCS metadata:

```sh
uv run python measure_raw_gps_volume.py \
  --start-date "$START_DATE" --end-date "$END_DATE" --bucket "$GCS_BUCKET"
```

This requires GCS access but makes no BigQuery queries. Compressed Parquet size understates the JSON spool's storage requirement.
