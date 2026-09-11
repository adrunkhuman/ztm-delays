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
| `POLLER_SPOOL_DIR` | Durable buffer directory; default `/var/lib/ztm-poller-spool`. |
| `ZTM_API_PROXY` | Optional SOCKS proxy for city API traffic. |

Timing, freshness, and buffer limits are defined in [poller.py](poller.py).

## Storage and failure behaviour

```text
raw/gps/vehicle_type=bus/date=YYYY-MM-DD/hour=HH/part-<row-digest>.parquet
raw/gps/vehicle_type=tram/date=YYYY-MM-DD/hour=HH/part-<row-digest>.parquet
```

Multiple parts per hour are expected. A batch's row digest gives retries the same object name; different batches remain separate parts. The [raw schema](../contracts/raw_gps_v1.json) stores timestamps in UTC, with date/hour partitions based on Warsaw time.

Stale and far-future pings are rejected before buffering. By default, flushing runs every 15 minutes and holds recent rows for five minutes. Graceful shutdown flushes the remaining buffer. Failed uploads stay buffered for retry; persisted spool state survives container replacement. The default 100 MiB spool cap fails loudly rather than allowing unbounded disk growth.

A private GCS heartbeat records per-mode attempts, successes, and failures. Heartbeat upload failure is logged without stopping collection. The frontend sees only the sanitized snapshot captured during serving export.

## Container

The [image](Dockerfile) runs Tailscale in userspace mode. City API traffic uses its SOCKS proxy; GCS traffic stays direct. Configure a Polish `TS_EXIT_NODE` and enable `POLLER_REQUIRE_POLISH_EGRESS=true` to check the route at startup. This is not continuous egress monitoring.

Persist `/var/lib/tailscale` and the spool directory. `TS_AUTHKEY` is needed for initial registration when persisted identity is absent. Never share that identity directory between live containers: stop the old instance before replacing it. Keep auth keys in runtime configuration, not build arguments.

The Docker healthcheck checks local processes and Tailscale readiness, not the city API. Use heartbeat freshness and logs to diagnose upstream failures.

## Checks

From `poller/`, `uv run pytest` and `uv run ruff check .` run offline checks. To size storage from GCS metadata:

```sh
uv run python measure_raw_gps_volume.py \
  --start-date "$START_DATE" --end-date "$END_DATE" --bucket "$GCS_BUCKET"
```

This requires GCS access but makes no BigQuery queries. Compressed Parquet size understates the JSON spool's storage requirement.
