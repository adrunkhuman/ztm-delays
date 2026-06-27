# ZTM GPS Poller

One container polls Warsaw ZTM buses and trams every 10 seconds and writes append-safe Parquet files to hourly GCS partitions.

## Required Environment

- `ZTM_API_TOKEN`: city API token used as the `Authorization` header.
- `GOOGLE_APPLICATION_CREDENTIALS`: path to the mounted GCP service account key.
- `TS_AUTHKEY`: Tailscale auth key used to join the tailnet non-interactively. Required only when no persisted `tailscaled.state` exists.

## Optional Environment

- `GCS_BUCKET`: defaults to `ztm-analytics-bucket`.
- `GCS_PREFIX`: defaults to `raw/gps`.
- `POLL_INTERVAL_SECONDS`: defaults to `10`.
- `API_TIMEOUT_SECONDS`: defaults to `5`.
- `MAX_PING_AGE_SECONDS`: defaults to `300`; older API rows are dropped.
- `FUTURE_PING_TOLERANCE_SECONDS`: defaults to `60`; farther-future API rows are dropped.
- `PARTIAL_FLUSH_INTERVAL_SECONDS`: defaults to `900`; flushes buffered rows every 15 minutes to bound hard-crash loss.
- `FLUSH_LAG_SECONDS`: defaults to `MAX_PING_AGE_SECONDS`; rows are uploaded only after this age unless the poller is shutting down.
- `LOG_LEVEL`: defaults to `INFO`.
- `TS_EXIT_NODE`: defaults to `100.103.142.113` (`pl-waw-wg-101.mullvad.ts.net`, Warsaw).
- `TS_HOSTNAME`: defaults to `ztm-poller`.
- `TS_SOCKS_ADDR`: defaults to `127.0.0.1:1055`.
- `TS_STATE_DIR`: defaults to `/var/lib/tailscale`; mount this path to persist Tailscale device identity.
- `STARTUP_GRACE_SECONDS`: defaults to `300`; keeps the container alive briefly if the poller exits during startup.
- `ZTM_API_PROXY`: normally set by `entrypoint.sh`; can be set manually for local proxy smoke tests.

## Output

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=YYYY-MM-DD/hour=HH/part-<24-char-row-digest>.parquet
gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=YYYY-MM-DD/hour=HH/part-<24-char-row-digest>.parquet
```

Multiple part files per hour are expected. Part names are deterministic from the uploaded rows. Retrying the same row batch writes the same object path and is treated as success; a different batch or split creates a different valid part. Downstream loads must tolerate multiple parts per hour.

`Time` is parsed as Europe/Warsaw local time and written as UTC Parquet timestamp because the raw BigQuery schema declares it as `TIMESTAMP`.

## Operational Semantics

Run exactly one poller instance. Older split deployments must be removed before deploying this version; running both old `poller-bus` and `poller-tram` services after this change duplicates bus and tram raw files.

Rows are buffered in memory and uploaded in append-safe `part-*.parquet` files. By default, the poller flushes rows every 15 minutes, but only after they are older than `FLUSH_LAG_SECONDS`. This batches late-but-valid API rows while bounding hard-crash loss under normal GCS availability.

On graceful shutdown, all currently buffered rows are flushed, including rows newer than the lag window. If an upload fails, those rows stay buffered and are retried on the next flush attempt. A hard crash, forced container kill, or host restart can still lose rows that were not yet successfully uploaded, including rows younger than `FLUSH_LAG_SECONDS`. With defaults and healthy GCS, normal exposure is up to roughly `FLUSH_LAG_SECONDS + PARTIAL_FLUSH_INTERVAL_SECONDS`; there is no durable local spool.

The city API can return stale or future-dated pings. The poller drops rows outside the configured freshness window before buffering.

## Local Run

```bash
uv sync
export ZTM_API_TOKEN=...
uv run python poller.py
```

Safe live API smoke test, with no GCS client initialization and no upload:

```bash
export ZTM_API_TOKEN=...
uv run python poller.py --once --no-upload
```

## Docker

The Docker image runs `tailscaled` in userspace networking mode and exposes a local SOCKS5 proxy. Only ZTM API requests use that proxy; GCS uploads stay on direct container networking.

Userspace mode does not require `NET_ADMIN` or `/dev/net/tun`. If Tailscale auth or exit-node setup fails, the container exits.

Persist `/var/lib/tailscale` across redeploys so the container keeps the same Tailscale device identity and Mullvad approval:

```text
/home/ubuntu/ztm-poller-tailscale -> /var/lib/tailscale
```

If `tailscaled.state` exists in that mounted directory, `TS_AUTHKEY` is optional and the existing device identity is reused. If `TS_AUTHKEY` is present, it is still passed to `tailscale up`; after bootstrap, prefer removing it to prove redeploys use only persisted state. If the state file is missing, `TS_AUTHKEY` is required to bootstrap a new device.

If a new container node must be manually approved for Mullvad VPN access, the poller usually stays alive by retrying API failures. `STARTUP_GRACE_SECONDS` also prevents an early poller crash from immediately removing the container before approval can be completed. Startup grace does not apply to failures before `poller.py` starts, including `tailscaled` readiness or `tailscale up` failures.

```bash
docker build -t ztm-gps-poller ./poller
docker run --rm \
  -e ZTM_API_TOKEN \
    -e TS_AUTHKEY \
    -e GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp-key.json \
    -v /secure/path/service-account-key.json:/run/secrets/gcp-key.json:ro \
    -v /home/ubuntu/ztm-poller-tailscale:/var/lib/tailscale \
    ztm-gps-poller
```

Safe container smoke test, with no GCS client initialization and no upload:

```bash
docker run --rm \
  -e ZTM_API_TOKEN \
  -e TS_AUTHKEY \
  ztm-gps-poller --once --no-upload
```

In Coolify, configure `TS_AUTHKEY` as a runtime environment variable only. Do not pass it as a build argument. After the first successful boot with a persisted state mount, redeploys should reuse the same Tailscale device and should not require a fresh Mullvad approval.
