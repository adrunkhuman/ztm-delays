# ZTM GPS Poller

One container polls Warsaw ZTM buses and trams every 10 seconds and writes closed hourly Parquet files to GCS.

## Required Environment

- `ZTM_API_TOKEN`: city API token used as the `Authorization` header.
- `GOOGLE_APPLICATION_CREDENTIALS`: path to the mounted GCP service account key.
- `TS_AUTHKEY`: Tailscale auth key used to join the tailnet non-interactively.

## Optional Environment

- `GCS_BUCKET`: defaults to `ztm-analytics-bucket`.
- `GCS_PREFIX`: defaults to `raw/gps`.
- `POLL_INTERVAL_SECONDS`: defaults to `10`.
- `API_TIMEOUT_SECONDS`: defaults to `5`.
- `MAX_PING_AGE_SECONDS`: defaults to `300`; older API rows are dropped.
- `FUTURE_PING_TOLERANCE_SECONDS`: defaults to `60`; farther-future API rows are dropped.
- `LOG_LEVEL`: defaults to `INFO`.
- `TS_EXIT_NODE`: defaults to `100.103.142.113` (`pl-waw-wg-101.mullvad.ts.net`, Warsaw).
- `TS_HOSTNAME`: defaults to `ztm-poller`.
- `TS_SOCKS_ADDR`: defaults to `127.0.0.1:1055`.
- `STARTUP_GRACE_SECONDS`: defaults to `300`; keeps the container alive briefly if the poller exits during startup.
- `ZTM_API_PROXY`: normally set by `entrypoint.sh`; can be set manually for local proxy smoke tests.

## Output

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=YYYY-MM-DD/hour=HH/part-YYYYMMDDTHHMMSSffffffZ.parquet
gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=YYYY-MM-DD/hour=HH/part-YYYYMMDDTHHMMSSffffffZ.parquet
```

Multiple part files per hour are expected. This avoids overwriting an already-uploaded hour if the API later returns stale pings for that hour.

`Time` is parsed as Europe/Warsaw local time and written as UTC Parquet timestamp because the raw BigQuery schema declares it as `TIMESTAMP`.

## Operational Semantics

Run exactly one poller instance. Older split deployments must be removed before deploying this version; running both old `poller-bus` and `poller-tram` services after this change duplicates bus and tram raw files.

Rows are buffered in memory until their Warsaw-local hour closes. On graceful shutdown, the current hour is flushed too. If an upload fails, that hour stays buffered and is retried on the next flush attempt. A crash, forced container kill, or host restart loses rows that were buffered but not uploaded; there is no durable local spool.

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

If the new container node must be manually approved for Mullvad VPN access, the poller usually stays alive by retrying API failures. `STARTUP_GRACE_SECONDS` also prevents an early poller crash from immediately removing the container before approval can be completed.

```bash
docker build -t ztm-gps-poller ./poller
docker run --rm \
  -e ZTM_API_TOKEN \
  -e TS_AUTHKEY \
  -e GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp-key.json \
  -v /secure/path/service-account-key.json:/run/secrets/gcp-key.json:ro \
  ztm-gps-poller
```

Safe container smoke test, with no GCS client initialization and no upload:

```bash
docker run --rm \
  -e ZTM_API_TOKEN \
  -e TS_AUTHKEY \
  ztm-gps-poller --once --no-upload
```

In Coolify, configure `TS_AUTHKEY` as a runtime environment variable only. Do not pass it as a build argument.
