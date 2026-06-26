# ZTM GPS Poller

One container polls one Warsaw ZTM vehicle type every 10 seconds and writes closed hourly Parquet files to GCS.

## Required Environment

- `ZTM_API_TOKEN`: city API token used as the `Authorization` header.
- `VEHICLE_TYPE`: `bus` or `tram` (`1` and `2` also accepted).
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
- `TS_HOSTNAME`: defaults to `ztm-poller-${VEHICLE_TYPE}`.
- `TS_SOCKS_ADDR`: defaults to `127.0.0.1:1055`.
- `ZTM_API_PROXY`: normally set by `entrypoint.sh`; can be set manually for local proxy smoke tests.

## Output

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=YYYY-MM-DD/hour=HH/part-YYYYMMDDTHHMMSSffffffZ.parquet
gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=YYYY-MM-DD/hour=HH/part-YYYYMMDDTHHMMSSffffffZ.parquet
```

Multiple part files per hour are expected. This avoids overwriting an already-uploaded hour if the API later returns stale pings for that hour.

`Time` is parsed as Europe/Warsaw local time and written as UTC Parquet timestamp because the raw BigQuery schema declares it as `TIMESTAMP`.

## Local Run

```bash
uv sync
export ZTM_API_TOKEN=...
VEHICLE_TYPE=bus uv run python poller.py
```

Safe live API smoke test, with no GCS client initialization and no upload:

```bash
export ZTM_API_TOKEN=...
VEHICLE_TYPE=bus uv run python poller.py --once --no-upload
```

## Docker

The Docker image runs `tailscaled` in userspace networking mode and exposes a local SOCKS5 proxy. Only ZTM API requests use that proxy; GCS uploads stay on direct container networking.

Userspace mode does not require `NET_ADMIN` or `/dev/net/tun`. If Tailscale auth or exit-node setup fails, the container exits.

```bash
docker build -t ztm-gps-poller ./poller
docker run --rm \
  -e ZTM_API_TOKEN \
  -e TS_AUTHKEY \
  -e VEHICLE_TYPE=bus \
  -e GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp-key.json \
  -v /secure/path/service-account-key.json:/run/secrets/gcp-key.json:ro \
  ztm-gps-poller
```

Safe container smoke test, with no GCS client initialization and no upload:

```bash
docker run --rm \
  -e ZTM_API_TOKEN \
  -e TS_AUTHKEY \
  -e VEHICLE_TYPE=bus \
  ztm-gps-poller --once --no-upload
```

In Coolify, configure `TS_AUTHKEY` as a runtime environment variable only. Do not pass it as a build argument.
