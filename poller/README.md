# ZTM GPS Poller

One container polls one Warsaw ZTM vehicle type every 10 seconds and writes closed hourly Parquet files to GCS.

## Required Environment

- `ZTM_API_TOKEN`: city API token used as the `Authorization` header.
- `VEHICLE_TYPE`: `bus` or `tram` (`1` and `2` also accepted).
- `GOOGLE_APPLICATION_CREDENTIALS`: path to the mounted GCP service account key.

## Optional Environment

- `GCS_BUCKET`: defaults to `ztm-analytics-bucket`.
- `GCS_PREFIX`: defaults to `raw/gps`.
- `POLL_INTERVAL_SECONDS`: defaults to `10`.
- `API_TIMEOUT_SECONDS`: defaults to `5`.
- `LOG_LEVEL`: defaults to `INFO`.

## Output

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type=bus/date=YYYY-MM-DD/hour=HH.parquet
gs://ztm-analytics-bucket/raw/gps/vehicle_type=tram/date=YYYY-MM-DD/hour=HH.parquet
```

`Time` is parsed as Europe/Warsaw local time and written as UTC Parquet timestamp because the raw BigQuery schema declares it as `TIMESTAMP`.

## Local Run

```bash
uv sync
VEHICLE_TYPE=bus uv run python poller.py
```

Safe live API smoke test, with no GCS client initialization and no upload:

```bash
VEHICLE_TYPE=bus uv run python poller.py --once --no-upload
```

## Docker

```bash
docker build -t ztm-gps-poller ./poller
docker run --rm \
  -e ZTM_API_TOKEN \
  -e VEHICLE_TYPE=bus \
  -e GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp-key.json \
  -v /secure/path/service-account-key.json:/run/secrets/gcp-key.json:ro \
  ztm-gps-poller
```
