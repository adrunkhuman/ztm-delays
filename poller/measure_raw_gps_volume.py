from __future__ import annotations

import argparse
import io
import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Protocol

import pyarrow.parquet as pq
from google.cloud import storage

RAW_GPS_PATH_PATTERN = re.compile(
    r"(?:^|/)vehicle_type=(?P<vehicle_type>[^/]+)/date=(?P<gps_date>\d{4}-\d{2}-\d{2})/"
    r"hour=(?P<hour>\d{2})/[^/]+\.parquet$"
)
VEHICLE_TYPES = ("bus", "tram")

if TYPE_CHECKING:
    from collections.abc import Iterable


class ParquetBlob(Protocol):
    """Blob interface needed to read Parquet metadata."""

    def download_as_bytes(self) -> bytes:
        """Download blob contents."""


@dataclass(frozen=True)
class RawGpsVolumeRequest:
    """Bounded raw GPS volume measurement request."""

    bucket_name: str
    gcs_prefix: str
    start_date: date
    end_date: date
    include_row_counts: bool = False


@dataclass(frozen=True)
class RawGpsObject:
    """One raw GPS GCS object matched from the Hive-style path."""

    name: str
    vehicle_type: str
    gps_date: date
    hour: int
    size_bytes: int
    row_count: int | None = None


@dataclass(frozen=True)
class VolumeSummaryRow:
    """Aggregated raw GPS volume for one date/hour/mode bucket."""

    gps_date: str
    hour: int | None
    vehicle_type: str | None
    object_count: int
    size_bytes: int
    row_count: int | None


def main() -> int:
    """Measure raw GPS object volume in GCS without touching BigQuery."""
    args = _parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if end_date < start_date:
        raise SystemExit("--end-date must be on or after --start-date")

    client = storage.Client()
    objects = list(
        iter_raw_gps_objects(
            client,
            RawGpsVolumeRequest(
                bucket_name=args.bucket,
                gcs_prefix=args.prefix,
                start_date=start_date,
                end_date=end_date,
                include_row_counts=args.include_row_counts,
            ),
        )
    )
    summary = summarize_raw_gps_objects(objects)
    payload = {
        "bucket": args.bucket,
        "prefix": args.prefix.strip("/"),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "include_row_counts": args.include_row_counts,
        "total": asdict(summary["total"][0]),
        "by_date": [asdict(row) for row in summary["by_date"]],
        "by_hour": [asdict(row) for row in summary["by_hour"]],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def iter_raw_gps_objects(client: storage.Client, request: RawGpsVolumeRequest) -> Iterable[RawGpsObject]:
    """Yield raw GPS object metadata for the requested date window."""
    bucket = client.bucket(request.bucket_name)
    normalized_prefix = request.gcs_prefix.strip("/")
    for vehicle_type in VEHICLE_TYPES:
        for gps_date in _date_range(request.start_date, request.end_date):
            prefix = f"{normalized_prefix}/vehicle_type={vehicle_type}/date={gps_date.isoformat()}/"
            for blob in bucket.list_blobs(prefix=prefix):
                raw_object = parse_raw_gps_blob(blob.name, blob.size or 0)
                if raw_object is None:
                    continue
                if request.include_row_counts:
                    raw_object = RawGpsObject(
                        name=raw_object.name,
                        vehicle_type=raw_object.vehicle_type,
                        gps_date=raw_object.gps_date,
                        hour=raw_object.hour,
                        size_bytes=raw_object.size_bytes,
                        row_count=_parquet_row_count(blob),
                    )
                yield raw_object


def parse_raw_gps_blob(name: str, size_bytes: int) -> RawGpsObject | None:
    """Parse one raw GPS object path, returning None for non-matching paths."""
    match = RAW_GPS_PATH_PATTERN.search(name)
    if match is None:
        return None
    return RawGpsObject(
        name=name,
        vehicle_type=match.group("vehicle_type"),
        gps_date=date.fromisoformat(match.group("gps_date")),
        hour=int(match.group("hour")),
        size_bytes=size_bytes,
    )


def summarize_raw_gps_objects(objects: Iterable[RawGpsObject]) -> dict[str, list[VolumeSummaryRow]]:
    """Summarize raw GPS objects by total, date, and hour."""
    by_date: dict[tuple[date, str], list[RawGpsObject]] = defaultdict(list)
    by_hour: dict[tuple[date, int, str], list[RawGpsObject]] = defaultdict(list)
    all_objects = list(objects)
    for raw_object in all_objects:
        by_date[(raw_object.gps_date, raw_object.vehicle_type)].append(raw_object)
        by_hour[(raw_object.gps_date, raw_object.hour, raw_object.vehicle_type)].append(raw_object)

    return {
        "total": [_summary_row(None, None, None, all_objects)],
        "by_date": [
            _summary_row(gps_date, None, vehicle_type, rows)
            for (gps_date, vehicle_type), rows in sorted(by_date.items())
        ],
        "by_hour": [
            _summary_row(gps_date, hour, vehicle_type, rows)
            for (gps_date, hour, vehicle_type), rows in sorted(by_hour.items())
        ],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure raw GPS GCS object volume for a bounded date window.")
    parser.add_argument("--bucket", default="ztm-analytics-bucket")
    parser.add_argument("--prefix", default="raw/gps")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--include-row-counts",
        action="store_true",
        help="download each bounded Parquet object to read row-count metadata",
    )
    return parser.parse_args()


def _date_range(start_date: date, end_date: date) -> Iterable[date]:
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def _summary_row(
    gps_date: date | None,
    hour: int | None,
    vehicle_type: str | None,
    rows: list[RawGpsObject],
) -> VolumeSummaryRow:
    row_counts = [row.row_count for row in rows if row.row_count is not None]
    return VolumeSummaryRow(
        gps_date=gps_date.isoformat() if gps_date else "all",
        hour=hour,
        vehicle_type=vehicle_type,
        object_count=len(rows),
        size_bytes=sum(row.size_bytes for row in rows),
        row_count=sum(row_counts) if row_counts else None,
    )


def _parquet_row_count(blob: ParquetBlob) -> int:
    metadata = pq.read_metadata(io.BytesIO(blob.download_as_bytes()))
    return metadata.num_rows


if __name__ == "__main__":
    raise SystemExit(main())
