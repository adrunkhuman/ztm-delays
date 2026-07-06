from __future__ import annotations

import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

import measure_raw_gps_volume as volume

EXPECTED_PARQUET_ROW_COUNT = 2


def test_parse_raw_gps_blob_matches_hive_path() -> None:
    raw_object = volume.parse_raw_gps_blob("raw/gps/vehicle_type=bus/date=2026-07-05/hour=17/part-abc.parquet", 1234)

    assert raw_object == volume.RawGpsObject(
        name="raw/gps/vehicle_type=bus/date=2026-07-05/hour=17/part-abc.parquet",
        vehicle_type="bus",
        gps_date=date(2026, 7, 5),
        hour=17,
        size_bytes=1234,
    )


def test_parse_raw_gps_blob_ignores_non_raw_gps_path() -> None:
    assert volume.parse_raw_gps_blob("raw/gps/date=2026-07-05/hour=17/part-abc.parquet", 1234) is None


def test_summarize_raw_gps_objects_groups_by_date_and_hour() -> None:
    objects = [
        volume.RawGpsObject("a", "bus", date(2026, 7, 5), 17, 100, row_count=10),
        volume.RawGpsObject("b", "bus", date(2026, 7, 5), 17, 200, row_count=20),
        volume.RawGpsObject("c", "tram", date(2026, 7, 5), 18, 300, row_count=30),
    ]

    summary = volume.summarize_raw_gps_objects(objects)

    assert summary["total"] == [
        volume.VolumeSummaryRow("all", None, None, object_count=3, size_bytes=600, row_count=60)
    ]
    assert summary["by_date"] == [
        volume.VolumeSummaryRow("2026-07-05", None, "bus", object_count=2, size_bytes=300, row_count=30),
        volume.VolumeSummaryRow("2026-07-05", None, "tram", object_count=1, size_bytes=300, row_count=30),
    ]
    assert summary["by_hour"] == [
        volume.VolumeSummaryRow("2026-07-05", 17, "bus", object_count=2, size_bytes=300, row_count=30),
        volume.VolumeSummaryRow("2026-07-05", 18, "tram", object_count=1, size_bytes=300, row_count=30),
    ]


def test_iter_raw_gps_objects_lists_bounded_prefixes_without_downloading() -> None:
    matching_blob = FakeListingBlob(
        "raw/gps/vehicle_type=bus/date=2026-07-05/hour=17/part-abc.parquet",
        size=1234,
    )
    ignored_blob = FakeListingBlob("raw/gps/vehicle_type=bus/date=2026-07-05/hour=17/not-parquet.txt", size=10)
    client = FakeClient(
        {
            "raw/gps/vehicle_type=bus/date=2026-07-05/": [matching_blob, ignored_blob],
            "raw/gps/vehicle_type=tram/date=2026-07-06/": [
                FakeListingBlob("raw/gps/vehicle_type=tram/date=2026-07-06/hour=18/part-def.parquet", size=456)
            ],
        }
    )

    objects = list(
        volume.iter_raw_gps_objects(
            client,
            volume.RawGpsVolumeRequest(
                bucket_name="bucket",
                gcs_prefix="raw/gps",
                start_date=date(2026, 7, 5),
                end_date=date(2026, 7, 6),
            ),
        )
    )

    assert client.bucket_obj.requested_prefixes == [
        "raw/gps/vehicle_type=bus/date=2026-07-05/",
        "raw/gps/vehicle_type=bus/date=2026-07-06/",
        "raw/gps/vehicle_type=tram/date=2026-07-05/",
        "raw/gps/vehicle_type=tram/date=2026-07-06/",
    ]
    assert objects == [
        volume.RawGpsObject(
            "raw/gps/vehicle_type=bus/date=2026-07-05/hour=17/part-abc.parquet",
            "bus",
            date(2026, 7, 5),
            17,
            1234,
        ),
        volume.RawGpsObject(
            "raw/gps/vehicle_type=tram/date=2026-07-06/hour=18/part-def.parquet",
            "tram",
            date(2026, 7, 6),
            18,
            456,
        ),
    ]
    assert matching_blob.downloaded is False


def test_parquet_row_count_reads_metadata_from_blob_bytes() -> None:
    table = pa.Table.from_pylist([{"vehicle": "1"}, {"vehicle": "2"}])
    data = io.BytesIO()
    pq.write_table(table, data)

    assert volume._parquet_row_count(FakeBlob(data.getvalue())) == EXPECTED_PARQUET_ROW_COUNT


class FakeBlob:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def download_as_bytes(self) -> bytes:
        return self.data


class FakeListingBlob:
    def __init__(self, name: str, *, size: int) -> None:
        self.name = name
        self.size = size
        self.downloaded = False

    def download_as_bytes(self) -> bytes:
        self.downloaded = True
        return b""


class FakeBucket:
    def __init__(self, blobs_by_prefix: dict[str, list[FakeListingBlob]]) -> None:
        self.blobs_by_prefix = blobs_by_prefix
        self.requested_prefixes: list[str] = []

    def list_blobs(self, *, prefix: str) -> list[FakeListingBlob]:
        self.requested_prefixes.append(prefix)
        return self.blobs_by_prefix.get(prefix, [])


class FakeClient:
    def __init__(self, blobs_by_prefix: dict[str, list[FakeListingBlob]]) -> None:
        self.bucket_obj = FakeBucket(blobs_by_prefix)

    def bucket(self, bucket_name: str) -> FakeBucket:
        assert bucket_name == "bucket"
        return self.bucket_obj
