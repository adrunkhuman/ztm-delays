from __future__ import annotations

import io
from argparse import Namespace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import GoogleAPIError

import poller

if TYPE_CHECKING:
    from google.cloud import storage

EXPECTED_TIMEOUT_SECONDS = 5.0


def test_extract_records_keeps_only_dict_records() -> None:
    payload = {"result": [{"Lines": "187"}, "bad", {"Lines": "517"}]}

    records = poller._extract_records(payload)

    assert records == [{"Lines": "187"}, {"Lines": "517"}]


def test_parse_record_converts_warsaw_time_to_utc() -> None:
    record: dict[str, object] = {
        "Lines": "187",
        "Brigade": "01",
        "Lat": 52.2297,
        "Lon": 21.0122,
        "Time": "2026-01-15 12:00:00",
        "VehicleNumber": "1234",
    }

    row = poller._parse_record(record, vehicle_type_id=1)

    assert row == {
        "Lines": "187",
        "Brigade": "01",
        "Lat": 52.2297,
        "Lon": 21.0122,
        "Time": datetime(2026, 1, 15, 11, tzinfo=UTC),
        "VehicleNumber": "1234",
        "vehicle_type": 1,
    }


def test_parse_record_skips_invalid_records() -> None:
    row = poller._parse_record({"Lines": "187"}, vehicle_type_id=1)

    assert row is None


def test_flush_hours_removes_buffer_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded_hours: list[datetime] = []
    closed_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {closed_hour: [_gps_row()]}

    def fake_upload_hour(
        _bucket: object, _config: poller.Config, buffer_hour: datetime, _rows: list[poller.GpsRow]
    ) -> None:
        uploaded_hours.append(buffer_hour)

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_hours(object(), _config(), buffers, current_hour)  # ty: ignore[invalid-argument-type]

    assert uploaded_hours == [closed_hour]
    assert buffers == {}


def test_flush_hours_keeps_buffer_after_upload_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    closed_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {closed_hour: [_gps_row()]}

    def fake_upload_hour(
        _bucket: object, _config: poller.Config, _buffer_hour: datetime, _rows: list[poller.GpsRow]
    ) -> None:
        raise GoogleAPIError("transient failure")

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_hours(object(), _config(), buffers, current_hour)  # ty: ignore[invalid-argument-type]

    assert buffers == {closed_hour: [_gps_row()]}


def test_flush_hours_includes_current_hour_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded_hours: list[datetime] = []
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {current_hour: [_gps_row()]}

    def fake_upload_hour(
        _bucket: object, _config: poller.Config, buffer_hour: datetime, _rows: list[poller.GpsRow]
    ) -> None:
        uploaded_hours.append(buffer_hour)

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_hours(object(), _config(), buffers, current_hour, flush_current=True)  # ty: ignore[invalid-argument-type]

    assert uploaded_hours == [current_hour]
    assert buffers == {}


def test_upload_hour_writes_append_safe_part_file_with_expected_schema() -> None:
    bucket = FakeBucket()

    poller._upload_hour(
        cast("storage.Bucket", bucket),
        _config(),
        datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ),
        [_gps_row()],
    )

    assert bucket.path is not None
    assert bucket.path.startswith("raw/gps/vehicle_type=bus/date=2026-01-15/hour=10/part-")
    assert bucket.path.endswith("Z.parquet")
    assert bucket.blob_obj.content_type == "application/octet-stream"

    table = pq.read_table(io.BytesIO(bucket.blob_obj.data))
    assert table.schema.names == poller.SCHEMA.names
    assert table.num_rows == 1


def test_load_config_uses_cli_smoke_mode_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("VEHICLE_TYPE", "bus")

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.run_once is True
    assert config.no_upload is True
    assert config.vehicle_type_id == 1


def test_load_config_rejects_non_positive_timing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("VEHICLE_TYPE", "bus")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")

    with pytest.raises(RuntimeError, match="POLL_INTERVAL_SECONDS"):
        poller._load_config(Namespace(once=True, no_upload=True))


def test_main_once_no_upload_does_not_initialize_gcs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("VEHICLE_TYPE", "bus")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once", "--no-upload"])
    monkeypatch.setattr(poller.requests, "Session", FakeSession)
    monkeypatch.setattr(poller.storage, "Client", _fail_if_called)

    assert poller.main() == 0


def _config() -> poller.Config:
    return poller.Config(
        api_token="token",
        vehicle_type_id=1,
        vehicle_type_name="bus",
        gcs_bucket="bucket",
        gcs_prefix="raw/gps",
        poll_interval_seconds=10,
        api_timeout_seconds=5,
        run_once=False,
        no_upload=False,
    )


def _gps_row() -> poller.GpsRow:
    return {
        "Lines": "187",
        "Brigade": "01",
        "Lat": 52.2297,
        "Lon": 21.0122,
        "Time": datetime(2026, 1, 15, 10, tzinfo=UTC),
        "VehicleNumber": "1234",
        "vehicle_type": 1,
    }


class FakeBlob:
    def __init__(self) -> None:
        self.data = b""
        self.content_type = ""

    def upload_from_file(self, file_obj: io.BytesIO, content_type: str) -> None:
        self.data = file_obj.read()
        self.content_type = content_type


class FakeBucket:
    def __init__(self) -> None:
        self.blob_obj = FakeBlob()
        self.path: str | None = None

    def blob(self, path: str) -> FakeBlob:
        self.path = path
        return self.blob_obj


class FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, list[dict[str, object]]]:
        return {
            "result": [
                {
                    "Lines": "187",
                    "Brigade": "01",
                    "Lat": 52.2297,
                    "Lon": 21.0122,
                    "Time": "2026-01-15 12:00:00",
                    "VehicleNumber": "1234",
                }
            ]
        }


class FakeSession:
    def post(self, _url: str, *, headers: dict[str, str], json: dict[str, int], timeout: float) -> FakeResponse:
        assert headers == {"Authorization": "token"}
        assert json == {"type": 1}
        assert timeout == EXPECTED_TIMEOUT_SECONDS
        return FakeResponse()


def _fail_if_called() -> None:
    raise AssertionError("GCS client should not be initialized")
