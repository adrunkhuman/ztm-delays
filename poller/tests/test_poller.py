from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from google.api_core.exceptions import GoogleAPIError

import poller

if TYPE_CHECKING:
    import pytest


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


def test_flush_closed_hours_removes_buffer_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded_hours: list[datetime] = []
    closed_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {closed_hour: [_gps_row()]}

    def fake_upload_hour(
        _bucket: object, _config: poller.Config, buffer_hour: datetime, _rows: list[poller.GpsRow]
    ) -> None:
        uploaded_hours.append(buffer_hour)

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_closed_hours(object(), _config(), buffers, current_hour)  # ty: ignore[invalid-argument-type]

    assert uploaded_hours == [closed_hour]
    assert buffers == {}


def test_flush_closed_hours_keeps_buffer_after_upload_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    closed_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {closed_hour: [_gps_row()]}

    def fake_upload_hour(
        _bucket: object, _config: poller.Config, _buffer_hour: datetime, _rows: list[poller.GpsRow]
    ) -> None:
        raise GoogleAPIError("transient failure")

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_closed_hours(object(), _config(), buffers, current_hour)  # ty: ignore[invalid-argument-type]

    assert buffers == {closed_hour: [_gps_row()]}


def test_load_config_uses_cli_smoke_mode_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WARSAW_API_TOKEN", "token")
    monkeypatch.setenv("VEHICLE_TYPE", "bus")

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.run_once is True
    assert config.no_upload is True
    assert config.vehicle_type_id == 1


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
