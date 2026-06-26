from __future__ import annotations

import io
from argparse import Namespace
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, cast

import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import GoogleAPIError

import poller

if TYPE_CHECKING:
    import requests
    from google.cloud import storage

EXPECTED_TIMEOUT_SECONDS = 5.0
EXPECTED_BUS_TYPE = 1
EXPECTED_TRAM_TYPE = 2
ENTRYPOINT = Path(__file__).resolve().parents[1] / "entrypoint.sh"


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
        _upload_context: poller.UploadContext,
        buffer_hour: datetime,
        _rows: list[poller.GpsRow],
    ) -> None:
        uploaded_hours.append(buffer_hour)

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_hours(_upload_context(), buffers, current_hour)

    assert uploaded_hours == [closed_hour]
    assert buffers == {}


def test_flush_hours_keeps_buffer_after_upload_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    closed_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {closed_hour: [_gps_row()]}

    def fake_upload_hour(
        _upload_context: poller.UploadContext,
        _buffer_hour: datetime,
        _rows: list[poller.GpsRow],
    ) -> None:
        raise GoogleAPIError("transient failure")

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_hours(_upload_context(), buffers, current_hour)

    assert buffers == {closed_hour: [_gps_row()]}


def test_flush_hours_includes_current_hour_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded_hours: list[datetime] = []
    current_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    buffers = {current_hour: [_gps_row()]}

    def fake_upload_hour(
        _upload_context: poller.UploadContext,
        buffer_hour: datetime,
        _rows: list[poller.GpsRow],
    ) -> None:
        uploaded_hours.append(buffer_hour)

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    poller._flush_hours(_upload_context(), buffers, current_hour, flush_current=True)

    assert uploaded_hours == [current_hour]
    assert buffers == {}


def test_upload_hour_writes_append_safe_part_file_with_expected_schema() -> None:
    bucket = FakeBucket()

    poller._upload_hour(
        _upload_context(cast("storage.Bucket", bucket)),
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

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.run_once is True
    assert config.no_upload is True
    assert config.vehicle_types == poller.VEHICLE_TYPES


def test_load_config_uses_api_proxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("ZTM_API_PROXY", "socks5h://127.0.0.1:1055")

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.api_proxy == "socks5h://127.0.0.1:1055"


def test_load_config_ignores_blank_api_proxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("ZTM_API_PROXY", "   ")

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.api_proxy is None


def test_filter_fresh_rows_keeps_current_and_drops_stale_and_future() -> None:
    now = datetime(2026, 1, 15, 12, tzinfo=UTC)
    stale_row = _gps_row(time=now - timedelta(minutes=6))
    current_row = _gps_row(time=now - timedelta(minutes=1))
    future_row = _gps_row(time=now + timedelta(minutes=2))

    fresh_rows, dropped_stale, dropped_future = poller._filter_fresh_rows(
        [stale_row, current_row, future_row], now, _config()
    )

    assert fresh_rows == [current_row]
    assert dropped_stale == 1
    assert dropped_future == 1


def test_filter_fresh_rows_uses_inclusive_boundaries() -> None:
    now = datetime(2026, 1, 15, 12, tzinfo=UTC)
    oldest_allowed = _gps_row(time=now - timedelta(minutes=5))
    newest_allowed = _gps_row(time=now + timedelta(minutes=1))

    fresh_rows, dropped_stale, dropped_future = poller._filter_fresh_rows(
        [oldest_allowed, newest_allowed], now, _config()
    )

    assert fresh_rows == [oldest_allowed, newest_allowed]
    assert dropped_stale == 0
    assert dropped_future == 0


def test_poll_api_uses_configured_proxy() -> None:
    proxy = "socks5h://127.0.0.1:1055"
    FakeSession.expected_proxies = {"http": proxy, "https": proxy}

    try:
        rows = poller._poll_api(
            cast("requests.Session", FakeSession()), _config(api_proxy=proxy), poller.VehicleType(1, "bus")
        )
    finally:
        FakeSession.expected_proxies = None

    assert len(rows) == 1


def test_load_config_rejects_non_positive_timing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")

    with pytest.raises(RuntimeError, match="POLL_INTERVAL_SECONDS"):
        poller._load_config(Namespace(once=True, no_upload=True))


def test_main_once_no_upload_does_not_initialize_gcs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once", "--no-upload"])
    FakeSession.seen_vehicle_type_ids = []
    monkeypatch.setattr(poller.requests, "Session", FakeSession)
    monkeypatch.setattr(poller.storage, "Client", _fail_if_called)

    try:
        assert poller.main() == 0
        assert FakeSession.seen_vehicle_type_ids == [1, 2]
    finally:
        FakeSession.seen_vehicle_type_ids = []


def test_main_once_uploads_bus_and_tram_to_separate_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = FakeBucket()
    now = datetime.now(UTC)

    def fake_poll_api(
        _session: object, _config: poller.Config, vehicle_type: poller.VehicleType
    ) -> list[poller.GpsRow]:
        return [_gps_row(time=now, vehicle_type=vehicle_type.id)]

    class FakeClient:
        def bucket(self, name: str) -> FakeBucket:
            assert name == "ztm-analytics-bucket"
            return bucket

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller, "_poll_api", fake_poll_api)
    monkeypatch.setattr(poller.storage, "Client", FakeClient)

    assert poller.main() == 0

    bus_path = next(path for path in bucket.blobs if "/vehicle_type=bus/" in path)
    tram_path = next(path for path in bucket.blobs if "/vehicle_type=tram/" in path)
    assert pq.read_table(io.BytesIO(bucket.blobs[bus_path].data)).to_pylist()[0]["vehicle_type"] == EXPECTED_BUS_TYPE
    assert pq.read_table(io.BytesIO(bucket.blobs[tram_path].data)).to_pylist()[0]["vehicle_type"] == EXPECTED_TRAM_TYPE


def test_poll_vehicle_type_failure_does_not_block_next_vehicle(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    buffers = {"bus": defaultdict(list), "tram": defaultdict(list)}

    def fake_poll_api(
        _session: object, _config: poller.Config, vehicle_type: poller.VehicleType
    ) -> list[poller.GpsRow]:
        if vehicle_type.name == "bus":
            raise poller.requests.ConnectionError("blocked")
        return [_gps_row(time=now, vehicle_type=vehicle_type.id)]

    monkeypatch.setattr(poller, "_poll_api", fake_poll_api)

    for vehicle_type in poller.VEHICLE_TYPES:
        poller._poll_vehicle_type(
            cast("requests.Session", object()), _config(), vehicle_type, buffers[vehicle_type.name]
        )

    assert buffers["bus"] == {}
    assert sum(len(rows) for rows in buffers["tram"].values()) == 1


def test_poll_vehicle_type_buffers_only_fresh_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    buffers: dict[datetime, list[poller.GpsRow]] = defaultdict(list)
    rows = [
        _gps_row(time=now - timedelta(minutes=6)),
        _gps_row(time=now),
        _gps_row(time=now + timedelta(minutes=2)),
    ]

    monkeypatch.setattr(poller, "_poll_api", lambda *_args: rows)

    poller._poll_vehicle_type(cast("requests.Session", object()), _config(), poller.VehicleType(1, "bus"), buffers)

    assert [row for buffered_rows in buffers.values() for row in buffered_rows] == [rows[1]]


def test_entrypoint_runs_tailscale_userspace_proxy_before_poller() -> None:
    script = ENTRYPOINT.read_text()

    assert '"${VEHICLE_TYPE:?VEHICLE_TYPE is required}"' not in script
    assert 'TS_HOSTNAME="ztm-poller"' in script
    assert "tailscaled" in script
    assert "--tun=userspace-networking" in script
    assert '--socks5-server="${TS_SOCKS_ADDR}"' in script
    assert "tailscale up" in script
    assert '--exit-node="${TS_EXIT_NODE}"' in script
    assert 'export ZTM_API_PROXY="socks5h://${TS_SOCKS_ADDR}"' in script
    assert 'uv run --locked --no-dev python poller.py "$@"' in script


def test_entrypoint_fails_before_poller_when_tailscaled_is_not_ready() -> None:
    script = ENTRYPOINT.read_text()
    readiness_check = "if [ ! -S /var/run/tailscale/tailscaled.sock ]; then"
    failure = 'echo "tailscaled did not become ready" >&2'
    poller_start = 'uv run --locked --no-dev python poller.py "$@"'

    assert script.index(readiness_check) < script.index(failure) < script.index(poller_start)


def test_entrypoint_keeps_container_alive_after_early_poller_failure() -> None:
    script = ENTRYPOINT.read_text()

    assert 'STARTUP_GRACE_SECONDS="${STARTUP_GRACE_SECONDS:-300}"' in script
    assert 'if [ "${POLLER_STATUS}" -ne 0 ]; then' in script
    assert 'if [ "${RUNTIME_SECONDS}" -lt "${STARTUP_GRACE_SECONDS}" ]; then' in script
    assert 'sleep "${REMAINING_SECONDS}"' in script


def _config(api_proxy: str | None = None) -> poller.Config:
    return poller.Config(
        api_token="token",
        vehicle_types=poller.VEHICLE_TYPES,
        gcs_bucket="bucket",
        gcs_prefix="raw/gps",
        poll_interval_seconds=10,
        api_timeout_seconds=5,
        api_proxy=api_proxy,
        max_ping_age_seconds=300,
        future_ping_tolerance_seconds=60,
        run_once=False,
        no_upload=False,
    )


def _upload_context(bucket: storage.Bucket | None = None) -> poller.UploadContext:
    return poller.UploadContext(
        bucket=cast("storage.Bucket", bucket or FakeBucket()),
        config=_config(),
        vehicle_type_name="bus",
    )


def _gps_row(time: datetime | None = None, vehicle_type: int = 1) -> poller.GpsRow:
    return {
        "Lines": "187",
        "Brigade": "01",
        "Lat": 52.2297,
        "Lon": 21.0122,
        "Time": time or datetime(2026, 1, 15, 10, tzinfo=UTC),
        "VehicleNumber": "1234",
        "vehicle_type": vehicle_type,
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
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, path: str) -> FakeBlob:
        self.path = path
        self.blob_obj = FakeBlob()
        self.blobs[path] = self.blob_obj
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
    expected_proxies: ClassVar[dict[str, str] | None] = None
    seen_vehicle_type_ids: ClassVar[list[int]] = []

    def post(
        self,
        _url: str,
        *,
        headers: dict[str, str],
        json: dict[str, int],
        timeout: float,
        proxies: dict[str, str] | None,
    ) -> FakeResponse:
        assert headers == {"Authorization": "token"}
        assert json["type"] in {1, 2}
        self.seen_vehicle_type_ids.append(json["type"])
        assert timeout == EXPECTED_TIMEOUT_SECONDS
        assert proxies == self.expected_proxies
        return FakeResponse()


def _fail_if_called() -> None:
    raise AssertionError("GCS client should not be initialized")
