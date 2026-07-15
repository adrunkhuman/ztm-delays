from __future__ import annotations

import io
import json
from argparse import Namespace
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, cast

import pyarrow.parquet as pq
import pytest
from google.api_core.exceptions import GoogleAPIError, PreconditionFailed

import poller

if TYPE_CHECKING:
    import requests
    from google.cloud import storage

EXPECTED_TIMEOUT_SECONDS = 5.0
EXPECTED_BUS_TYPE = 1
EXPECTED_TRAM_TYPE = 2
EXPECTED_DEFAULT_PARTIAL_FLUSH_SECONDS = 900
CUSTOM_PARTIAL_FLUSH_SECONDS = 600
CUSTOM_FLUSH_LAG_SECONDS = 120
CUSTOM_HEARTBEAT_SECONDS = 30
HEARTBEAT_ACCEPTED_ROWS = 10
EXPECTED_RETRY_FLUSH_CALLS = 2
EGRESS_CHECK_URL = "https://example.test/egress"
CUSTOM_SPOOL_MAX_BYTES = 12345
DOCKERFILE = Path(__file__).resolve().parents[1] / "Dockerfile"


@pytest.fixture(autouse=True)
def _poller_spool_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(tmp_path / "spool"))


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


def test_parse_warsaw_time_uses_first_occurrence_during_fall_transition() -> None:
    assert poller._parse_warsaw_time("2026-10-25 02:30:00") == datetime(2026, 10, 25, 0, 30, tzinfo=UTC)


def test_parse_record_skips_nonexistent_spring_transition_time() -> None:
    record: dict[str, object] = {
        "Lines": "187",
        "Brigade": "01",
        "Lat": 52.2297,
        "Lon": 21.0122,
        "Time": "2026-03-29 02:30:00",
        "VehicleNumber": "1234",
    }

    assert poller._parse_record(record, vehicle_type_id=1) is None


def test_parse_record_skips_invalid_records() -> None:
    row = poller._parse_record({"Lines": "187"}, vehicle_type_id=1)

    assert row is None


def test_flush_buffered_rows_uploads_only_rows_older_than_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    uploaded_rows: list[poller.GpsRow] = []
    buffer_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    older_row = _gps_row(time=datetime(2026, 1, 15, 9, 5, tzinfo=UTC))
    younger_row = _gps_row(time=datetime(2026, 1, 15, 9, 20, tzinfo=UTC))
    buffers = {buffer_hour: [older_row, younger_row]}

    def fake_upload_hour(
        _upload_context: poller.UploadContext,
        _buffer_hour: datetime,
        rows: list[poller.GpsRow],
    ) -> None:
        uploaded_rows.extend(rows)

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    flush_succeeded = poller._flush_buffered_rows(
        _upload_context(),
        buffers,
        flush_before=datetime(2026, 1, 15, 10, 15, tzinfo=poller.WARSAW_TZ),
    )

    assert flush_succeeded is True
    assert uploaded_rows == [older_row]
    assert buffers == {buffer_hour: [younger_row]}


def test_flush_buffered_rows_keeps_buffer_after_upload_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    buffer_hour = datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ)
    buffers = {buffer_hour: [_gps_row()]}

    def fake_upload_hour(
        _upload_context: poller.UploadContext,
        _buffer_hour: datetime,
        _rows: list[poller.GpsRow],
    ) -> None:
        raise GoogleAPIError("transient failure")

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    flush_succeeded = poller._flush_buffered_rows(
        _upload_context(),
        buffers,
        flush_before=datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ),
    )

    assert flush_succeeded is False
    assert buffers == {buffer_hour: [_gps_row()]}


def test_flush_buffered_rows_uploads_everything_on_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
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

    flush_succeeded = poller._flush_buffered_rows(_upload_context(), buffers, flush_all=True)

    assert flush_succeeded is True
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
    assert bucket.path.endswith(".parquet")
    assert bucket.blob_obj.if_generation_match == 0
    assert bucket.blob_obj.content_type == "application/octet-stream"

    table = pq.read_table(io.BytesIO(bucket.blob_obj.data))
    assert table.schema.names == poller.SCHEMA.names
    assert table.num_rows == 1


def test_upload_hour_treats_existing_deterministic_part_as_success() -> None:
    bucket = FakeBucket(raise_precondition_failed=True)

    poller._upload_hour(
        _upload_context(cast("storage.Bucket", bucket)),
        datetime(2026, 1, 15, 10, tzinfo=poller.WARSAW_TZ),
        [_gps_row()],
    )

    assert bucket.blob_obj.if_generation_match == 0


def test_rows_digest_is_stable_for_same_rows_in_different_order() -> None:
    first_row = _gps_row(time=datetime(2026, 1, 15, 10, 0, tzinfo=UTC))
    second_row = _gps_row(time=datetime(2026, 1, 15, 10, 1, tzinfo=UTC))

    assert poller._rows_digest([first_row, second_row]) == poller._rows_digest([second_row, first_row])


def test_rows_digest_includes_coordinates() -> None:
    first_row = _gps_row()
    second_row = _gps_row()
    second_row["Lat"] = first_row["Lat"] + 0.1

    assert poller._rows_digest([first_row]) != poller._rows_digest([second_row])


def test_load_config_uses_cli_smoke_mode_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.run_once is True
    assert config.no_upload is True
    assert config.vehicle_types == poller.VEHICLE_TYPES
    assert config.partial_flush_interval_seconds == EXPECTED_DEFAULT_PARTIAL_FLUSH_SECONDS
    assert config.flush_lag_seconds == config.max_ping_age_seconds


def test_load_config_uses_partial_flush_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("PARTIAL_FLUSH_INTERVAL_SECONDS", str(CUSTOM_PARTIAL_FLUSH_SECONDS))
    monkeypatch.setenv("FLUSH_LAG_SECONDS", str(CUSTOM_FLUSH_LAG_SECONDS))

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.partial_flush_interval_seconds == CUSTOM_PARTIAL_FLUSH_SECONDS
    assert config.flush_lag_seconds == CUSTOM_FLUSH_LAG_SECONDS


def test_load_config_uses_spool_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(tmp_path))
    monkeypatch.setenv("POLLER_SPOOL_MAX_BYTES", str(CUSTOM_SPOOL_MAX_BYTES))

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.spool_dir == tmp_path
    assert config.spool_max_bytes == CUSTOM_SPOOL_MAX_BYTES


def test_load_config_uses_heartbeat_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_HEARTBEAT_GCS_PATH", "/private/heartbeat.json")
    monkeypatch.setenv("POLLER_HEARTBEAT_INTERVAL_SECONDS", str(CUSTOM_HEARTBEAT_SECONDS))

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.heartbeat_gcs_path == "private/heartbeat.json"
    assert config.heartbeat_interval_seconds == CUSTOM_HEARTBEAT_SECONDS


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


def test_load_config_uses_polish_egress_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("ZTM_API_PROXY", "socks5h://127.0.0.1:1055")
    monkeypatch.setenv("POLLER_REQUIRE_POLISH_EGRESS", "true")
    monkeypatch.setenv("POLLER_EGRESS_CHECK_URL", EGRESS_CHECK_URL)

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.require_polish_egress is True
    assert config.egress_check_url == EGRESS_CHECK_URL


def test_load_config_rejects_polish_egress_without_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_REQUIRE_POLISH_EGRESS", "true")

    with pytest.raises(RuntimeError, match="ZTM_API_PROXY"):
        poller._load_config(Namespace(once=True, no_upload=True))


def test_load_config_rejects_unsafe_egress_check_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("ZTM_API_PROXY", "socks5h://127.0.0.1:1055")
    monkeypatch.setenv("POLLER_REQUIRE_POLISH_EGRESS", "true")
    monkeypatch.setenv("POLLER_EGRESS_CHECK_URL", "http://user:secret@example.test/egress?token=secret")

    with pytest.raises(RuntimeError, match="HTTPS URL"):
        poller._load_config(Namespace(once=True, no_upload=True))


@pytest.mark.parametrize(
    "egress_check_url",
    [
        "https://user:secret@example.test/egress",
        "https://example.test/egress?token=secret",
        "https://example.test/egress#fragment",
    ],
)
def test_load_config_rejects_secret_bearing_egress_check_url(
    monkeypatch: pytest.MonkeyPatch, egress_check_url: str
) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("ZTM_API_PROXY", "socks5h://127.0.0.1:1055")
    monkeypatch.setenv("POLLER_REQUIRE_POLISH_EGRESS", "true")
    monkeypatch.setenv("POLLER_EGRESS_CHECK_URL", egress_check_url)

    with pytest.raises(RuntimeError, match="credentials, query, or fragment"):
        poller._load_config(Namespace(once=True, no_upload=True))


def test_load_config_ignores_unsafe_egress_check_url_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_EGRESS_CHECK_URL", "http://user:secret@example.test/egress?token=secret")

    config = poller._load_config(Namespace(once=True, no_upload=True))

    assert config.require_polish_egress is False


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


def test_assert_polish_egress_uses_configured_proxy() -> None:
    proxy = "socks5h://127.0.0.1:1055"
    session = FakeSession()
    FakeSession.expected_egress_proxies = {"http": proxy, "https": proxy}

    try:
        poller._assert_polish_egress(
            cast("requests.Session", session),
            _config(api_proxy=proxy, require_polish_egress=True, egress_check_url=EGRESS_CHECK_URL),
        )
        assert session.requested_egress_url == EGRESS_CHECK_URL
    finally:
        FakeSession.expected_egress_proxies = None


def test_assert_polish_egress_rejects_non_polish_country() -> None:
    proxy = "socks5h://127.0.0.1:1055"
    session = FakeSession()
    session.egress_country = "DE"
    FakeSession.expected_egress_proxies = {"http": proxy, "https": proxy}

    try:
        with pytest.raises(RuntimeError, match="expected PL"):
            poller._assert_polish_egress(
                cast("requests.Session", session),
                _config(api_proxy=proxy, require_polish_egress=True),
            )
    finally:
        FakeSession.expected_egress_proxies = None


@pytest.mark.parametrize("payload", [{"country": 123}, ["PL"], "PL"])
def test_assert_polish_egress_rejects_malformed_payload(payload: object) -> None:
    class MalformedEgressResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return payload

    class MalformedEgressSession:
        def get(self, *_args: object, **_kwargs: object) -> MalformedEgressResponse:
            return MalformedEgressResponse()

    with pytest.raises(RuntimeError, match="invalid payload"):
        poller._assert_polish_egress(
            cast("requests.Session", MalformedEgressSession()),
            _config(api_proxy="socks5h://127.0.0.1:1055", require_polish_egress=True),
        )


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


def test_main_checks_polish_egress_before_gcs_initialization(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy = "socks5h://127.0.0.1:1055"
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("ZTM_API_PROXY", proxy)
    monkeypatch.setenv("POLLER_REQUIRE_POLISH_EGRESS", "true")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller.requests, "Session", FakeSession)
    monkeypatch.setattr(poller.storage, "Client", _fail_if_called)
    FakeSession.egress_country = "DE"
    FakeSession.expected_egress_proxies = {"http": proxy, "https": proxy}

    try:
        with pytest.raises(RuntimeError, match="expected PL"):
            poller.main()
    finally:
        FakeSession.egress_country = "PL"
        FakeSession.expected_egress_proxies = None


def test_main_once_no_upload_skips_polish_egress_check_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once", "--no-upload"])
    FakeSession.seen_vehicle_type_ids = []
    FakeSession.egress_country = "DE"
    monkeypatch.setattr(poller.requests, "Session", FakeSession)
    monkeypatch.setattr(poller.storage, "Client", _fail_if_called)

    try:
        assert poller.main() == 0
        assert FakeSession.seen_vehicle_type_ids == [1, 2]
    finally:
        FakeSession.seen_vehicle_type_ids = []
        FakeSession.egress_country = "PL"


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
    heartbeat_path = "health/poller/latest.json"
    assert pq.read_table(io.BytesIO(bucket.blobs[bus_path].data)).to_pylist()[0]["vehicle_type"] == EXPECTED_BUS_TYPE
    assert pq.read_table(io.BytesIO(bucket.blobs[tram_path].data)).to_pylist()[0]["vehicle_type"] == EXPECTED_TRAM_TYPE
    assert bucket.blobs[heartbeat_path].content_type == "application/json"


def test_main_uploads_seeded_spool_and_removes_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = FakeBucket()
    spool_dir = tmp_path / "seeded-spool"
    config = _config(spool_dir=spool_dir)
    buffers = poller._empty_buffers(config)
    buffers["bus"][datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)].append(
        _gps_row(time=datetime(2026, 1, 15, 10, 5, tzinfo=UTC))
    )
    poller._save_spool(config, buffers)

    class FakeClient:
        def bucket(self, name: str) -> FakeBucket:
            assert name == "ztm-analytics-bucket"
            return bucket

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(spool_dir))
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: [])
    monkeypatch.setattr(poller.storage, "Client", FakeClient)

    assert poller.main() == 0

    bus_path = next(path for path in bucket.blobs if "/vehicle_type=bus/" in path)
    assert pq.read_table(io.BytesIO(bucket.blobs[bus_path].data)).num_rows == 1
    assert not (spool_dir / poller.SPOOL_FILE_NAME).exists()


def test_main_flushes_after_spool_cap_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = FakeBucket()

    def fake_poll_api(
        _session: object, _config: poller.Config, vehicle_type: poller.VehicleType
    ) -> list[poller.GpsRow]:
        return [_gps_row(time=datetime.now(UTC), vehicle_type=vehicle_type.id)]

    class FakeClient:
        def bucket(self, name: str) -> FakeBucket:
            assert name == "ztm-analytics-bucket"
            return bucket

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(tmp_path / "tiny-spool"))
    monkeypatch.setenv("POLLER_SPOOL_MAX_BYTES", "1")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller, "_poll_api", fake_poll_api)
    monkeypatch.setattr(poller.storage, "Client", FakeClient)

    with pytest.raises(RuntimeError, match="POLLER_SPOOL_MAX_BYTES"):
        poller.main()

    assert any("/vehicle_type=bus/" in path for path in bucket.blobs)
    assert not any("/vehicle_type=tram/" in path for path in bucket.blobs)


def test_heartbeat_payload_reports_degraded_and_down_states() -> None:
    updated_at = datetime(2026, 1, 15, 12, tzinfo=UTC)
    config = _config()
    states = {
        "bus": poller.PollState("bus"),
        "tram": poller.PollState("tram"),
    }

    poller._update_poll_state(
        states["bus"],
        poller.PollResult("bus", updated_at, succeeded=True, accepted_rows=HEARTBEAT_ACCEPTED_ROWS, dropped_stale=1),
    )
    poller._update_poll_state(
        states["tram"], poller.PollResult("tram", updated_at, succeeded=False, error_type="request_error")
    )

    payload = poller._heartbeat_payload(config, states, updated_at)

    assert payload["status"] == "degraded"
    assert payload["updated_at"] == "2026-01-15T12:00:00Z"
    assert payload["vehicle_types"]["bus"]["last_accepted_rows"] == HEARTBEAT_ACCEPTED_ROWS
    assert payload["vehicle_types"]["bus"]["last_dropped_stale_rows"] == 1
    assert payload["vehicle_types"]["tram"]["consecutive_failures"] == 1
    assert payload["vehicle_types"]["tram"]["last_error_type"] == "request_error"

    poller._update_poll_state(
        states["bus"], poller.PollResult("bus", updated_at, succeeded=False, error_type="request_error")
    )

    assert poller._heartbeat_payload(config, states, updated_at)["status"] == "down"

    poller._update_poll_state(
        states["bus"], poller.PollResult("bus", updated_at + timedelta(seconds=10), succeeded=True)
    )
    poller._update_poll_state(
        states["tram"], poller.PollResult("tram", updated_at + timedelta(seconds=10), succeeded=True)
    )

    recovered_payload = poller._heartbeat_payload(config, states, updated_at + timedelta(seconds=10))
    assert recovered_payload["status"] == "ok"
    assert recovered_payload["vehicle_types"]["bus"]["consecutive_failures"] == 0
    assert recovered_payload["vehicle_types"]["bus"]["last_error_type"] is None


def test_write_heartbeat_uploads_private_gcs_json() -> None:
    bucket = FakeBucket()
    config = _config()
    states = {"bus": poller.PollState("bus"), "tram": poller.PollState("tram")}
    now = datetime(2026, 1, 15, 12, tzinfo=UTC)
    for state in states.values():
        poller._update_poll_state(state, poller.PollResult(state.vehicle_type_name, now, succeeded=True))

    assert poller._write_heartbeat(cast("storage.Bucket", bucket), config, states) is True

    assert bucket.path == "health/poller/latest.json"
    assert bucket.blob_obj.content_type == "application/json"
    assert json.loads(bucket.blob_obj.data)["status"] == "ok"


def test_write_heartbeat_returns_false_when_upload_fails() -> None:
    class FailingBlob:
        def upload_from_string(self, _data: bytes, content_type: str) -> None:
            assert content_type == "application/json"
            raise GoogleAPIError("transient failure")

    class FailingBucket:
        def blob(self, path: str) -> FailingBlob:
            assert path == "health/poller/latest.json"
            return FailingBlob()

    states = {"bus": poller.PollState("bus"), "tram": poller.PollState("tram")}

    assert poller._write_heartbeat(cast("storage.Bucket", FailingBucket()), _config(), states) is False


def test_main_returns_failure_when_shutdown_flush_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def bucket(self, name: str) -> FakeBucket:
            assert name == "ztm-analytics-bucket"
            return FakeBucket()

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: [])
    monkeypatch.setattr(poller.storage, "Client", FakeClient)
    monkeypatch.setattr(poller, "_flush_vehicle_buffers", lambda *_args, **kwargs: not kwargs.get("flush_all"))

    assert poller.main() == 1


def test_main_retries_partial_flush_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def bucket(self, name: str) -> FakeBucket:
            assert name == "ztm-analytics-bucket"
            return FakeBucket()

    stop_checks = iter([False, False, True])
    flush_results = iter([False, True])
    flush_calls = []

    def fake_flush_vehicle_buffers(*_args: object, **kwargs: object) -> bool:
        flush_calls.append(kwargs)
        if kwargs.get("flush_all"):
            return True
        return next(flush_results)

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setattr("sys.argv", ["poller.py"])
    monkeypatch.setattr(poller, "_build_signal_handler", lambda: lambda: next(stop_checks))
    monkeypatch.setattr(poller.time, "monotonic", iter([1000.0, 1001.0]).__next__)
    monkeypatch.setattr(poller, "_sleep_remaining", lambda *_args: None)
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: [])
    monkeypatch.setattr(poller.storage, "Client", FakeClient)
    monkeypatch.setattr(poller, "_flush_vehicle_buffers", fake_flush_vehicle_buffers)

    assert poller.main() == 0
    partial_flush_calls = [call for call in flush_calls if not call.get("flush_all")]
    assert len(partial_flush_calls) == EXPECTED_RETRY_FLUSH_CALLS


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


def test_spool_round_trips_buffered_rows(tmp_path: Path) -> None:
    config = _config(spool_dir=tmp_path)
    buffer_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    row = _gps_row(time=datetime(2026, 1, 15, 10, 5, tzinfo=UTC))
    buffers = poller._empty_buffers(config)
    buffers["bus"][buffer_hour].append(row)

    poller._save_spool(config, buffers)

    restored = poller._load_spool(config)
    assert restored["bus"] == {buffer_hour: [row]}
    assert restored["tram"] == {}


def test_load_spool_rejects_malformed_json(tmp_path: Path) -> None:
    (tmp_path / poller.SPOOL_FILE_NAME).write_text("{broken", encoding="utf-8")

    with pytest.raises(RuntimeError, match="spool is unreadable"):
        poller._load_spool(_config(spool_dir=tmp_path))


@pytest.mark.parametrize("hour", ["2026-01-15T11:00:00", "2026-01-15T11:30:00+01:00"])
def test_load_spool_rejects_invalid_buffer_hour(tmp_path: Path, hour: str) -> None:
    payload = {"version": 1, "vehicle_types": {"bus": {hour: []}}}
    (tmp_path / poller.SPOOL_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="spool has invalid contents"):
        poller._load_spool(_config(spool_dir=tmp_path))


def test_load_spool_rejects_naive_gps_timestamp(tmp_path: Path) -> None:
    row = poller._gps_row_to_spool(_gps_row())
    row["Time"] = "2026-01-15T10:05:00"
    payload = {"version": 1, "vehicle_types": {"bus": {"2026-01-15T11:00:00+01:00": [row]}}}
    (tmp_path / poller.SPOOL_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="spool has invalid contents"):
        poller._load_spool(_config(spool_dir=tmp_path))


@pytest.mark.parametrize(
    "payload",
    [
        {"version": 1},
        {"version": 1, "vehicle_types": {"bus": []}},
    ],
)
def test_load_spool_rejects_invalid_vehicle_sections(tmp_path: Path, payload: dict[str, object]) -> None:
    (tmp_path / poller.SPOOL_FILE_NAME).write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="spool has invalid contents"):
        poller._load_spool(_config(spool_dir=tmp_path))


def test_save_spool_removes_file_when_buffers_empty(tmp_path: Path) -> None:
    config = _config(spool_dir=tmp_path)
    buffers = poller._empty_buffers(config)
    buffers["bus"][datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)].append(_gps_row())
    poller._save_spool(config, buffers)

    buffers["bus"].clear()
    poller._save_spool(config, buffers)

    assert not (tmp_path / poller.SPOOL_FILE_NAME).exists()


def test_save_spool_rejects_snapshots_over_cap(tmp_path: Path) -> None:
    config = _config(spool_dir=tmp_path, spool_max_bytes=1)
    buffers = poller._empty_buffers(config)
    buffers["bus"][datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)].append(_gps_row())

    with pytest.raises(RuntimeError, match="POLLER_SPOOL_MAX_BYTES"):
        poller._save_spool(config, buffers)


def test_flush_shutdown_keeps_spool_after_upload_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _config(spool_dir=tmp_path)
    buffers = poller._empty_buffers(config)
    buffer_hour = datetime(2026, 1, 15, 11, tzinfo=poller.WARSAW_TZ)
    row = _gps_row(time=datetime(2026, 1, 15, 10, 5, tzinfo=UTC))
    buffers["bus"][buffer_hour].append(row)

    def fake_upload_hour(
        _upload_context: poller.UploadContext,
        _buffer_hour: datetime,
        _rows: list[poller.GpsRow],
    ) -> None:
        raise GoogleAPIError("transient failure")

    monkeypatch.setattr(poller, "_upload_hour", fake_upload_hour)

    assert poller._flush_shutdown(cast("storage.Bucket", FakeBucket()), config, buffers) is False

    restored = poller._load_spool(config)
    assert restored["bus"] == {buffer_hour: [row]}


def test_dockerfile_defines_local_worker_healthcheck() -> None:
    dockerfile = DOCKERFILE.read_text()

    assert "COPY healthcheck.sh ./" in dockerfile
    assert "chmod +x entrypoint.sh healthcheck.sh" in dockerfile
    assert (
        'HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD ["./healthcheck.sh"]' in dockerfile
    )


def _config(
    api_proxy: str | None = None,
    *,
    require_polish_egress: bool = False,
    egress_check_url: str = "https://ipinfo.io/json",
    spool_dir: Path | None = None,
    spool_max_bytes: int = poller.DEFAULT_SPOOL_MAX_BYTES,
) -> poller.Config:
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
        partial_flush_interval_seconds=900,
        flush_lag_seconds=300,
        heartbeat_gcs_path="health/poller/latest.json",
        heartbeat_interval_seconds=60,
        require_polish_egress=require_polish_egress,
        egress_check_url=egress_check_url,
        spool_dir=spool_dir or Path("ztm-poller-spool-test"),
        spool_max_bytes=spool_max_bytes,
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
    def __init__(self, *, raise_precondition_failed: bool = False) -> None:
        self.data = b""
        self.content_type = ""
        self.if_generation_match: int | None = None
        self.raise_precondition_failed = raise_precondition_failed

    def upload_from_file(self, file_obj: io.BytesIO, content_type: str, if_generation_match: int | None = None) -> None:
        self.if_generation_match = if_generation_match
        if self.raise_precondition_failed:
            raise PreconditionFailed("already exists")
        self.data = file_obj.read()
        self.content_type = content_type

    def upload_from_string(self, data: bytes, content_type: str) -> None:
        self.data = data
        self.content_type = content_type


class FakeBucket:
    def __init__(self, *, raise_precondition_failed: bool = False) -> None:
        self.raise_precondition_failed = raise_precondition_failed
        self.blob_obj = FakeBlob(raise_precondition_failed=raise_precondition_failed)
        self.path: str | None = None
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, path: str) -> FakeBlob:
        self.path = path
        self.blob_obj = FakeBlob(raise_precondition_failed=self.raise_precondition_failed)
        self.blobs[path] = self.blob_obj
        return self.blob_obj


class FakeResponse:
    def __init__(self, country: str = "PL") -> None:
        self.country = country

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


class FakeEgressResponse:
    def __init__(self, country: str) -> None:
        self.country = country

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, str]:
        return {"country": self.country}


class FakeSession:
    expected_proxies: ClassVar[dict[str, str] | None] = None
    expected_egress_proxies: ClassVar[dict[str, str] | None] = None
    seen_vehicle_type_ids: ClassVar[list[int]] = []
    egress_country: ClassVar[str] = "PL"

    def __init__(self) -> None:
        self.requested_egress_url: str | None = None

    def get(self, url: str, *, timeout: float, proxies: dict[str, str] | None) -> FakeEgressResponse:
        self.requested_egress_url = url
        assert url in {"https://ipinfo.io/json", EGRESS_CHECK_URL}
        assert timeout == EXPECTED_TIMEOUT_SECONDS
        assert proxies == self.expected_egress_proxies
        return FakeEgressResponse(self.egress_country)

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
