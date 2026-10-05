# ruff: noqa: PLR2004 -- Keep synthetic expected counts beside their fixtures.
from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import requests
from google.api_core.exceptions import GoogleAPIError

import poller
from poller_health_counters import ATTEMPT_RESERVE_BYTES, CHECKPOINT_NAME, DEFAULT_PREFIX, HealthCounters, utc_hour
from tests.test_poller import FakeBucket, _config, _gps_row

if TYPE_CHECKING:
    from google.cloud import storage

START = datetime(2026, 10, 25, 0, 37, 12, tzinfo=UTC)
HOUR = "2026-10-25T00:00:00Z"
KEY = "health/poller/hourly/2026-10-25/00.json"
CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "poller_health_v1.json"


class HealthBucket:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, bytes]] = []
        self.fail = False
        self.ambiguous_failure = False

    def blob(self, key: str) -> HealthBlob:
        return HealthBlob(self, key)


class HealthBlob:
    def __init__(self, bucket: HealthBucket, key: str) -> None:
        self.bucket = bucket
        self.key = key

    def upload_from_string(self, data: bytes, content_type: str) -> None:
        assert content_type == "application/json"
        self.bucket.calls.append((self.key, data))
        if self.bucket.ambiguous_failure:
            self.bucket.objects[self.key] = data
        if self.bucket.fail:
            raise GoogleAPIError("offline simulated failure")
        self.bucket.objects[self.key] = data


def result(at: datetime = START, mode: str = "bus", **counts: int) -> poller.PollResult:
    return poller.PollResult(mode, at, succeeded=True, **counts)


def upload(health: HealthCounters, bucket: HealthBucket, now: datetime = START, *, partial: bool = False) -> bool:
    return health.upload(cast("storage.Bucket", bucket), DEFAULT_PREFIX, now, include_partial=partial)


def test_wire_contract_exact_fields_and_minute_statistics(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(
        result(
            parsed_rows=10,
            accepted_rows=4,
            dropped_stale=5,
            dropped_future=1,
            accepted_vehicle_count=3,
            accepted_line_count=2,
        )
    )
    health.record(result(parsed_rows=2, accepted_rows=2, accepted_vehicle_count=1, accepted_line_count=1))
    # Failures contribute attempts only, even if a caller accidentally supplies counts.
    health.record(replace(result(parsed_rows=100, accepted_rows=100), succeeded=False))
    health.record(result(START + timedelta(minutes=2), "tram"))
    bucket = HealthBucket()
    assert upload(health, bucket, partial=True)
    payload = json.loads(bucket.objects[KEY])
    contract = json.loads(CONTRACT.read_text())
    assert set(payload) == set(contract["hour_fields"])
    assert payload["version"] == contract["version"] == 1
    assert payload["hour_start"] == HOUR
    assert payload["collection_started_at"] == "2026-10-25T00:37:12Z"
    assert payload["poll_interval_seconds"] == 10
    assert set(payload["vehicle_types"]) == {"bus", "tram"}
    for mode in payload["vehicle_types"].values():
        assert set(mode) == set(contract["mode_fields"])
        for minute in mode["minutes"]:
            assert set(minute) == set(contract["minute_fields"])
            assert all(type(value) is int and value >= 0 for value in minute.values())
    assert payload["vehicle_types"]["bus"]["minutes"] == [
        {
            "minute": 37,
            "attempts": 3,
            "successes": 2,
            "parsed_rows": 12,
            "accepted_rows": 6,
            "dropped_stale_rows": 5,
            "dropped_future_rows": 1,
            "accepted_vehicle_count_sum": 4,
            "accepted_line_count_sum": 3,
        }
    ]
    assert payload["vehicle_types"]["tram"]["minutes"] == [
        {
            "minute": 39,
            "attempts": 1,
            "successes": 1,
            "parsed_rows": 0,
            "accepted_rows": 0,
            "dropped_stale_rows": 0,
            "dropped_future_rows": 0,
            "accepted_vehicle_count_sum": 0,
            "accepted_line_count_sum": 0,
        }
    ]


def test_zero_polls_no_objects_and_no_invented_history(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    bucket = HealthBucket()
    assert upload(health, bucket, partial=True)
    assert not bucket.calls
    health.save()
    restored = HealthCounters(tmp_path, 10)
    assert restored.collection_started_at is None
    assert not restored.hours


def test_poll_stats_are_unique_fresh_per_poll_not_raw_history(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    rows = [
        _gps_row(time=now),
        _gps_row(time=now),
        _gps_row(time=now) | {"Lines": "517"},
        _gps_row(time=now) | {"VehicleNumber": "5678"},
        _gps_row(time=now - timedelta(days=1)) | {"VehicleNumber": "stale", "Lines": "old"},
        _gps_row(time=now + timedelta(days=1)) | {"VehicleNumber": "future", "Lines": "new"},
    ]
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: rows)
    buffers = poller._empty_buffers(_config())
    first = poller._poll_vehicle_type(requests.Session(), _config(), poller.VEHICLE_TYPES[0], buffers["bus"])
    second = poller._poll_vehicle_type(requests.Session(), _config(), poller.VEHICLE_TYPES[0], buffers["bus"])
    for observed in (first, second):
        assert (observed.parsed_rows, observed.accepted_rows, observed.dropped_stale, observed.dropped_future) == (
            6,
            4,
            1,
            1,
        )
        assert (observed.accepted_vehicle_count, observed.accepted_line_count) == (2, 2)
        assert observed.succeeded
        assert observed.attempted_at >= now


@pytest.mark.parametrize(
    ("parsed", "stale", "future", "status", "reason"),
    [
        (0, 0, 0, "unknown", None),
        (10, 0, 0, "healthy", None),
        (10, 5, 0, "healthy", None),
        (10, 6, 0, "degraded", "stale_heavy"),
        (10, 0, 6, "degraded", "future_heavy"),
        (10, 3, 3, "degraded", "stale_and_future_heavy"),
        (10, 10, 0, "degraded", "stale_heavy"),
    ],
)
def test_feed_health_ratios_do_not_redefine_request_success(
    parsed: int, stale: int, future: int, status: str, reason: str | None
) -> None:
    state = poller.PollState("bus")
    poller._update_poll_state(
        state,
        result(parsed_rows=parsed, accepted_rows=parsed - stale - future, dropped_stale=stale, dropped_future=future),
    )
    payload = poller._heartbeat_payload(_config(), {"bus": state}, START)
    assert payload["status"] == "ok"
    assert state.last_success_at == START
    assert state.consecutive_failures == 0
    assert (state.feed_status, state.feed_reason) == (status, reason)
    assert state.last_parsed_rows == parsed
    poller._update_poll_state(state, poller.PollResult("bus", START + timedelta(seconds=10), succeeded=False))
    assert state.last_success_at == START
    assert (state.feed_status, state.feed_reason) == ("unknown", None)


def test_attempts_checkpointed_even_when_no_gps_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = _config(spool_dir=tmp_path)
    health = HealthCounters(tmp_path, 10)
    states = {name: poller.PollState(name) for name in ("bus", "tram")}

    def api(_session: object, _config: poller.Config, mode: poller.VehicleType) -> list[poller.GpsRow]:
        if mode.name == "tram":
            # The first attempt was persisted before making the next request.
            previous = HealthCounters(tmp_path, 10)
            assert (
                sum(
                    item["attempts"]
                    for snapshot in previous.hours.values()
                    for item in snapshot["vehicle_types"]["bus"]["minutes"]
                )
                == 1
            )
            raise requests.ConnectionError("offline simulated failure")
        return [_gps_row(time=datetime.now(UTC) - timedelta(days=1))]

    monkeypatch.setattr(poller, "_poll_api", api)
    buffers = poller._empty_buffers(config)
    assert poller._poll_vehicle_types(requests.Session(), config, buffers, states, health) == 0
    assert not (tmp_path / "buffers.json").exists()
    restored = HealthCounters(tmp_path, 10)
    bus = next(iter(restored.hours.values()))["vehicle_types"]["bus"]["minutes"][0]
    tram = next(iter(restored.hours.values()))["vehicle_types"]["tram"]["minutes"][0]
    assert (bus["attempts"], bus["successes"], bus["parsed_rows"], bus["dropped_stale_rows"]) == (1, 1, 1, 1)
    assert (tram["attempts"], tram["successes"], tram["parsed_rows"]) == (1, 0, 0)


def test_partial_shutdown_restart_same_hour_cumulative_idempotent(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result(parsed_rows=1, accepted_rows=1, accepted_vehicle_count=1, accepted_line_count=1))
    bucket = HealthBucket()
    assert upload(health, bucket, partial=True)
    first = bucket.objects[KEY]
    assert upload(health, bucket, partial=True)
    assert bucket.objects[KEY] == first
    # A partial upload must not delete state needed to extend that hour on restart.
    restarted = HealthCounters(tmp_path, 10)
    restarted.record(
        result(
            START + timedelta(seconds=10),
            parsed_rows=1,
            accepted_rows=1,
            accepted_vehicle_count=1,
            accepted_line_count=1,
        )
    )
    assert restarted.collection_started_at == "2026-10-25T00:37:12Z"
    assert upload(restarted, bucket, partial=True)
    minute = json.loads(bucket.objects[KEY])["vehicle_types"]["bus"]["minutes"][0]
    assert (minute["attempts"], minute["parsed_rows"], minute["accepted_vehicle_count_sum"]) == (2, 2, 2)
    assert upload(restarted, bucket, START + timedelta(hours=1))
    assert not restarted.hours
    again = HealthCounters(tmp_path, 10)
    assert again.collection_started_at == "2026-10-25T00:37:12Z"
    assert upload(again, bucket, START + timedelta(hours=1))
    assert len(bucket.calls) == 4


def test_failed_late_upload_restart_rollover_and_ambiguous_retry(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    bucket = HealthBucket()
    bucket.fail = bucket.ambiguous_failure = True
    closed_at = START + timedelta(hours=1)
    assert not upload(health, bucket, closed_at)
    prior = bucket.objects[KEY]
    restarted = HealthCounters(tmp_path, 10)
    restarted.record(result(closed_at, "tram"))
    assert not upload(restarted, bucket, closed_at)
    assert bucket.objects[KEY] == prior
    assert len(restarted.hours) == 2
    bucket.fail = False
    assert upload(restarted, bucket, closed_at)
    assert bucket.objects[KEY] == prior
    assert list(restarted.hours) == ["2026-10-25T01:00:00Z"]
    assert upload(restarted, bucket, closed_at, partial=True)
    payload = json.loads(bucket.objects["health/poller/hourly/2026-10-25/01.json"])
    assert payload["collection_started_at"] == "2026-10-25T00:37:12Z"
    assert payload["vehicle_types"]["bus"]["minutes"] == []
    assert payload["vehicle_types"]["tram"]["minutes"][0]["minute"] == 37


@pytest.mark.parametrize("date", [(2026, 10, 25), (2026, 3, 29)])
def test_dst_keys_use_utc_not_repeated_or_skipped_local_hours(tmp_path: Path, date: tuple[int, int, int]) -> None:
    health = HealthCounters(tmp_path, 10)
    bucket = HealthBucket()
    first = datetime(*date, 0, 30, tzinfo=UTC)
    second = first + timedelta(hours=1)
    for at in (first, second):
        health.record(result(at.astimezone(poller.WARSAW_TZ)))
    assert upload(health, bucket, second + timedelta(hours=1))
    expected = {f"{DEFAULT_PREFIX}/{first:%Y-%m-%d}/{hour:02d}.json" for hour in (0, 1)}
    assert set(bucket.objects) == expected
    assert not health.hours


def test_only_one_finalized_object_per_hour_normal_operation(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    bucket = HealthBucket()
    for offset in range(60):
        at = START.replace(minute=offset)
        health.record(result(at))
        health.record(result(at, "tram"))
        assert upload(health, bucket, at)
    assert not bucket.calls
    assert upload(health, bucket, START + timedelta(hours=1))
    assert len(bucket.calls) == 1
    payload = json.loads(bucket.objects[KEY])
    assert len(payload["vehicle_types"]["bus"]["minutes"]) == 60
    assert len(payload["vehicle_types"]["tram"]["minutes"]) == 60


def test_cap_stops_before_api_without_evicting_pending_hours(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10, max_bytes=1800)
    health.record(result())
    health.record(result(START + timedelta(hours=1)))
    saved = health.path.read_bytes()
    assert len(saved) <= health.max_bytes
    assert len(saved) + ATTEMPT_RESERVE_BYTES > health.max_bytes
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: pytest.fail("must fail before API call"))
    config = _config(spool_dir=tmp_path)
    states = {name: poller.PollState(name) for name in ("bus", "tram")}
    with pytest.raises(RuntimeError, match="POLLER_HEALTH_MAX_BYTES"):
        poller._poll_vehicle_types(requests.Session(), config, poller._empty_buffers(config), states, health)
    assert health.path.read_bytes() == saved
    assert HealthCounters(tmp_path, 10, max_bytes=1800).hours == health.hours


def test_checkpoint_cap_failure_preserves_previous_snapshot_and_reports(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    previous = health.path.read_bytes()
    health.max_bytes = len(previous)
    with pytest.raises(RuntimeError, match="POLLER_HEALTH_MAX_BYTES"):
        health.record(result(START + timedelta(minutes=1)))
    assert health.path.read_bytes() == previous
    assert "cannot checkpoint health attempt" in caplog.text
    assert len(health.hours[HOUR]["vehicle_types"]["bus"]["minutes"]) == 2
    assert not health.path.with_suffix(".tmp").exists()


def test_atomic_replace_failure_preserves_checkpoint_and_can_retry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    prior = health.path.read_bytes()
    original = Path.replace

    def fail_replace(source: Path, target: Path) -> Path:
        if target == health.path:
            raise OSError("offline simulated disk error")
        return original(source, target)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail_replace)
        with pytest.raises(OSError, match="disk error"):
            health.record(result())
    assert health.path.read_bytes() == prior
    assert not health.path.with_suffix(".tmp").exists()
    health.save()
    restored = HealthCounters(tmp_path, 10)
    assert restored.hours[HOUR]["vehicle_types"]["bus"]["minutes"][0]["attempts"] == 2


def test_upload_success_checkpoint_failure_retries_identical_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    bucket = HealthBucket()
    prior = health.path.read_bytes()
    with monkeypatch.context() as patch:
        patch.setattr(health, "save", lambda: (_ for _ in ()).throw(OSError("disk error")))
        with pytest.raises(OSError, match="disk error"):
            upload(health, bucket, START + timedelta(hours=1))
    assert HOUR in health.hours
    assert health.path.read_bytes() == prior
    assert upload(HealthCounters(tmp_path, 10), bucket, START + timedelta(hours=1))
    assert bucket.calls[0] == bucket.calls[1]


@pytest.mark.parametrize(
    "invalid", ["broken", '{"version":2}', '{"version":1,"collection_started_at":null,"hours":[]}']
)
def test_corrupt_checkpoint_fails_without_discarding(tmp_path: Path, invalid: str) -> None:
    path = tmp_path / CHECKPOINT_NAME
    path.write_text(invalid)
    with pytest.raises(RuntimeError, match="refusing to discard"):
        HealthCounters(tmp_path, 10)
    assert path.read_text() == invalid


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("minute", 60),
        ("attempts", -1),
        ("attempts", True),
        ("successes", 2),
        ("parsed_rows", 1),
        ("accepted_vehicle_count_sum", 1),
    ],
)
def test_invalid_restored_counters_fail_closed(tmp_path: Path, field: str, value: int) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    payload = json.loads(health.path.read_bytes())
    payload["hours"][HOUR]["vehicle_types"]["bus"]["minutes"][0][field] = value
    health.path.write_text(json.dumps(payload))
    with pytest.raises(RuntimeError, match="refusing to discard"):
        HealthCounters(tmp_path, 10)


def test_oversized_checkpoint_rejected_before_loading(tmp_path: Path) -> None:
    path = tmp_path / CHECKPOINT_NAME
    path.write_bytes(b" " * 1025)
    with pytest.raises(RuntimeError, match="POLLER_HEALTH_MAX_BYTES"):
        HealthCounters(tmp_path, 10, max_bytes=1024)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_nonfinite_poll_interval_cannot_enter_wire_contract(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", value)
    with pytest.raises(RuntimeError, match="POLL_INTERVAL_SECONDS"):
        poller._load_config(poller.Namespace(once=False, no_upload=False))


def test_naive_attempt_time_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        utc_hour(START.replace(tzinfo=None))


def test_health_knobs_and_docker_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_HEALTH_GCS_PREFIX", "/private/diagnostics/")
    monkeypatch.setenv("POLLER_HEALTH_MAX_BYTES", "12345")
    config = poller._load_config(poller.Namespace(once=False, no_upload=False))
    assert config.health_gcs_prefix == "private/diagnostics"
    assert config.health_max_bytes == 12345
    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
    assert "COPY poller.py poller_health_counters.py ./" in dockerfile


@pytest.mark.parametrize(("name", "value"), [("POLLER_HEALTH_GCS_PREFIX", "/"), ("POLLER_HEALTH_MAX_BYTES", "0")])
def test_invalid_health_knobs(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv(name, value)
    with pytest.raises(RuntimeError, match=name):
        poller._load_config(poller.Namespace(once=False, no_upload=False))


def test_main_startup_drains_closed_hours_before_polling_and_shutdown_partial(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seed = HealthCounters(tmp_path, 10)
    seed.record(result())
    bucket = FakeBucket()

    class Client:
        def bucket(self, _name: str) -> FakeBucket:
            return bucket

    def api(*_args: object) -> list[poller.GpsRow]:
        assert KEY in bucket.blobs  # Startup retry happens before the next request.
        return []

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(tmp_path))
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller, "_poll_api", api)
    monkeypatch.setattr(poller.storage, "Client", Client)

    # Frozen wall clock beyond the seeded hour, shared by main and attempt timestamps.
    class Clock(datetime):
        @classmethod
        def now(cls, _tz: object = None) -> datetime:
            return START + timedelta(hours=2)

    monkeypatch.setattr(poller, "datetime", Clock)
    assert poller.main() == 0
    assert "health/poller/hourly/2026-10-25/02.json" in bucket.blobs
    heartbeat = json.loads(bucket.blobs["health/poller/latest.json"].data)
    assert heartbeat["collection_started_at"] == "2026-10-25T00:37:12Z"
    restored = HealthCounters(tmp_path, 10)
    assert list(restored.hours) == ["2026-10-25T02:00:00Z"]
    assert restored.collection_started_at == "2026-10-25T00:37:12Z"


def test_heartbeat_root_marker_survives_restart_after_closed_hour_retirement(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    assert upload(health, HealthBucket(), START + timedelta(hours=1))
    assert not health.hours
    restarted = HealthCounters(tmp_path, 10)
    restarted.record(result(START + timedelta(hours=2)))
    bucket = FakeBucket()
    states = {"bus": poller.PollState("bus")}
    assert poller._write_heartbeat(cast("storage.Bucket", bucket), _config(), states, restarted.collection_started_at)
    payload = json.loads(bucket.blob_obj.data)
    assert payload["collection_started_at"] == "2026-10-25T00:37:12Z"
    assert payload["collection_started_at"] == health.collection_started_at
    assert "collection_started_at" not in payload["vehicle_types"]["bus"]


def test_legacy_heartbeat_callers_work_without_root_marker() -> None:
    config = _config()
    states = {"bus": poller.PollState("bus")}
    poller._update_poll_state(states["bus"], result())
    legacy = poller._heartbeat_payload(config, states, START)
    assert "collection_started_at" not in legacy
    assert legacy["status"] == "ok"
    assert poller._heartbeat_payload(config, states, START, None) == legacy
    bucket = FakeBucket()
    assert poller._write_heartbeat(cast("storage.Bucket", bucket), config, states)
    uploaded = json.loads(bucket.blob_obj.data)
    assert "collection_started_at" not in uploaded
    assert uploaded["status"] == legacy["status"]


def test_failed_uploads_retry_on_ticks_without_stopping_polls(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10)
    health.record(result())
    bucket = HealthBucket()
    bucket.fail = True
    config = _config(spool_dir=tmp_path)
    checks = iter([False, False, False, True])
    runtime = poller.RuntimeState(
        session=requests.Session(),
        bucket=cast("storage.Bucket", bucket),
        config=config,
        buffers=poller._empty_buffers(config),
        poll_states={name: poller.PollState(name) for name in ("bus", "tram")},
        stop_requested=lambda: next(checks),
        last_partial_flush=1000,
        last_heartbeat=-60,
        health=health,
    )
    heartbeat_calls: list[int] = []

    def heartbeat(*_args: object) -> bool:
        heartbeat_calls.append(1)
        return False

    class Clock(datetime):
        @classmethod
        def now(cls, _tz: object = None) -> datetime:
            return START + timedelta(hours=2)

    monkeypatch.setattr(poller, "datetime", Clock)
    monkeypatch.setattr(poller.time, "monotonic", iter([1000.0, 1030.0, 1060.0]).__next__)
    monkeypatch.setattr(poller, "_sleep_remaining", lambda *_args: None)
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: [])
    monkeypatch.setattr(poller, "_write_heartbeat", heartbeat)
    poller._run_poll_loop(runtime)
    assert len(bucket.calls) == len(heartbeat_calls) == 2
    assert bucket.calls[0] == bucket.calls[1]
    restored = HealthCounters(tmp_path, 10)
    assert HOUR in restored.hours  # Failed closed-hour uploads were not lost.
    current = restored.hours["2026-10-25T02:00:00Z"]
    for mode in current["vehicle_types"].values():
        assert mode["minutes"][0]["attempts"] == 3
    assert runtime.last_heartbeat == 1060


def test_failed_shutdown_health_upload_returns_nonzero_then_restart_extends(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bucket = HealthBucket()
    bucket.fail = True

    class Client:
        def bucket(self, _name: str) -> HealthBucket:
            return bucket

    class Clock(datetime):
        @classmethod
        def now(cls, _tz: object = None) -> datetime:
            return START

    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(tmp_path))
    monkeypatch.setattr("sys.argv", ["poller.py", "--once"])
    monkeypatch.setattr(poller, "datetime", Clock)
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: [])
    monkeypatch.setattr(poller.storage, "Client", Client)
    assert poller.main() == 1
    assert HOUR in HealthCounters(tmp_path, 10).hours
    bucket.fail = False
    assert poller.main() == 0
    for mode in json.loads(bucket.objects[KEY])["vehicle_types"].values():
        assert mode["minutes"][0]["attempts"] == 2
        assert mode["minutes"][0]["successes"] == 2


@pytest.mark.parametrize(
    "error", [requests.ConnectionError("request"), ValueError("payload"), json.JSONDecodeError("json", "", 0)]
)
def test_all_failed_poll_paths_have_attempt_time_and_zero_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error: Exception
) -> None:
    def api(*_args: object) -> list[poller.GpsRow]:
        raise error

    monkeypatch.setattr(poller, "_poll_api", api)
    config = _config()
    observed = poller._poll_vehicle_type(
        requests.Session(), config, poller.VEHICLE_TYPES[0], poller._empty_buffers(config)["bus"]
    )
    assert not observed.succeeded
    assert observed.attempted_at.utcoffset() == timedelta(0)
    assert (
        observed.parsed_rows,
        observed.accepted_rows,
        observed.accepted_vehicle_count,
        observed.accepted_line_count,
    ) == (0, 0, 0, 0)
    health = HealthCounters(tmp_path, 10)
    health.record(observed)
    minute = next(iter(health.hours.values()))["vehicle_types"]["bus"]["minutes"][0]
    assert (minute["attempts"], minute["successes"], minute["parsed_rows"]) == (1, 0, 0)


def test_rollover_outage_memory_and_disk_bounded_without_discarding(tmp_path: Path) -> None:
    health = HealthCounters(tmp_path, 10, max_bytes=4096)
    bucket = HealthBucket()
    bucket.fail = True
    attempts = 0
    for offset in range(100):
        at = START + timedelta(hours=offset)
        try:
            health.check_capacity()
        except RuntimeError:
            break
        health.record(result(at))
        attempts += 1
        assert not upload(health, bucket, at, partial=True)
    else:
        pytest.fail("pending state grew without hitting the cap")
    assert 1 < attempts < 100
    assert health.path.stat().st_size <= health.max_bytes
    restored = HealthCounters(tmp_path, 10, max_bytes=4096)
    assert len(restored.hours) == attempts
    assert all(len(snapshot["vehicle_types"]["bus"]["minutes"]) == 1 for snapshot in restored.hours.values())
    bucket.fail = False
    assert upload(restored, bucket, START + timedelta(hours=100))
    assert len(bucket.objects) == attempts
    assert not restored.hours


def test_no_upload_does_not_touch_health_checkpoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ZTM_API_TOKEN", "token")
    monkeypatch.setenv("POLLER_SPOOL_DIR", str(tmp_path))
    monkeypatch.setattr("sys.argv", ["poller.py", "--once", "--no-upload"])
    monkeypatch.setattr(poller, "_poll_api", lambda *_args: [])
    assert poller.main() == 0
    assert not (tmp_path / CHECKPOINT_NAME).exists()
