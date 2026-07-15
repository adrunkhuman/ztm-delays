# ruff: noqa: S603

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

POLLER_DIR = Path(__file__).resolve().parents[1]
ENTRYPOINT = POLLER_DIR / "entrypoint.sh"
HEALTHCHECK = POLLER_DIR / "healthcheck.sh"
EARLY_POLLER_EXIT_STATUS = 7

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX process and socket contracts")


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _fake_commands(tmp_path: Path, *, create_socket: bool = True) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    call_log = tmp_path / "calls.log"
    tailscaled_body = """
import os
import signal
import socket
import sys

with open(os.environ["CALL_LOG"], "a", encoding="utf-8") as log:
    log.write("tailscaled " + " ".join(sys.argv[1:]) + "\\n")

def stop(_signum, _frame):
    with open(os.environ["CALL_LOG"], "a", encoding="utf-8") as log:
        log.write("tailscaled-term\\n")
    raise SystemExit

signal.signal(signal.SIGTERM, stop)
"""
    if create_socket:
        tailscaled_body += """
import time

time.sleep(float(os.environ.get("TAILSCALE_SOCKET_DELAY", "0")))
server = socket.socket(socket.AF_UNIX)
server.bind(os.environ["TAILSCALE_SOCKET"])
server.listen()
signal.pause()
"""
    else:
        tailscaled_body += "signal.pause()\n"
    _write_executable(bin_dir / "tailscaled", f"#!/usr/bin/env python3\n{tailscaled_body}")
    _write_executable(
        bin_dir / "tailscale",
        """#!/bin/sh
printf 'tailscale %s\n' "$*" >>"${CALL_LOG}"
exit "${TAILSCALE_STATUS:-0}"
""",
    )
    _write_executable(
        bin_dir / "uv",
        """#!/bin/sh
printf 'uv %s\n' "$*" >>"${CALL_LOG}"
exit "${POLLER_STATUS:-0}"
""",
    )
    return bin_dir, call_log


def _entrypoint_env(tmp_path: Path, bin_dir: Path, call_log: Path) -> dict[str, str]:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "tailscaled.state").touch()
    return {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "CALL_LOG": str(call_log),
        "TS_STATE_DIR": str(state_dir),
        "TAILSCALE_SOCKET": str(tmp_path / "tailscaled.sock"),
        "TAILSCALED_PID_FILE": str(tmp_path / "tailscaled.pid"),
        "POLLER_PID_FILE": str(tmp_path / "poller.pid"),
        "TAILSCALE_READY_ATTEMPTS": "20",
        "TAILSCALE_READY_INTERVAL_SECONDS": "0.05",
        "STARTUP_GRACE_SECONDS": "0",
        "TS_EXIT_NODE": "exit-node.test",
        "TS_HOSTNAME": "poller.test",
    }


def _wait_for_path(path: Path, timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out waiting for {path}")
        time.sleep(0.01)


def test_entrypoint_starts_proxy_before_poller_and_cleans_up(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path)
    env = _entrypoint_env(tmp_path, bin_dir, call_log)

    result = subprocess.run(
        ["/bin/sh", str(ENTRYPOINT), "--once"],
        cwd=POLLER_DIR,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    calls = call_log.read_text(encoding="utf-8").splitlines()
    socket_arg = f"--socket={env['TAILSCALE_SOCKET']}"
    assert calls[0].startswith("tailscaled --tun=userspace-networking --socks5-server=")
    assert socket_arg in calls[0]
    assert calls[1] == f"tailscale {socket_arg} up --exit-node=exit-node.test --hostname=poller.test"
    assert calls[2] == "uv run --locked --no-dev python poller.py --once"
    assert Path(env["TAILSCALED_PID_FILE"]).read_text(encoding="utf-8").strip().isdigit()
    assert Path(env["POLLER_PID_FILE"]).read_text(encoding="utf-8").strip().isdigit()


def test_entrypoint_fails_before_poller_without_state_or_authkey(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path)
    env = _entrypoint_env(tmp_path, bin_dir, call_log)
    Path(env["TS_STATE_DIR"]).joinpath("tailscaled.state").unlink()
    env.pop("TS_AUTHKEY", None)

    result = subprocess.run(
        ["/bin/sh", str(ENTRYPOINT)],
        cwd=POLLER_DIR,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 1
    assert "TS_AUTHKEY is required" in result.stderr
    assert not call_log.exists()


def test_entrypoint_does_not_start_poller_when_proxy_is_not_ready(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path, create_socket=False)
    env = _entrypoint_env(tmp_path, bin_dir, call_log)
    env["TAILSCALE_READY_ATTEMPTS"] = "1"
    env["TAILSCALE_READY_INTERVAL_SECONDS"] = "0"

    result = subprocess.run(
        ["/bin/sh", str(ENTRYPOINT)],
        cwd=POLLER_DIR,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 1
    assert "tailscaled did not become ready" in result.stderr
    calls = call_log.read_text(encoding="utf-8").splitlines()
    assert "tailscaled-term" in calls
    assert not any(line.startswith("uv ") for line in calls)


def test_entrypoint_honors_startup_grace_after_early_poller_failure(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path)
    env = _entrypoint_env(tmp_path, bin_dir, call_log)
    env["POLLER_STATUS"] = str(EARLY_POLLER_EXIT_STATUS)
    env["STARTUP_GRACE_SECONDS"] = "5"
    _write_executable(bin_dir / "date", "#!/bin/sh\nprintf '100\\n'\n")
    _write_executable(
        bin_dir / "sleep",
        """#!/bin/sh
if [ "$1" = "0.05" ]; then
  exec /bin/sleep "$1"
fi
printf 'sleep %s\n' "$1" >>"${CALL_LOG}"
""",
    )

    result = subprocess.run(
        ["/bin/sh", str(ENTRYPOINT)],
        cwd=POLLER_DIR,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == EARLY_POLLER_EXIT_STATUS
    assert "poller exited during startup grace" in result.stderr
    assert "sleep 5" in call_log.read_text(encoding="utf-8").splitlines()


def test_entrypoint_forwards_shutdown_and_waits_for_poller(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path)
    env = _entrypoint_env(tmp_path, bin_dir, call_log)
    _write_executable(
        bin_dir / "uv",
        """#!/bin/sh
printf 'uv %s\n' "$*" >>"${CALL_LOG}"
trap 'printf "uv-term\\n" >>"${CALL_LOG}"; exit 0' TERM
: >"${POLLER_READY_FILE}"
while true; do
  /bin/sleep 1
done
""",
    )
    ready_file = tmp_path / "poller.ready"
    env["POLLER_READY_FILE"] = str(ready_file)

    process = subprocess.Popen(
        ["/bin/sh", str(ENTRYPOINT)],
        cwd=POLLER_DIR,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_path(ready_file)
        process.terminate()
        _stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0, stderr
        assert "uv-term" in call_log.read_text(encoding="utf-8").splitlines()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_entrypoint_stops_during_proxy_readiness_without_starting_poller(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path)
    env = _entrypoint_env(tmp_path, bin_dir, call_log)
    env["TAILSCALE_SOCKET_DELAY"] = "2"
    process = subprocess.Popen(
        ["/bin/sh", str(ENTRYPOINT)],
        cwd=POLLER_DIR,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_path(Path(env["TAILSCALED_PID_FILE"]))
        process.terminate()
        _stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0, stderr
        assert not Path(env["POLLER_PID_FILE"]).exists()
        assert not any(line.startswith("uv ") for line in call_log.read_text(encoding="utf-8").splitlines())
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_healthcheck_verifies_processes_socket_and_tailscale_status(tmp_path: Path) -> None:
    bin_dir, call_log = _fake_commands(tmp_path)
    socket_path = tmp_path / "tailscaled.sock"
    server = socket.socket(socket.AF_UNIX)  # ty: ignore[unresolved-attribute]
    server.bind(str(socket_path))
    tailscaled = subprocess.Popen(["/bin/sleep", "30"])
    poller = subprocess.Popen(["/bin/sleep", "30"])
    try:
        tailscaled_pid_file = tmp_path / "tailscaled.pid"
        poller_pid_file = tmp_path / "poller.pid"
        tailscaled_pid_file.write_text(str(tailscaled.pid), encoding="utf-8")
        poller_pid_file.write_text(str(poller.pid), encoding="utf-8")
        env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "CALL_LOG": str(call_log),
            "TAILSCALED_PID_FILE": str(tailscaled_pid_file),
            "POLLER_PID_FILE": str(poller_pid_file),
            "TAILSCALE_SOCKET": str(socket_path),
        }

        result = subprocess.run(
            ["/bin/sh", str(HEALTHCHECK)],
            cwd=POLLER_DIR,
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert call_log.read_text(encoding="utf-8").splitlines()[-1] == (f"tailscale --socket={socket_path} status")
    finally:
        tailscaled.terminate()
        poller.terminate()
        tailscaled.wait()
        poller.wait()
        server.close()
