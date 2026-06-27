#!/bin/sh
set -eu

TAILSCALED_PID_FILE="${TAILSCALED_PID_FILE:-/tmp/tailscaled.pid}"
POLLER_PID_FILE="${POLLER_PID_FILE:-/tmp/ztm-poller.pid}"
TAILSCALE_SOCKET="${TAILSCALE_SOCKET:-/var/run/tailscale/tailscaled.sock}"

check_pid_file() {
  pid_file="$1"
  process_name="$2"

  if [ ! -s "${pid_file}" ]; then
    echo "${process_name} pid file missing: ${pid_file}" >&2
    exit 1
  fi

  pid=$(cat "${pid_file}")
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "${process_name} process is not running: ${pid}" >&2
    exit 1
  fi
}

check_pid_file "${TAILSCALED_PID_FILE}" "tailscaled"
check_pid_file "${POLLER_PID_FILE}" "poller"

if [ ! -S "${TAILSCALE_SOCKET}" ]; then
  echo "tailscale socket missing: ${TAILSCALE_SOCKET}" >&2
  exit 1
fi

tailscale status >/dev/null 2>&1
