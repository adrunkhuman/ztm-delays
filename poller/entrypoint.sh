#!/bin/sh
set -eu

TS_SOCKS_ADDR="${TS_SOCKS_ADDR:-127.0.0.1:1055}"
TS_EXIT_NODE="${TS_EXIT_NODE:-100.103.142.113}"
TS_STATE_DIR="${TS_STATE_DIR:-/var/lib/tailscale}"
TS_STATE_FILE="${TS_STATE_DIR}/tailscaled.state"
TAILSCALE_SOCKET="${TAILSCALE_SOCKET:-/var/run/tailscale/tailscaled.sock}"
TAILSCALED_PID_FILE="${TAILSCALED_PID_FILE:-/tmp/tailscaled.pid}"
POLLER_PID_FILE="${POLLER_PID_FILE:-/tmp/ztm-poller.pid}"
STARTUP_GRACE_SECONDS="${STARTUP_GRACE_SECONDS:-300}"
TAILSCALE_READY_ATTEMPTS="${TAILSCALE_READY_ATTEMPTS:-30}"
TAILSCALE_READY_INTERVAL_SECONDS="${TAILSCALE_READY_INTERVAL_SECONDS:-1}"
STARTED_AT=$(date +%s)
TERMINATE_REQUESTED=0

if [ -z "${TS_HOSTNAME:-}" ]; then
  TS_HOSTNAME="ztm-poller"
fi

mkdir -p "$(dirname "${TAILSCALE_SOCKET}")" "${TS_STATE_DIR}"

if [ ! -f "${TS_STATE_FILE}" ] && [ -z "${TS_AUTHKEY:-}" ]; then
  echo "TS_AUTHKEY is required when ${TS_STATE_FILE} does not exist" >&2
  exit 1
fi

terminate() {
  TERMINATE_REQUESTED=1
  if [ -n "${POLLER_PID:-}" ]; then
    kill -TERM "${POLLER_PID}" 2>/dev/null || true
  fi
}

stop_tailscaled() {
  if [ -n "${TAILSCALED_PID:-}" ]; then
    kill -TERM "${TAILSCALED_PID}" 2>/dev/null || true
    wait "${TAILSCALED_PID}" 2>/dev/null || true
  fi
}

trap terminate INT TERM

tailscaled \
  --tun=userspace-networking \
  --socks5-server="${TS_SOCKS_ADDR}" \
  --state="${TS_STATE_FILE}" \
  --socket="${TAILSCALE_SOCKET}" &
TAILSCALED_PID=$!
echo "${TAILSCALED_PID}" >"${TAILSCALED_PID_FILE}"

for _attempt in $(seq 1 "${TAILSCALE_READY_ATTEMPTS}"); do
  if [ -S "${TAILSCALE_SOCKET}" ]; then
    break
  fi
  sleep "${TAILSCALE_READY_INTERVAL_SECONDS}"
  if [ "${TERMINATE_REQUESTED}" -eq 1 ]; then
    break
  fi
done

if [ "${TERMINATE_REQUESTED}" -eq 1 ]; then
  stop_tailscaled
  exit 0
fi

if [ ! -S "${TAILSCALE_SOCKET}" ]; then
  echo "tailscaled did not become ready" >&2
  stop_tailscaled
  exit 1
fi

set +e
if [ -n "${TS_AUTHKEY:-}" ]; then
  tailscale --socket="${TAILSCALE_SOCKET}" up \
    --authkey="${TS_AUTHKEY}" \
    --exit-node="${TS_EXIT_NODE}" \
    --hostname="${TS_HOSTNAME}"
else
  tailscale --socket="${TAILSCALE_SOCKET}" up \
    --exit-node="${TS_EXIT_NODE}" \
    --hostname="${TS_HOSTNAME}"
fi
TAILSCALE_STATUS=$?
set -e

if [ "${TERMINATE_REQUESTED}" -eq 1 ]; then
  stop_tailscaled
  exit 0
fi
if [ "${TAILSCALE_STATUS}" -ne 0 ]; then
  stop_tailscaled
  exit "${TAILSCALE_STATUS}"
fi

export ZTM_API_PROXY="socks5h://${TS_SOCKS_ADDR}"

uv run --locked --no-dev python poller.py "$@" &
POLLER_PID=$!
echo "${POLLER_PID}" >"${POLLER_PID_FILE}"
set +e
while true; do
  wait "${POLLER_PID}"
  POLLER_STATUS=$?
  if kill -0 "${POLLER_PID}" 2>/dev/null; then
    continue
  fi
  break
done
set -e

if [ "${POLLER_STATUS}" -ne 0 ] && [ "${TERMINATE_REQUESTED}" -eq 0 ]; then
  NOW=$(date +%s)
  RUNTIME_SECONDS=$((NOW - STARTED_AT))
  if [ "${RUNTIME_SECONDS}" -lt "${STARTUP_GRACE_SECONDS}" ]; then
    REMAINING_SECONDS=$((STARTUP_GRACE_SECONDS - RUNTIME_SECONDS))
    echo "poller exited during startup grace; keeping container alive for ${REMAINING_SECONDS}s" >&2
    sleep "${REMAINING_SECONDS}"
  fi
fi

stop_tailscaled
exit "${POLLER_STATUS}"
