#!/bin/sh
set -eu

TS_SOCKS_ADDR="${TS_SOCKS_ADDR:-127.0.0.1:1055}"
TS_EXIT_NODE="${TS_EXIT_NODE:-100.103.142.113}"
TS_STATE_DIR="${TS_STATE_DIR:-/var/lib/tailscale}"
TS_STATE_FILE="${TS_STATE_DIR}/tailscaled.state"
TAILSCALED_PID_FILE="${TAILSCALED_PID_FILE:-/tmp/tailscaled.pid}"
POLLER_PID_FILE="${POLLER_PID_FILE:-/tmp/ztm-poller.pid}"
STARTUP_GRACE_SECONDS="${STARTUP_GRACE_SECONDS:-300}"
STARTED_AT=$(date +%s)

if [ -z "${TS_HOSTNAME:-}" ]; then
  TS_HOSTNAME="ztm-poller"
fi

mkdir -p /var/run/tailscale "${TS_STATE_DIR}"

if [ ! -f "${TS_STATE_FILE}" ] && [ -z "${TS_AUTHKEY:-}" ]; then
  echo "TS_AUTHKEY is required when ${TS_STATE_FILE} does not exist" >&2
  exit 1
fi

tailscaled \
  --tun=userspace-networking \
  --socks5-server="${TS_SOCKS_ADDR}" \
  --state="${TS_STATE_FILE}" &
TAILSCALED_PID=$!
echo "${TAILSCALED_PID}" >"${TAILSCALED_PID_FILE}"

terminate() {
  if [ -n "${POLLER_PID:-}" ]; then
    kill -TERM "${POLLER_PID}" 2>/dev/null || true
  fi
  kill -TERM "${TAILSCALED_PID}" 2>/dev/null || true
}

trap terminate INT TERM

for _attempt in $(seq 1 30); do
  if [ -S /var/run/tailscale/tailscaled.sock ]; then
    break
  fi
  sleep 1
done

if [ ! -S /var/run/tailscale/tailscaled.sock ]; then
  echo "tailscaled did not become ready" >&2
  exit 1
fi

if [ -n "${TS_AUTHKEY:-}" ]; then
  tailscale up \
    --authkey="${TS_AUTHKEY}" \
    --exit-node="${TS_EXIT_NODE}" \
    --hostname="${TS_HOSTNAME}"
else
  tailscale up \
    --exit-node="${TS_EXIT_NODE}" \
    --hostname="${TS_HOSTNAME}"
fi

export ZTM_API_PROXY="socks5h://${TS_SOCKS_ADDR}"

uv run --locked --no-dev python poller.py "$@" &
POLLER_PID=$!
echo "${POLLER_PID}" >"${POLLER_PID_FILE}"
set +e
wait "${POLLER_PID}"
POLLER_STATUS=$?
set -e

if [ "${POLLER_STATUS}" -ne 0 ]; then
  NOW=$(date +%s)
  RUNTIME_SECONDS=$((NOW - STARTED_AT))
  if [ "${RUNTIME_SECONDS}" -lt "${STARTUP_GRACE_SECONDS}" ]; then
    REMAINING_SECONDS=$((STARTUP_GRACE_SECONDS - RUNTIME_SECONDS))
    echo "poller exited during startup grace; keeping container alive for ${REMAINING_SECONDS}s" >&2
    sleep "${REMAINING_SECONDS}"
  fi
fi

terminate
wait "${TAILSCALED_PID}" 2>/dev/null || true
exit "${POLLER_STATUS}"
