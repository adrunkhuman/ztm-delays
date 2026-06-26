#!/bin/sh
set -eu

: "${TS_AUTHKEY:?TS_AUTHKEY is required}"
: "${VEHICLE_TYPE:?VEHICLE_TYPE is required}"

TS_SOCKS_ADDR="${TS_SOCKS_ADDR:-127.0.0.1:1055}"
TS_EXIT_NODE="${TS_EXIT_NODE:-100.103.142.113}"
TS_STATE_DIR="${TS_STATE_DIR:-/var/lib/tailscale}"

if [ -z "${TS_HOSTNAME:-}" ]; then
  TS_HOSTNAME="ztm-poller-${VEHICLE_TYPE}"
fi

mkdir -p /var/run/tailscale "${TS_STATE_DIR}"

tailscaled \
  --tun=userspace-networking \
  --socks5-server="${TS_SOCKS_ADDR}" \
  --state="${TS_STATE_DIR}/tailscaled.state" &
TAILSCALED_PID=$!

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

tailscale up \
  --authkey="${TS_AUTHKEY}" \
  --exit-node="${TS_EXIT_NODE}" \
  --hostname="${TS_HOSTNAME}"

export ZTM_API_PROXY="socks5h://${TS_SOCKS_ADDR}"

uv run --locked --no-dev python poller.py "$@" &
POLLER_PID=$!
set +e
wait "${POLLER_PID}"
POLLER_STATUS=$?
set -e

terminate
wait "${TAILSCALED_PID}" 2>/dev/null || true
exit "${POLLER_STATUS}"
