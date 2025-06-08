#!/usr/bin/env bash
# Watchdog for Crypto Data-Service: restart services if unhealthy
set -euo pipefail
LOGTAG="watchdog"

check_health() {
  if ! curl -sf http://127.0.0.1:8001/health | jq -e '.ok==true' >/dev/null; then
    logger -t "$LOGTAG" "API health failed; restarting data-api.service"
    systemctl restart data-api.service
  fi
}

ensure_active() {
  local svc="$1"
  if ! systemctl is-active --quiet "$svc"; then
    logger -t "$LOGTAG" "$svc inactive; restarting"
    systemctl restart "$svc"
  fi
}

check_health
ensure_active data-streamer.service
ensure_active ob-streamer.service
ensure_active futures-metrics.service
