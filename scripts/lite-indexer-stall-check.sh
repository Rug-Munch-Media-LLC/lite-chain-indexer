#!/usr/bin/env bash
#
# lite-indexer-stall-check.sh — restart the indexer if a chain stalls.
#
# Runs the lag health check (python -m lite_indexer.health --lag --json).
# If ANY chain reports status "stale" (state file not updated for several
# poll intervals — typically a wedged event loop, e.g. a synchronous
# ClickHouse connect blocking all 14 chain tasks) the service is
# restarted. Also restarts when the service is not running at all.
#
# Intended to be driven by the lite-indexer-stall-check.timer systemd
# timer (every 10 minutes).
#
# Exit codes:
#   0 — healthy, no action
#   1 — chains behind but indexer running (no restart; informational)
#   2 — stalled chain detected → restarted service
#   3 — service was not running → started it
#   4 — lag check itself failed (no restart; avoid restart storms)
#
set -u

SERVICE="lite-indexer.service"
PYTHON="${PYTHON:-/usr/bin/python3}"
TIMEOUT_S="${TIMEOUT_S:-90}"

# Relative to the repo; editable install means the module resolves anyway.
# shellcheck disable=SC2317
log() { echo "[$(date -Iseconds)] $*"; }

# ── 0. Is the service running at all? ───────────────────────────────
if ! systemctl is-active --quiet "${SERVICE}"; then
    log "service ${SERVICE} not active — starting"
    systemctl start "${SERVICE}" 2>&1 | log || true
    exit 3
fi

# ── 1. Run the lag check (bounded by TIMEOUT_S) ─────────────────────
LAG_OUT="$(timeout "${TIMEOUT_S}" "${PYTHON}" -m lite_indexer.health --lag --json 2>/dev/null)"
LAG_RC=$?
# Exit codes from the health module:
#   0   — all chains healthy
#   1   — some chains behind/errored (JSON still valid — parse it)
#   124 — timeout killed the process
#   >1  — real check error
case ${LAG_RC} in
0 | 1)
    # Health module ran successfully — JSON is valid in LAG_OUT.
    # Continue to parse stale/behind chains below.
    ;;
124)
    log "lag check timed out after ${TIMEOUT_S}s — skipping restart"
    exit 4
    ;;
*)
    log "lag check failed (rc=${LAG_RC}) — skipping restart"
    exit 4
    ;;
esac

STALE_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
stale = [c for c, info in data.items() if info.get("status") == "stale"]
print(" ".join(stale))
')"

if [ -n "${STALE_CHAINS}" ]; then
    log "stalled chain(s): ${STALE_CHAINS} — restarting ${SERVICE}"
    systemctl restart "${SERVICE}" 2>&1 | log || true
    exit 2
fi

# ── 2. Chains behind but making progress — informational ────────────
BEHIND_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
behind = []
for c, info in data.items():
    if info.get("status") == "behind":
        behind.append(f"{c}:{info.get(\"lag_blocks\", 0)}")
print(" ".join(behind))
')"

if [ -n "${BEHIND_CHAINS}" ]; then
    log "chains behind (normal catch-up): ${BEHIND_CHAINS}"
    exit 1
fi

log "all chains healthy"
exit 0
