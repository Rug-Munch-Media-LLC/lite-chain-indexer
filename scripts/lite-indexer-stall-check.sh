#!/usr/bin/env bash
#
# lite-indexer-stall-check.sh — watchdog for the lite-indexer service.
#
# Three responsibilities (driven by the lite-indexer-stall-check.timer
# systemd timer, every 10 minutes):
#
#   1. PROCESS WATCHDOG — if the lite-indexer.service is not active (dead
#      process, failed start), start it immediately.
#   2. STALL DETECTION — run the lag health check
#      (python -m lite_indexer.health --lag --json). If ANY chain reports
#      status "stale" (state file not updated for several poll intervals —
#      typically a wedged event loop, e.g. a synchronous ClickHouse
#      connect blocking all chain tasks) the service is restarted.
#   3. NO-ADVANCE DETECTION — if any chain's state file has not been
#      updated for more than STALL_MINUTES (default 30 minutes) the chain
#      has not advanced and the service is restarted (auto-reset).
#
# Exit codes:
#   0 — healthy, no action
#   1 — chains behind but indexer running (no restart; informational)
#   2 — stalled chain detected → restarted service
#   3 — service was not running → started it
#   4 — lag check itself failed (no restart; avoid restart storms)
#
# Env overrides: SERVICE, STALL_MINUTES, TIMEOUT_S, PYTHON
#
set -u

SERVICE="${SERVICE:-lite-indexer.service}"
PYTHON="${PYTHON:-/usr/bin/python3}"
TIMEOUT_S="${TIMEOUT_S:-90}"
# A chain that hasn't advanced its state file in this many minutes is
# considered stalled → auto-reset (restart the service).
STALL_MINUTES="${STALL_MINUTES:-30}"
STALL_AGE_S=$((STALL_MINUTES * 60))

# shellcheck disable=SC2317
log() { echo "[$(date -Iseconds)] $*"; }

# ── 0. Is the service running at all? (process watchdog) ─────────────
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

# ── 2. Parse the lag JSON. ────────────────────────────────────────────
# NOTE: keep inline Python single-quote-safe. No \" inside the f-strings —
# bash passes the text verbatim inside '...' and backslash-quote is a
# Python SyntaxError (observed 08/04: the BEHIND_CHAINS block broke and
# the watchdog silently reported "all chains healthy" while chains were
# behind).
#
# The health CLI can emit a few import-time structlog lines to stdout
# before setup_logging() reroutes to stderr (health.py main() now does
# that). Every parser below therefore scans ALL lines and uses the LAST
# one that parses as a JSON object — never json.load() over the whole
# stream, which would fail on log noise and mask stalled chains.
STALE_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    sys.exit(0)
stale = [c for c, info in data.items() if info.get("status") == "stale"]
print(" ".join(stale))
')"

# Chains whose state file has not advanced for more than STALL_MINUTES.
# These are the >30-min no-advance cases the timer must auto-reset.
NO_ADVANCE_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
stall = float(sys.argv[1])
data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    sys.exit(0)
no_advance = []
for c, info in data.items():
    age = info.get("state_age_seconds")
    if age is not None and age > stall:
        no_advance.append(c + ":" + str(int(age)))
print(" ".join(no_advance))
' "${STALL_AGE_S}")"

if [ -n "${STALE_CHAINS}" ]; then
    log "stalled chain(s): ${STALE_CHAINS} — restarting ${SERVICE}"
    systemctl restart "${SERVICE}" 2>&1 | log || true
    exit 2
fi

if [ -n "${NO_ADVANCE_CHAINS}" ]; then
    log "chain(s) not advanced for >${STALL_MINUTES}min: ${NO_ADVANCE_CHAINS} — restarting ${SERVICE}"
    systemctl restart "${SERVICE}" 2>&1 | log || true
    exit 2
fi

# ── 3. Chains behind but making progress — informational ────────────
BEHIND_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    sys.exit(0)
behind = []
for c, info in data.items():
    if info.get("status") == "behind":
        behind.append(c + ":" + str(info.get("lag_blocks", 0)))
print(" ".join(behind))
')"

if [ -n "${BEHIND_CHAINS}" ]; then
    log "chains behind (normal catch-up): ${BEHIND_CHAINS}"
    exit 1
fi

log "all chains healthy"
exit 0
