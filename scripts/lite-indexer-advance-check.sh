#!/usr/bin/env bash
#
# lite-indexer-advance-check.sh — verify every expected chain is ADVANCING.
#
# Distinct from lite-indexer-stall-check.sh (which restarts the service on
# a wedged chain): this check asserts the full chain roster is present and
# moving. A missing chain state file (chain not indexed at all) is the
# failure mode this catches — the stall check only sees chains that exist.
#
# It runs `python -m lite_indexer.health --lag --json` and requires:
#   1. every chain in EXPECTED_CHAINS appears in the report (no missing),
#   2. no chain reports status "stale" or "error",
#   3. every chain's state file has advanced within STALL_MINUTES.
# Chains that are "behind" (catching up after a gap) are still advancing
# and do NOT fail this check — only stalled/missing chains do.
#
# Exit codes:
#   0 — all expected chains present and advancing
#   1 — one or more chains behind (informational; still advancing)
#   2 — a chain is stale/error/missing → needs operator attention
#   4 — the lag check itself failed (no restart storm, no false alert)
#
# Env overrides: EXPECTED_CHAINS, STALL_MINUTES, TIMEOUT_S, PYTHON
#
set -u

PYTHON="${PYTHON:-/usr/bin/python3}"
TIMEOUT_S="${TIMEOUT_S:-90}"
# A chain whose state file has not advanced in this many minutes is NOT
# advancing → treated as stale for this check.
STALL_MINUTES="${STALL_MINUTES:-30}"
STALL_AGE_S=$((STALL_MINUTES * 60))

# 14 EVM chains LiteChainIndexer indexes by default. Deployments that also
# index solana/tron should export the full roster:
#   EXPECTED_CHAINS="ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,fantom,linea,scroll,zksync,mantle,gnosis,celo,solana,tron"
EXPECTED_CHAINS="${EXPECTED_CHAINS:-ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,fantom,linea,scroll,zksync,mantle,gnosis,celo}"
# Export so the inline Python below can read them via os.environ.
export EXPECTED_CHAINS STALL_AGE_S

log() { echo "[$(date -Iseconds)] $*"; }

# ── 1. Run the lag check (bounded) ───────────────────────────────────
LAG_OUT="$(timeout "${TIMEOUT_S}" "${PYTHON}" -m lite_indexer.health --lag --json 2>/dev/null)"
LAG_RC=$?
case ${LAG_RC} in
0 | 1) ;;
124)
    log "lag check timed out after ${TIMEOUT_S}s — skipping (no alert)"
    exit 4
    ;;
*)
    log "lag check failed (rc=${LAG_RC}) — skipping (no alert)"
    exit 4
    ;;
esac

# ── 2. Parse the JSON report. ─────────────────────────────────────────
# The health CLI prints the JSON report to stdout, but a few import-time
# structlog lines can leak onto stdout before setup_logging() reconfigures
# logging to stderr (health.py main() now does this, but we stay robust).
# So: scan every line and use the LAST one that parses as a JSON object.
# The inline Python prints four pipe-separated CSV fields:
#   MISSING|STALE|BEHIND|NO_ADVANCE
# (read with IFS='|' so empty fields survive; no JSON found prints ALL|||
# and we skip without a false alert.)
PYTHON_OUT="$(
    printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, os, sys

expected = os.environ.get("EXPECTED_CHAINS", "").split(",")
expected = {c.strip() for c in expected if c.strip()}
stall_age = float(os.environ.get("STALL_AGE_S", "1800"))

data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    print("ALL|||")
    sys.exit(0)

missing = sorted(expected - set(data.keys()))
# A chain is NOT advancing when its status is anything but ok/behind
# (stale, no_state, error) or its state file has not moved recently.
not_advancing = sorted(
    c for c, info in data.items()
    if c in expected and (
        info.get("status") not in ("ok", "behind")
        or (
            info.get("state_age_seconds") is not None
            and info["state_age_seconds"] > stall_age
        )
    )
)
behind = sorted(
    c for c, info in data.items()
    if info.get("status") == "behind" and c in expected
)
print("|".join([",".join(missing), ",".join(not_advancing), ",".join(behind)]))
'
)"

IFS='|' read -r MISSING NOT_ADVANCING CHAINS_BEHIND <<EOF
${PYTHON_OUT}
EOF

if [ "${MISSING}" = "ALL" ]; then
    log "lag check produced no parseable JSON — skipping (no alert)"
    exit 4
fi

# ── 3. Evaluate ──────────────────────────────────────────────────────
ALERT=0

if [ -n "${MISSING}" ]; then
    log "MISSING chains (not advancing — no state file): ${MISSING}"
    ALERT=1
fi
if [ -n "${NOT_ADVANCING}" ]; then
    log "NOT ADVANCING chains: ${NOT_ADVANCING}"
    ALERT=1
fi

if [ "${ALERT}" -eq 1 ]; then
    log "advance check FAILED — ${MISSING}${NOT_ADVANCING:+ $NOT_ADVANCING} not advancing"
    exit 2
fi

if [ -n "${CHAINS_BEHIND}" ]; then
    log "chains behind (normal catch-up, still advancing): ${CHAINS_BEHIND}"
    exit 1
fi

log "all ${EXPECTED_CHAINS//,/ } chains present and advancing"
exit 0
