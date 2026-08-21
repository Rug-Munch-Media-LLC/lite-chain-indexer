#!/usr/bin/env bash
#
# solana-cursor-catchup.sh — one-shot recovery for the Solana SPL indexer
# when its slot cursor falls far behind the network tip.
#
# INCIDENT (2026-08-21): after the 08-18/08-20 free-RPC exhaustion
# collapse, lite_solana.json's cursor sat at slot 439,169,260 while the
# network tip was 440,600,076 (1.43M slots ≈ 4-6 days behind). The Solana
# strategy fetches getSignaturesForAddress (newest-first) and filters by
# the current [from_slot, to_slot] window (strategies/solana/strategy.py
# step 5) — a window ~1.4M slots behind the tip matches nothing, so every
# cycle returns 0 transfer events while advancing only 1000 slots. Catch-up
# at that rate would take weeks; wallet_memory.solana_transfers and
# solana_spl_balance_changes stay stale the whole time.
#
# FIX: jump the cursor to (tip - BEHIND_DELTA). The skipped slot range is
# accepted as a data gap — backfilling ~1.4M slots of per-mint signatures
# via free RPCs is infeasible (getSignaturesForAddress pages backwards at
# <=1000 signatures per mint per call). The gap is bounded by how long the
# stall went unnoticed (this is what ch-freshness-watchdog is for).
#
# SAFETY:
#   * REFUSES TO RUN while lite-indexer.service is active — the live
#     process rewrites the state files at the end of every cycle and would
#     clobber the jump. Run during a planned stop (orchestrator: stop →
#     this script → start).
#   * Backs up both state files to <file>.bak.<epoch> before touching.
#   * Read-only on-chain: a single getSlot (finalized) per endpoint tried.
#
# Usage:
#   solana-cursor-catchup.sh [--dry-run]
#
# Env overrides: SERVICE, BEHIND_DELTA, STATE_DIR, RPC_URLS (space-sep),
#                MAX_LAG_SLOTS (skip jump if lag below this; default 50000)
#
set -u

SERVICE="${SERVICE:-lite-indexer.service}"
STATE_DIR="${STATE_DIR:-/var/data/chain-indexer/state}"
BEHIND_DELTA="${BEHIND_DELTA:-2000}"
MAX_LAG_SLOTS="${MAX_LAG_SLOTS:-50000}"
RPC_URLS="${RPC_URLS:-https://api.mainnet-beta.solana.com https://solana-rpc.publicnode.com}"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

log() { echo "[$(date -Iseconds)] $*"; }
die() {
    log "ERROR: $*"
    exit 1
}

# ── Guard: never race the live indexer ─────────────────────────────────
# (dry-run only reads state + tip, so it is allowed while active)
if [[ "$DRY_RUN" -eq 0 ]] && systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
    die "$SERVICE is ACTIVE. Stop it first (orchestrator window); state files would be clobbered."
fi

STATE_FILE="$STATE_DIR/lite_solana.json"
HIGHWATER_FILE="$STATE_DIR/highwater_solana.json"
[[ -f "$STATE_FILE" ]] || die "state file missing: $STATE_FILE"

CURSOR=$(python3 -c "import json;print(json.load(open('$STATE_FILE'))['last_block'])") ||
    die "cannot read last_block from $STATE_FILE"

# ── Fetch finalized tip (first endpoint that answers) ─────────────────
TIP=""
for url in $RPC_URLS; do
    RESP=$(curl -s -m 15 -X POST "$url" -H 'Content-Type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}' 2>/dev/null)
    TIP=$(printf '%s' "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('result',''))" 2>/dev/null || true)
    if [[ "$TIP" =~ ^[0-9]+$ ]]; then
        log "tip=$TIP from $url"
        break
    fi
    TIP=""
done
[[ -n "$TIP" ]] || die "no Solana RPC endpoint returned a slot"

LAG=$((TIP - CURSOR))
log "cursor=$CURSOR tip=$TIP lag=$LAG slots (threshold $MAX_LAG_SLOTS)"

if ((LAG < MAX_LAG_SLOTS)); then
    log "lag below threshold — no jump needed."
    exit 0
fi

NEW_CURSOR=$((TIP - BEHIND_DELTA))
if ((NEW_CURSOR <= CURSOR)); then
    die "computed cursor $NEW_CURSOR <= current $CURSOR; refusing."
fi

if ((DRY_RUN)); then
    log "DRY RUN: would set last_block $CURSOR -> $NEW_CURSOR in:"
    log "  $STATE_FILE"
    log "  $HIGHWATER_FILE"
    exit 0
fi

EPOCH=$(date +%s)
cp -a "$STATE_FILE" "$STATE_FILE.bak.$EPOCH" || die "backup failed"
[[ -f "$HIGHWATER_FILE" ]] && cp -a "$HIGHWATER_FILE" "$HIGHWATER_FILE.bak.$EPOCH"

python3 - "$STATE_FILE" "$NEW_CURSOR" <<'PYEOF' || die "state rewrite failed"
import json, sys
path, cursor = sys.argv[1], int(sys.argv[2])
with open(path) as f:
    state = json.load(f)
state["last_block"] = cursor
state["source"] = "solana-cursor-catchup"
with open(path, "w") as f:
    json.dump(state, f, indent=2)
    f.write("\n")
PYEOF

if [[ -f "$HIGHWATER_FILE" ]]; then
    python3 - "$HIGHWATER_FILE" "$NEW_CURSOR" <<'PYEOF' || die "highwater rewrite failed"
import json, sys
path, cursor = sys.argv[1], int(sys.argv[2])
with open(path) as f:
    hw = json.load(f)
hw["last_block"] = cursor
with open(path, "w") as f:
    json.dump(hw, f, indent=2)
    f.write("\n")
PYEOF
fi

log "DONE: solana cursor $CURSOR -> $NEW_CURSOR (gap $((NEW_CURSOR - CURSOR)) slots accepted)."
log "Backups: $STATE_FILE.bak.$EPOCH"
log "Next: start $SERVICE; solana_transfers/solana_spl_balance_changes should resume within one sync cycle."
exit 0
