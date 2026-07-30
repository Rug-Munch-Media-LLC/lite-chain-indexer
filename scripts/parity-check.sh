#!/usr/bin/env bash
# Parity monitor — checks that LiteChainIndexer is keeping up with chain tip.
#
# Usage: bash scripts/parity-check.sh [chain ...]
#   Checks state files for all chains (or specified ones) against RPC tip.

set -euo pipefail

DATA_DIR="${DATA_DIR:-/var/data/chain-indexer}"
STATE_DIR="${DATA_DIR}/state"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

check_chain() {
    local chain="$1"
    local state_file="${STATE_DIR}/lite_${chain}.json"
    local max_lag="${2:-50}"

    if [ ! -f "$state_file" ]; then
        echo -e "${YELLOW}⚠ ${chain}: no state file${NC}"
        return
    fi

    local last_block
    last_block=$(python3 -c "import json; print(json.load(open('${state_file}'))['last_block'])" 2>/dev/null || echo "")

    if [ -z "$last_block" ]; then
        echo -e "${YELLOW}⚠ ${chain}: could not read last_block from state${NC}"
        return
    fi

    # Get endpoint from config
    local endpoint
    case "$chain" in
        ethereum) endpoint="https://eth.drpc.org" ;;
        base)     endpoint="https://base.drpc.org" ;;
        bsc)      endpoint="https://bsc.drpc.org" ;;
        polygon)  endpoint="https://polygon.drpc.org" ;;
        arbitrum) endpoint="https://arb1.arbitrum.io/rpc" ;;
        optimism) endpoint="https://mainnet.optimism.io" ;;
        *)        echo -e "${YELLOW}⚠ ${chain}: unknown chain${NC}"; return ;;
    esac

    local tip_hex
    tip_hex=$(curl -s -X POST "$endpoint" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('result','0x0'))" 2>/dev/null || echo "0x0")

    local tip=$((tip_hex))
    local lag=$((tip - last_block))

    if [ "$lag" -lt "$max_lag" ]; then
        echo -e "${GREEN}✓ ${chain}: block ${last_block} / tip ${tip} (lag: ${lag})${NC}"
    else
        echo -e "${RED}✗ ${chain}: FAR BEHIND — block ${last_block} / tip ${tip} (lag: ${lag})${NC}"
    fi
}

echo "=== LiteChainIndexer Parity Check ==="
echo ""

if [ $# -gt 0 ]; then
    for chain in "$@"; do
        check_chain "$chain"
    done
else
    for state_file in "${STATE_DIR}"/lite_*.json; do
        [ -f "$state_file" ] || continue
        chain=$(basename "$state_file" | sed 's/lite_//;s/\.json$//')
        check_chain "$chain"
    done
fi

echo ""
echo "Done."
