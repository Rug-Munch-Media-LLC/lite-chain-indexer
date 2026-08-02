#!/usr/bin/env bash
# LiteChainIndexer — daily Parquet cold-storage rollup.
#
# Exports rows older than PARQUET_COLD_DAYS from ClickHouse into
# per-chain/per-date Parquet files, uploads them to the B2 permanent
# tier, then deletes them from ClickHouse to reclaim disk.
#
# This is the pipeline referenced by storage/duckdb_query.py
# (``lite-indexer-parquet-daily.sh``) — it was previously documented
# but missing; this script implements it.
#
# Usage:
#   bash scripts/lite-indexer-parquet-daily.sh [--upload] [--delete] [--chain CHAIN]
#
# Flags:
#   --upload   Upload Parquet files to the B2 remote via rclone
#   --delete   Delete exported rows from ClickHouse (reclaim disk)
#   --chain    Only export a single chain (default: all supported chains)
#
# Environment:
#   DATA_DIR                Data directory (default: /var/data/chain-indexer)
#   PARQUET_COLD_DAYS       Min row age before export (default: 7)
#   PARQUET_REMOTE          rclone remote path (default: b2-crypt:chain-data/parquet)
#   RCLONE_BIN              rclone binary (default: rclone)
#
# Safe by default: without --delete nothing is removed from ClickHouse,
# and the export is idempotent (already-exported dates are skipped).

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export DATA_DIR="${DATA_DIR:-/var/data/chain-indexer}"
export PARQUET_REMOTE="${PARQUET_REMOTE:-b2-crypt:chain-data/parquet}"

UPLOAD=""
DELETE=""
CHAIN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
    --upload)
        UPLOAD="--upload"
        shift
        ;;
    --delete)
        DELETE="--delete"
        shift
        ;;
    --chain)
        CHAIN="$2"
        shift 2
        ;;
    *)
        echo "Unknown option: $1" >&2
        exit 2
        ;;
    esac
done

echo "==> LiteChainIndexer parquet cold-storage rollup ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
echo "    DATA_DIR=${DATA_DIR}  COLD_DAYS=${PARQUET_COLD_DAYS:-7}  REMOTE=${PARQUET_REMOTE}"

if [[ -n "$CHAIN" ]]; then
    CHAINS=("$CHAIN")
else
    # All supported chains (must stay in sync with SUPPORTED_CHAINS).
    CHAINS=(ethereum base bsc polygon arbitrum optimism avalanche fantom linea scroll zksync mantle gnosis celo solana tron)
fi

FAILED=0
for chain in "${CHAINS[@]}"; do
    echo "==> Exporting chain: ${chain}"
    if ! python3 -m lite_indexer.cli export-parquet --chain "$chain" $UPLOAD $DELETE; then
        echo "    ✗ export failed for ${chain}" >&2
        FAILED=1
    fi
done

if [[ $FAILED -eq 0 ]]; then
    echo "==> Parquet rollup complete"
else
    echo "==> Parquet rollup completed with errors" >&2
    exit 1
fi
