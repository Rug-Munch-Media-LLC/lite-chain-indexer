#!/usr/bin/env bash
# Daily: export yesterday's data to Parquet, upload to B2, prune ClickHouse
#
# Runs from cron at 03:30 UTC (see the "Wire into cron" step):
#   30 3 * * * /bin/bash /opt/lite-chain-indexer/scripts/lite-indexer-parquet-daily.sh >> /backups/lite-indexer-parquet.log 2>&1
#
# For each supported chain this:
#   1. exports yesterday's rows from ClickHouse to Parquet
#      (PARQUET_DIR/{chain}/{date}/, verified row count + schema),
#   2. uploads the directory to b2-crypt:chain-data/parquet/{chain}/{date}/
#      via `rclone copy` + `rclone check`,
#   3. records the run in wallet_memory.parquet_export_state,
#   4. deletes the rows from ClickHouse ONLY after the upload is verified
#      (--delete-after-upload), and
#   5. publishes a bootstrap snapshot (--publish-snapshot): mirrors each
#      exported (chain, date) to b2-crypt:chain-data/snapshots/{date}/{chain}/
#      with a manifest.json so `lite-indexer bootstrap` can replay it.
#
# Idempotent: dates already exported are skipped (state marker + table).

set -euo pipefail

# Make `python3 -m lite_indexer` resolve from any cwd (cron runs
# the script with an absolute path and no working directory guarantee).
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$REPO_DIR"

YESTERDAY=$(date -u -d "yesterday" +%Y-%m-%d)
CHAINS="ethereum bsc polygon arbitrum optimism base avalanche gnosis"

echo "==> lite-indexer parquet daily export ($(date -u +%Y-%m-%dT%H:%M:%SZ)) date=${YESTERDAY}"

FAILED=""
for chain in $CHAINS; do
    echo "=== Exporting $chain $YESTERDAY ==="
    if ! python3 -m lite_indexer export-parquet \
        --chain "$chain" \
        --date "$YESTERDAY" \
        --upload \
        --remote "b2-crypt-v2:chain-data/parquet" \
        --delete-after-upload \
        --publish-snapshot; then
        echo "!!! export failed for $chain — continuing with the rest"
        FAILED="$FAILED $chain"
    fi
done

if [ -n "$FAILED" ]; then
    echo "==> export complete with failures:$FAILED"
    exit 1
fi

echo "==> lite-indexer parquet daily export complete ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
