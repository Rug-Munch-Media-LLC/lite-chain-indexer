#!/usr/bin/env bash
# Compact staging micro-batches into hive-partitioned parquet on B2.
#
# Additive to lite-indexer-parquet-daily.sh: the daily job keeps writing
# hourly staging files (chain/{YYYY}/{MM}/{DD}/{HH}/...); this job
# collapses one month (or day, for >=1M rows/day chains) of staging into a
# single sorted, zstd-level-3 hive-partitioned parquet file with
# fixed_size_binary address/hash columns, then (with --delete-staging)
# removes the consumed staging micro-batches — write-first, delete-second.
#
# Usage:
#   scripts/lite-indexer-compact.sh --table evm_transfers --chain ethereum --period 2026-07
#   scripts/lite-indexer-compact.sh --table dex_events --chain base --grain day --period 2026-07-15
#   scripts/lite-indexer-compact.sh --dry-run                        # no writes, no deletes
#   scripts/lite-indexer-compact.sh                                  # default: evm_transfers, previous month
#
# Environment: B2_ACCOUNT_ID / B2_APPLICATION_KEY (+ optional
# B2_S3_ENDPOINT / B2_S3_REGION). NOTE: the production b2-crypt: remote is
# rclone-crypt encrypted — point COMPACTED_PARQUET_REMOTE at an
# unencrypted B2 remote before running (see compaction.py docstring).

set -euo pipefail

# Make `python3 -m lite_indexer.storage.compaction` resolve from any cwd
# (cron runs the script with an absolute path and no working directory).
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$REPO_DIR"

# Default: the previous calendar month (a completed partition).
PREV_MONTH=$(date -u -d "$(date -u +%Y-%m-01) -1 month" +%Y-%m)

if [ $# -eq 0 ]; then
    echo "==> lite-indexer compact: defaulting to evm_transfers, chain=ethereum, period=${PREV_MONTH}" >&2
    exec python3 -m lite_indexer.storage.compaction \
        --table evm_transfers \
        --chain ethereum \
        --period "$PREV_MONTH" \
        --grain month \
        --delete-staging
fi

exec python3 -m lite_indexer.storage.compaction "$@"
