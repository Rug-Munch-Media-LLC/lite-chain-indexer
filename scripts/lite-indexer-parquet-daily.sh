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
# Chain set is DERIVED, never hand-written. A hardcoded 8-chain list here is
# what left 13 chains unarchived for 37 days (2026-09-16 -> 2026-10-23) while
# every run exited 0. Canonical list: fleet-infra scripts/parquet/lib.
# PD_LIB is injected by the systemd unit (the canonical list lives in the
# private infra repo, not here). Required: never guess a path, never fall back
# to a hardcoded chain list.
PD_LIB=${PD_LIB:?PD_LIB must point at the canonical parquet lib directory}
if [ ! -r "$PD_LIB/parquet-chains.sh" ]; then
    echo "FATAL: canonical chain list missing at $PD_LIB/parquet-chains.sh" >&2
    exit 78
fi
# shellcheck source=/dev/null
. "$PD_LIB/parquet-chains.sh"
CH_BIN=${CH_BIN:-docker exec rmi-clickhouse clickhouse-client}
DB=${DB:-wallet_memory}
if ! pd_chains_validate; then
    echo "FATAL: chain list incomplete — refusing to export a partial day" >&2
    exit 78
fi
CHAINS=$(pd_chains | tr '\n' ' ')
echo "==> units: $(echo $CHAINS | wc -w) chains"

echo "==> lite-indexer parquet daily export ($(date -u +%Y-%m-%dT%H:%M:%SZ)) date=${YESTERDAY}"

FAILED=""
for chain in $CHAINS; do
    echo "=== Exporting $chain $YESTERDAY ==="
    if ! python3 -m lite_indexer export-parquet \
        --chain "$chain" \
        --date "$YESTERDAY" \
        --upload \
        --remote "rugmunch-b2:rmi-chain-data/parquet" \
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

# ── B2 90-DAY ROLLING RETENTION ────────────────────────────────────────────
# The architecture: local burns ~1 day -> B2 holds a ROLLING 90-day window ->
# beyond 90 days is served by BigLake providers with fallback, never from here.
# Nothing was enforcing the 90-day window on the remote, so the archive grew
# without bound in one direction while the retention rule existed only as
# intent. This is the missing half of the loop.
#
# Safety: --min-age compares against the object's own mtime, so a date-partition
# older than 90 days is removed while anything recent survives. A failure here
# never affects the export that just succeeded.
B2_RETENTION_DAYS="${B2_RETENTION_DAYS:-90}"
REMOTE_ROOT="rugmunch-b2:rmi-chain-data/parquet"
if [ "${SKIP_B2_RETENTION:-0}" != "1" ] && command -v rclone >/dev/null 2>&1; then
    echo "==> B2 retention: pruning objects older than ${B2_RETENTION_DAYS}d under ${REMOTE_ROOT}"
    if rclone delete "$REMOTE_ROOT" --min-age "${B2_RETENTION_DAYS}d" --fast-list 2>&1 | tail -3; then
        echo "==> B2 retention: 90-day rolling window enforced"
    else
        echo "!!! B2 retention returned non-zero (non-fatal; export already verified)"
    fi
fi

echo "==> lite-indexer parquet daily export complete ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
