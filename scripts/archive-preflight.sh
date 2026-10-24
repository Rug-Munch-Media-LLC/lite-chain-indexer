#!/usr/bin/env bash
# Preflight for the archive timers: free space, ClickHouse, B2.
#
# A spec row with no schedule is documentation, not a backup; a schedule with
# no preflight is a nightly 3am mystery. This gate refuses to let an export
# start when the invariants it depends on are already broken.
set -euo pipefail

MIN_FREE_GIB="${MIN_FREE_GIB:-40}"
STAGE="${STAGE_DIR:-/data/clickhouse/user_files/archive-stream}"
remote="${ARCHIVE_REMOTE:-b2-crypt-v2:chain-data/parquet}"
rc=0

free_gib=$(df --output=avail -BG "$STAGE" | tail -1 | tr -dc '0-9')
if (( free_gib < MIN_FREE_GIB )); then
  echo "PREFLIGHT FAIL free=${free_gib}GiB < ${MIN_FREE_GIB}GiB on $STAGE" >&2
  rc=1
else
  echo "preflight ok free=${free_gib}GiB"
fi

if ! docker exec rmi-clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
  echo "PREFLIGHT FAIL clickhouse not answering" >&2
  rc=1
else
  echo "preflight ok clickhouse"
fi

if ! rclone lsd "$remote" >/dev/null 2>&1; then
  echo "PREFLIGHT FAIL b2 unreachable: $remote" >&2
  rc=1
else
  echo "preflight ok b2"
fi

exit "$rc"
