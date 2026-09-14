#!/usr/bin/env bash
# Retention drift checker — thin wrapper over `python3 -m lite_indexer.retention`.
#
# Reads live table TTLs from ClickHouse (system.tables.engine_full) and diffs
# them against the canonical policy (lite_indexer/config/storage.py
# RETENTION_POLICY). Exit 0: no drift, or ClickHouse unreachable (the check
# is skipped, not failed). Exit 1: a provisioned table diverges from policy.
#
# Usage: bash scripts/check-retention-drift.sh
set -euo pipefail

exec python3 -m lite_indexer.retention "$@"
