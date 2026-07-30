#!/usr/bin/env bash
# Rotate old NDJSON data files.
# Compresses data older than N days into .gz archives.
#
# Usage: bash scripts/rotate-data.sh [days]
#   days: Age threshold in days (default: 7)

set -euo pipefail

DATA_DIR="${DATA_DIR:-/var/data/chain-indexer}"
DAYS="${1:-7}"

echo "==> Rotating NDJSON data older than ${DAYS} days in ${DATA_DIR}"

find "${DATA_DIR}" -name "events.jsonl" -type f -mtime "+${DAYS}" | while read -r file; do
    dir=$(dirname "$file")
    echo "  Compressing: ${file}"
    gzip "$file"
    # Remove original after successful compression
    if [ -f "${file}.gz" ]; then
        rm -f "$file"
        echo "    → ${file}.gz"
    fi
done

echo "==> Rotation complete"

# Show disk usage
echo ""
echo "=== Data Directory Size ==="
du -sh "${DATA_DIR}" 2>/dev/null || echo "  (not accessible)"
