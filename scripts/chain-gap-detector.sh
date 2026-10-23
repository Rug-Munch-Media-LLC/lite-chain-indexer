#!/usr/bin/env bash
# Chain gap detector — per-chain coverage vs the live highwater (2026-10-11).
#
# For each EVM chain: compare ClickHouse's newest block/timestamp against the
# daemon's highwater file. Three distinct conditions, deliberately separated:
#
#   HOLE     ClickHouse is missing blocks the walker already walked, and the
#            data is older than GAP_MINUTES. Real data loss, gates.
#   BACKFILL The walker is healthy and draining a known deep backlog (chain
#            listed in BACKFILL_CHAINS). Reported with blocks/time behind;
#            does NOT gate while the backlog drains — the data is scheduled,
#            not lost.
#   STALE    ClickHouse data is older than GAP_MINUTES, chain not declared a
#            backfill target. Gates.
#
# Fixes 2026-10-23 (the timer was failed for 12 days on false positives):
#   1. non-EVM chains (solana) were compared against evm_transfers, so they
#      reported a permanent bogus hole of ~453,000,000 "blocks".
#   2. highwater is written per checkpoint while ClickHouse's max visible
#      block lags a flush/checkpoint window, so every chain reported
#      "ch behind hw by 1..190 blocks" on every run. A block delta counts as
#      a HOLE only when the data is ALSO stale (> GAP_MINUTES).
#   3. highwater legitimately runs ahead of the newest transfer row on sparse
#      chains (no matching Transfer log for a long stretch) — never a hole.
#   4. a deep, deliberately-draining backlog (fraxtal/blast) is BACKFILL, not
#      a hole.
#
# Exit 1 only when a real HOLE or an undeclared STALE chain exists.
set -uo pipefail

STATE_DIR="${STATE_DIR:-/var/data/chain-indexer/state}"
CH="${CH:-docker exec rmi-clickhouse clickhouse-client -q}"
GAP_MINUTES="${GAP_MINUTES:-120}"
# Blocks of lag that are just the checkpoint/flush window, not lost data.
FLUSH_TOLERANCE="${FLUSH_TOLERANCE:-1000}"
# Chains draining a known deep backlog — reported, not gated.
BACKFILL_CHAINS="${BACKFILL_CHAINS:-fraxtal,blast}"
# Solana does not write evm_transfers; it has its own cursor/catch-up path.
NON_EVM_CHAINS="${NON_EVM_CHAINS:-solana}"

in_csv() { case ",$2," in *",$1,"*) return 0 ;; *) return 1 ;; esac }

holes=0
backfills=0
printf "%-12s %-12s %-12s %-20s %s\n" CHAIN CH_MAX HW BLOCK_TS STATUS
for f in "$STATE_DIR"/highwater_*.json; do
	[ -e "$f" ] || continue
	c=$(basename "$f" .json | sed 's/highwater_//')
	hw=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["last_block"])' "$f" 2>/dev/null)
	[ -n "${hw:-}" ] || hw=""

	if in_csv "$c" "$NON_EVM_CHAINS"; then
		printf "%-12s %-12s %-12s %-20s %s\n" "$c" "-" "${hw:-?}" "-" "n/a: non-EVM (own cursor check)"
		continue
	fi

	row=$($CH "SELECT max(block_number), max(block_timestamp), dateDiff('minute', max(block_timestamp), now()) FROM wallet_memory.evm_transfers WHERE chain='$c'" 2>/dev/null)
	chm=$(printf '%s' "$row" | cut -f1)
	ts=$(printf '%s' "$row" | cut -f2)
	lagm=$(printf '%s' "$row" | cut -f3)

	# No rows at all for this chain: either brand new or never written.
	if [ -z "$chm" ] || [ "$chm" = "0" ]; then
		if in_csv "$c" "$BACKFILL_CHAINS"; then
			printf "%-12s %-12s %-12s %-20s %s\n" "$c" "-" "${hw:-?}" "-" "BACKFILL: no rows yet (draining)"
			backfills=$((backfills + 1))
		else
			printf "%-12s %-12s %-12s %-20s %s\n" "$c" "-" "${hw:-?}" "-" "STALE: no rows in ClickHouse"
			holes=$((holes + 1))
		fi
		continue
	fi

	behind=""
	if [ -n "$hw" ] && [ "$hw" -gt "$chm" ] 2>/dev/null; then
		behind=$((hw - chm))
	fi

	if [ -n "${lagm:-}" ] && [ "$lagm" -gt "$GAP_MINUTES" ] 2>/dev/null; then
		if in_csv "$c" "$BACKFILL_CHAINS"; then
			printf "%-12s %-12s %-12s %-20s %s\n" "$c" "$chm" "$hw" "$ts" "BACKFILL: ${behind:-0} blocks / ${lagm}m behind (draining)"
			backfills=$((backfills + 1))
		elif [ -n "$behind" ] && [ "$behind" -gt "$FLUSH_TOLERANCE" ] 2>/dev/null; then
			printf "%-12s %-12s %-12s %-20s %s\n" "$c" "$chm" "$hw" "$ts" "HOLE: ch behind hw by $behind blocks, ${lagm}m stale"
			holes=$((holes + 1))
		else
			printf "%-12s %-12s %-12s %-20s %s\n" "$c" "$chm" "$hw" "$ts" "STALE: ch data ${lagm}m old (no fresh blocks)"
			holes=$((holes + 1))
		fi
	elif [ -n "$behind" ] && [ "$behind" -gt "$FLUSH_TOLERANCE" ] 2>/dev/null; then
		printf "%-12s %-12s %-12s %-20s %s\n" "$c" "$chm" "$hw" "$ts" "ok (hw ahead by ${behind}; sparse/flush window)"
	else
		printf "%-12s %-12s %-12s %-20s %s\n" "$c" "$chm" "$hw" "$ts" "ok"
	fi
done

echo "holes: $holes  backfill_in_progress: $backfills"
[ "$holes" -eq 0 ] && exit 0 || exit 1
