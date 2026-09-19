#!/usr/bin/env bash
#
# lite-indexer-stall-check.sh — watchdog for the lite-indexer service.
#
# Three responsibilities (driven by the lite-indexer-stall-check.timer
# systemd timer, every 10 minutes):
#
#   1. PROCESS WATCHDOG — if the lite-indexer.service is not active (dead
#      process, failed start), start it immediately.
#   2. STALL DETECTION — run the lag health check
#      (python -m lite_indexer.health --lag --json). Chains that are BEHIND
#      HEAD but still advancing (state file fresh) are CATCHING UP: notify
#      (throttled), never restart — 2026-09-17 (bsc/zksync restart thrash
#      interrupted all ingestion and fixed nothing). Restarts happen only on
#      NO-ADVANCE (state genuinely frozen) or a dead service.
#   3. NO-ADVANCE DETECTION — if any chain's state file has not been
#      updated for more than the env-configured STALL_MINUTES threshold
#      (see below; default 30) the chain has not advanced and the service
#      is restarted (auto-reset).
#   4. RESTART COOLDOWN — stall/no-advance restarts are throttled to at
#      most one per RESTART_COOLDOWN_MIN (default 30 minutes). The
#      indexer's RPC pool keeps circuit-breaker state (403/429 hard bans,
#      exponential backoff) IN MEMORY — an aggressive watchdog that
#      restarts every 10 minutes wipes that state and re-hammers the very
#      endpoints that triggered the stall (08/05 incident: 429 storm on
#      ethereum/base/optimism). The cooldown lets the breakers work.
#      The process watchdog (path 1) is deliberately NOT throttled: a
#      dead service must always be (re)started.
#
# Exit codes:
#   0 — healthy, no action
#   1 — chains behind but indexer running (no restart; informational);
#       also used when a restart was requested but throttled by cooldown
#   2 — stalled chain detected → restarted service
#   3 — service was not running → started it
#   4 — lag check itself failed (no restart; avoid restart storms)
#
# Env overrides: SERVICE, STALL_MINUTES, TIMEOUT_S, PYTHON,
#                RESTART_COOLDOWN_MIN, RESTART_MARKER, FORCE_RESTART_AFTER,
#                EVM_DATA_LAG_MIN, EVM_LAG_NOTIFY_FILE,
#                EVM_LAG_NOTIFY_COOLDOWN_S
#
set -u

# Load deployment env defensively so MANUAL runs behave exactly like timer
# runs. 2026-09-17 incident: manual runs lacked CLICKHOUSE_PASSWORD, so the
# fresh-data exemption silently no-op'd and the watchdog restarted healthy,
# actively-writing chains (self-inflicted thrash).
ENV_FILE="${LITE_INDEXER_ENV_FILE:-/srv/secrets/lite-indexer.env}"
if [ -f "$ENV_FILE" ]; then
	# shellcheck disable=SC1091
	set -a
	. "$ENV_FILE"
	set +a
fi

SERVICE="${SERVICE:-lite-indexer.service}"
PYTHON="${PYTHON:-/usr/bin/python3}"
TIMEOUT_S="${TIMEOUT_S:-90}"
# A chain that hasn't advanced its state file in this many minutes is
# considered stalled → auto-reset (restart the service).
STALL_MINUTES="${STALL_MINUTES:-30}"
STALL_AGE_S=$((STALL_MINUTES * 60))
# Minimum seconds between stall/no-advance restarts. Persisted in
# RESTART_MARKER so the cooldown survives timer re-fires.
RESTART_COOLDOWN_MIN="${RESTART_COOLDOWN_MIN:-30}"
RESTART_MARKER="${RESTART_MARKER:-/var/data/chain-indexer/lite-indexer-last-restart}"
# Escalation: after this many CONSECUTIVE throttled stalls the cooldown is
# bypassed — a wedge that survives multiple cooldowns needs the reset more
# than the RPC circuits need the rest.
FORCE_RESTART_AFTER="${FORCE_RESTART_AFTER:-3}"
THROTTLE_STREAK_FILE="${THROTTLE_STREAK_FILE:-/var/data/chain-indexer/lite-indexer-throttle-streak}"
# 2026-09-17: chains BEHIND HEAD but still advancing are catching up, not
# stuck. The full-daemon restart for them interrupted ALL ingestion and never
# helped (bsc/zksync thrash). Catch-up is now notify-only (throttled).
CATCHUP_NOTIFY_FILE="${CATCHUP_NOTIFY_FILE:-/var/data/chain-indexer/lite-indexer-catchup-notify-ts}"
CATCHUP_NOTIFY_COOLDOWN_S="${CATCHUP_NOTIFY_COOLDOWN_S:-21600}"

# shellcheck disable=SC2317
log() { echo "[$(date -Iseconds)] $*"; }

# ── Alert ladder (house pattern: webhook -> n8n -> redis telegram) ────
# Stall events must page humans, not just restart silently. The 2026-08-24
# incident (ethereum 23h stale, bsc 357h) went unnoticed because every
# watchdog action was log-only.
alert_json() {
	local title="$1" message="$2" severity="${3:-warning}"
	local payload
	payload=$(printf '{"title":"%s","message":"%s","severity":"%s","source":"%s","timestamp":"%s"}' \
		"$title" "$message" "$severity" "lite-indexer@$(hostname)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)")
	curl -sSf -X POST "http://127.0.0.1:9099/alert" \
		-H "Content-Type: application/json" -d "$payload" \
		--connect-timeout 5 -m 15 >/dev/null 2>&1 && return 0
	curl -sSf -X POST "http://127.0.0.1:5678/webhook/alert" \
		-H "Content-Type: application/json" -d "$payload" \
		--connect-timeout 5 -m 15 >/dev/null 2>&1 && return 0
	if command -v redis-cli >/dev/null 2>&1; then
		redis-cli LPUSH rag:telegram_queue "$payload" >/dev/null 2>&1 && return 0
	fi
	log "ALERT DELIVERY FAILED (all rungs) — ${title}: ${message}"
	return 1
}

# ── Restart helper with cooldown ──────────────────────────────────────
# shellcheck disable=SC2317
restart_if_cooldown_elapsed() {
	# $1 — human-readable reason for the log line.
	# Returns 0 if the service was restarted, 1 if throttled.
	local reason="$1"
	local now last_restart elapsed
	now=$(date +%s)
	last_restart=$(cat "${RESTART_MARKER}" 2>/dev/null || echo 0)
	elapsed=$((now - last_restart))
	if [ "${elapsed}" -lt $((RESTART_COOLDOWN_MIN * 60)) ]; then
		local streak
		streak=$(cat "${THROTTLE_STREAK_FILE}" 2>/dev/null || echo 0)
		streak=$((streak + 1))
		echo "${streak}" >"${THROTTLE_STREAK_FILE}" 2>/dev/null || true
		if [ "${streak}" -ge "${FORCE_RESTART_AFTER}" ]; then
			log "restart throttled ${streak}x in a row — FORCING restart despite cooldown: ${reason}"
			systemctl restart "${SERVICE}" 2>&1 | log || true
			date +%s >"${RESTART_MARKER}" 2>/dev/null || true
			echo 0 >"${THROTTLE_STREAK_FILE}" 2>/dev/null || true
			alert_json "LiteIndexer FORCE-restarted (${streak} consecutive throttles)" \
				"${reason}; cooldown bypassed automatically after ${streak} consecutive throttles — chains stuck across multiple cooldowns" \
				"critical" || true
			return 0
		fi
		log "restart throttled: ${reason} — last restart ${elapsed}s ago (<${RESTART_COOLDOWN_MIN}min cooldown); skip ${streak}/${FORCE_RESTART_AFTER} before forced restart"
		# Notify once per throttle streak (streak 1): repeating the same
		# condition every 10 minutes is alert fatigue, not signal.
		if [ "${streak}" -eq 1 ]; then
			alert_json "LiteIndexer STILL stalled (restart throttled 1/${FORCE_RESTART_AFTER})" \
				"${reason}; last restart ${elapsed}s ago — forced restart at ${FORCE_RESTART_AFTER} consecutive throttles, or investigate RPC providers manually" \
				"critical" || true
		fi
		return 1
	fi
	log "restarting ${SERVICE}: ${reason}"
	systemctl restart "${SERVICE}" 2>&1 | log || true
	date +%s >"${RESTART_MARKER}" 2>/dev/null || true
	echo 0 >"${THROTTLE_STREAK_FILE}" 2>/dev/null || true
	# Notify at most once per cooldown window — churny restarts during
	# catch-up were flooding Telegram.
	local notify_file notify_ts
	notify_file="${NOTIFY_TS_FILE:-/var/data/chain-indexer/lite-indexer-restart-notify-ts}"
	notify_ts=$(cat "${notify_file}" 2>/dev/null || echo 0)
	if [ $((now - notify_ts)) -ge "${NOTIFY_COOLDOWN_S:-7200}" ]; then
		echo "${now}" >"${notify_file}" 2>/dev/null || true
		alert_json "LiteIndexer restarted" "${reason}" "warning" || true
	else
		log "restart notify suppressed (cooldown ${NOTIFY_COOLDOWN_S:-7200}s): ${reason}"
	fi
	return 0
}

# ── Migration guard ──────────────────────────────────────────────────
# The MergeTree->ReplacingMergeTree migration deliberately STOPS the indexer
# (for a gap-free copy) and pauses merges. Restarting it mid-copy would write
# to the source table after its copy snapshot, and those rows would be lost at
# the swap — the 2026-09-14 second-pass copy was corrupted exactly this way.
# If a migration is in flight, do nothing.
if [[ -f "${MIGRATION_LOCK:-/var/lib/lite-indexer/migration.lock}" ]] ||
	pgrep -f "run-ch-migration.sh" >/dev/null 2>&1; then
	log "migration in progress — watchdog skipping (indexer intentionally stopped)"
	exit 0
fi

# ── 0. Is the service running at all? (process watchdog) ─────────────
if ! systemctl is-active --quiet "${SERVICE}"; then
	log "service ${SERVICE} not active — starting"
	systemctl start "${SERVICE}" 2>&1 | log || true
	exit 3
fi

# ── 1. Run the lag check (bounded by TIMEOUT_S) ─────────────────────
LAG_OUT="$(timeout "${TIMEOUT_S}" "${PYTHON}" -m lite_indexer.health --lag --json 2>/dev/null)"
LAG_RC=$?
# Exit codes from the health module:
#   0   — all chains healthy
#   1   — some chains behind/errored (JSON still valid — parse it)
#   124 — timeout killed the process
#   >1  — real check error
case ${LAG_RC} in
0 | 1)
	# Health module ran successfully — JSON is valid in LAG_OUT.
	# Continue to parse stale/behind chains below.
	;;
124)
	log "lag check timed out after ${TIMEOUT_S}s — skipping restart"
	exit 4
	;;
*)
	log "lag check failed (rc=${LAG_RC}) — skipping restart"
	exit 4
	;;
esac

# ── 2. Parse the lag JSON. ────────────────────────────────────────────
# NOTE: keep inline Python single-quote-safe. No \" inside the f-strings —
# bash passes the text verbatim inside '...' and backslash-quote is a
# Python SyntaxError (observed 08/04: the BEHIND_CHAINS block broke and
# the watchdog silently reported "all chains healthy" while chains were
# behind).
#
# The health CLI can emit a few import-time structlog lines to stdout
# before setup_logging() reroutes to stderr (health.py main() now does
# that). Every parser below therefore scans ALL lines and uses the LAST
# one that parses as a JSON object — never json.load() over the whole
# stream, which would fail on log noise and mask stalled chains.
STALE_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    sys.exit(0)
stale = [c for c, info in data.items() if info.get("status") == "stale"]
print(" ".join(stale))
')"

# Chains whose state file has not advanced for more than STALL_MINUTES.
# These are the >STALL_MINUTES no-advance cases the timer must auto-reset.
NO_ADVANCE_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
stall = float(sys.argv[1])
data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    sys.exit(0)
no_advance = []
for c, info in data.items():
    age = info.get("state_age_seconds")
    if age is not None and age > stall:
        no_advance.append(c + ":" + str(int(age)))
print(" ".join(no_advance))
' "${STALL_AGE_S}")"

# ── 2a2. FRESH-DATA EXEMPTION (2026-09-16) ───────────────────────────
# Chains actively WRITING data (ClickHouse ingestion within
# FRESH_WINDOW_MIN) are recovering/catching up — their state files lag
# while windows complete. Restarts for these were looping and starving
# the catch-up (and spamming Telegram). Exempt fresh-data chains from
# the stale/no-advance restart paths.
FRESH_WINDOW_MIN="${FRESH_WINDOW_MIN:-45}"
FRESH_CHAINS="$(
	timeout 30 "${PYTHON}" - <<'PYEOF'
import os

try:
    import clickhouse_driver

    win = int(os.getenv("FRESH_WINDOW_MIN", "15"))
    c = clickhouse_driver.Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        user=os.getenv("CLICKHOUSE_USER", "rmi"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    # 2026-09-17: during catch-up a chain writes into dex/deployer tables even
    # while its evm_transfers batch is still in flight — missing that made the
    # watchdog restart actively-working chains (thrash). Check all write paths.
    rows = c.execute(
        f"SELECT chain FROM ("
        f"  SELECT chain, ingested_at FROM wallet_memory.evm_transfers"
        f"  UNION ALL SELECT chain, ingested_at FROM wallet_memory.dex_events"
        f"  UNION ALL SELECT chain, ingested_at FROM wallet_memory.deployer_events"
        f") WHERE ingested_at >= now() - INTERVAL {win} MINUTE GROUP BY chain"
    )
    fresh = {str(r[0]) for r in rows}
    try:
        if c.execute(
            f"SELECT 1 FROM wallet_memory.solana_spl_balance_changes "
            f"WHERE ingested_at >= now() - INTERVAL {win} MINUTE LIMIT 1"
        ):
            fresh.add("solana")
    except Exception:
        pass
    print(" ".join(sorted(fresh)))
except Exception:
    print("")
PYEOF
)"
if [ -n "${FRESH_CHAINS}" ]; then
	STALE_CHAINS="$(
		printf '%s' "${STALE_CHAINS}" | "${PYTHON}" -c '
import sys
fresh = set(sys.argv[1].split())
print(" ".join(p for p in sys.stdin.read().split() if p.split(":", 1)[0] not in fresh))
' "${FRESH_CHAINS}"
	)"
	NO_ADVANCE_CHAINS="$(
		printf '%s' "${NO_ADVANCE_CHAINS}" | "${PYTHON}" -c '
import sys
fresh = set(sys.argv[1].split())
print(" ".join(p for p in sys.stdin.read().split() if p.split(":", 1)[0] not in fresh))
' "${FRESH_CHAINS}"
	)"
	log "fresh-data exemption: fresh=[${FRESH_CHAINS}] remaining stale=[${STALE_CHAINS}] no-advance=[${NO_ADVANCE_CHAINS}]"
fi

# ── 1b. SOLANA silent-extraction guard (2026-08-24 mode) ─────────────
# Runs BEFORE the catch-up exits so it can never be masked by them.
# The reset MUST be paired with an immediate restart: the running daemon
# re-saves its in-memory highwater within ~15s and would overwrite a
# file-only reset (observed 2026-09-17). So: only reset when a restart is
# actually permitted; otherwise defer and say so.
SOL_STATE_FILE="${SOL_STATE_FILE:-/var/data/chain-indexer/state/highwater_solana.json}"
SOL_HW="$("${PYTHON}" -c "import json; print(int(json.load(open('${SOL_STATE_FILE}')).get('last_block') or 0))" 2>/dev/null || echo 0)"
SOL_DATA="$(docker exec rmi-clickhouse clickhouse-client -q "SELECT max(slot) FROM wallet_memory.solana_spl_balance_changes" 2>/dev/null || echo 0)"
if [ "${SOL_HW:-0}" -gt 0 ] && [ "${SOL_DATA:-0}" -gt 0 ] && [ "$((SOL_HW - SOL_DATA))" -gt 200000 ]; then
	SINCE_RESTART=$(( $(date +%s) - $(cat "${RESTART_MARKER}" 2>/dev/null || echo 0) ))
	if [ "${SINCE_RESTART}" -ge $((RESTART_COOLDOWN_MIN * 60)) ]; then
		printf '{"last_block": %s, "last_timestamp": "%s", "chain": "solana", "reset_by": "watchdog-data-freshness"}\n' "${SOL_DATA}" "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" >"${SOL_STATE_FILE}"
		if restart_if_cooldown_elapsed "data-freshness failure: solana:hw=${SOL_HW}:data=${SOL_DATA} (highwater reset to last real data)"; then
			exit 2
		fi
	else
		log "data-freshness: solana broken (hw=${SOL_HW} data=${SOL_DATA}) but restart throttled — reset deferred (would be overwritten)"
		exit 1
	fi
fi

# ── 1c. EVM silent-indexing guard (2026-09-19) ───────────────────────
# The Solana guard above was chain-specific, so polygon and other EVM chains
# advanced their highwater state file while the rows actually landing in
# ClickHouse carried hours-old block timestamps — ingestion "succeeded" yet
# the chain head was frozen (polygon: highwater advancing, max
# block_timestamp 5.7h behind). Generalise the highwater↔real-data check to
# every CONFIGURED EVM chain (any highwater_*.json present in the state dir
# and present in wallet_memory.evm_transfers, solana excluded) using block
# TIMESTAMPS: alarm when the state file's wall-clock save time leads the
# newest indexed block_timestamp by more than EVM_DATA_LAG_MIN minutes.
# NOTIFY-ONLY by design: rewinding an EVM highwater to an hours-old block
# would queue a multi-hundred-thousand-block backfill (out of scope), and
# catch-up stays notify-only elsewhere — so no reset/restart here.
EVM_DATA_LAG_MIN="${EVM_DATA_LAG_MIN:-30}"
EVM_LAG_NOTIFY_FILE="${EVM_LAG_NOTIFY_FILE:-/var/data/chain-indexer/lite-indexer-evm-lag-notify-ts}"
EVM_LAG_NOTIFY_COOLDOWN_S="${EVM_LAG_NOTIFY_COOLDOWN_S:-21600}"
EVM_LAG_FOUND=0
export EVM_DATA_LAG_MIN
EVM_DATA_LAG_CHAINS="$(
	timeout 30 "${PYTHON}" - <<'PYEOF'
import datetime
import glob
import json
import os

try:
    import clickhouse_driver

    c = clickhouse_driver.Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        user=os.getenv("CLICKHOUSE_USER", "rmi"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    rows = c.execute(
        "SELECT chain, max(block_timestamp) FROM wallet_memory.evm_transfers GROUP BY chain"
    )
    ch_max = {str(r[0]): r[1] for r in rows if r[0] is not None}
except Exception:
    raise SystemExit(0)

threshold = float(os.getenv("EVM_DATA_LAG_MIN", "30"))
state_dir = os.getenv("STATE_DIR", "/var/data/chain-indexer/state")
out = []
for path in sorted(glob.glob(state_dir + "/highwater_*.json")):
    chain = os.path.basename(path)[len("highwater_"):-len(".json")]
    if chain == "solana":
        continue
    if chain not in ch_max:
        continue  # not configured / no rows for this chain
    try:
        hw = json.load(open(path))
        hw_ts = datetime.datetime.fromisoformat(
            str(hw.get("last_timestamp")).replace("Z", "+00:00")
        )
        if hw_ts.tzinfo is None:
            hw_ts = hw_ts.replace(tzinfo=datetime.timezone.utc)
        ch_ts = ch_max[chain]
        if ch_ts.tzinfo is None:
            ch_ts = ch_ts.replace(tzinfo=datetime.timezone.utc)
    except Exception:
        continue
    lag_min = (hw_ts - ch_ts).total_seconds() / 60.0
    if lag_min > threshold:
        out.append(
            "%s:hw=%s:ch=%s:lag=%.0fm"
            % (
                chain,
                hw_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ch_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                lag_min,
            )
        )
print(" ".join(out))
PYEOF
)"
if [ -n "${EVM_DATA_LAG_CHAINS}" ]; then
	EVM_LAG_FOUND=1
	EVM_LAG_TS="$(cat "${EVM_LAG_NOTIFY_FILE}" 2>/dev/null || echo 0)"
	if [ "$(( $(date +%s) - EVM_LAG_TS ))" -ge "${EVM_LAG_NOTIFY_COOLDOWN_S}" ]; then
		date +%s >"${EVM_LAG_NOTIFY_FILE}" 2>/dev/null || true
		alert_json "LiteIndexer EVM chain head frozen (>${EVM_DATA_LAG_MIN}m)" \
			"highwater advancing but indexed block_timestamp is stale (notify-only, no restart): ${EVM_DATA_LAG_CHAINS}" \
			"critical" || true
	fi
	log "EVM data-freshness: ${EVM_DATA_LAG_CHAINS} (notify-only, no restart)"
fi
if [ -n "${STALE_CHAINS}" ]; then
	# 2026-09-17 GLOBAL-PROGRESS GUARD: if ANY chain is writing data right now,
	# the daemon is demonstrably working — chains waiting for their turn (or
	# slow catch-up batches) are NOT stuck, and bouncing the daemon would
	# interrupt every in-flight batch. Restart only when NOTHING is writing
	# AND chains are frozen. (bsc/zksync/ethereum 2026-09-16 thrash: the
	# watchdog interrupted 87k-row batches.)
	if [ -n "${FRESH_CHAINS}" ]; then
		CATCHUP_TS="$(cat "${CATCHUP_NOTIFY_FILE}" 2>/dev/null || echo 0)"
		if [ "$(( $(date +%s) - CATCHUP_TS ))" -ge "${CATCHUP_NOTIFY_COOLDOWN_S}" ]; then
			date +%s >"${CATCHUP_NOTIFY_FILE}" 2>/dev/null || true
			alert_json "LiteIndexer catching up (no restart)" "daemon active (fresh: ${FRESH_CHAINS}); waiting/frozen: ${STALE_CHAINS}" "info" || true
		fi
		log "daemon active (fresh=[${FRESH_CHAINS}]) — no restart; lagging: ${STALE_CHAINS}"
		exit 1
	fi
	if [ -n "${NO_ADVANCE_CHAINS}" ]; then
		if restart_if_cooldown_elapsed "stalled chain(s) >${STALL_MINUTES}m state age: ${STALE_CHAINS} (no-advance: ${NO_ADVANCE_CHAINS})"; then
			exit 2
		fi
		exit 1
	fi
	exit 1
fi
# ── 2b. DATA-FRESHNESS check (2026-09-01 self-healing) ───────────────
# Blind spot fixed: the checks above trust the STATE FILE. The 2026-08-24
# solana incident: the strategy extracted 0 events for 8 days while the
# highwater advanced normally — state file fresh, ZERO rows written. The
# watchdog saw "advancing" and never fired. We now compare the chain's
# actual last-written row (ClickHouse max(slot) on its LIVE table — for
# solana that is solana_spl_balance_changes; solana_transfers is dormant
# since 2026-08-24 and produced a false extraction-failure alert on 09-17) A chain whose data is >DATA_STALE_MIN behind its
# state file is silently failing extraction → reset the highwater to the
# last slot that produced real data (from the table, not the state file)
# and restart, so the fixed strategy re-extracts the gap.
DATA_FRESH_CHAINS="$(
	timeout 30 "${PYTHON}" - <<'PYEOF'
import json, os, sys
import clickhouse_driver

STATE_DIR = "/var/data/chain-indexer/state"
STALE_MIN = 120  # data older than this vs state file = silent extraction failure

try:
    c = clickhouse_driver.Client(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        user=os.getenv("CLICKHOUSE_USER", "rmi"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    # Per-chain last real data slot from the tables themselves.
    rows = c.execute(
        "SELECT chain, max(slot) FROM wallet_memory.solana_spl_balance_changes GROUP BY chain"
    )
    ch_max = {r[0]: int(r[1] or 0) for r in rows}
except Exception:
    sys.exit(0)

stale = []
for chain in ("solana",):
    hw_path = f"{STATE_DIR}/highwater_{chain}.json"
    try:
        hw = json.load(open(hw_path))
        hw_block = int(hw.get("last_block") or 0)
    except Exception:
        continue
    try:
        (max_slot,) = c.execute(
            f"SELECT max(slot) FROM wallet_memory.solana_spl_balance_changes"
        )[0]
    except Exception:
        continue
    gap = int(hw.get("last_block") or 0) - int(max_slot or 0)
    # State highwater more than 200k slots AHEAD of real data = the
    # extraction is silently producing nothing (2026-08-24 mode).
    if int(hw.get("last_block") or 0) > 0 and int(max_slot or 0) > 0 and gap > 200000:
        stale.append(f"{chain}:hw={hw['last_block']}:data={max_slot}")
print(" ".join(stale))
PYEOF
)"

if [ -n "${DATA_FRESH_CHAINS}" ]; then
	# Reset the highwater to the last slot with REAL data so the fixed
	# strategy re-extracts the gap instead of advancing past it.
	for chain in solana; do
		LAST_SLOT="$(timeout 20 "${PYTHON}" -c "
import clickhouse_driver, os
c = clickhouse_driver.Client(host=os.getenv('CLICKHOUSE_HOST','127.0.0.1'), user=os.getenv('CLICKHOUSE_USER','rmi'), password=os.getenv('CLICKHOUSE_PASSWORD',''))
print(c.execute('SELECT max(slot) FROM wallet_memory.solana_spl_balance_changes')[0][0])
" 2>/dev/null || echo 0)"
		if [ "${LAST_SLOT:-0}" -gt 0 ] 2>/dev/null; then
			printf '{"last_block": %s, "last_timestamp": "%s", "chain": "%s", "reset_by": "watchdog-data-freshness"}
' "${LAST_SLOT}" "$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" "${chain}" >"/var/data/chain-indexer/state/highwater_${chain}.json"
			log "highwater_${chain} reset to last real data slot ${LAST_SLOT} (silent extraction failure detected)"
		fi
	done
	if restart_if_cooldown_elapsed "data-freshness failure: ${DATA_FRESH_CHAINS} (highwater reset to last real data)"; then
		exit 2
	fi
	exit 1
fi

# ── 3. Chains behind but making progress — informational ────────────
BEHIND_CHAINS="$(printf '%s' "${LAG_OUT}" | "${PYTHON}" -c '
import json, sys
data = None
for line in sys.stdin.read().splitlines():
    try:
        candidate = json.loads(line)
    except Exception:
        continue
    if isinstance(candidate, dict):
        data = candidate
if data is None:
    sys.exit(0)
behind = []
for c, info in data.items():
    if info.get("status") == "behind":
        behind.append(c + ":" + str(info.get("lag_blocks", 0)))
print(" ".join(behind))
')"

if [ -n "${BEHIND_CHAINS}" ]; then
	log "chains behind (normal catch-up): ${BEHIND_CHAINS}"
	exit 1
fi

if [ "${EVM_LAG_FOUND:-0}" -eq 1 ]; then
	log "EVM data-freshness lag present — notify-only (see above)"
	exit 1
fi

log "all chains healthy"
exit 0
