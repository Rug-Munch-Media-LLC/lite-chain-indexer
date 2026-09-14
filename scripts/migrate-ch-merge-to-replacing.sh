#!/usr/bin/env bash
# migrate-ch-merge-to-replacing.sh
#
# Staged, idempotent, resumable migration: plain MergeTree -> ReplacingMergeTree
# + PARTITION BY for the wallet_memory hot transfer tables.
#
# WHY THIS EXISTS
#   ClickHouse 26.7.1 REJECTS in-place `ALTER TABLE ... ENGINE = ...` (both the
#   `ENGINE=` and `MODIFY ENGINE=` forms are SYNTAX_ERROR). Converting an engine
#   therefore requires create-new + copy + swap. At 2.0B rows (evm_transfers,
#   ~400 GiB on disk) that must be resumable and reversible:
#     * ReplacingMergeTree dedups on the ORDER BY key -> safe re-index / rewind
#       backfill (BACKFILL_INCLUDE_REWINDS=1) without row explosion.
#     * PARTITION BY toYYYYMM(block_timestamp) -> partition-level TTL and
#       cheaper merges at 2B rows.
#
# DESIGN NOTES (validated on CH 26.7.1, see docs/MIGRATION_MERGETREE_REPLACING.md)
#   * `CREATE TABLE new AS old ENGINE = ...` preserves columns, codecs, column
#     TTLs and PROJECTIONS, but DROPS the table-level TTL -> we re-state it.
#   * evm_transfers has projections; ReplacingMergeTree + projections requires
#     deduplicate_merge_projection_mode = 'rebuild' (default 'throw' errors).
#   * The copy sets optimize_on_insert = 0 so raw row-count parity holds. The
#     source already contains duplicate ORDER BY keys (re-index history); dedup
#     is the POINT of this migration and happens lazily during merges / at query
#     time with FINAL. Without optimize_on_insert=0 a small INSERT collapses
#     duplicates and parity checks fail.
#   * Copy unit is per-chain where the chain column is the ORDER BY prefix
#     (primary-key range scan); per-month where it is not. The source is NOT
#     partitioned, so a month unit is a full scan -- see the runbook.
#   * Swap is `EXCHANGE TABLES` (atomic DB) then RENAME old -> t_old_<ts>.
#     Nothing is ever dropped. Rollback command is printed.
#
# USAGE
#   scripts/migrate-ch-merge-to-replacing.sh --dry-run
#   scripts/migrate-ch-merge-to-replacing.sh --phase create
#   scripts/migrate-ch-merge-to-replacing.sh --phase copy
#   scripts/migrate-ch-merge-to-replacing.sh --phase swap
#   scripts/migrate-ch-merge-to-replacing.sh --validate-clone
#
# ENV
#   CH_DB                 database (default wallet_memory)
#   CH_CONTAINER          docker container holding the server (default rmi-clickhouse)
#   CH_BIN                client binary (default clickhouse-client)
#   MIGRATION_STATE_DIR   resume state directory
#   CH_DISK_PATH          path to check free space (default /data/clickhouse)
#   CH_COPY_MARGIN        required free-space multiplier over table size (default 1.25)
#
set -euo pipefail

DB="${CH_DB:-wallet_memory}"
CH_CONTAINER="${CH_CONTAINER:-rmi-clickhouse}"
CH_BIN="${CH_BIN:-clickhouse-client}"
STATE_DIR="${MIGRATION_STATE_DIR:-/var/lib/lite-indexer/migration-merge-to-replacing}"
DISK_PATH="${CH_DISK_PATH:-/data/clickhouse}"
COPY_MARGIN="${CH_COPY_MARGIN:-1.25}"

TS="$(date -u +%Y%m%d%H%M%S)"
PHASE="all"
DRY_RUN=0
VALIDATE_CLONE=0
ONLY_TABLE=""
UNIT_OVERRIDE=""
SAMPLE_ROWS=10000

# NOTE: only UNPARTITIONED plain-MergeTree tables belong here — migration buys
# them partition-level TTL drops + dedup. Deliberately EXCLUDED:
#   * solana_spl_balance_changes — already PARTITION BY toYYYYMM(block_timestamp)
#     (TTL burns), and ORDER BY (mint, slot, token_account) is NOT unique (a token
#     account can change several times per slot) -> RMT would silently drop rows.
#   * deployer_history — already partitioned by toYYYYMM(deployed_at).
#   * scam_addresses / deployer_events / evm_launches / solana_launches /
#     tron_launches — tiny or not time-series; TTL burn is negligible.
TABLES=(evm_transfers internal_transfers solana_transfers dex_events wallet_labels)
declare -A TS_COL=(
    [evm_transfers]="block_timestamp"
    [internal_transfers]="block_timestamp"
    [solana_transfers]="block_timestamp"
    [dex_events]="block_timestamp"
    [wallet_labels]="loaded_at"
)
declare -A ORDER_BY=(
    [evm_transfers]="chain, block_number, log_index"
    [internal_transfers]="chain, block_number, tx_hash, trace_address"
    [solana_transfers]="signature, instruction_path"
    [dex_events]="chain, block_number, log_index"
    [wallet_labels]="address, label_name, source"
)
# Table-level TTL is re-stated explicitly: CREATE ... AS ... ENGINE= drops it
# (column-level TTLs and projections ARE preserved by CREATE ... AS).
# Values mirror the live schema (migration 012 defines the longer-term policy).
declare -A TTL_EXPR=(
    [evm_transfers]="ingested_at + toIntervalDay(90)"
    [internal_transfers]="block_timestamp + toIntervalDay(90)"
    [solana_transfers]="ingested_at + toIntervalDay(30)"
    [dex_events]="ingested_at + toIntervalDay(90)"
    [wallet_labels]="loaded_at + toIntervalMonth(6)"
)
# Copy unit: chain = primary-key prefix range scan (cheap, resumable).
# partition = month of block_timestamp (full scan on an unpartitioned source).
declare -A COPY_UNIT=(
    [evm_transfers]="chain"
    [internal_transfers]="partition"
    [solana_transfers]="partition"
    [dex_events]="chain"
    [wallet_labels]="partition"
)

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
warn() { printf '[%s] WARN: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2; }
die() {
    printf '[%s] ERROR: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
    exit 1
}

usage() {
    sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

if command -v docker >/dev/null 2>&1 && docker inspect "$CH_CONTAINER" >/dev/null 2>&1; then
    _ch_mode="docker"
else
    _ch_mode="local"
fi

ch() {
    if [[ "$_ch_mode" == "docker" ]]; then
        docker exec "$CH_CONTAINER" "$CH_BIN" --query "$1"
    else
        "$CH_BIN" --query "$1"
    fi
}

sql_escape() { printf '%s' "$1" | sed "s/'/''/g"; }

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --validate-clone) VALIDATE_CLONE=1 ;;
        --phase)
            PHASE="${2:?--phase needs a value}"
            shift
            ;;
        --table)
            ONLY_TABLE="${2:?--table needs a value}"
            shift
            ;;
        --unit)
            UNIT_OVERRIDE="${2:?--unit needs a value}"
            shift
            ;;
        --sample-rows)
            SAMPLE_ROWS="${2:?--sample-rows needs a value}"
            shift
            ;;
        -h | --help) usage ;;
        *) die "unknown argument: $1 (try --help)" ;;
        esac
        shift
    done
    case "$PHASE" in create | copy | swap | all) ;; *) die "--phase must be create|copy|swap|all" ;; esac
    if [[ -n "$UNIT_OVERRIDE" ]]; then
        case "$UNIT_OVERRIDE" in chain | partition) ;; *) die "--unit must be chain|partition" ;; esac
    fi
}

table_list() {
    if [[ -n "$ONLY_TABLE" ]]; then
        local found=0 t
        for t in "${TABLES[@]}"; do if [[ "$t" == "$ONLY_TABLE" ]]; then found=1; fi; done
        [[ "$found" == 1 ]] || die "unknown table: $ONLY_TABLE"
        printf '%s\n' "$ONLY_TABLE"
    else
        printf '%s\n' "${TABLES[@]}"
    fi
}

new_name() { printf '%s_new' "$1"; }
old_name() { printf '%s_old_%s' "$1" "$TS"; }
test_name() { printf '%s__migtest' "$1"; }
state_file() { printf '%s/%s.%s.state' "$STATE_DIR" "$DB" "$1"; }

unit_for() {
    local t="$1"
    if [[ -n "$UNIT_OVERRIDE" ]]; then printf '%s' "$UNIT_OVERRIDE"; else printf '%s' "${COPY_UNIT[$t]}"; fi
}

ts_for() { printf '%s' "${TS_COL[$1]}"; }

ddl_create() {
    local t="$1" name="$2"
    printf 'CREATE TABLE IF NOT EXISTS `%s`.`%s` AS `%s`.`%s` ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(`%s`) ORDER BY (%s) TTL %s SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = %s' \
        "$DB" "$name" "$DB" "$t" "$(ts_for "$t")" "${ORDER_BY[$t]}" "${TTL_EXPR[$t]}" "'rebuild'"
}

unit_predicate() {
    local t="$1" unit="$2" value="$3"
    if [[ "$unit" == "chain" ]]; then
        printf "chain = '%s'" "$(sql_escape "$value")"
    else
        printf 'toYYYYMM(`%s`) = %s' "$(ts_for "$t")" "$value"
    fi
}

enumerate_units() {
    local t="$1" unit="$2"
    if [[ "$unit" == "chain" ]]; then
        ch "SELECT DISTINCT chain FROM \`$DB\`.\`$t\` ORDER BY chain"
    else
        ch "SELECT DISTINCT toYYYYMM(\`$(ts_for "$t")\`) FROM \`$DB\`.\`$t\` ORDER BY 1"
    fi
}

table_exists() {
    local name="$1"
    [[ "$(ch "SELECT count() FROM system.tables WHERE database='$(sql_escape "$DB")' AND name='$(sql_escape "$name")'")" == "1" ]]
}

col_int() { ch "SELECT $1 FROM system.tables WHERE database='$(sql_escape "$DB")' AND name='$(sql_escape "$2")'"; }

count_rows() { ch "SELECT count() FROM \`$DB\`.\`$1\`"; }

bytes_on_disk() {
    ch "SELECT ifNull(sum(bytes_on_disk),0) FROM system.parts WHERE database='$(sql_escape "$DB")' AND table='$(sql_escape "$1")' AND active"
}

human_bytes() { numfmt --to=iec --suffix=B "$1" 2>/dev/null || printf '%sB' "$1"; }

# Safety gate: ReplacingMergeTree dedups on the ORDER BY key, so the key must be
# unique per logical row -- otherwise the migration SILENTLY drops legitimate
# rows (this is exactly why solana_spl_balance_changes is excluded). uniq() is
# HLL (memory-bounded) so the check does not OOM on billion-row tables; it is
# approximate, so only a *material* collision ratio blocks. SKIP_UNIQUE_CHECK=1
# overrides.
key_collision_ratio() {
    local t="$1"
    # Unbiased hash sample over the WHOLE table (a plain LIMIT samples by part order,
    # which skewed a known-unique key to a bogus ~52%). A failed check must NOT read
    # as "unique", so the caller validates the output is numeric.
    ch "SELECT if(count() = 0, 0, 1 - uniq(${ORDER_BY[$t]}) / count()) \
FROM \`$DB\`.\`$t\` WHERE cityHash64(${ORDER_BY[$t]}) % ${UNIQUE_SAMPLE_MOD:-20} = 0"
}

# ---------------------------------------------------------------------------
preflight() {
    local t="$1" name_new
    name_new="$(new_name "$t")"
    log "preflight: $DB.$t"

    table_exists "$t" || die "source table $DB.$t does not exist"

    local engine pk rows bytes parts
    engine="$(col_int engine "$t")"
    pk="$(col_int partition_key "$t")"
    rows="$(count_rows "$t")"
    bytes="$(bytes_on_disk "$t")"
    parts="$(ch "SELECT count() FROM system.parts WHERE database='$(sql_escape "$DB")' AND table='$(sql_escape "$t")' AND active")"

    printf '  engine        : %s\n' "$engine"
    printf '  partition_key : %s\n' "${pk:-<none>}"
    printf '  rows          : %s\n' "$rows"
    printf '  bytes on disk : %s (%s)\n' "$(human_bytes "$bytes")" "$bytes"
    printf '  active parts  : %s\n' "$parts"

    if [[ "$engine" == "ReplacingMergeTree" && -n "$pk" ]]; then
        log "  -> already migrated (ReplacingMergeTree, PARTITION BY $pk); nothing to do"
        return 2
    fi
    if [[ "$engine" != "MergeTree" ]]; then
        die "refusing: $DB.$t engine is '$engine', expected plain MergeTree"
    fi

    # ORDER BY key uniqueness: RMT dedups on this key, so a material collision
    # ratio means the migration would drop legitimate rows. Runs only for tables
    # actually being migrated (already-migrated tables returned above).
    if [[ "${SKIP_UNIQUE_CHECK:-0}" != "1" && "$rows" != "0" ]]; then
        local ratio pct
        ratio="$(key_collision_ratio "$t")"
        if [[ ! "$ratio" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
            warn "key uniqueness check unavailable for $DB.$t (query failed, likely the \
ClickHouse memory cap); proceeding WITHOUT the RMT row-loss guard"
        else
            pct="$(awk -v r="$ratio" 'BEGIN{printf "%.4f", r*100}')"
            printf '  key collisions: ~%s%% (sampled/approximate; ORDER BY: %s)\n' "$pct" "${ORDER_BY[$t]}"
            # The check is sampled + HLL, so a few percent is estimation noise (a
            # known-unique key measured ~3.9% on 312 M rows). Only a GROSS collision
            # ratio blocks; the definitive unsafe tables are excluded in TABLES above.
            if awk -v r="$ratio" 'BEGIN{exit !(r > 0.15)}'; then
                die "refusing: ORDER BY (${ORDER_BY[$t]}) collides on ~${pct}% of $DB.$t rows \
(order-of-magnitude collision — RMT would drop rows). Fix the key or set SKIP_UNIQUE_CHECK=1."
            elif awk -v r="$ratio" 'BEGIN{exit !(r > 0.05)}'; then
                warn "ORDER BY (${ORDER_BY[$t]}) has ~${pct}% approximate collision in $DB.$t \
(>> the ~2-3% HLL/sampling error); verify before copy"
            fi
        fi
    fi

    local avail=""
    if [[ -d "$DISK_PATH" ]]; then
        avail="$(df -B1 --output=avail "$DISK_PATH" 2>/dev/null | tail -1 | tr -d ' ')"
    fi
    if [[ -n "$avail" ]]; then
        printf '  free on %s: %s (%s)\n' "$DISK_PATH" "$(human_bytes "$avail")" "$avail"
        local need
        need="$(awk -v b="$bytes" -v m="$COPY_MARGIN" 'BEGIN{printf "%.0f", b*m}')"
        printf '  required   : %s (table x %s)\n' "$(human_bytes "$need")" "$COPY_MARGIN"
        if ((avail < need)); then
            if [[ "$DRY_RUN" == 1 ]]; then
                warn "insufficient free space for a safe copy of $DB.$t \
(need $(human_bytes "$need") incl. merge headroom, have $(human_bytes "$avail")) -- dry-run continues"
            else
                die "insufficient free space for a safe copy of $DB.$t \
(need $(human_bytes "$need") incl. merge headroom, have $(human_bytes "$avail"))"
            fi
        fi
    else
        warn "disk path '$DISK_PATH' not found; skipping free-space check"
    fi

    local ddl
    ddl="$(ddl_create "$t" "$name_new")"
    printf '  DDL           : %s\n' "$ddl"

    local unit u
    unit="$(unit_for "$t")"
    printf '  copy unit     : %s\n' "$unit"
    if [[ "$unit" == "partition" && "$(col_int partition_key "$t")" == "" ]]; then
        warn "source is UNPARTITIONED: each month unit is a FULL SCAN of $DB.$t"
    fi
    if [[ "$DRY_RUN" == 1 ]]; then
        printf '  units         :\n'
        while IFS= read -r u; do
            [[ -n "$u" ]] || continue
            printf "    - %-12s -> INSERT INTO %s.%s SETTINGS optimize_on_insert=0 SELECT * FROM %s.%s WHERE %s\n" \
                "$u" "$DB" "$name_new" "$DB" "$t" "$(unit_predicate "$t" "$unit" "$u")"
        done < <(enumerate_units "$t" "$unit")
        printf '  swap          : EXCHANGE TABLES %s.%s AND %s.%s\n' "$DB" "$t" "$DB" "$name_new"
        printf '                  RENAME TABLE %s.%s TO %s.%s\n' "$DB" "$name_new" "$DB" "$(old_name "$t")"
        printf '  rollback      : EXCHANGE TABLES %s.%s AND %s.%s\n' "$DB" "$t" "$DB" "$(old_name "$t")"
    fi
    return 0
}

# ---------------------------------------------------------------------------
create_target() {
    local t="$1" name_new
    name_new="$(new_name "$t")"
    if table_exists "$name_new"; then
        log "target $DB.$name_new already exists; validating"
        local e pk
        e="$(col_int engine "$name_new")"
        pk="$(col_int partition_key "$name_new")"
        [[ "$e" == "ReplacingMergeTree" && -n "$pk" ]] || die "existing $DB.$name_new is not ReplacingMergeTree+partitioned"
        return 0
    fi
    local ddl
    ddl="$(ddl_create "$t" "$name_new")"
    if [[ "$DRY_RUN" == 1 ]]; then
        log "DRY-RUN create: $ddl"
        return 0
    fi
    log "creating $DB.$name_new"
    ch "$ddl"
    # Freeze merges on the target for the whole copy window: ReplacingMergeTree
    # dedups during merges, which would break raw row-count parity. Merges are
    # resumed after the swap.
    ch "SYSTEM STOP MERGES \`$DB\`.\`$name_new\`"
    log "merges stopped on $DB.$name_new (resumed after swap)"
}

state_fingerprint() {
    local t="$1" name_new
    name_new="$(new_name "$t")"
    ch "SHOW CREATE TABLE \`$DB\`.\`$name_new\`" | sha256sum | awk '{print $1}'
}

state_init() {
    local t="$1" sf="$2" fp
    fp="$(state_fingerprint "$t")"
    if [[ -f "$sf" ]]; then
        local have
        have="$(sed -n 's/^# fingerprint=//p' "$sf" | head -1)"
        if [[ "$have" != "$fp" ]]; then
            die "state fingerprint mismatch for $DB.$(new_name "$t"); refusing to resume. \
Review and remove $sf to restart."
        fi
        return 0
    fi
    mkdir -p "$STATE_DIR"
    printf '# fingerprint=%s\n' "$fp" >"$sf"
}

unit_done() {
    local sf="$1" unit="$2"
    grep -q "^${unit}=OK" "$sf" 2>/dev/null
}

mark_unit_done() {
    local sf="$1" unit="$2" rows="$3"
    printf '%s=OK rows=%s\n' "$unit" "$rows" >>"$sf"
}

copy_units() {
    local t="$1" name_new
    name_new="$(new_name "$t")"
    local unit sf u src_rows tgt_rows t0 t1
    unit="$(unit_for "$t")"
    sf="$(state_file "$t")"

    if [[ "$DRY_RUN" == 1 ]]; then
        log "DRY-RUN copy: $DB.$t -> $DB.$name_new (unit=$unit)"
        return 0
    fi
    state_init "$t" "$sf"

    # Idempotent guard: ensure merges are still frozen (resume / server restart).
    ch "SYSTEM STOP MERGES \`$DB\`.\`$name_new\`"

    while IFS= read -r u; do
        [[ -n "$u" ]] || continue
        if unit_done "$sf" "$u"; then
            log "unit $u already copied; verifying parity"
        else
            log "copying unit $u ..."
            t0=$SECONDS
            # shellcheck disable=SC2016
            # Bounded-memory copy: the 8 GiB CH container OOM'd a default
            # INSERT SELECT (rc=241, 2026-09-14). Fewer threads + smaller blocks
            # keep the copy within budget alongside other server activity.
            ch "INSERT INTO \`$DB\`.\`$name_new\` SETTINGS optimize_on_insert=0, max_threads=2, max_insert_block_size=65536 SELECT * FROM \`$DB\`.\`$t\` WHERE $(unit_predicate "$t" "$unit" "$u")"
            t1=$SECONDS
            log "unit $u inserted in $((t1 - t0))s"
        fi
        src_rows="$(ch "SELECT count() FROM \`$DB\`.\`$t\` WHERE $(unit_predicate "$t" "$unit" "$u")")"
        tgt_rows="$(ch "SELECT count() FROM \`$DB\`.\`$name_new\` WHERE $(unit_predicate "$t" "$unit" "$u")")"
        if [[ "$src_rows" != "$tgt_rows" ]]; then
            die "parity FAIL for $DB.$t unit '$u': source=$src_rows target=$tgt_rows (not swapping)"
        fi
        unit_done "$sf" "$u" || mark_unit_done "$sf" "$u" "$src_rows"
        log "unit $u parity OK ($src_rows rows)"
    done < <(enumerate_units "$t" "$unit")
}

parity_total() {
    local t="$1" name_new
    name_new="$(new_name "$t")"
    local s n
    s="$(count_rows "$t")"
    n="$(count_rows "$name_new")"
    printf '  total rows: source=%s target=%s\n' "$s" "$n"
    [[ "$s" == "$n" ]] || die "TOTAL parity FAIL for $DB.$t: source=$s target=$n"
    log "total parity OK: $s rows"
}

swap_tables() {
    local t="$1" name_new old
    name_new="$(new_name "$t")"
    old="$(old_name "$t")"
    if [[ "$DRY_RUN" == 1 ]]; then
        log "DRY-RUN swap: EXCHANGE TABLES $DB.$t AND $DB.$name_new; RENAME $DB.$name_new -> $DB.$old"
        return 0
    fi
    log "EXCHANGE TABLES $DB.$t AND $DB.$name_new"
    ch "EXCHANGE TABLES \`$DB\`.\`$t\` AND \`$DB\`.\`$name_new\`"
    log "RENAME TABLE $DB.$name_new TO $DB.$old"
    ch "RENAME TABLE \`$DB\`.\`$name_new\` TO \`$DB\`.\`$old\`"
    log "resuming merges on $DB.$t (dedup is now lazy, as designed)"
    ch "SYSTEM START MERGES \`$DB\`.\`$t\`"
    log "migration complete for $DB.$t"
    printf '  ROLLBACK: EXCHANGE TABLES %s.%s AND %s.%s\n' "$DB" "$t" "$DB" "$old"
    printf '  (old data retained as %s.%s; nothing was dropped)\n' "$DB" "$old"
}

# ---------------------------------------------------------------------------
validate_clone() {
    local t="$1" cl
    cl="$(test_name "$t")"
    log "clone validation: $DB.$cl (sample=$SAMPLE_ROWS rows, dropped afterwards)"
    local ddl
    ddl="$(ddl_create "$t" "$cl")"
    printf 'DDL: %s\n' "$ddl"

    ch "DROP TABLE IF EXISTS \`$DB\`.\`$cl\`"
    ch "$ddl"
    # Merges must be frozen for raw parity (RMT dedups during merges).
    ch "SYSTEM STOP MERGES \`$DB\`.\`$cl\`"

    local src_total inserted
    src_total="$(count_rows "$t")"
    ch "INSERT INTO \`$DB\`.\`$cl\` SETTINGS optimize_on_insert=0 SELECT * FROM \`$DB\`.\`$t\` LIMIT $SAMPLE_ROWS"
    inserted="$(ch "SELECT count() FROM \`$DB\`.\`$cl\`")"

    local expected
    expected="$SAMPLE_ROWS"
    if ((src_total < SAMPLE_ROWS)); then expected="$src_total"; fi

    printf '%s\n' "--- clone report: $DB.$cl ---"
    ch "SELECT name, engine, partition_key, sorting_key, engine_full FROM system.tables WHERE database='$(sql_escape "$DB")' AND name='$(sql_escape "$cl")' FORMAT Vertical"
    printf 'expected_raw=%s inserted_raw=%s deduped_final=%s\n' \
        "$expected" "$inserted" "$(ch "SELECT count() FROM \`$DB\`.\`$cl\` FINAL")"

    [[ "$inserted" == "$expected" ]] || die "clone raw count mismatch: expected=$expected got=$inserted"

    ch "SYSTEM START MERGES \`$DB\`.\`$cl\`"
    ch "DROP TABLE \`$DB\`.\`$cl\`"
    log "clone $DB.$cl dropped"
}

# ---------------------------------------------------------------------------
main() {
    parse_args "$@"
    log "mode=$([ "$VALIDATE_CLONE" == 1 ] && echo validate-clone || echo "phase:$PHASE") dry_run=$DRY_RUN db=$DB"

    local t
    if [[ "$VALIDATE_CLONE" == 1 ]]; then
        while IFS= read -r t; do validate_clone "$t"; done < <(table_list)
        log "clone validation complete"
        return 0
    fi

    while IFS= read -r t; do
        local rc=0
        preflight "$t" || rc=$?
        if [[ "$rc" == 2 ]]; then continue; fi
        if [[ "$DRY_RUN" == 1 ]]; then continue; fi
        case "$PHASE" in
        create) create_target "$t" ;;
        copy)
            create_target "$t"
            copy_units "$t"
            parity_total "$t"
            ;;
        swap)
            parity_total "$t"
            swap_tables "$t"
            ;;
        all)
            create_target "$t"
            copy_units "$t"
            parity_total "$t"
            swap_tables "$t"
            ;;
        esac
    done < <(table_list)

    log "done"
}

main "$@"
