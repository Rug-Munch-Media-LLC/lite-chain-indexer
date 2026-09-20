"""Storage configuration for LiteChainIndexer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Load the shared env file (same file the daemon's systemd unit sources
# via EnvironmentFile) BEFORE any os.environ reads below, so every entry
# point resolves the same ${KEYED_RPC_KEY} / ${KEYED_RPC_KEY_2} / ${SOLANA_PROVIDER_KEY} RPC
# placeholders. Idempotent and non-destructive (existing env wins).
from lite_indexer.config.indexing import DATA_DIR, STATE_DIR
from lite_indexer.env import load_env_file

load_env_file()

# ClickHouse target tables
CH_TABLE = "wallet_memory.evm_transfers"
CH_DEX_TABLE = "wallet_memory.dex_events"
CH_DEPLOYER_TABLE = "wallet_memory.deployer_events"
# ── Internal-Transfer Trace Collection ────────────────────────────────
# The indexer normally only sees ERC-20 Transfer logs. Internal ETH flows
# (WETH unwraps, bridge deposits, fee flows, SELFDESTRUCT payouts) are
# invisible to eth_getLogs and live in wallet_memory.internal_transfers,
# collected from free trace_* / debug_traceBlockByNumber RPCs. Default
# ON — the collector is stable, bounded per-cycle (TRACE_MAX_BLOCKS_PER_CYCLE),
# and rate-limit-aware (TRACE_CONCURRENCY). Disable with TRACE_ENABLED=0 or
# --no-traces CLI flag.
TRACE_ENABLED: bool = os.environ.get("TRACE_ENABLED", "1") == "1"

# Trace calls can be slow on busy blocks (a full block trace returns a
# nested call tree) — use a longer timeout than the default RPC call.
TRACE_CALL_TIMEOUT: float = float(os.environ.get("TRACE_CALL_TIMEOUT", "60.0"))

# Max blocks swept per sync cycle when traces are enabled (bounds RPC
# load on free tiers — trace endpoints are rate-limited far below the
# getLogs tier).
TRACE_MAX_BLOCKS_PER_CYCLE: int = int(os.environ.get("TRACE_MAX_BLOCKS_PER_CYCLE", "100"))

# Concurrent trace calls per chain (trace endpoints are rate-limited).
TRACE_CONCURRENCY: int = int(os.environ.get("TRACE_CONCURRENCY", "2"))

# ClickHouse table for internal-transfer rows.
CH_TRACE_TABLE: str = "wallet_memory.internal_transfers"

# Address-activity keystone table — every transaction an address touched
# (free-RPC SolanaProvider getSignaturesForAddress parity). One row per
# (address, tx_signature); provisioned by migrations/014_address_activity.sql.
CH_ADDRESS_ACTIVITY_TABLE: str = os.environ.get("CH_ADDRESS_ACTIVITY_TABLE") or "wallet_memory.address_activity"

# ── Cross-Chain Table References ───────────────────────────────────────
# Read-side table references. The main transfer table is CH_TABLE above;
# these aliases keep readers decoupled from the writer's naming so either
# can evolve independently.
CH_DATABASE: str = os.environ.get("CH_DATABASE") or os.environ.get("CLICKHOUSE_DATABASE") or "wallet_memory"
CH_EVM_TRANSFERS_TABLE: str = os.environ.get("CH_EVM_TRANSFERS_TABLE") or CH_TABLE
CH_INTERNAL_TRANSFERS_TABLE: str = os.environ.get("CH_INTERNAL_TRANSFERS_TABLE") or CH_TRACE_TABLE
CH_SOLANA_TABLE: str = os.environ.get("CH_SOLANA_TABLE") or "wallet_memory.solana_spl_balance_changes"
CH_LABELS_TABLE: str = os.environ.get("CH_LABELS_TABLE") or "wallet_memory.wallet_labels"
CH_SCAM_TABLE: str = os.environ.get("CH_SCAM_TABLE") or "wallet_memory.scam_addresses"
CH_BRIDGE_MAPPINGS_TABLE: str = os.environ.get("CH_BRIDGE_MAPPINGS_TABLE") or "wallet_memory.bridge_mappings"
# ── ClickHouse Retention / Column Pruning ─────────────────────────────
# Keep full-precision rows for N days, then degrade non-summary columns
# to their type defaults via column-level TTL and (optionally) delete
# expired rows. Cuts storage 30-50% while keeping summary queries fast.
CH_TTL_ENABLED: bool = os.environ.get("CH_TTL_ENABLED", "1") != "0"
# ── Wallet Access Path Optimizations ──────────────────────────────────
# Each competitive feature below is independently usable and gated by
# its own env flag (default: on). All DDL is idempotent and non-fatal —
# a missing table or missing privileges degrades to a debug log, exactly
# like the retention policies.

# 1. Address-first access path (electrs-inspired): add projections on
#    evm_transfers ordered by (chain, address, block_number, log_index)
#    so "all transfers for wallet X" is served by an address-keyed
#    projection instead of a full table scan. ClickHouse projections
#    cannot UNION ALL, so the wallet history is split into two
#    projections: one keyed on from_address (out), one on to_address (in).
WALLET_PROJECTION_ENABLED: bool = os.environ.get("WALLET_PROJECTION_ENABLED", "1") != "0"

# 2. Token metadata reference table (cryo-inspired): one row per token in
#    wallet_memory.token_meta_ref instead of token_symbol/token_name on
#    every transfer row (~60 bytes x 278M rows). Always enabled and
#    non-configurable — the reference table approach is proven correct and
#    enrichment is never written to transfer rows anymore. Transfer rows
#    store empty symbol/name; the reference table is upserted once per
#    unique token per batch.
TOKEN_META_ENABLED: bool = True

# 3. Per-token incremental balance cache (electrs lazy cache): maintain
#    wallet_memory.holder_balances incrementally per transfer so holder
#    queries never aggregate the full evm_transfers table.
BALANCE_CACHE_ENABLED: bool = os.environ.get("BALANCE_CACHE_ENABLED", "1") != "0"

# 4. Auto-maintained holder balances: a SummingMergeTree materialized
#    view fed by evm_transfers inserts. The answer is pre-computed and
#    maintained by ClickHouse itself (no indexer-side read-modify-write).
HOLDER_BALANCES_MV_ENABLED: bool = os.environ.get("HOLDER_BALANCES_MV_ENABLED", "1") != "0"

# 5. DEX skip-to-head (env-gated, default OFF): when the sync window is
#    deeper than DEX_SKIP_TO_HEAD_HOURS behind the chain head, skip the
#    dex getLogs fetch for that cycle — old-block dex rows are useless to
#    the scanners and the freed RPC budget accelerates the transfer
#    catch-up. Dex resumes automatically once transfers reach fresh
#    blocks. NOTE: skipped ranges stay a hole in dex_events until a
#    backfill runs — enable only when draining a deep backlog fast
#    matters more than historical completeness.
DEX_SKIP_TO_HEAD: bool = os.environ.get("DEX_SKIP_TO_HEAD", "0") != "0"
DEX_SKIP_TO_HEAD_HOURS: int = int(os.environ.get("DEX_SKIP_TO_HEAD_HOURS", "24") or "24")

# Wallet-access table / projection names.
CH_TOKEN_META_TABLE: str = "wallet_memory.token_meta_ref"
CH_HOLDER_BALANCES_TABLE: str = "wallet_memory.holder_balances"
CH_HOLDER_BALANCES_MV: str = "wallet_memory.holder_balances_mv"
WALLET_OUT_PROJECTION: str = "wallet_out_proj"
WALLET_IN_PROJECTION: str = "wallet_in_proj"

@dataclass(frozen=True)
class RetentionRule:
    """Hot-window retention for one ClickHouse table.

    Attributes:
        table: Bare table name (no database prefix).
        days: Hot-window length in days; older partitions/rows are dropped
            by the table-level TTL once they are archived to B2 Parquet.
        time_column: Column the TTL anchors on. Event time is preferred so
            backfills of old blocks do not extend retention.
    """

    table: str
    days: int
    time_column: str


# ── Hot/cold retention classes (SINGLE SOURCE OF TRUTH) ────────────────
# Lean hot window in ClickHouse; everything older lives permanently as B2
# Parquet and is queried in place via s3(). Archive-before-drop is
# mandatory — a TTL must never outrun the archive pipeline. Keep this map in
# lock-step with migrations/013_retention_classes.sql, the runtime DDLs
# (which read this dict), and tests/test_retention.py.
RETENTION_POLICY: dict[str, RetentionRule] = {
    "evm_transfers": RetentionRule("evm_transfers", 30, "block_timestamp"),
    "dex_events": RetentionRule("dex_events", 90, "block_timestamp"),
    "internal_transfers": RetentionRule("internal_transfers", 90, "block_timestamp"),
    "solana_transfers": RetentionRule("solana_transfers", 90, "block_timestamp"),
    "scam_addresses": RetentionRule("scam_addresses", 180, "first_seen_at"),
    "wallet_labels": RetentionRule("wallet_labels", 180, "loaded_at"),
    "deployer_history": RetentionRule("deployer_history", 180, "deployed_at"),
    "evm_launches": RetentionRule("evm_launches", 365, "block_timestamp"),
    "solana_launches": RetentionRule("solana_launches", 365, "block_timestamp"),
    "tron_launches": RetentionRule("tron_launches", 365, "block_timestamp"),
}


# Retention (days) per table before column-level degradation.
RETENTION_DAYS: dict[str, int] = {
    "evm_transfers": 30,
    "dex_events": 90,
    "deployer_events": 365,
    "internal_transfers": 90,
    "address_activity": 365,
}

# Columns to KEEP (summary) for old rows — everything else degrades.
# evm_transfers summary: from, to, token, value, block_ts (plus row keys)
# dex_events summary: swap identity + pair + tokens (details degrade)
EVT_SUMMARY_COLUMNS: tuple[str, ...] = (
    "chain",
    "block_number",
    "tx_hash",
    "from_address",
    "to_address",
    "token_address",
    "value",
    "block_timestamp",
    "log_index",
)
DEX_SUMMARY_COLUMNS: tuple[str, ...] = (
    "type",
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "contract_address",
    "dex",
    "version",
    "token0",
    "token1",
)
# ── ClickHouse Health Pause ─────────────────────────────────────────────
# Graceful degradation when ClickHouse is unreachable: the sync loop
# probes CH before fetching events from RPC and, when CH is down, aborts
# the cycle with status "ch_unavailable" WITHOUT advancing state and
# WITHOUT burning free-tier RPC budget on fetches it cannot persist.
# The daemon treats "ch_unavailable" as a pause (no state movement, no
# error spam) rather than a crash.
CH_HEALTH_CHECK_ENABLED: bool = os.environ.get("CH_HEALTH_CHECK_ENABLED", "1") == "1"

# How often (seconds) the CH health probe result is cached before the
# sync loop re-probes (probe = SELECT 1 via the existing CH client).
CH_HEALTH_INTERVAL: int = int(os.environ.get("CH_HEALTH_INTERVAL", "30"))
# ── Parquet Cold Storage Tier ─────────────────────────────────────────
# Daily rollup of old ClickHouse rows into Parquet, uploaded to the B2
# permanent tier, then deleted from ClickHouse to reclaim disk.
# Optional dependency: pyarrow.
PARQUET_DIR: Path = DATA_DIR / "parquet"
# Remote root for the parquet archive. Canonical prefix (matches the
# live hourly export script /root/scripts/lite-indexer-parquet-daily.sh
# and the DuckDB read path in lite_indexer.storage.duckdb_query).
PARQUET_REMOTE: str = os.environ.get("PARQUET_REMOTE", "b2-crypt-v2:chain-data/parquet")
PARQUET_RCLONE_BIN: str = os.environ.get("PARQUET_RCLONE_BIN", "rclone")
# Min age (days) of rows eligible for cold storage (keep hot window).
# Used for the --days backfill mode; the daily job exports an explicit
# date (--date) instead.
PARQUET_COLD_DAYS: int = int(os.environ.get("PARQUET_COLD_DAYS", "7"))
# State dir records which (chain, date) files were already exported so
# the daily job is idempotent even when ClickHouse is unavailable.
PARQUET_STATE_DIR: Path = STATE_DIR / "parquet"
# ClickHouse audit table recording every (chain, date) export attempt
# and its status (pending / exporting / uploaded / deleted / failed).
CH_PARQUET_STATE_TABLE: str = "wallet_memory.parquet_export_state"

# ── On-chain-verifiable index attestations (lite_indexer/attestation.py) ──
# An incremental Merkle accumulator over committed (chain, range) leaves so a
# pre-indexed dataset can be verified trustlessly against the chain without
# trusting the producer. Write-only / observe by default and never blocks the
# sync path (failures are logged and swallowed).
ATTESTATION_ENABLED: bool = os.environ.get("ATTESTATION_ENABLED", "1") != "0"
# State + manifest directory (default STATE_DIR/attestations).
ATTESTATION_DIR: Path = Path(os.environ.get("ATTESTATION_DIR", str(STATE_DIR / "attestations")))

# ── Snapshot Bootstrap (public parquet replay) ─────────────────────────
# ``lite-indexer bootstrap`` downloads public parquet snapshots from B2
# and imports them into ClickHouse in minutes instead of re-indexing a
# chain from genesis over free RPCs. BOOTSTRAP_B2_BUCKET is the rclone
# remote root that also holds the live cold tier; BOOTSTRAP_PARQUET_PATH
# is the prefix inside it where snapshot folders ({date}/{chain}/) live.
BOOTSTRAP_B2_BUCKET: str = os.environ.get("BOOTSTRAP_B2_BUCKET", "b2-crypt:chain-data")
BOOTSTRAP_PARQUET_PATH: str = os.environ.get("BOOTSTRAP_PARQUET_PATH", "snapshots")
# Local staging dir for downloaded snapshots before the ClickHouse import.
BOOTSTRAP_LOCAL_DIR: Path = Path(os.environ.get("BOOTSTRAP_LOCAL_DIR", str(DATA_DIR / "bootstrap")))
# Optional mount root for snapshot parquet files (empty = use local dir).
BOOTSTRAP_CH_FILE_ROOT: str = os.environ.get("BOOTSTRAP_CH_FILE_ROOT", "")
# rclone timeout (seconds) for snapshot downloads/uploads.
BOOTSTRAP_RCLONE_TIMEOUT: int = int(os.environ.get("BOOTSTRAP_RCLONE_TIMEOUT", "1200"))

# ── External flat-file bootstrap (provider-agnostic) ───────────────────
# ``lite-indexer bootstrap --source <loc>`` hydrates from ANY pre-indexed
# parquet dataset (a local directory, an http(s) base URL, or a pyarrow.fs
# URL such as s3:// or gs://) without touching an RPC. The layout is a
# ``manifest.json`` plus parquet chunks under the base, or a hive-style
# ``chain=<chain>/*.parquet`` glob when no manifest is present. This is
# deliberately decoupled from the curated RPC overlay: the location carries
# no provider identity.
FLATFILE_SOURCE: str = os.environ.get("LITE_INDEXER_FLATFILE_SOURCE", "")
# Local staging dir where remote chunks are materialized before the
# ClickHouse ``file()`` import (one subdir per source).
FLATFILE_STAGE_DIR: Path = Path(os.environ.get("LITE_INDEXER_FLATFILE_STAGE_DIR", str(DATA_DIR / "flatfile")))
# ── Transparent Historical Access (THA) ────────────────────────────────
# Unified hot (ClickHouse) + cold (B2 parquet) reads through one access
# layer (lite_indexer.storage.historical.HistoricalAccess). When enabled,
# the API's query() consults the access layer so every read spans the
# whole archive transparently: hot rows (< HISTORICAL_HOT_TTL_DAYS) come
# from ClickHouse for sub-second reads, cold rows back to genesis come
# from B2 parquet via DuckDB. Disabling keeps the legacy hot-only path.
HISTORICAL_ACCESS_ENABLED: bool = os.environ.get("HISTORICAL_ACCESS_ENABLED", "1") != "0"

# How many days of data are considered "hot" (kept at full fidelity in
# ClickHouse). Auto-detected from the table TTL expression when possible;
# this value is the fallback and the floor for detection.
HISTORICAL_HOT_TTL_DAYS: int = int(os.environ.get("HISTORICAL_HOT_TTL_DAYS", "30"))

# Port for the rclone HTTP proxy that serves B2 parquet to ClickHouse's
# url() table function (scripts/serve-b2-http.sh). Also the port the
# *_historical views in migrations/006_historical_views.sql read from.
B2_HTTP_PROXY_PORT: int = int(os.environ.get("B2_HTTP_PROXY_PORT", "8090"))

# Timeout (seconds) for cold queries. Cold reads are slower than hot
# reads (DuckDB over parquet on B2), so they get their own budget. The
# timeout is enforced at the async boundary (the DuckDB thread is not
# cancellable, but the caller is never blocked past this budget).
COLD_QUERY_TIMEOUT: int = int(os.environ.get("COLD_QUERY_TIMEOUT", "60"))

# rclone remote root for the cold parquet tier. Defaults to the live
# daily-export location (PARQUET_REMOTE); the bootstrap bucket root is
# the same remote with a different prefix, so b2_bucket defaults here.
HISTORICAL_PARQUET_REMOTE: str = os.environ.get("HISTORICAL_PARQUET_REMOTE", PARQUET_REMOTE)
# ── NDJSON Write-Ahead Log ─────────────────────────────────────────────
# Redundant local replay buffer (events.jsonl per chain/date). The durable
# store is ClickHouse, so this is DISABLED by default — it costs ~15% CPU
# (zstd compression every 15 min, rglob disk scans per write, per-write
# disk-cap checks) plus a separate cron rotation. The code is retained
# behind this flag for rollback.
#
# Recovery path WITHOUT NDJSON: restart from state.to_block. On crash we
# lose at most one sync cycle (~30s of data), which the next cycle
# re-fetches from RPC. No replay buffer is required.
NDJSON_ENABLED: bool = os.environ.get("NDJSON_ENABLED", "0") == "1"
# ── Balance Reconciliation (TrueBlocks) ────────────────────────────────
# Compare computed balances from indexed evm_transfers deltas against
# on-chain state (eth_getBalance). Optional + non-blocking: only runs on
# a random sample of wallets (default 100 per chain) and caches results
# in wallet_memory.reconciliation_results.
RECONCILIATION_ENABLED: bool = os.environ.get("RECONCILIATION_ENABLED", "1") != "0"
RECONCILIATION_SAMPLE_SIZE: int = int(os.environ.get("RECONCILIATION_SAMPLE_SIZE", "100"))
CH_RECONCILIATION_TABLE: str = "wallet_memory.reconciliation_results"
# ── Reorg Staging (Envio/TrueBlocks) ──────────────────────────────────
# Track the last N blocks per chain as "unripe" using a ClickHouse
# checkpoint table (wallet_memory.block_checkpoints). On hash mismatch,
# roll back evm_transfers + dex_events above the fork point.
#
# Default ON for EVM chains. Solana/Tron never invoke the EVM reorg
# module (the staging lives under strategies/evm/), so protection is
# inherently off for non-EVM chains.
REORG_PROTECTION_ENABLED: bool = os.environ.get("REORG_PROTECTION_ENABLED", "1") != "0"
CH_BLOCK_CHECKPOINTS_TABLE: str = "wallet_memory.block_checkpoints"
