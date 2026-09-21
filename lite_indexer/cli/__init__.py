"""
CLI entry point for LiteChainIndexer.

Usage:
    lite-indexer --chain ethereum
    lite-indexer --chain ethereum --from-block 25645000
    lite-indexer bootstrap --all

The subcommand handlers live in focused sibling modules (``_daemon``,
``_export``, ``_integrity``, ``_backfill``, ``_bootstrap``, ``_query``);
this module keeps the entrypoints (``main`` / ``async_main``) and the
argparse surface identical to the pre-split ``lite_indexer.cli`` module.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from lite_indexer.cli._attest import run_attest_cli
from lite_indexer.cli._backfill import run_backfill_dex_prices, run_backfill_whales
from lite_indexer.cli._bootstrap import run_bootstrap_cli
from lite_indexer.cli._daemon import main_daemon
from lite_indexer.cli._export import run_export_parquet
from lite_indexer.cli._integrity import run_integrity_check_cli, run_reconcile_cli, run_recover_cli
from lite_indexer.cli._lake import add_lake_parsers, run_lake_command
from lite_indexer.cli._query import run_query_cli
from lite_indexer.config import (
    BATCH_SIZE,
    DATA_DIR,
    DISK_CAP_BYTES,
    MAX_CONCURRENT_CHAINS,
    PRICING_ENABLED,
    RECONCILIATION_SAMPLE_SIZE,
)
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger, setup_logging
from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.strategies import SUPPORTED_CHAINS

__all__ = [
    "main",
    "async_main",
    "main_daemon",
    "parse_args",
    "run_attest_cli",
    "run_backfill_dex_prices",
    "run_backfill_whales",
    "run_bootstrap_cli",
    "run_export_parquet",
    "run_integrity_check_cli",
    "run_lake_command",
    "run_query_cli",
    "run_reconcile_cli",
    "run_recover_cli",
]

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="LiteChainIndexer — lightweight multi-chain blockchain indexer using free RPCs",
    )
    parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to index (default: ethereum). All chains: " + ", ".join(sorted(SUPPORTED_CHAINS.keys())),
    )
    parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        help="Override starting block (testing / backfill)",
    )
    parser.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="Override ending block (testing / backfill)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Max blocks per batch (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--disk-cap",
        type=int,
        default=DISK_CAP_BYTES,
        help=f"Disk cap in bytes (default: {DISK_CAP_BYTES} = 10 GB)",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run in daemon mode — continuously index all chains",
    )
    parser.add_argument(
        "--chains",
        type=str,
        default="ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,linea,scroll,zksync,mantle,gnosis,solana,tron",
        help="Comma-separated list of chains to index in daemon mode",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=None,
        help=f"Max chains syncing concurrently in daemon mode (default: {MAX_CONCURRENT_CHAINS}). "
        "Caps peak memory — 14 parallel chains is what caused the 08/03 OOM kills.",
    )
    parser.add_argument(
        "--dex",
        action="store_true",
        help="Enable DEX swap event indexing (V2 + V3)",
    )
    parser.add_argument(
        "--deployers",
        action="store_true",
        help="Enable deployer tracking via factory PairCreated/PoolCreated events",
    )
    parser.add_argument(
        "--traces",
        action="store_true",
        help="Enable internal-transfer trace collection (WETH unwraps, bridge deposits, "
        "fee flows) via free trace_* RPCs. Off by default — trace endpoints are "
        "rate-limited on free tiers. Also settable via TRACE_ENABLED=true.",
    )
    parser.add_argument(
        "--start-from-head",
        action="store_true",
        help="Start indexing from chain tip instead of backfilling from genesis. "
        "Ignores any existing state file and begins from (tip - 100).",
    )
    parser.add_argument(
        "--recover",
        action="store_true",
        help="Recover state files from ClickHouse: query the actual max block per "
        "chain, reset the state file + high-water mark to it, and (with "
        "--backfill) rewind to the earliest missing range so the next sync "
        "re-fetches it. Fixes the 08/03+08/04 state-regression class of bugs "
        "even when the in-process guard never ran.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="With --recover: also rewind state to the earliest missing range "
        "detected in ClickHouse so the next sync backfills the gap. "
        "Without --recover this is a no-op.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help=f"Data directory for NDJSON + state (default: {DATA_DIR})",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO from LOG_LEVEL env var)",
    )
    sub = parser.add_subparsers(dest="command")

    # ── integrity-check subcommand ──────────────────────────────────
    # Daily durable-store audit: per-chain min/max/distinct/count from
    # ClickHouse with a contiguity percentage. Flags chains whose row
    # counts do not match the expected block span so operators know
    # before missing data compounds. Intended for a daily cron.
    integrity_parser = sub.add_parser(
        "integrity-check",
        help="Audit per-chain ClickHouse row counts vs expected ranges",
        description=(
            "Daily integrity check: for each chain, compare the ClickHouse "
            "row/range stats (min/max block, distinct blocks, total rows) "
            "against the expected contiguous span and report a contiguity "
            "percentage. Chains with gaps or empty stores are flagged."
        ),
    )
    integrity_parser.add_argument(
        "--chains",
        type=str,
        default="ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,linea,scroll,zksync,mantle,gnosis,solana,tron",
        help="Comma-separated chains to audit (default: all supported)",
    )

    # ── reconcile subcommand (TrueBlocks-style balance reconciliation) ─
    # Runs a balance-completeness check on a random sample of wallets
    # (default 100 per chain) and caches results in
    # wallet_memory.reconciliation_results. Intended for a daily cron.
    reconcile_parser = sub.add_parser(
        "reconcile",
        help="Run balance reconciliation on a random wallet sample",
        description=(
            "TrueBlocks-style reconciliation: compare computed balances "
            "from indexed evm_transfers deltas against on-chain "
            "eth_getBalance for a random sample of wallets, then cache "
            "results in wallet_memory.reconciliation_results. "
            "Gated by RECONCILIATION_ENABLED (default on)."
        ),
    )
    reconcile_parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to reconcile (default: ethereum)",
    )
    reconcile_parser.add_argument(
        "--sample-size",
        type=int,
        default=RECONCILIATION_SAMPLE_SIZE,
        help=f"Wallets to sample per run (default: {RECONCILIATION_SAMPLE_SIZE})",
    )

    # ── attest subcommand (on-chain-verifiable index attestations) ───
    attest_parser = sub.add_parser(
        "attest",
        help="Verify / inspect on-chain-verifiable range attestations",
        description=(
            "Trustless dataset verification: prove an indexed range is a "
            "leaf of the persisted Merkle accumulator and (optionally) that "
            "its committed block hash matches a chain-supplied block hash. "
            "No provider or producer trust required."
        ),
    )
    attest_sub = attest_parser.add_subparsers(dest="attest_action")
    attest_verify = attest_sub.add_parser("verify", help="Verify a manifest file")
    attest_verify.add_argument("manifest", help="Path to a manifest JSON file")
    attest_verify.add_argument(
        "--expected-root",
        default=None,
        help="Trusted accumulator root (hex) the manifest must match",
    )
    attest_show = attest_sub.add_parser("show", help="Show a chain's accumulator state")
    attest_show.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain whose attestation state to show (default: ethereum)",
    )

    # ── export-parquet subcommand ────────────────────────────────────
    export_parser = sub.add_parser(
        "export-parquet",
        help="Export ClickHouse rows to Parquet (cold storage tier)",
        description=(
            "Export a (chain, date) into per-chain/per-date Parquet files, "
            "optionally upload them to the B2 permanent tier and delete them "
            "from ClickHouse only after the upload is verified. "
            "Idempotent: dates already exported are skipped."
        ),
    )
    export_parser.add_argument(
        "--chain",
        default="all",
        choices=sorted(SUPPORTED_CHAINS.keys()) + ["all"],
        help="Chain to export or 'all' (default: all)",
    )
    export_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to export as YYYY-MM-DD (default: yesterday)",
    )
    export_parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Backfill mode: min row age in days before export (default: PARQUET_COLD_DAYS env or 7). "
        "Mutually exclusive with --date.",
    )
    export_parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload Parquet files to the B2 remote via rclone",
    )
    export_parser.add_argument(
        "--delete-after-upload",
        dest="delete_after_upload",
        action="store_true",
        help="Delete exported rows from ClickHouse only after the upload is verified",
    )
    export_parser.add_argument(
        "--delete",
        dest="delete_after_upload",
        action="store_true",
        help=argparse.SUPPRESS,  # legacy alias for --delete-after-upload
    )
    export_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the export without writing, uploading, or deleting",
    )
    export_parser.add_argument(
        "--remote",
        type=str,
        default=None,
        help="rclone remote root (default: PARQUET_REMOTE env or b2-crypt-v2:chain-data/parquet)",
    )
    export_parser.add_argument(
        "--publish-snapshot",
        action="store_true",
        help="Mirror each exported (chain, date) to the bootstrap snapshot "
        "location with a manifest.json so `lite-indexer bootstrap` can "
        "discover and replay it",
    )
    export_parser.add_argument(
        "--snapshot-bucket",
        type=str,
        default=None,
        help="rclone remote root for bootstrap snapshots (default: BOOTSTRAP_B2_BUCKET env or b2-crypt:chain-data)",
    )
    export_parser.add_argument(
        "--snapshot-path",
        type=str,
        default=None,
        help="Path prefix inside the snapshot bucket (default: BOOTSTRAP_PARQUET_PATH env or snapshots)",
    )
    export_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for local Parquet output + state (default: DATA_DIR)",
    )
    # ── backfill-whales subcommand ───────────────────────────────────
    # Re-scans recent dex_events and marks whale rows retroactively using
    # the token_prices USD feed + per-chain raw thresholds + top-0.1%
    # per-pair amounts. Intended for an operator cron after the whale
    # feature ships (existing rows all have whale=0).
    backfill_parser = sub.add_parser(
        "backfill-whales",
        help="Re-scan recent dex_events and mark whale rows retroactively",
        description=(
            "Recomputes the whale flag for recent dex_events rows using "
            "token_prices USD values, per-chain raw amount thresholds, and "
            "the top-0.1% per-pair amount, then issues ALTER TABLE UPDATE "
            "mutations for the rows that changed. Idempotent — re-running "
            "is a no-op. Use --dry-run to preview counts without mutating."
        ),
    )
    backfill_parser.add_argument(
        "--chain",
        default=None,
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Restrict to a single chain (default: all chains)",
    )
    backfill_parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="How many days back to scan (default: 90, matches dex_events retention)",
    )
    backfill_parser.add_argument(
        "--limit",
        type=int,
        default=500_000,
        help="Max rows to scan (default: 500000)",
    )
    backfill_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview whale_up/whale_down counts without issuing mutations",
    )

    # ── backfill-dex-prices subcommand ───────────────────────────────
    # Fills the ~98.8%-zero price_usd column on dex_events retroactively
    # from the token_prices table. Ingestion writes price_usd=0 when the
    # price feed had no value yet; this job joins recent zero-price rows
    # to token_prices and issues ALTER TABLE UPDATE mutations in batches.
    price_parser = sub.add_parser(
        "backfill-dex-prices",
        help="Backfill price_usd/volume_usd on dex_events from token_prices",
        description=(
            "Re-scans recent dex_events rows with price_usd = 0, joins "
            "them against wallet_memory.token_prices, and issues "
            "ALTER TABLE UPDATE mutations that fill price_usd (token0 "
            "price) and volume_usd (max-leg notional) for rows that have "
            "a resolvable price. Idempotent — re-running is a no-op. Use "
            "--dry-run to preview counts without mutating."
        ),
    )
    price_parser.add_argument(
        "--chain",
        default=None,
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Restrict to a single chain (default: all chains)",
    )
    price_parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="How many days back to scan (default: 90, matches dex_events retention)",
    )
    price_parser.add_argument(
        "--limit",
        type=int,
        default=500_000,
        help="Max rows to scan (default: 500000)",
    )
    price_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview updated/unchanged/unpriced counts without issuing mutations",
    )

    # ── bootstrap subcommand ─────────────────────────────────────────
    # Replay public B2 parquet snapshots into ClickHouse instead of
    # re-indexing from genesis over free RPCs (days → minutes). This is
    # the answer to "your backfill is RPC-bound": download the free public
    # snapshot, import via ClickHouse file(), verify row counts, and set
    # the state high-water mark so `lite-indexer daemon` resumes live.
    bootstrap_parser = sub.add_parser(
        "bootstrap",
        help="Bootstrap indexer state from public B2 parquet snapshots",
        description=(
            "Replay a public B2 parquet snapshot into ClickHouse instead "
            "of re-indexing from genesis over free RPCs. Downloads the "
            "snapshot manifest + parquet files, imports them via "
            "INSERT INTO ... SELECT * FROM file('*.parquet'), verifies "
            "row counts against the manifest, and advances the state "
            "high-water mark so the daemon resumes live from the snapshot "
            "end block."
        ),
    )
    bootstrap_parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to bootstrap (default: ethereum)",
    )
    bootstrap_parser.add_argument(
        "--all",
        action="store_true",
        help="Bootstrap every supported chain",
    )
    bootstrap_parser.add_argument(
        "--list",
        action="store_true",
        help="List available snapshots on B2 (no import)",
    )
    bootstrap_parser.add_argument(
        "--from-snapshot",
        type=str,
        default="latest",
        help="Snapshot date to use, YYYY-MM-DD, or 'latest' (default: latest)",
    )
    bootstrap_parser.add_argument(
        "--target-db",
        type=str,
        default="wallet_memory",
        help="ClickHouse database to import into (default: wallet_memory)",
    )
    bootstrap_parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="rclone remote root for snapshots (default: BOOTSTRAP_B2_BUCKET env)",
    )
    bootstrap_parser.add_argument(
        "--snapshots-path",
        type=str,
        default=None,
        help="Path prefix inside the bucket (default: BOOTSTRAP_PARQUET_PATH env)",
    )
    bootstrap_parser.add_argument(
        "--rclone-bin",
        type=str,
        default=None,
        help="Path to the rclone binary (default: PARQUET_RCLONE_BIN env or 'rclone')",
    )
    bootstrap_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for state files + download staging (default: DATA_DIR)",
    )
    bootstrap_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the import plan without downloading or writing",
    )
    bootstrap_parser.add_argument(
        "--source",
        type=str,
        default=None,
        help=(
            "Provider-agnostic external flat-file dataset: a local directory, an "
            "http(s) base URL, or a pyarrow.fs URL (s3://, gs://). When set, imports "
            "from it instead of the B2 snapshot (default: LITE_INDEXER_FLATFILE_SOURCE env)"
        ),
    )
    bootstrap_parser.add_argument(
        "--force",
        action="store_true",
        help="With --source: import even when the dataset overlaps the chain high-water",
    )

    # ── query subcommand (on-demand historical RPC) ──────────────────
    # Answer a query about ANY block/tx/address historically WITHOUT
    # indexing the whole chain: the QueryRouter routes to ClickHouse
    # (indexed) → B2 parquet (cold) → live RPC (on-demand), so the CLI
    # never says "data not available". Exactly one of --tx / --address /
    # --block / --deployer must be given.
    query_parser = sub.add_parser(
        "query",
        help="On-demand historical RPC query (no full indexing required)",
        description=(
            "Answer a query about ANY block/tx/address historically by "
            "routing to the best data source: ClickHouse (indexed) → B2 "
            "parquet (cold) → live RPC (on-demand). Exactly one of "
            "--tx / --address / --block / --deployer must be given."
        ),
    )
    query_parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to query (default: ethereum)",
    )
    query_parser.add_argument(
        "--tx",
        type=str,
        default=None,
        help="Transaction hash to look up (routed CH → cold → RPC)",
    )
    query_parser.add_argument(
        "--address",
        type=str,
        default=None,
        help="Wallet address for transfer history (routed CH → cold → RPC)",
    )
    query_parser.add_argument(
        "--block",
        type=int,
        default=None,
        help="Block number to fetch (blocks are always live RPC)",
    )
    query_parser.add_argument(
        "--deployer",
        type=str,
        default=None,
        help="Contract address to resolve its deployer (routed CH → cold → RPC)",
    )
    query_parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        help="Inclusive start block for --address queries",
    )
    query_parser.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="Inclusive end block for --address queries",
    )
    query_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max rows for --address queries (default: 100)",
    )
    query_parser.add_argument(
        "--no-rpc",
        action="store_true",
        help="Disable the on-demand RPC fallback (indexed data only)",
    )
    add_lake_parsers(sub)
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    """Async entry point."""
    if args.log_level:
        setup_logging(args.log_level)

    data_path = Path(args.data_dir) if args.data_dir else None

    # ── export-parquet subcommand ────────────────────────────────────
    if args.command == "export-parquet":
        return run_export_parquet(args)

    if args.command and args.command.startswith(("lake-", "sqd-", "fill-")):
        return run_lake_command(args)

    # ── integrity-check subcommand ───────────────────────────────────
    if args.command == "integrity-check":
        return run_integrity_check_cli(args)

    # ── recover mode ─────────────────────────────────────────────────
    # Recover state files from the ClickHouse durable store BEFORE any
    # indexing. Works in single-chain mode (--chain) and daemon mode
    # (--chains). With --backfill the state is additionally rewound to
    # the earliest missing range so the next sync re-fetches it.
    if args.recover:
        chains_list = [c.strip() for c in args.chains.split(",") if c.strip()]
        if args.chain and args.chain != "ethereum":
            chains_list = [args.chain]
        return await run_recover_cli(chains_list, args.data_dir, args.backfill)

    # ── backfill-whales subcommand ───────────────────────────────────
    if args.command == "backfill-whales":
        return run_backfill_whales(args)

    # ── backfill-dex-prices subcommand ───────────────────────────────
    if args.command == "backfill-dex-prices":
        return run_backfill_dex_prices(args)

    # ── bootstrap subcommand ─────────────────────────────────────────
    if args.command == "bootstrap":
        return await run_bootstrap_cli(args)

    # ── query subcommand (on-demand historical RPC) ──────────────────
    if args.command == "query":
        return await run_query_cli(args)

    # ── reconcile subcommand (TrueBlocks-style) ──────────────────────
    if args.command == "reconcile":
        return await run_reconcile_cli(args)

    # ── attest subcommand (trustless dataset verification) ───────────
    if args.command == "attest":
        return run_attest_cli(args)

    if args.daemon:
        chains_list = [c.strip() for c in args.chains.split(",") if c.strip()]
        return await main_daemon(
            chains_list,
            dex_enabled=args.dex,
            deployers_enabled=args.deployers,
            traces_enabled=args.traces,
            disk_cap=args.disk_cap,
            start_from_head=args.start_from_head,
            data_dir=args.data_dir,
            max_concurrent=args.max_concurrent or MAX_CONCURRENT_CHAINS,
        )

    # Single-chain mode
    indexer = LiteIndexer(
        chain=args.chain,
        from_block=args.from_block,
        to_block=args.to_block,
        batch_size=args.batch_size,
        disk_cap=args.disk_cap,
        start_from_head=args.start_from_head,
        data_dir=data_path if data_path else None,
    )
    indexer.dex_enabled = args.dex
    indexer.deployers_enabled = args.deployers
    indexer.traces_enabled = args.traces

    # Startup USD price refresh (best-effort, non-blocking): populate the
    # token_prices feed + cache so the whale flag and USD columns on the
    # first batch are computed with prices instead of raw-amount fallbacks.
    if PRICING_ENABLED:
        try:
            from lite_indexer.pricing import refresh_prices

            startup_client = ClickHouseWriter().client()
            if startup_client is not None:
                stats = await refresh_prices(startup_client, chains=[args.chain])
                logger.info("prices_refreshed_startup", stats=stats)
        except Exception as exc:  # noqa: BLE001 — refresh is best-effort
            logger.warning("price_refresh_startup_failed", error=str(exc)[:100])

    try:
        summary = await indexer.sync_loop()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("sync_interrupted", chain=args.chain)
        return 1
    except Exception as exc:
        logger.error("sync_fatal", chain=args.chain, error=str(exc), exc_info=True)
        print(json.dumps({"status": "error", "error": str(exc)}))  # noqa: T201
        return 1

    print(json.dumps(summary, default=str))  # noqa: T201
    return 0 if summary["status"] in ("ok", "no_events", "caught_up") else 1


def main() -> None:
    """CLI entry point.

    Installs the optional uvloop event-loop policy first (graceful fallback
    to the default asyncio loop when uvloop is absent/unsupported), then
    runs the async entrypoint.
    """
    from lite_indexer.runtime import install_event_loop_policy

    setup_logging()
    install_event_loop_policy()
    args = parse_args()
    sys.exit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
